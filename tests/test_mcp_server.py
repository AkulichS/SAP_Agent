"""mcp_server tool functions against the offline SAP stub (_StubConnection).

These confirm the stub contract the whole graph depends on. No real SAP system;
pyrfc is absent so SAPConnectionManager serves canned data.

Every tool returns the unified envelope {status, messages, meta, data}.
"""

import json
import threading


import mcp_server
from sap_connection_manager import HAS_PYRFC, _StubConnection


def test_running_in_stub_mode():
    # Guard: the suite is meaningless if a real pyrfc connection is in play.
    assert HAS_PYRFC is False


def _assert_envelope(out):
    assert set(out.keys()) == {"status", "messages", "meta", "data"}


# ---------------------------------------------------------------------------
# sap_read_table
# ---------------------------------------------------------------------------

def test_read_table_known():
    out = mcp_server.sap_read_table("COKP", "", "", 100)
    _assert_envelope(out)
    assert out["status"] == "ok"
    assert out["meta"]["row_count"] == 1
    assert out["data"]["rows"][0]["KOKRS"] == "X500"


def test_read_table_unknown_is_empty():
    out = mcp_server.sap_read_table("ZZZ_NOPE", "", "", 100)
    assert out["meta"]["row_count"] == 0
    assert out["data"]["rows"] == []


def test_read_table_field_projection():
    out = mcp_server.sap_read_table("BKPF", "", "BUKRS,GJAHR", 100)
    assert set(out["data"]["rows"][0].keys()) == {"BUKRS", "GJAHR"}


# ---------------------------------------------------------------------------
# sap_execute_step — SUBMIT async vs inline, FM, unknown
# ---------------------------------------------------------------------------

def test_submit_async_requires_poll():
    out = mcp_server.sap_execute_step("SUBMIT", "RKO7KO8G", "[]", async_mode=True)
    _assert_envelope(out)
    assert out["status"] == "submitted"
    assert out["meta"]["requires_poll"] is True
    assert out["meta"]["job_name"] == "STUB_RKO7KO8G"
    assert out["meta"]["job_id"]          # 8-digit counter, value depends on call order


def test_submit_inline_returns_spool():
    out = mcp_server.sap_execute_step("SUBMIT", "RKO7KO8G", "[]", async_mode=False)
    _assert_envelope(out)
    assert out["status"] == "ok"
    assert out["meta"]["requires_poll"] is False
    spool = out["data"]["text"]
    assert spool["available"] is True
    assert spool["line_count"] > 0
    # RKO7KO8G is the KO8G error fixture (settlement-rule KD205 failures).
    assert "Settlement completed with errors." in spool["text"]


def test_execute_fm_sync_ok():
    out = mcp_server.sap_execute_step("FM", "BAPI_SOMETHING", "{}")
    assert out["status"] == "ok"
    assert out["meta"]["requires_poll"] is False


def test_execute_unknown_action_errors():
    out = mcp_server.sap_execute_step("WAT", "OBJ", "{}")
    assert out["status"] == "error"


# ---------------------------------------------------------------------------
# sap_job_status — stub always reports FINISHED
# ---------------------------------------------------------------------------

def test_job_status_finished():
    out = mcp_server.sap_job_status("STUB_X", "00000001")
    _assert_envelope(out)
    assert out["status"] == "ok"
    assert out["data"]["state"] == "FINISHED"
    assert out["data"]["is_finished"] is True
    assert out["data"]["is_running"] is False


# ---------------------------------------------------------------------------
# sap_read_spool — text join + truncation
# ---------------------------------------------------------------------------

def test_read_spool_full():
    # Full read (generous max_lines) returns every stub line, no truncation. Derive
    # the expected count from the stub fixture so it tracks edits to the canned spool.
    expected = len(_StubConnection._SPOOL_TEXT["RKO7KO8G"])
    out = mcp_server.sap_read_spool("STUB_RKO7KO8G", "00000001", max_lines=500)
    _assert_envelope(out)
    assert out["meta"]["truncated"] is False
    assert out["meta"]["line_count"] == expected
    spool = out["data"]["text"]
    assert spool["line_count"] == expected
    assert "KD205" in spool["text"]


def test_read_spool_truncates():
    out = mcp_server.sap_read_spool("STUB_RKO7KO8G", "00000001", max_lines=2)
    assert out["meta"]["truncated"] is True
    assert out["meta"]["line_count"] == 2
    assert out["data"]["text"]["line_count"] == 2


# ---------------------------------------------------------------------------
# sap_run_tool — generic passthrough + shape-based normalisation
# ---------------------------------------------------------------------------

def test_run_tool_read_table_normalises_rows():
    out = mcp_server.sap_run_tool("TOOL_READ_TABLE", json.dumps({"table": "COKP"}))
    _assert_envelope(out)
    assert out["status"] == "ok"
    assert out["meta"]["object_name"] == "TOOL_READ_TABLE"
    assert out["meta"]["row_count"] == 1
    assert out["data"]["rows"][0]["KOKRS"] == "X500"
    # raw always preserved — the verbatim RFC payload now carries rows under "rows"
    # (EV_RESULT_DATA is payload-only; table/count moved to EV_META).
    assert out["data"]["raw"]["rows"][0]["KOKRS"] == "X500"


def test_run_tool_read_job_spool_normalises_text():
    out = mcp_server.sap_run_tool(
        "TOOL_READ_JOB_SPOOL", json.dumps({"jobname": "STUB_RKO7KO8G", "jobcount": "1"}))
    _assert_envelope(out)
    assert out["status"] == "ok"
    assert out["meta"]["row_count"] == 0            # text-like tool → no rows
    spool = out["data"]["text"]
    assert spool["available"] is True               # status "S" → available
    assert spool["line_count"] > 0
    assert "KO8G" in spool["text"]                  # stub KO8G spool header


def test_run_tool_unknown_tool_passes_through_raw():
    # A tool with no ABAP handler in the stub still returns a clean envelope; its raw
    # payload stays reachable under data.raw (no rows/text to normalise).
    out = mcp_server.sap_run_tool("TOOL_FUTURE_CUSTOM", json.dumps({"x": "1"}))
    _assert_envelope(out)
    assert out["status"] == "ok"
    assert "rows" not in out["data"] and "text" not in out["data"]
    assert out["data"]["raw"] == {}


# ---------------------------------------------------------------------------
# sap_check_period
# ---------------------------------------------------------------------------

def test_check_period_ok():
    out = mcp_server.sap_check_period(
        "TOOLS", "TOOL_READ_TABLE", json.dumps({"table": "COKP"}))
    _assert_envelope(out)
    assert out["status"] == "ok"


# ---------------------------------------------------------------------------
# Tool registration — the sync bodies above are what runs, but never on the loop
#
# Under SSE one server process serves every concurrent run, so a tool that
# executed inline would serialize the whole fleet behind one blocking RFC call.
# ---------------------------------------------------------------------------

TOOL_NAMES = {"sap_check_period", "sap_execute_step", "sap_run_tool", "sap_read_table",
              "sap_job_status", "sap_read_spool", "sap_read_job_log", "sap_system_identity"}


async def test_every_tool_is_registered_async():
    listed = {t.name: t for t in await mcp_server.mcp.list_tools()}
    assert TOOL_NAMES <= set(listed)
    registered = mcp_server.mcp._tool_manager._tools
    assert all(registered[name].is_async for name in TOOL_NAMES), \
        "a sync tool would block the shared server's event loop"


async def test_registration_preserves_the_tool_schema():
    """The thread shim wraps the function — the parameters clients see must not change."""
    listed = {t.name: t for t in await mcp_server.mcp.list_tools()}
    props  = listed["sap_read_table"].inputSchema["properties"]
    assert set(props) == {"table", "where", "fields", "max_rows"}
    assert props["max_rows"]["type"] == "integer"
    assert listed["sap_read_table"].description.strip().startswith("Read rows from any SAP")


async def test_tool_call_runs_off_the_event_loop():
    loop_thread = threading.get_ident()
    ran_on: list[int] = []

    original = mcp_server._rfc

    def _spy(*args, **kwargs):
        ran_on.append(threading.get_ident())
        return original(*args, **kwargs)

    mcp_server._rfc = _spy
    try:
        await mcp_server.mcp.call_tool("sap_read_table",
                                       {"table": "COKP", "where": "", "fields": ""})
    finally:
        mcp_server._rfc = original

    assert ran_on and all(tid != loop_thread for tid in ran_on)


async def test_concurrent_tool_calls_do_not_serialize():
    """Two runs calling the shared server at once must overlap, not queue."""
    import asyncio

    in_call = threading.Barrier(2, timeout=5)
    original = mcp_server._rfc

    def _blocking(*args, **kwargs):
        in_call.wait()                      # deadlocks (BrokenBarrier) if calls serialize
        return original(*args, **kwargs)

    mcp_server._rfc = _blocking
    # The shim's limiter is sized to the connection pool; two slots are needed here
    # regardless of what SAP_POOL_SIZE happens to be in this environment.
    tokens = mcp_server._rfc_limiter.total_tokens
    mcp_server._rfc_limiter.total_tokens = max(2, tokens)
    try:
        results = await asyncio.wait_for(asyncio.gather(*[
            mcp_server.mcp.call_tool("sap_read_table",
                                     {"table": "COKP", "where": "", "fields": ""})
            for _ in range(2)
        ]), timeout=10)
    finally:
        mcp_server._rfc = original
        mcp_server._rfc_limiter.total_tokens = tokens

    assert len(results) == 2


def test_rfc_returns_its_connection_to_the_pool():
    before = mcp_server.conn_mgr.stats()
    mcp_server.sap_read_table("COKP", "", "", 100)
    after = mcp_server.conn_mgr.stats()
    assert after["live"] == after["idle"]        # nothing left checked out
    assert after["live"] >= before["live"]
