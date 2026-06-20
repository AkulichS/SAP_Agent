"""mcp_server tool functions against the offline SAP stub (_StubConnection).

These confirm the stub contract the whole graph depends on. No real SAP system;
pyrfc is absent so SAPConnectionManager serves canned data.
"""

import json

import pytest

import mcp_server
from sap_connection_manager import HAS_PYRFC


def test_running_in_stub_mode():
    # Guard: the suite is meaningless if a real pyrfc connection is in play.
    assert HAS_PYRFC is False


# ---------------------------------------------------------------------------
# sap_read_table
# ---------------------------------------------------------------------------

def test_read_table_known():
    out = mcp_server.sap_read_table("COKP", "", "", 100)
    assert out["status"] == "S"
    assert out["count"] == 1
    assert out["rows"][0]["KOKRS"] == "X500"


def test_read_table_unknown_is_empty():
    out = mcp_server.sap_read_table("ZZZ_NOPE", "", "", 100)
    assert out["count"] == 0
    assert out["rows"] == []


def test_read_table_field_projection():
    out = mcp_server.sap_read_table("BKPF", "", "BUKRS,GJAHR", 100)
    assert set(out["rows"][0].keys()) == {"BUKRS", "GJAHR"}


# ---------------------------------------------------------------------------
# sap_execute_step — SUBMIT async vs inline, FM, unknown
# ---------------------------------------------------------------------------

def test_submit_async_requires_poll():
    out = mcp_server.sap_execute_step("SUBMIT", "RKO7KO8G", "[]", async_mode=True)
    assert out["status"] == "submitted"
    assert out["requires_poll"] is True
    assert out["job_name"] == "STUB_RKO7KO8G"
    assert out["job_id"]          # 8-digit counter, value depends on call order


def test_submit_inline_returns_spool():
    out = mcp_server.sap_execute_step("SUBMIT", "RKO7KO8G", "[]", async_mode=False)
    assert out["status"] == "ok"
    assert out["requires_poll"] is False
    assert "settled successfully" in out["spool_text"]


def test_execute_fm_sync_ok():
    out = mcp_server.sap_execute_step("FM", "BAPI_SOMETHING", "{}")
    assert out["status"] == "ok"
    assert out["requires_poll"] is False


def test_execute_unknown_action_errors():
    out = mcp_server.sap_execute_step("WAT", "OBJ", "{}")
    assert out["status"] == "error"


# ---------------------------------------------------------------------------
# sap_job_status — stub always reports FINISHED
# ---------------------------------------------------------------------------

def test_job_status_finished():
    out = mcp_server.sap_job_status("STUB_X", "00000001")
    assert out["status"] == "FINISHED"
    assert out["is_finished"] is True
    assert out["is_running"] is False


# ---------------------------------------------------------------------------
# sap_read_spool — text join + truncation
# ---------------------------------------------------------------------------

def test_read_spool_full():
    out = mcp_server.sap_read_spool("STUB_RKO7KO8G", "00000001", max_lines=500)
    assert out["truncated"] is False
    assert out["line_count"] == 6
    assert "settled successfully" in out["spool_text"]


def test_read_spool_truncates():
    out = mcp_server.sap_read_spool("STUB_RKO7KO8G", "00000001", max_lines=2)
    assert out["truncated"] is True
    assert out["line_count"] == 2


# ---------------------------------------------------------------------------
# sap_check_period
# ---------------------------------------------------------------------------

def test_check_period_ok():
    out = mcp_server.sap_check_period(
        "TOOLS", "TOOL_READ_TABLE", json.dumps({"table": "COKP"}))
    assert out["status"] == "S"
