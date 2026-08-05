"""
Connector-bus tests.

  1. The SAP-RFC connector still behaves exactly as the old inline dispatch did
     (state mapping, opaque-handle publishing, neutral data.text passthrough,
     placeholders).
  2. THE GUARDRAIL — a brand-new backend, registered only by name, runs through
     the engine's execute → poll → validate path with ZERO edits to
     graph_builder.py. The day a module-specific `if` lands in the engine, the
     fake connector below stops working and this test fails.
  3. McpConnector — a real second device proxying another MCP server: generic
     result normalization, and a synchronous (no-poll) device driving the engine.
"""

from __future__ import annotations

import json
from types import SimpleNamespace

import pytest

from connectors import (NEUTRAL_DATA_KEYS, BaseConnector, McpConnector,
                        envelope_data, foreign_data_keys, get_connector,
                        register_connector, register_mcp_connector,
                        registered_connectors, warn_foreign_data_keys)
from connectors.mcp import _to_envelope
from connectors.registry import _FACTORIES
from connectors.sap_rfc import MUTATING_TOOLS, SapRfcConnector
from config_settings import connector_catalog
from graph_builder import (make_analysis_node, make_execute_node, make_poll_node,
                           make_validate_node)


def _tool(name, description="", schema=None):
    """A fake MCP tool descriptor (mimics what session.list_tools() yields)."""
    return SimpleNamespace(name=name, description=description, inputSchema=schema)


class _ToolSession:
    """A session that advertises tools and records call_tool dispatches."""

    def __init__(self, tools=(), payload=None):
        self._tools = list(tools)
        self._payload = payload if payload is not None else {"status": "ok", "data": {}}
        self.calls: list = []

    async def list_tools(self):
        return SimpleNamespace(tools=self._tools)

    async def call_tool(self, name, arguments=None):
        self.calls.append((name, arguments or {}))
        block = SimpleNamespace(type="text", text=json.dumps(self._payload))
        return SimpleNamespace(content=[block])


# ===========================================================================
# SapRfcConnector — the default device
# ===========================================================================

async def test_sap_connector_publishes_opaque_handle(make_session):
    """An async SUBMIT envelope gains meta.handle = {job_name, job_id}."""
    session = make_session(sap_execute_step={
        "status": "submitted", "messages": [],
        "meta": {"action_type": "SUBMIT", "requires_poll": True,
                 "job_name": "ZJOB", "job_id": "42"},
        # An async submit produces no payload yet — job identity rides meta.
        "data": {},
    })
    env = await SapRfcConnector(session).run("SUBMIT", "ZPROG", [], async_mode=True)
    assert env["meta"]["handle"] == {"job_name": "ZJOB", "job_id": "42"}


async def test_sap_connector_publishes_handle_for_inline_job(make_session):
    """An inline-wait SUBMIT (no poll) also publishes a handle so validate can
    derive placeholders from it."""
    session = make_session(sap_execute_step={
        "status": "ok", "messages": [],
        "meta": {"action_type": "SUBMIT", "requires_poll": False,
                 "job_name": "ZJOB", "job_id": "7"},
        "data": {"text": {"available": True, "line_count": 1, "text": "done"}},
    })
    env = await SapRfcConnector(session).run("SUBMIT", "ZPROG", [], async_mode=False)
    assert env["meta"]["handle"] == {"job_name": "ZJOB", "job_id": "7"}


async def test_sap_connector_preserves_neutral_text(make_session):
    """The RFC emits its output on the neutral data.text channel natively; the
    connector passes it through untouched (no spool→text translation anymore)."""
    session = make_session(sap_read_spool={
        "status": "ok", "messages": [], "meta": {},
        "data": {"text": {"available": True, "line_count": 2, "text": "L1\nL2"}}})
    env = await SapRfcConnector(session).read_output({"job_name": "J", "job_id": "1"})
    assert env["data"]["text"] == {"available": True, "line_count": 2, "text": "L1\nL2"}
    assert "spool" not in env["data"]


async def test_sap_connector_placeholders(make_session):
    conn = SapRfcConnector(make_session())
    assert conn.placeholders({"job_name": "ZJOB", "job_id": "9"}) == {
        "job_name": "ZJOB", "job_id": "9"}


@pytest.mark.parametrize("native, normalized", [
    ("FINISHED", "finished"), ("ABORTED", "aborted"),
    ("RUNNING", "running"), ("SCHEDULED", "running"),
])
async def test_sap_connector_poll_maps_states(make_session, native, normalized):
    session = make_session(sap_job_status={
        "status": "ok", "messages": [], "meta": {}, "data": {"state": native}})
    res = await SapRfcConnector(session).poll({"job_name": "ZJOB", "job_id": "42"})
    assert res["state"] == normalized


async def test_sap_connector_tools_passthrough(make_session):
    """A TOOLS action sends object_name straight to sap_run_tool as the handler
    name (no shim, no hardcoded default) with params carried verbatim."""
    session = make_session(sap_run_tool={"status": "ok", "messages": [],
                                         "meta": {}, "data": {"rows": []}})
    await SapRfcConnector(session).run("TOOLS", "TOOL_READ_JOB_LOG", {"jobname": "J"})
    name, args = session.calls[-1]
    assert name == "sap_run_tool" and args["object_name"] == "TOOL_READ_JOB_LOG"
    assert json.loads(args["params_json"]) == {"jobname": "J"}


def test_unknown_connector_raises_clearly():
    with pytest.raises(KeyError, match="Unknown connector"):
        get_connector(object(), "does_not_exist")


def test_sap_rfc_registered_by_default():
    assert "sap_rfc" in registered_connectors()


# ===========================================================================
# THE GUARDRAIL — a non-SAP device drives the engine with zero graph edits
# ===========================================================================

class InMemoryConnector(BaseConnector):
    """A device that knows nothing about SAP. Its handle is an opaque ticket
    (deliberately NOT job_name/job_id shaped); poll reports finished immediately;
    read_output returns canned text under the backend-neutral data.text key;
    placeholders publishes the ticket under its own token name. If the engine ever
    peeks inside the handle or branches on connector name, one of these stops being
    enough and the guardrail breaks."""

    name = "fake"

    def __init__(self, session=None):
        super().__init__(session)
        self.ran: list = []
        self.read_handles: list = []

    async def run(self, action_type, object_name, params,
                  *, async_mode=False, test_run=False):
        self.ran.append((action_type, object_name, params))
        return {"status": "submitted", "messages": [],
                "meta": {"requires_poll": True, "handle": {"ticket": "TCK-1"}},
                "data": {}}

    async def poll(self, handle):
        return {"state": "finished", "raw": {"seen_handle": handle}}

    async def read_output(self, handle, *, max_lines=500):
        self.read_handles.append(handle)
        return {"status": "ok", "messages": [], "meta": {},
                "data": {"text": {"available": True, "line_count": 1,
                                  "text": "RESULT OK"}}}

    def placeholders(self, handle):
        return {"ticket_id": (handle or {}).get("ticket", "")}


@pytest.fixture
def fake_connector():
    """Register a shared fake device for the test, then unregister it."""
    device = InMemoryConnector()
    register_connector("fake", lambda session: device)
    try:
        yield device
    finally:
        _FACTORIES.pop("fake", None)


async def test_new_backend_runs_through_engine_unmodified(
        fake_connector, make_session, make_llms, make_state):
    """execute → poll → validate over a step targeting connector 'fake', using the
    UNMODIFIED node factories. Proof that a new backend = one connector class."""
    session = make_session()          # never consulted — the fake ignores it
    step = {
        "step_id": "A", "action_type": "SUBMIT", "object_name": "ANYTHING",
        "connector": "fake", "async": True,
        "validate": {"mode": "keyword",
                     "keyword": {"source": "spool", "ok_patterns": ["OK"]}},
    }
    state = make_state([step])

    # --- execute: the fake's async envelope, opaque handle and all ---
    execute_out = await make_execute_node(session)(state)
    execute = execute_out["current_execute"]
    assert execute["meta"]["handle"] == {"ticket": "TCK-1"}
    assert fake_connector.ran == [("SUBMIT", "ANYTHING", [])]
    state.update(execute_out)

    # --- poll: engine passes the opaque handle straight back, gets 'finished' ---
    poll_out = await make_poll_node(session)(state)
    assert poll_out["current_poll"]["sap_status"] == "FINISHED"
    state.update(poll_out)

    # poll carries the opaque handle forward for validate.
    assert poll_out["current_poll"]["handle"] == {"ticket": "TCK-1"}

    # --- validate: reads the fake's output, keyword-matches 'OK' → verdict ok ---
    validate_out = await make_validate_node(session, make_llms())(state)
    assert validate_out["current_validate"]["verdict"] == "ok"

    # Phase 2: read_output received the OPAQUE handle (not a job-shaped reconstruction),
    # and the output came back under data.text — proof both fixes hold end-to-end.
    assert fake_connector.read_handles == [{"ticket": "TCK-1"}]

    # The engine never called the real MCP session for this backend.
    assert session.calls == []


async def test_connector_published_placeholders_reach_verification_params(
        fake_connector, make_session, make_llms, make_state):
    """A validate.run action's {{ticket_id}} param resolves from the value the
    connector published via placeholders(handle) — not from any SAP job_ctx."""
    session = make_session()
    step = {
        "step_id": "A", "action_type": "SUBMIT", "object_name": "X",
        "connector": "fake", "async": True,
        "validate": {
            "mode": "keyword",
            "keyword": {"source": "spool", "ok_patterns": ["OK"]},
            "run": {"action_type": "CALL", "object_name": "VERIFY",
                    "connector": "fake", "params": {"ref": "{{ticket_id}}"}},
        },
    }
    state = make_state([step])
    state.update(await make_execute_node(session)(state))
    state.update(await make_poll_node(session)(state))
    await make_validate_node(session, make_llms())(state)

    # The verification action ran with the placeholder resolved from the opaque handle.
    assert ("CALL", "VERIFY", {"ref": "TCK-1"}) in fake_connector.ran


# ===========================================================================
# envelope_data factory + foreign-key guardrail — the neutral data contract
# ===========================================================================

def test_envelope_data_keeps_only_neutral_channels():
    """The factory emits exactly the channels it is given, drops the None ones,
    and — being the only constructor — cannot express a non-neutral key."""
    assert envelope_data(rows=[{"A": 1}]) == {"rows": [{"A": 1}]}
    assert envelope_data(text={"text": "x"}) == {"text": {"text": "x"}}
    assert envelope_data() == {}                      # all-None → valid empty payload
    full = envelope_data(text={"text": "x"}, rows=[], raw={"k": 1})
    assert set(full) <= NEUTRAL_DATA_KEYS
    # A falsy-but-present channel (rows=[]) is still emitted; only None is dropped.
    assert "rows" in full


def test_foreign_data_keys_flags_non_neutral():
    assert foreign_data_keys({"text": {}, "rows": []}) == set()
    assert foreign_data_keys({"spool": {}, "response": 1}) == {"spool", "response"}
    assert foreign_data_keys({}) == set() and foreign_data_keys(None) == set()


def test_warn_foreign_data_keys_logs_but_does_not_raise(caplog):
    env = {"status": "ok", "data": {"spool": {"text": "x"}}}
    with caplog.at_level("WARNING"):
        out = warn_foreign_data_keys(env, where="test")
    assert out is env                                  # returns env for chaining
    assert "spool" in caplog.text and "non-neutral" in caplog.text
    # A conformant envelope stays silent.
    caplog.clear()
    warn_foreign_data_keys({"data": envelope_data(rows=[])}, where="test")
    assert caplog.text == ""


# ===========================================================================
# McpConnector — a real second device (proxy to another MCP server)
# ===========================================================================

def test_mcp_envelope_normalizes_by_shape():
    """Arbitrary tool payloads → unified envelope, generically by shape."""
    rows = _to_envelope({"data": [{"A": 1}]}, "T")
    assert rows["data"]["rows"] == [{"A": 1}] and rows["status"] == "ok"

    text = _to_envelope({"status": "S", "text": "L1\nL2"}, "T")
    assert text["data"]["text"] == {"available": True, "line_count": 2, "text": "L1\nL2"}

    err = _to_envelope({"messages": [{"TYPE": "E", "MESSAGE": "boom"}]}, "T")
    assert err["status"] == "error"

    # A bespoke payload with no known shape is still reachable under data.raw.
    raw = _to_envelope({"whatever": 1}, "T")
    assert raw["data"]["raw"] == {"whatever": 1} and raw["status"] == "ok"


async def test_mcp_connector_run_calls_target_tool(make_session):
    """run() calls the named tool on the TARGET session and normalizes the result."""
    target = make_session(GET_BALANCE={"data": [{"BAL": "0"}]})
    conn = McpConnector(target=target, name="mcp_demo")
    env = await conn.run("CALL", "GET_BALANCE", {"acct": "1000"})
    assert env["data"]["rows"] == [{"BAL": "0"}]
    # It addressed the target by tool name, passing params verbatim as arguments.
    assert target.calls[-1] == ("GET_BALANCE", {"acct": "1000"})


async def test_mcp_connector_resolves_async_provider(make_session):
    """A target may be an async provider () -> session (memoize the session there)."""
    target = make_session(PING={"text": "pong"})
    calls = {"n": 0}

    async def provider():
        calls["n"] += 1
        return target

    conn = McpConnector(target=provider, name="mcp_demo")
    env = await conn.run("CALL", "PING", {})
    assert env["data"]["text"]["text"] == "pong" and calls["n"] == 1


async def test_mcp_connector_is_sync_only():
    """poll/read_output are unimplemented — a device declares only what it supports."""
    conn = McpConnector(target=object(), name="mcp_demo")
    with pytest.raises(NotImplementedError):
        await conn.poll({})
    with pytest.raises(NotImplementedError):
        await conn.read_output({})


@pytest.fixture
def mcp_device(make_session):
    """Register an MCP proxy device 'mcp_demo' backed by a fake target server."""
    target = make_session(VERIFY={"status": "S", "text": "RESULT OK"})
    register_mcp_connector("mcp_demo", target=target)
    try:
        yield target
    finally:
        _FACTORIES.pop("mcp_demo", None)


def test_mcp_connector_registers_and_surfaces_in_catalog(mcp_device):
    assert "mcp_demo" in registered_connectors()
    assert "mcp_demo" in connector_catalog()
    assert isinstance(get_connector(None, "mcp_demo"), McpConnector)


async def test_sync_mcp_device_runs_through_engine(
        mcp_device, make_session, make_llms, make_state):
    """A SYNCHRONOUS second device (no poll path) drives execute → validate through
    the unmodified nodes. Proves the engine never assumes async exists."""
    session = make_session()          # SAP session — untouched by this backend
    step = {
        "step_id": "A", "action_type": "CALL", "object_name": "VERIFY",
        "connector": "mcp_demo", "async": False,
        "validate": {"mode": "keyword",
                     "keyword": {"source": "spool", "ok_patterns": ["OK"]}},
    }
    state = make_state([step])

    execute_out = await make_execute_node(session)(state)
    execute = execute_out["current_execute"]
    assert execute["status"] == "ok" and not _meta_requires_poll(execute)
    state.update(execute_out)

    validate_out = await make_validate_node(session, make_llms())(state)
    assert validate_out["current_validate"]["verdict"] == "ok"
    assert session.calls == []        # the SAP session was never used


def _meta_requires_poll(env: dict) -> bool:
    return bool((env.get("meta") or {}).get("requires_poll"))


# ===========================================================================
# Connector-provided diagnostics (the analysis ReAct agent) — Phase 4
# ===========================================================================

async def test_sap_diagnostics_list_filters_mutating_tools():
    """list_diagnostic_tools offers read-only tools in function-schema shape and
    never the mutating ones."""
    session = _ToolSession(tools=[
        _tool("sap_read_table", "read a table", {"type": "object"}),
        *(_tool(m) for m in MUTATING_TOOLS),
    ])
    schemas = await SapRfcConnector(session).list_diagnostic_tools()
    names = [s["function"]["name"] for s in schemas]
    assert names == ["sap_read_table"]
    assert schemas[0]["type"] == "function"


async def test_sap_diagnostics_list_is_cached():
    session = _ToolSession(tools=[_tool("sap_read_table")])
    conn = SapRfcConnector(session)
    await conn.list_diagnostic_tools()
    await conn.list_diagnostic_tools()
    assert conn._diag_schema_cache is not None   # second call served from cache


async def test_sap_diagnostics_refuses_mutating_call():
    session = _ToolSession()
    env = await SapRfcConnector(session).call_diagnostic("sap_run_tool", {})
    assert env["status"] == "error" and session.calls == []   # never dispatched
    assert "modifies SAP" in env["messages"][0]["MESSAGE"]


async def test_sap_diagnostics_dispatches_read_only_call():
    session = _ToolSession(payload={"status": "ok", "data": {"rows": [{"A": 1}]}})
    env = await SapRfcConnector(session).call_diagnostic(
        "sap_read_table", {"table": "T"})
    assert session.calls == [("sap_read_table", {"table": "T"})]
    assert env["data"]["rows"] == [{"A": 1}]


async def test_mcp_diagnostics_proxy_target():
    """A non-SAP device's diagnostics come from ITS server."""
    session = _ToolSession(tools=[_tool("get_balance", "read balance")],
                           payload={"data": [{"BAL": "0"}]})
    conn = McpConnector(target=session, name="mcp_demo")
    schemas = await conn.list_diagnostic_tools()
    assert [s["function"]["name"] for s in schemas] == ["get_balance"]
    env = await conn.call_diagnostic("get_balance", {"acct": "1"})
    assert env["data"]["rows"] == [{"BAL": "0"}]


async def test_base_connector_has_no_diagnostics():
    class Bare(BaseConnector):
        name = "bare"
    conn = Bare()
    assert await conn.list_diagnostic_tools() == []
    err = await conn.call_diagnostic("anything", {})
    assert err["status"] == "error"


def _react_script(*replies):
    """A FakeLLM content callable that returns each reply in turn (last repeats)."""
    seq = iter(replies)
    last = replies[-1]

    def _content(_messages):
        return next(seq, last)
    return _content


async def test_analysis_diagnoses_with_step_connector_tools(
        make_session, make_llms, make_state):
    """THE PHASE-4 GUARDRAIL: a failed step on connector 'diag' is diagnosed with
    'diag's own tools — the analysis agent calls its call_diagnostic, never a
    hardcoded SAP tool set, with zero graph edits."""

    class DiagConnector(BaseConnector):
        name = "diag"

        def __init__(self, session=None):
            super().__init__(session)
            self.called: list = []

        async def list_diagnostic_tools(self):
            return [{"type": "function", "function": {
                "name": "inspect_thing",
                "parameters": {"type": "object", "properties": {}}}}]

        async def call_diagnostic(self, name, args):
            self.called.append((name, args))
            return {"status": "ok", "messages": [], "data": {"rows": [{"V": "1"}]}}

    device = DiagConnector()
    register_connector("diag", lambda _session: device)
    try:
        llms = make_llms(analysis=_react_script(
            '{"type":"tool_call","tool":"inspect_thing","args":{"x":1}}',
            '{"type":"final_answer","result":{"status":"completed","errors":[],'
            '"resolutions":[{"summary":"fixed"}]}}'))
        analysis = make_analysis_node(make_session(), llms)
        step = {"step_id": "A", "connector": "diag",
                "on_error": {"analysis_guidance": "investigate"}}
        state = make_state([step], current_error_context={
            "source": "validate", "summary": "boom", "default_depth": "diagnose"})

        out = await analysis(state)

        # The agent investigated using the STEP CONNECTOR's tool, not any SAP tool.
        assert device.called == [("inspect_thing", {"x": 1})]
        assert out["current_analysis"]["tools_used"] == ["inspect_thing"]
    finally:
        _FACTORIES.pop("diag", None)
