"""FastAPI web-layer tests via Starlette's TestClient.

Covers the REST auth surface and the WebSocket flows in-process — no browser.
The WS start-flow test drives the full web → RunManager → fan-out → relay path
with the graph runner faked, so it stays offline and deterministic. (The real
MCP-subprocess launch is covered separately by test_mcp_stdio.py.)
"""

import pytest
from fastapi.testclient import TestClient
from starlette.websockets import WebSocketDisconnect

import auth
import graph_builder
import web_app
from run_manager import get_run_manager
from web_app import app


@pytest.fixture
def client():
    with TestClient(app) as c:
        yield c


@pytest.fixture(autouse=True)
def _reset_runs():
    """Keep the process-wide RunManager singleton clean between tests."""
    yield
    mgr = get_run_manager()
    for run in list(mgr._runs.values()):
        for t in (run.task, run._fan_task):
            if t is not None and not t.done():
                t.cancel()
    mgr._runs.clear()


def _session_cookie(client, **user):
    user.setdefault("username", "alice")
    user.setdefault("display_name", "Alice")
    user.setdefault("role", "operator")
    user.setdefault("company_codes", ["RU06"])
    client.cookies.set(auth.COOKIE_NAME, auth.create_session_token(user))


# ===========================================================================
# REST: login
# ===========================================================================

def test_login_success(client, monkeypatch):
    pw = "secret123"
    user = {"username": "tester", "display_name": "T", "role": "operator",
            "company_codes": [], "password_hash": auth.hash_password(pw)}
    monkeypatch.setattr(auth, "get_user", lambda u: user if u == "tester" else None)

    r = client.post("/api/login", json={"username": "tester", "password": pw})
    assert r.status_code == 200
    assert r.json()["username"] == "tester"
    assert auth.COOKIE_NAME in r.cookies


def test_login_wrong_password(client, monkeypatch):
    user = {"username": "tester", "password_hash": auth.hash_password("right")}
    monkeypatch.setattr(auth, "get_user", lambda u: user if u == "tester" else None)
    r = client.post("/api/login", json={"username": "tester", "password": "WRONG"})
    assert r.status_code == 401


def test_login_unknown_user(client, monkeypatch):
    monkeypatch.setattr(auth, "get_user", lambda u: None)
    r = client.post("/api/login", json={"username": "ghost", "password": "x"})
    assert r.status_code == 401


# ===========================================================================
# REST: me / companies / logout
# ===========================================================================

def test_me_requires_auth(client):
    assert client.get("/api/me").status_code == 401


def test_me_with_session(client):
    _session_cookie(client, username="alice", company_codes=["RU06"])
    r = client.get("/api/me")
    assert r.status_code == 200
    data = r.json()
    assert data["username"] == "alice"
    assert "RU06" in [c["code"] for c in data["companies"]]


def test_companies_requires_auth(client):
    assert client.get("/api/companies").status_code == 401


def test_companies_lists_only_authorized(client):
    _session_cookie(client, company_codes=["RU06"])
    r = client.get("/api/companies")
    assert r.status_code == 200
    codes = [c["code"] for c in r.json()]
    assert codes == ["RU06"]          # not RU47 / RU72
    assert "run_status" in r.json()[0]


def test_logout_clears_cookie(client):
    _session_cookie(client)
    r = client.post("/api/logout")
    assert r.status_code == 200 and r.json()["ok"] is True


# ===========================================================================
# WebSocket: auth gating
# ===========================================================================

def test_ws_unauthenticated_closes_4001(client):
    with pytest.raises(WebSocketDisconnect) as ei:
        with client.websocket_connect("/ws?company=RU06") as ws:
            ws.receive_json()
    assert ei.value.code == 4001


def test_ws_unauthorized_company_closes_4003(client):
    _session_cookie(client, company_codes=["RU06"])
    with pytest.raises(WebSocketDisconnect) as ei:
        with client.websocket_connect("/ws?company=RU72") as ws:
            ws.receive_json()
    assert ei.value.code == 4003


# ===========================================================================
# WebSocket: full start flow (graph runner faked)
# ===========================================================================

def test_ws_start_flow_streams_run_to_completion(client, monkeypatch):
    async def fake_run(send, msg_queue, **_kw):
        await msg_queue.get()                       # consume the seeded "start"
        await send({"type": "run_init", "steps": [{"step_id": "S1"}]})
        await send({"type": "step_start", "step_id": "S1", "step_index": 0, "total": 1})
        await send({"type": "step_end", "step_id": "S1", "status": "ok"})
        await send({"type": "run_end", "status": "completed"})

    monkeypatch.setattr(graph_builder, "run_period_close_web", fake_run)
    _session_cookie(client, company_codes=["RU06"])

    with client.websocket_connect("/ws?company=RU06") as ws:
        assert ws.receive_json()["type"] == "company_status"   # sent on connect
        ws.send_json({"type": "start"})

        seen = []
        for _ in range(10):
            ev = ws.receive_json()
            seen.append(ev["type"])
            if ev["type"] == "run_end":
                assert ev["status"] == "completed"
                break

    assert "run_init" in seen
    assert "step_end" in seen
    assert "run_end" in seen


# ===========================================================================
# WebSocket: dashboard (/ws/dashboard)
# ===========================================================================

def test_ws_dashboard_unauthenticated_closes_4001(client):
    with pytest.raises(WebSocketDisconnect) as ei:
        with client.websocket_connect("/ws/dashboard") as ws:
            ws.receive_json()
    assert ei.value.code == 4001


def test_ws_dashboard_start_streams_tagged_events(client, monkeypatch):
    async def fake_run(send, msg_queue, **_kw):
        await msg_queue.get()
        await send({"type": "run_init", "steps": [{"step_id": "S1"}]})
        await send({"type": "run_end", "status": "completed"})

    monkeypatch.setattr(graph_builder, "run_period_close_web", fake_run)
    _session_cookie(client, company_codes=["RU06"])

    with client.websocket_connect("/ws/dashboard") as ws:
        ws.send_json({"type": "start", "company": "RU06"})
        seen = []
        for _ in range(10):
            ev = ws.receive_json()
            seen.append(ev)
            if ev.get("type") == "run_end":
                break

    # Dashboard tags every event with its company code.
    assert all(ev.get("company") == "RU06" for ev in seen)
    assert {"run_init", "run_end"} <= {ev["type"] for ev in seen}


# ===========================================================================
# WebSocket: rollback + restart
# ===========================================================================

def test_ws_rollback_and_restart_streams_rollback_then_rerun(client, monkeypatch):
    from config import load_config
    start_from = load_config("configs/period_close_RU06.yaml")["steps"][0]["step_id"]

    async def fake_rollback(send, msg_queue, start_from_step, steps_to_rollback, **_kw):
        await send({"type": "rollback_start", "from_step": start_from_step})
        await send({"type": "rollback_end"})
        await send({"type": "run_init", "steps": [{"step_id": start_from_step}]})
        await send({"type": "step_end", "step_id": start_from_step, "status": "ok"})
        await send({"type": "run_end", "status": "completed"})

    monkeypatch.setattr(graph_builder, "run_rollback_and_restart_web", fake_rollback)
    _session_cookie(client, company_codes=["RU06"])

    with client.websocket_connect("/ws?company=RU06") as ws:
        assert ws.receive_json()["type"] == "company_status"
        ws.send_json({"type": "rollback_and_restart", "start_from_step": start_from})
        seen = []
        for _ in range(12):
            ev = ws.receive_json()
            seen.append(ev["type"])
            if ev["type"] == "run_end":
                break

    assert "rollback_start" in seen
    assert "rollback_end" in seen
    assert "run_end" in seen


def test_ws_rollback_rejects_unknown_step(client, monkeypatch):
    # rollback_start returns False for a step not in the config → handler sends error.
    _session_cookie(client, company_codes=["RU06"])
    with client.websocket_connect("/ws?company=RU06") as ws:
        assert ws.receive_json()["type"] == "company_status"
        ws.send_json({"type": "rollback_and_restart", "start_from_step": "NO_SUCH_STEP"})
        ev = ws.receive_json()
    assert ev["type"] == "error"


# ===========================================================================
# FULL end-to-end web run
# ---------------------------------------------------------------------------
# Drives the REAL runner: web → RunManager → run_period_close_web → live MCP
# subprocess → SAP stub → real LangGraph → keyword validation → run_end.
# Only the LLM factory is faked (never invoked — validation is keyword mode),
# so no API key/network. A controlled single-step config keeps it deterministic
# regardless of how the production company configs line up with stub data.
# ===========================================================================

async def _in_memory_checkpointer(_db_path=None):
    """Async-checkpointer stand-in: avoids aiosqlite's background thread emitting an
    'event loop closed' warning when the TestClient portal loop tears down."""
    from langgraph.checkpoint.memory import InMemorySaver
    return InMemorySaver()


_E2E_CONFIG = """
company_config:
  company_code: "E2E"
  controlling_area: "X500"
  currency: "RUB"
defaults:
  max_retries: 1
  poll_interval_sec: 1
  poll_timeout_sec: 60
  test_run: true
  reset_each_run: true
llm_profiles:
  analysis: {provider: groq, model: x}
  validation: {provider: groq, model: x}
steps:
  - step_id: "E2E_STEP"
    description: "End-to-end stub settlement"
    action_type: SUBMIT
    object_name: "RKO7KO8G"
    async: false
    validate:
      mode: keyword
      keyword:
        source: spool
        ok_patterns: ["settled"]
        error_patterns: ["FATAL"]
"""


def test_full_web_run_end_to_end(client, monkeypatch, make_llms, tmp_path):
    cfg_file = tmp_path / "period_close_E2E.yaml"
    cfg_file.write_text(_E2E_CONFIG, encoding="utf-8")

    # In-memory checkpointer; never build a real LLM client.
    monkeypatch.setattr(graph_builder, "build_async_checkpointer", _in_memory_checkpointer)
    monkeypatch.setattr(graph_builder, "build_llms_from_config", lambda _cfg: make_llms())
    # Point the company registry at our controlled config.
    monkeypatch.setattr(web_app, "get_company",
                        lambda code: {"code": "E2E", "config_file": str(cfg_file)})

    _session_cookie(client, company_codes=["E2E"])

    with client.websocket_connect("/ws?company=E2E") as ws:
        assert ws.receive_json()["type"] == "company_status"
        ws.send_json({"type": "start"})

        seen = []
        run_end = None
        for _ in range(60):
            ev = ws.receive_json()
            seen.append(ev["type"])
            if ev["type"] == "run_end":
                run_end = ev
                break

    assert run_end is not None, f"no run_end; saw: {seen}"
    assert run_end.get("status") == "completed", f"run_end={run_end}; saw: {seen}"
    assert "run_init" in seen
    assert "step_start" in seen
    # The real graph actually executed against the stub and validated the spool.
    assert "step_end" in seen


_E2E_FAIL_CONFIG = """
company_config:
  company_code: "E2E"
  controlling_area: "X500"
  currency: "RUB"
defaults:
  max_retries: 1
  test_run: true
  reset_each_run: true
llm_profiles:
  analysis: {provider: groq, model: x}
  validation: {provider: groq, model: x}
steps:
  - step_id: "E2E_FAIL"
    description: "Step that fails validation and escalates"
    action_type: SUBMIT
    object_name: "RKO7KO8G"
    async: false
    validate:
      mode: keyword
      keyword:
        source: spool
        ok_patterns: ["NEVERMATCH"]
        error_patterns: ["error"]
"""


def test_full_web_run_interrupt_then_abort(client, monkeypatch, make_llms, tmp_path,
                                           patch_react_agent):
    cfg_file = tmp_path / "period_close_E2E_FAIL.yaml"
    cfg_file.write_text(_E2E_FAIL_CONFIG, encoding="utf-8")

    monkeypatch.setattr(graph_builder, "build_async_checkpointer", _in_memory_checkpointer)
    monkeypatch.setattr(graph_builder, "build_llms_from_config", lambda _cfg: make_llms())
    # Analysis deterministically escalates to the operator (no real ReAct agent/LLM).
    patch_react_agent('{"action":"user_input","corrected_params":null,'
                      '"diagnosis":"needs a human","user_instructions":"resolve manually"}')
    monkeypatch.setattr(web_app, "get_company",
                        lambda code: {"code": "E2E", "config_file": str(cfg_file)})

    _session_cookie(client, company_codes=["E2E"])

    with client.websocket_connect("/ws?company=E2E") as ws:
        assert ws.receive_json()["type"] == "company_status"
        ws.send_json({"type": "start"})

        saw_interrupt = False
        run_end = None
        for _ in range(80):
            ev = ws.receive_json()
            if ev["type"] == "interrupt":
                saw_interrupt = True
                ws.send_json({"type": "decision", "action": "abort"})  # operator aborts
            elif ev["type"] == "run_end":
                run_end = ev
                break

    assert saw_interrupt, "expected an operator interrupt"
    assert run_end is not None and run_end.get("status") == "aborted", f"run_end={run_end}"
