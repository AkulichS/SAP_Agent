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


def test_me_with_session(client, settings_store):
    settings_store.create_company("RU06", "Company A", user="admin")
    _session_cookie(client, username="alice", company_codes=["RU06"])
    r = client.get("/api/me")
    assert r.status_code == 200
    data = r.json()
    assert data["username"] == "alice"
    assert "RU06" in [c["code"] for c in data["companies"]]


def test_companies_requires_auth(client):
    assert client.get("/api/companies").status_code == 401


def test_companies_lists_only_authorized(client, settings_store):
    for code in ("RU06", "RU47"):
        settings_store.create_company(code, code, user="admin")
    _session_cookie(client, company_codes=["RU06"])
    r = client.get("/api/companies")
    assert r.status_code == 200
    codes = [c["code"] for c in r.json()]
    assert codes == ["RU06"]          # registered, but not assigned to this user → no RU47
    assert "run_status" in r.json()[0]


def test_logout_clears_cookie(client):
    _session_cookie(client)
    r = client.post("/api/logout")
    assert r.status_code == 200 and r.json()["ok"] is True


# ===========================================================================
# REST: admin settings (per-company config deltas)
# ===========================================================================

import config_store
from config_store import ConfigStore


# Minimal base step library — the repo no longer ships configs/base.yaml as a seed
# file for a fresh DB, so the fixture seeds the DB tier directly instead.
_SETTINGS_BASE_STEPS = [
    {
        "step_id": "KO8G",
        "action_type": "SUBMIT",
        "async": True,
        "test_run": True,
        "validate": {"keyword": {"source": "rows"}},
    },
    {
        "step_id": "CO88",
        "action_type": "SUBMIT",
        "async": True,
        "test_run": True,
    },
]


@pytest.fixture
def settings_store(tmp_path, monkeypatch):
    """Point the process-wide store singleton at a throwaway DB, pre-seeded with a
    minimal base step library (base version 1 after seeding)."""
    st = ConfigStore(tmp_path / "cfg.db")
    st.save_base({"defaults": {"max_retries": 3}}, _SETTINGS_BASE_STEPS,
                expected_version=0, user="fixture")
    monkeypatch.setattr(config_store, "_store", st)
    return st


def _admin(client, codes=("RU06",)):
    _session_cookie(client, username="admin", role="admin", company_codes=list(codes))


def _ko8g_field(data, path):
    ko8g = next(s for s in data["steps"] if s["step_id"] == "KO8G")
    return next(f for f in ko8g["fields"] if f["path"] == path)


def test_settings_requires_admin(client, settings_store):
    _session_cookie(client, role="operator", company_codes=["RU06"])
    assert client.get("/api/settings/RU06").status_code == 403


def test_settings_company_authz(client, settings_store):
    _admin(client, codes=["RU06"])
    assert client.get("/api/settings/RU47").status_code == 403      # unknown + unassigned


def test_settings_reachable_for_company_created_in_registry(client, settings_store):
    """An admin who creates a company must be able to configure it, even though the new
    code is in nobody's users.yaml company_codes yet."""
    _admin(client, codes=["RU06"])
    assert client.post("/api/registry/companies",
                       json={"code": "RU99", "display_name": "New Co"}).status_code == 200
    assert client.get("/api/settings/RU99").status_code == 200
    r = client.put("/api/settings/RU99", json={"version": 0, "override": {"KO8G": {"test_run": False}},
                                               "run_context": {"currency": "RUB"}})
    assert r.status_code == 200
    assert r.json()["override"] == {"KO8G": {"test_run": False}}


def test_settings_operator_still_blocked_for_registered_company(client, settings_store):
    """Registry membership relaxes the company check for admins only — not the role check."""
    settings_store.create_company("RU99", "New Co", user="admin")
    _session_cookie(client, role="operator", company_codes=["RU99"])
    assert client.get("/api/settings/RU99").status_code == 403


def test_settings_get_put_reset_roundtrip(client, settings_store):
    _admin(client, codes=["RU06"])
    data = client.get("/api/settings/RU06").json()
    assert data["version"] == 0
    assert _ko8g_field(data, "test_run")["overridden"] is False

    r = client.put("/api/settings/RU06", json={
        "version": 0,
        "override": {"KO8G": {"test_run": False}},
        "run_context": {"currency": "RUB"},
    })
    assert r.status_code == 200
    data = r.json()
    assert data["version"] == 1
    assert _ko8g_field(data, "test_run")["overridden"] is True

    # stale version → 409
    assert client.put("/api/settings/RU06",
                      json={"version": 0, "override": {}}).status_code == 409

    # reset that step → back to inherited
    r = client.post("/api/settings/RU06/reset", json={"version": 1, "step_id": "KO8G"})
    assert r.status_code == 200
    assert _ko8g_field(r.json(), "test_run")["overridden"] is False


def test_settings_put_rejects_invalid(client, settings_store):
    _admin(client, codes=["RU06"])
    r = client.put("/api/settings/RU06", json={
        "version": 0,
        "override": {"KO8G": {"validate": {"keyword": {"source": "BOGUS"}}}},
    })
    assert r.status_code == 400


# ===========================================================================
# REST: base tier + form-schema + registry
# ===========================================================================

def test_form_schema_requires_admin(client, settings_store):
    _session_cookie(client, role="operator", company_codes=["RU06"])
    assert client.get("/api/settings/form-schema").status_code == 403


def test_form_schema_returns_sections(client, settings_store):
    _admin(client)
    d = client.get("/api/settings/form-schema").json()
    assert [s["key"] for s in d["sections"]][0] == "step"


def test_base_get_put_history_roundtrip(client, settings_store):
    _admin(client)
    data = client.get("/api/settings/base").json()
    assert data["version"] == 1          # settings_store seeds the base tier once
    n = len(data["steps"])
    assert n > 0 and "defaults" in data["globals"]

    # drop a step + edit a global, save
    steps = [s for s in data["steps"] if s["step_id"] != "CO88"]
    globals_ = {**data["globals"], "defaults": {**data["globals"].get("defaults", {}), "max_retries": 9}}
    r = client.put("/api/settings/base", json={"version": 1, "globals": globals_, "steps": steps})
    assert r.status_code == 200
    saved = r.json()
    assert saved["version"] == 2
    assert len(saved["steps"]) == n - 1
    assert saved["globals"]["defaults"]["max_retries"] == 9

    # stale version → 409
    assert client.put("/api/settings/base",
                      json={"version": 1, "globals": {}, "steps": []}).status_code == 409

    hist = client.get("/api/settings/base/history").json()
    assert [h["version"] for h in hist] == [2, 1]   # fixture seed (v1) + this test's save (v2)


def test_base_put_rejects_invalid_step(client, settings_store):
    _admin(client)
    r = client.put("/api/settings/base",
                   json={"version": 0, "globals": {}, "steps": [{"step_id": "X", "action_type": "NOPE"}]})
    assert r.status_code == 400


def test_registry_crud(client, settings_store):
    _admin(client)
    assert client.get("/api/registry/companies").json() == []
    r = client.post("/api/registry/companies", json={"code": "RU99", "display_name": "New Co"})
    assert r.status_code == 200
    assert {c["code"] for c in r.json()} == {"RU99"}
    # duplicate → 400
    assert client.post("/api/registry/companies", json={"code": "RU99"}).status_code == 400
    # rename
    r = client.put("/api/registry/companies/RU99", json={"display_name": "Renamed"})
    assert next(c for c in r.json() if c["code"] == "RU99")["display_name"] == "Renamed"
    # delete
    r = client.request("DELETE", "/api/registry/companies/RU99")
    assert r.json() == []


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
    from config_store import load_effective_config
    start_from = load_effective_config("RU06")["steps"][0]["step_id"]

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
