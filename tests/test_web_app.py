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


def _sys_admin(client, codes=("RU06",)):
    """The top role: every permission, and global company scope regardless of `codes`."""
    _session_cookie(client, username="root", role="sys_admin", company_codes=list(codes))


def _company_admin(client, codes=("RU06",)):
    """Company-scoped config admin: may edit its own companies' deltas, nothing above."""
    _session_cookie(client, username="admin", role="admin", company_codes=list(codes))


def _ko8g_field(data, path):
    ko8g = next(s for s in data["steps"] if s["step_id"] == "KO8G")
    return next(f for f in ko8g["fields"] if f["path"] == path)


def test_settings_requires_admin(client, settings_store):
    _session_cookie(client, role="operator", company_codes=["RU06"])
    assert client.get("/api/settings/RU06").status_code == 403


def test_settings_company_authz(client, settings_store):
    _company_admin(client, codes=["RU06"])
    assert client.get("/api/settings/RU47").status_code == 403      # unknown + unassigned


def test_company_admin_scoped_to_its_own_companies(client, settings_store):
    """A company admin is confined to its `company_codes` — being in the registry is not
    enough. This is what lets several admins hold overlapping or disjoint company sets."""
    settings_store.create_company("RU99", "New Co", user="root")
    _company_admin(client, codes=["RU06"])
    assert client.get("/api/settings/RU99").status_code == 403
    assert client.get("/api/settings/RU06").status_code == 200


def test_company_admin_cannot_reach_base_writes_or_registries(client, settings_store):
    """The whole point of the split: `admin` configures companies, `sys_admin` owns the
    product base, the registries and the user list."""
    _company_admin(client, codes=["RU06"])
    assert client.get("/api/settings/base").status_code == 200        # read-only view: allowed
    assert client.put("/api/settings/base",
                      json={"version": 1, "globals": {}, "steps": []}).status_code == 403
    assert client.get("/api/registry/companies").status_code == 403
    assert client.get("/api/registry/users").status_code == 403


def test_settings_reachable_for_company_created_in_registry(client, settings_store):
    """A sys_admin who creates a company must be able to configure it straight away —
    that now falls out of its global scope rather than a special case in the gate."""
    _sys_admin(client, codes=["RU06"])
    assert client.post("/api/registry/companies",
                       json={"code": "RU99", "display_name": "New Co"}).status_code == 200
    assert client.get("/api/settings/RU99").status_code == 200
    r = client.put("/api/settings/RU99", json={"version": 0, "override": {"KO8G": {"test_run": False}},
                                               "run_context": {"currency": "RUB"}})
    assert r.status_code == 200
    assert r.json()["override"] == {"KO8G": {"test_run": False}}


def test_settings_operator_still_blocked_for_registered_company(client, settings_store):
    """Company scope never substitutes for the permission: an operator holding RU99 may
    run it, but `config_company` is not in the operator role at all."""
    settings_store.create_company("RU99", "New Co", user="root")
    _session_cookie(client, role="operator", company_codes=["RU99"])
    assert client.get("/api/settings/RU99").status_code == 403


def test_settings_get_put_reset_roundtrip(client, settings_store):
    _company_admin(client, codes=["RU06"])
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
    _company_admin(client, codes=["RU06"])
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
    _company_admin(client)
    d = client.get("/api/settings/form-schema").json()
    assert [s["key"] for s in d["sections"]][0] == "step"


def test_base_get_put_history_roundtrip(client, settings_store):
    _sys_admin(client)
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
    _sys_admin(client)
    r = client.put("/api/settings/base",
                   json={"version": 0, "globals": {}, "steps": [{"step_id": "X", "action_type": "NOPE"}]})
    assert r.status_code == 400


def test_users_requires_admin(client, settings_store):
    _session_cookie(client, role="operator", company_codes=["RU06"])
    assert client.get("/api/registry/users").status_code == 403


def test_users_crud(client, settings_store):
    _sys_admin(client)
    # seed a sys_admin so the last-sys_admin guard never blocks the edits below
    settings_store.create_user("root", auth.hash_password("x"), role="sys_admin")

    assert {u["username"] for u in client.get("/api/registry/users").json()} == {"root"}

    # create — password is hashed server-side, never echoed back
    r = client.post("/api/registry/users", json={
        "username": "ivanov", "password": "secret", "display_name": "Ivanov",
        "email": "ivanov@example.com",
        "role": "operator", "company_codes": ["RU06", "RU72"]})
    assert r.status_code == 200
    ivanov = next(u for u in r.json()["users"] if u["username"] == "ivanov")
    assert "password_hash" not in ivanov and ivanov["company_codes"] == ["RU06", "RU72"]
    assert ivanov["email"] == "ivanov@example.com"

    # the created user can actually authenticate — with a password they must then replace
    assert auth.get_user("ivanov") and auth.verify_password(
        "secret", auth.get_user("ivanov")["password_hash"])
    assert auth.get_user("ivanov")["must_change_password"] is True

    # duplicate → 400
    assert client.post("/api/registry/users",
                       json={"username": "ivanov", "password": "y"}).status_code == 400
    # no password and no address to mail a generated one to → 400
    assert client.post("/api/registry/users",
                       json={"username": "nopass"}).status_code == 400
    # bad role → 400
    assert client.post("/api/registry/users",
                       json={"username": "x", "password": "y", "role": "root"}).status_code == 400

    # update: change role and address; there is no password field to change
    r = client.put("/api/registry/users/ivanov",
                   json={"role": "admin", "email": "i.ivanov@example.com",
                         "password": "newpw"})
    assert r.status_code == 200
    row = next(u for u in r.json() if u["username"] == "ivanov")
    assert row["role"] == "admin" and row["email"] == "i.ivanov@example.com"
    assert auth.verify_password("secret", auth.get_user("ivanov")["password_hash"])

    # delete
    r = client.request("DELETE", "/api/registry/users/ivanov")
    assert r.status_code == 200
    assert "ivanov" not in {u["username"] for u in r.json()}


def test_users_create_generates_password_when_none_given(client, settings_store, monkeypatch):
    """No typed password + an address on file ⇒ one is generated and mailed, and the
    admin never sees it."""
    _sys_admin(client)
    sent = {}
    monkeypatch.setattr(web_app.mailer, "is_configured", lambda: True)
    monkeypatch.setattr(web_app.mailer, "send_password_email",
                        lambda to, user, pw, *, reset: sent.update(to=to, pw=pw, reset=reset))

    r = client.post("/api/registry/users",
                    json={"username": "petrov", "email": "petrov@example.com"})
    assert r.status_code == 200
    assert r.json()["emailed"] is True and r.json()["password"] is None
    assert sent["to"] == "petrov@example.com" and sent["reset"] is False
    assert auth.verify_password(sent["pw"], auth.get_user("petrov")["password_hash"])


def test_users_reset_password_mails_and_locks_the_account(client, settings_store, monkeypatch):
    _sys_admin(client)
    settings_store.create_user("petrov", auth.hash_password("original"), role="operator",
                               email="petrov@example.com")
    sent = {}
    monkeypatch.setattr(web_app.mailer, "is_configured", lambda: True)
    monkeypatch.setattr(web_app.mailer, "send_password_email",
                        lambda to, user, pw, *, reset: sent.update(to=to, pw=pw, reset=reset))

    r = client.post("/api/registry/users/petrov/reset-password", json={})
    assert r.status_code == 200
    assert r.json()["emailed"] is True and r.json()["password"] is None
    assert sent["reset"] is True
    user = auth.get_user("petrov")
    assert not auth.verify_password("original", user["password_hash"])
    assert auth.verify_password(sent["pw"], user["password_hash"])
    assert user["must_change_password"] is True


def test_users_reset_password_returns_it_when_undeliverable(client, settings_store, monkeypatch):
    """No SMTP (or no address) is not a dead end: the one-time password comes back once
    so the admin can hand it over — otherwise a fresh deployment could onboard nobody."""
    _sys_admin(client)
    settings_store.create_user("petrov", auth.hash_password("original"), role="operator")
    monkeypatch.setattr(web_app.mailer, "is_configured", lambda: True)

    r = client.post("/api/registry/users/petrov/reset-password", json={})
    assert r.status_code == 200
    body = r.json()
    assert body["emailed"] is False and body["password"]
    assert auth.verify_password(body["password"], auth.get_user("petrov")["password_hash"])


def test_operator_company_scope_is_exclusive(client, settings_store):
    """Every company is closed by exactly one operator, so a second one is refused —
    while the same overlap between admins is fine (that is holiday cover)."""
    _sys_admin(client)
    settings_store.create_user("op1", auth.hash_password("x"), role="operator",
                               company_codes=["RU06", "RU72"])

    r = client.post("/api/registry/users", json={
        "username": "op2", "password": "y", "role": "operator",
        "company_codes": ["RU72"]})
    assert r.status_code == 400 and "RU72" in r.json()["error"]

    # …the same codes under a non-exclusive role are fine
    assert client.post("/api/registry/users", json={
        "username": "adm2", "password": "y", "role": "admin",
        "company_codes": ["RU72"]}).status_code == 200

    # …and an operator may of course keep its own codes on an unrelated edit
    assert client.put("/api/registry/users/op1",
                      json={"company_codes": ["RU06", "RU72"]}).status_code == 200

    # promoting the admin to operator would now take RU72 off op1 → refused
    assert client.put("/api/registry/users/adm2",
                      json={"role": "operator"}).status_code == 400


def test_company_scope_picker_offers_only_free_codes(client, settings_store):
    _sys_admin(client)
    for code in ("RU06", "RU72", "RU47"):
        settings_store.create_company(code, code, user="root")
    settings_store.create_user("op1", auth.hash_password("x"), role="operator",
                               company_codes=["RU72"])

    free = client.get("/api/registry/company-scope?role=operator").json()
    assert set(free["all"]) == {"RU06", "RU72", "RU47"}
    assert set(free["free"]) == {"RU06", "RU47"} and free["exclusive"] is True

    # editing op1 itself: keeping RU72 is not a clash, so it stays on offer
    own = client.get("/api/registry/company-scope?role=operator&username=op1").json()
    assert set(own["free"]) == {"RU06", "RU72", "RU47"}

    # a non-exclusive role sees the whole registry
    adm = client.get("/api/registry/company-scope?role=admin").json()
    assert set(adm["free"]) == {"RU06", "RU72", "RU47"} and adm["exclusive"] is False


def test_handed_over_password_fences_the_session_until_replaced(client, settings_store):
    """A one-time password gets you in and nothing else: every gated endpoint refuses
    until the user sets one of their own."""
    settings_store.create_user("temp", auth.hash_password("handedover"),
                               role="sys_admin", must_change_password=True)
    r = client.post("/api/login", json={"username": "temp", "password": "handedover"})
    assert r.status_code == 200 and r.json()["must_change_password"] is True

    assert client.get("/api/me").json()["must_change_password"] is True
    assert client.get("/api/registry/users").status_code == 403
    assert client.get("/api/settings/base").status_code == 403
    with pytest.raises(WebSocketDisconnect) as ei:
        with client.websocket_connect("/ws/dashboard") as ws:
            ws.receive_json()
    assert ei.value.code == 4003

    # setting a real password lifts the fence (and re-issues the cookie that carries it)
    assert client.post("/api/me/password", json={
        "current_password": "handedover", "new_password": "myownpassword"}).status_code == 200
    assert client.get("/api/me").json()["must_change_password"] is False
    assert client.get("/api/registry/users").status_code == 200


def test_self_service_password_reset(client, settings_store, monkeypatch):
    settings_store.create_user("petrov", auth.hash_password("original"), role="operator",
                               email="petrov@example.com")
    sent = {}
    monkeypatch.setattr(web_app.mailer, "is_configured", lambda: True)
    monkeypatch.setattr(web_app.mailer, "send_password_email",
                        lambda to, user, pw, *, reset: sent.update(to=to, pw=pw))

    r = client.post("/api/password-reset", json={"username": "petrov"})
    assert r.status_code == 200 and r.json()["ok"] is True
    assert sent["to"] == "petrov@example.com"
    user = auth.get_user("petrov")
    assert auth.verify_password(sent["pw"], user["password_hash"])
    assert user["must_change_password"] is True

    # an unknown account gets the identical reply — the login screen is not a directory
    sent.clear()
    same = client.post("/api/password-reset", json={"username": "ghost"})
    assert same.status_code == 200 and same.json() == r.json() and not sent


def test_self_service_password_reset_without_smtp(client, settings_store, monkeypatch):
    settings_store.create_user("petrov", auth.hash_password("original"), role="operator",
                               email="petrov@example.com")
    monkeypatch.setattr(web_app.mailer, "is_configured", lambda: False)
    r = client.post("/api/password-reset", json={"username": "petrov"})
    assert r.status_code == 503
    # the account is untouched — no silent lockout
    assert auth.verify_password("original", auth.get_user("petrov")["password_hash"])


def test_users_last_admin_guarded_via_api(client, settings_store):
    _sys_admin(client)
    settings_store.create_user("solo", auth.hash_password("x"), role="sys_admin")
    assert client.request("DELETE", "/api/registry/users/solo").status_code == 400
    assert client.put("/api/registry/users/solo",
                      json={"role": "operator"}).status_code == 400


def test_registry_crud(client, settings_store):
    _sys_admin(client)
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


def test_registry_listing_carries_the_delta_state(client, settings_store):
    """The Manage-companies detail pane reads these off the listing — one request, not
    one per company."""
    _sys_admin(client)
    client.post("/api/registry/companies", json={"code": "RU06", "display_name": "Co"})
    row = client.get("/api/registry/companies").json()[0]
    assert row["has_override"] is False and row["override_version"] == 0

    settings_store.save_override("RU06", {"defaults": {"max_retries": 9}}, {},
                                 expected_version=None, user="ivanov")
    row = client.get("/api/registry/companies").json()[0]
    assert row["has_override"] is True
    assert row["override_version"] == 1 and row["override_updated_by"] == "ivanov"


def test_base_settings_report_their_author(client, settings_store):
    """The Base panel in the registry dialog names who last moved the base."""
    _sys_admin(client)
    before = client.get("/api/settings/base").json()
    assert before["updated_by"] == "fixture"        # whoever seeded it
    client.put("/api/settings/base",
               json={"globals": {}, "steps": [], "version": before["version"]})
    body = client.get("/api/settings/base").json()
    assert body["version"] == before["version"] + 1
    assert body["updated_by"] == "root" and body["updated_at"]


# ===========================================================================
# Roles: permission set, company scope, read-only manager
# ===========================================================================

def test_me_publishes_the_permission_set(client, settings_store):
    """The UI hides controls off this list instead of keeping its own role table."""
    _session_cookie(client, role="manager", company_codes=["RU06"])
    data = client.get("/api/me").json()
    assert data["role"] == "manager"
    assert data["permissions"] == ["view"]          # read-only: no run, no config

    _sys_admin(client)
    perms = client.get("/api/me").json()["permissions"]
    assert {"run", "config_base", "manage_users", "manage_registry"} <= set(perms)


def test_sys_admin_scope_is_every_registered_company(client, settings_store):
    """Global scope is implied by the role, so a sys_admin never has to add itself to a
    company's list to see or configure it."""
    for code in ("RU06", "RU47"):
        settings_store.create_company(code, code, user="root")
    _sys_admin(client, codes=[])                     # empty company_codes on purpose
    assert {c["code"] for c in client.get("/api/companies").json()} == {"RU06", "RU47"}
    assert client.get("/api/settings/RU47").status_code == 200


def test_wildcard_scope_expands_to_the_registry(client, settings_store):
    """`"*"` in company_codes is the explicit form of the same thing — the intended way
    to give a manager oversight of the whole close."""
    for code in ("RU06", "RU47"):
        settings_store.create_company(code, code, user="root")
    _session_cookie(client, role="manager", company_codes=["*"])
    assert {c["code"] for c in client.get("/api/companies").json()} == {"RU06", "RU47"}


def test_manager_is_read_only_over_rest(client, settings_store):
    _session_cookie(client, role="manager", company_codes=["RU06"])
    assert client.get("/api/companies").status_code == 200          # sees the dashboard
    assert client.get("/api/settings/RU06").status_code == 403      # but configures nothing
    assert client.get("/api/registry/users").status_code == 403


# ===========================================================================
# WebSocket: auth gating
# ===========================================================================

def test_ws_company_refuses_control_from_read_only_role(client, monkeypatch):
    """A manager receives the whole event stream and can drive none of it. Enforced
    server-side — hiding the buttons in the UI is not access control."""
    started = []
    monkeypatch.setattr(graph_builder, "run_period_close_web",
                        lambda *a, **k: started.append(1))
    _session_cookie(client, role="manager", company_codes=["RU06"])

    with client.websocket_connect("/ws?company=RU06") as ws:
        assert ws.receive_json()["type"] == "company_status"        # watching is fine
        for msg in ({"type": "start"},
                    {"type": "decision", "action": "skip_step"},
                    {"type": "rollback_and_restart", "start_from_step": "S1"}):
            ws.send_json(msg)
            ev = ws.receive_json()
            assert ev["type"] == "error" and "read-only" in ev["message"]
    assert started == []                                            # nothing ever ran


def test_ws_dashboard_refuses_start_from_read_only_role(client, monkeypatch):
    started = []
    monkeypatch.setattr(graph_builder, "run_period_close_web",
                        lambda *a, **k: started.append(1))
    _session_cookie(client, role="manager", company_codes=["RU06"])

    with client.websocket_connect("/ws/dashboard") as ws:
        ws.send_json({"type": "start", "company": "RU06"})
        ev = ws.receive_json()
        assert ev["type"] == "error" and "read-only" in ev["message"]
    assert started == []


# ===========================================================================
# REST: self-service password change
# ===========================================================================

def test_change_own_password(client, settings_store):
    """Any role may rotate its own password — routing every reset through a sys_admin is
    what pushes teams toward shared accounts."""
    settings_store.create_user("ivanov", auth.hash_password("oldpassword"),
                               role="operator", company_codes=["RU06"])
    _session_cookie(client, username="ivanov", role="operator")

    # wrong current password → 403, and the stored hash is untouched
    assert client.post("/api/me/password", json={
        "current_password": "WRONG", "new_password": "newpassword"}).status_code == 403
    assert auth.verify_password("oldpassword", auth.get_user("ivanov")["password_hash"])

    # too short → 400
    assert client.post("/api/me/password", json={
        "current_password": "oldpassword", "new_password": "short"}).status_code == 400

    r = client.post("/api/me/password", json={
        "current_password": "oldpassword", "new_password": "newpassword"})
    assert r.status_code == 200
    assert auth.verify_password("newpassword", auth.get_user("ivanov")["password_hash"])


def test_change_own_password_requires_auth(client):
    assert client.post("/api/me/password", json={
        "current_password": "x", "new_password": "yyyyyyyy"}).status_code == 401




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


# Minimal single-step base seeded into a throwaway store for the end-to-end web
# run (see test_full_web_run_end_to_end). One inline SUBMIT whose stub spool
# contains "settled" → keyword validate passes → the run reaches run_end quickly.
_E2E_GLOBALS = {
    "company_config": {"company_code": "E2E", "controlling_area": "X500",
                       "currency": "RUB"},
    "defaults": {"max_retries": 1, "poll_interval_sec": 1, "poll_timeout_sec": 60,
                 "test_run": True, "reset_each_run": True},
    "llm_profiles": {"analysis": {"provider": "groq", "model": "x"},
                     "validation": {"provider": "groq", "model": "x"}},
}
_E2E_STEPS = [{
    "step_id": "E2E_STEP",
    "description": "End-to-end stub settlement",
    "action_type": "SUBMIT",
    "object_name": "RKO7KO8G",
    "async": False,
    "validate": {"mode": "keyword",
                 "keyword": {"source": "spool", "ok_patterns": ["settled"],
                             "error_patterns": ["FATAL"]}},
}]


def test_full_web_run_end_to_end(client, monkeypatch, make_llms, tmp_path):
    # The web run resolves config from the config-store by company_code
    # (_resolve_period_cfg ignores period_config_path when a company is given), so
    # seed a throwaway store with a minimal single-step base rather than a YAML file.
    # This keeps the test hermetic — independent of the developer's live
    # config_store.db (whose grown step list / tool_mode would otherwise leak in).
    st = ConfigStore(tmp_path / "e2e.db")
    st.save_base(_E2E_GLOBALS, _E2E_STEPS, expected_version=0, user="fixture")
    monkeypatch.setattr(config_store, "_store", st)

    # In-memory checkpointer; never build a real LLM client.
    monkeypatch.setattr(graph_builder, "build_async_checkpointer", _in_memory_checkpointer)
    monkeypatch.setattr(graph_builder, "build_llms_from_config", lambda _cfg: make_llms())
    # E2E is unregistered, so load_effective_config("E2E") returns the seeded base.
    monkeypatch.setattr(web_app, "get_company",
                        lambda code: {"code": "E2E", "config_file": ""})

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


# Minimal single-step base whose validate can never pass (ok pattern absent, error
# pattern present) → the step escalates to analysis. Seeded like _E2E_GLOBALS to keep
# the interrupt test hermetic. No tool_mode → text-ReAct path, so patch_react_agent
# drives the analysis verdict.
_E2E_FAIL_GLOBALS = {
    "company_config": {"company_code": "E2E", "controlling_area": "X500",
                       "currency": "RUB"},
    "defaults": {"max_retries": 1, "test_run": True, "reset_each_run": True},
    "llm_profiles": {"analysis": {"provider": "groq", "model": "x"},
                     "validation": {"provider": "groq", "model": "x"}},
}
_E2E_FAIL_STEPS = [{
    "step_id": "E2E_FAIL",
    "description": "Step that fails validation and escalates",
    "action_type": "SUBMIT",
    "object_name": "RKO7KO8G",
    "async": False,
    "validate": {"mode": "keyword",
                 "keyword": {"source": "spool", "ok_patterns": ["NEVERMATCH"],
                             "error_patterns": ["error"]}},
}]


def test_full_web_run_interrupt_then_abort(client, monkeypatch, make_llms, tmp_path,
                                           patch_react_agent):
    st = ConfigStore(tmp_path / "e2e_fail.db")
    st.save_base(_E2E_FAIL_GLOBALS, _E2E_FAIL_STEPS, expected_version=0, user="fixture")
    monkeypatch.setattr(config_store, "_store", st)

    monkeypatch.setattr(graph_builder, "build_async_checkpointer", _in_memory_checkpointer)
    monkeypatch.setattr(graph_builder, "build_llms_from_config", lambda _cfg: make_llms())
    # Analysis deterministically escalates to the operator (no real ReAct agent/LLM).
    patch_react_agent('{"action":"user_input","corrected_params":null,'
                      '"diagnosis":"needs a human","user_instructions":"resolve manually"}')
    monkeypatch.setattr(web_app, "get_company",
                        lambda code: {"code": "E2E", "config_file": ""})

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
