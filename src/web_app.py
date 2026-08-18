"""
web_app.py — FastAPI server for SAP Period Close web UI.

Endpoints
---------
  GET  /               → index.html (login overlay shown by JS if unauthenticated)
  POST /api/login      → validate credentials, set session cookie
  GET  /api/me         → current user info + permission set (401 if not logged in)
  POST /api/logout     → clear session cookie
  POST /api/me/password → change own password (any authenticated user)
  POST /api/password-reset → unauthenticated: mail the user a new one-time password
  GET  /api/companies  → authorized companies with live run status

  WS   /ws/dashboard   → aggregated status stream for all authorized companies
                          accepts: {type:"start", company:"RU06"}
  WS   /ws             → full event stream for one company (?company=RU06)
                          accepts: {type:"start"} (if idle), {type:"decision", ...}

Start:
  uvicorn web_app:app --host 0.0.0.0 --port 8000
"""

from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager
from pathlib import Path

import yaml
from dotenv import load_dotenv
from fastapi import FastAPI, Query, Request, Response, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, JSONResponse, PlainTextResponse
from fastapi.staticfiles import StaticFiles
from starlette.concurrency import run_in_threadpool

load_dotenv(Path(__file__).parent / ".env", override=True)

import auth
import config_settings
import mailer
import permissions
from config import reset_each_run_enabled
from config_schema import STEP_SCHEMA_VERSION, SchemaTooNew, migrate_document, step_json_schema
from config_store import VersionConflict, get_config_store, load_effective_config
from run_manager import get_company, get_run_manager, load_companies

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger(__name__)

@asynccontextmanager
async def lifespan(_app: FastAPI):
    # Pre-import graph_builder on startup so the first Start click isn't delayed by
    # ~60s of cold module loading on Windows (langgraph + langchain + mcp deps).
    import graph_builder  # noqa: F401
    logger.info("graph_builder pre-imported — first run will start immediately")
    yield
    # Shared per-process resources (checkpointer connection, LLM HTTP pool) are
    # opened lazily by config.py and live for the life of the app — close them here.
    import config
    await config.close_checkpointers()
    await config.close_http_clients()


app = FastAPI(title="SAP Period Close Agent", lifespan=lifespan)

STATIC_DIR = Path(__file__).parent / "static"
STATIC_DIR.mkdir(exist_ok=True)
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _get_session(request: Request) -> dict | None:
    token = request.cookies.get(auth.COOKIE_NAME)
    return auth.decode_session_token(token) if token else None


def _get_session_ws(cookies: dict) -> dict | None:
    token = cookies.get(auth.COOKIE_NAME)
    return auth.decode_session_token(token) if token else None


def _scoped_codes(session: dict, all_codes) -> list[str]:
    """The companies this session may reach — the one place the scope rule is applied
    (role-implied `*` for sys_admin, an explicit `"*"` entry, or the plain list)."""
    return permissions.resolve_scope(
        session.get("role"), session.get("company_codes", []), all_codes)


def _companies_for_user(session: dict) -> list[dict]:
    all_companies = load_companies()
    codes = set(_scoped_codes(session, [c["code"] for c in all_companies]))
    mgr = get_run_manager()
    result = []
    for c in all_companies:
        if c["code"] in codes:
            entry = dict(c)
            entry["run_status"] = mgr.get_status(c["code"])
            # Dev/testing flag: when on, a finished run can be re-run (Start stays active).
            try:
                cfg = load_effective_config(c["code"])
                entry["reset_each_run"] = reset_each_run_enabled(cfg.get("defaults", {}))
            except Exception as exc:
                logger.warning("reset_each_run lookup failed for %s: %s", c["code"], exc)
                entry["reset_each_run"] = False
            result.append(entry)
    return result


# ---------------------------------------------------------------------------
# HTML
# ---------------------------------------------------------------------------

@app.get("/", response_class=HTMLResponse)
async def index():
    html_file = STATIC_DIR / "index.html"
    if html_file.exists():
        return HTMLResponse(html_file.read_text(encoding="utf-8"))
    return HTMLResponse("<h1>static/index.html not found</h1>", status_code=404)


# ---------------------------------------------------------------------------
# Auth REST endpoints
# ---------------------------------------------------------------------------

@app.post("/api/login")
async def login(request: Request, response: Response):
    body = await request.json()
    username = body.get("username", "").strip()
    password = body.get("password", "")

    user = auth.get_user(username)
    if not user or not auth.verify_password(password, user["password_hash"]):
        return JSONResponse({"error": "Invalid username or password"}, status_code=401)

    token = auth.create_session_token(user)
    # Same shape the session cookie carries — role included, since scope resolution
    # depends on it (sys_admin reaches every registered company).
    companies = _companies_for_user({
        "role": user.get("role", permissions.DEFAULT_ROLE),
        "company_codes": user.get("company_codes", []),
    })

    role = user.get("role", permissions.DEFAULT_ROLE)
    response = JSONResponse({
        "username": user["username"],
        "display_name": user.get("display_name", username),
        "email": user.get("email"),
        "role": role,
        "permissions": sorted(permissions.permissions_for(role)),
        # A handed-over password (initial account, or a reset) lets you in exactly once:
        # the UI forces the change modal and `_require` refuses everything else until
        # the user has set a password only they know.
        "must_change_password": bool(user.get("must_change_password")),
        "companies": companies,
    })
    response.set_cookie(
        auth.COOKIE_NAME,
        token,
        httponly=True,
        samesite="lax",
        max_age=8 * 3600,
        path="/",
    )
    logger.info("Login: %s", username)
    return response


@app.get("/api/me")
async def me(request: Request):
    session = _get_session(request)
    if not session:
        return JSONResponse({"error": "Not authenticated"}, status_code=401)
    companies = _companies_for_user(session)
    role = session.get("role", permissions.DEFAULT_ROLE)
    return JSONResponse({
        "username": session["username"],
        "display_name": session.get("display_name", session["username"]),
        "email": session.get("email"),
        "role": role,
        # The UI hides controls off this list rather than keeping its own copy of the
        # role table — the server stays the single source of truth (and the authority:
        # hiding a button is not access control, every endpoint re-checks).
        "permissions": sorted(permissions.permissions_for(role)),
        "must_change_password": bool(session.get("must_change_password")),
        "companies": companies,
    })


@app.post("/api/logout")
async def logout(response: Response):
    response = JSONResponse({"ok": True})
    response.delete_cookie(auth.COOKIE_NAME, path="/")
    return response


_MIN_PASSWORD_LEN = 8


@app.post("/api/me/password")
async def change_own_password(request: Request):
    """Self-service password change for any authenticated user.

    Deliberately needs no permission: routing every password change through a
    sys_admin is what pushes teams toward shared accounts. Requires the current
    password, so a borrowed session cannot lock the real owner out."""
    session = _get_session(request)
    if not session:
        return JSONResponse({"error": "Not authenticated"}, status_code=401)
    body = await request.json()
    current = body.get("current_password") or ""
    new = body.get("new_password") or ""
    if len(new) < _MIN_PASSWORD_LEN:
        return JSONResponse(
            {"error": f"New password must be at least {_MIN_PASSWORD_LEN} characters"},
            status_code=400)
    username = session["username"]
    user = await run_in_threadpool(auth.get_user, username)
    if not user or not auth.verify_password(current, user["password_hash"]):
        return JSONResponse({"error": "Current password is incorrect"}, status_code=403)
    await run_in_threadpool(
        lambda: get_config_store().update_user(
            username, password_hash=auth.hash_password(new),
            must_change_password=False, user=username)
    )
    logger.info("Password changed by %s (self-service)", username)
    # Re-issue the cookie: this is what lifts the hand-over lock (`_require` reads the
    # flag off the session, so a stale cookie would keep the account fenced off).
    fresh = dict(user, must_change_password=False)
    response = JSONResponse({"ok": True})
    response.set_cookie(auth.COOKIE_NAME, auth.create_session_token(fresh),
                        httponly=True, samesite="lax", max_age=8 * 3600, path="/")
    return response


@app.post("/api/password-reset")
async def password_reset(request: Request):
    """Self-service reset from the login screen — deliberately unauthenticated.

    Mints a one-time password, mails it to the address on the account, and flags the
    account so that password only works for the one login that replaces it. The reply
    never says whether the username exists: a 200 here means "if that account exists,
    its owner has mail", which is the only answer that does not turn this into a user
    directory. A deployment with no SMTP configured says so plainly (that leaks nothing
    about accounts) so the user knows to call an administrator instead."""
    body = await request.json()
    username = (body.get("username") or "").strip()
    if not mailer.is_configured():
        return JSONResponse(
            {"error": "E-mail delivery is not configured on this deployment — "
                      "ask your system administrator to reset your password"},
            status_code=503)
    generic = JSONResponse(
        {"ok": True, "message": "If that account exists, a new password has been "
                                "sent to the address on file."})
    if not username:
        return generic
    user = await run_in_threadpool(auth.get_user, username)
    if not user or not (user.get("email") or "").strip():
        logger.info("Password reset requested for unknown/e-mail-less account %r", username)
        return generic
    password = auth.generate_password()
    try:
        await run_in_threadpool(
            mailer.send_password_email, user["email"], username, password, reset=True)
    except mailer.MailError as exc:
        logger.warning("Self-service reset for %s failed to send: %s", username, exc)
        return JSONResponse({"error": str(exc)}, status_code=502)
    # Only after the mail is away — a password the user never receives would lock them
    # out of an account they could still have signed into.
    await run_in_threadpool(
        lambda: get_config_store().update_user(
            username, password_hash=auth.hash_password(password),
            must_change_password=True, user=username)
    )
    logger.info("Password reset (self-service) for %s", username)
    return generic


@app.get("/api/companies")
async def companies(request: Request):
    session = _get_session(request)
    if not session:
        return JSONResponse({"error": "Not authenticated"}, status_code=401)
    return JSONResponse(_companies_for_user(session))


# ---------------------------------------------------------------------------
# Admin Settings REST endpoints (per-company config deltas)
# ---------------------------------------------------------------------------

def _require(request: Request, permission: str, code: str | None = None):
    """Return (session, None) when the caller carries `permission` and — if `code` is
    given — has that company in scope; else (None, JSONResponse) with the right status.

    Two independent questions, always in this order: does the *role* carry the verb
    (permissions.PERMISSIONS), and is the *company* in scope (company_codes, with `"*"`
    and the sys_admin role meaning all). Nothing here branches on a role name, so adding
    a role is a row in permissions.PERMISSIONS and no endpoint edits.

    One question comes first, before either of those: is this session still running on a
    handed-over password? If so it may do nothing but change it (see `/api/me/password`),
    whatever its role — a temporary password that reached someone over e-mail is not yet
    proof of who is holding it."""
    session = _get_session(request)
    if not session:
        return None, JSONResponse({"error": "Not authenticated"}, status_code=401)
    if session.get("must_change_password"):
        return None, JSONResponse({"error": "Set a password of your own to continue"},
                                  status_code=403)
    if not permissions.has_permission(session.get("role"), permission):
        return None, JSONResponse({"error": f"Permission '{permission}' required"},
                                  status_code=403)
    if code is not None and not permissions.in_scope(
            session.get("role"), session.get("company_codes", []), code):
        return None, JSONResponse({"error": "Not authorized for this company"},
                                  status_code=403)
    return session, None


@app.get("/api/settings/schema")
async def settings_schema(request: Request):
    _, err = _require(request, permissions.CONFIG_COMPANY)
    if err:
        return err
    return JSONResponse(step_json_schema())


@app.get("/api/settings/form-schema")
async def settings_form_schema(request: Request):
    """UI form descriptor (sections, fields, conditional visibility, logical enums)."""
    _, err = _require(request, permissions.CONFIG_COMPANY)
    if err:
        return err
    # Base globals feed only the advisory TOOLS catalog; if the base can't be loaded
    # (unseeded store, no seed file) fall back to the built-in default catalog.
    try:
        base = await run_in_threadpool(config_settings.build_base_settings)
        globals_ = base.get("globals")
    except Exception:
        globals_ = None
    return JSONResponse(config_settings.form_descriptor(globals_))


# --- Base tier: the product base (globals + ordered steps), edited as "Company Base" ---
# Registered before /api/settings/{code} so the literal "base" path wins the match.
#
# Reads take CONFIG_COMPANY, writes take CONFIG_BASE. That asymmetry is the point of the
# role split: a company `admin` can *see* the base their delta sits on top of, but only a
# `sys_admin` can move it — one base edit changes every company at once.

@app.get("/api/settings/base")
async def get_base_settings(request: Request):
    _, err = _require(request, permissions.CONFIG_COMPANY)
    if err:
        return err
    data = await run_in_threadpool(config_settings.build_base_settings)
    return JSONResponse(data)


@app.put("/api/settings/base")
async def put_base_settings(request: Request):
    session, err = _require(request, permissions.CONFIG_BASE)
    if err:
        return err
    body = await request.json()
    globals_ = body.get("globals", {}) or {}
    steps = body.get("steps", []) or []
    version = body.get("version", 0)
    try:
        config_settings.validate_base_steps(steps)
    except ValueError as exc:
        return JSONResponse({"error": str(exc)}, status_code=400)
    try:
        await run_in_threadpool(
            get_config_store().save_base, globals_, steps, version, session["username"],
        )
    except VersionConflict as exc:
        return JSONResponse(
            {"error": "Base changed elsewhere — reload and retry", "detail": str(exc)},
            status_code=409,
        )
    data = await run_in_threadpool(config_settings.build_base_settings)
    logger.info("Base config saved by %s", session["username"])
    return JSONResponse(data)


@app.get("/api/settings/base/history")
async def get_base_history(request: Request):
    _, err = _require(request, permissions.CONFIG_COMPANY)
    if err:
        return err
    rows = await run_in_threadpool(get_config_store().base_history)
    return JSONResponse(rows)


@app.delete("/api/settings/base/history/oldest")
async def delete_base_history_oldest(request: Request):
    session, err = _require(request, permissions.CONFIG_BASE)
    if err:
        return err
    try:
        result = await run_in_threadpool(get_config_store().delete_oldest_base_history)
    except ValueError as exc:
        return JSONResponse({"error": str(exc)}, status_code=400)
    logger.info("Base history v%s pruned by %s", result["deleted_version"], session["username"])
    return JSONResponse(result)


@app.get("/api/settings/base/export")
async def export_base_settings(request: Request):
    """Download the base (globals + steps) as a YAML file — the seed an admin hands to
    another deployment (Manage companies → Base → download/upload)."""
    _, err = _require(request, permissions.CONFIG_COMPANY)
    if err:
        return err
    data = await run_in_threadpool(config_settings.build_base_settings)
    # Self-describe the export with the running program's schema version so a future
    # program can recognise and migrate this file on import.
    text = yaml.safe_dump(
        {"schema_version": STEP_SCHEMA_VERSION,
         "globals": data["globals"], "steps": data["steps"]},
        allow_unicode=True, sort_keys=False)
    return PlainTextResponse(text, media_type="application/x-yaml",
                             headers={"Content-Disposition": 'attachment; filename="base.yaml"'})


@app.post("/api/settings/base/import")
async def import_base_settings(request: Request):
    """Replace the base with an uploaded YAML file (same shape as the export). The
    overwrite itself is confirmed client-side; this always writes against the current
    version (a deliberate full replace, not an optimistic-locked edit)."""
    session, err = _require(request, permissions.CONFIG_BASE)
    if err:
        return err
    raw = (await request.body()).decode("utf-8")
    try:
        doc = yaml.safe_load(raw) or {}
    except yaml.YAMLError as exc:
        return JSONResponse({"error": f"Invalid YAML: {exc}"}, status_code=400)
    # Grandfather-rule read: an untagged file is treated as v1; an older tagged file is
    # migrated up to the current shape; a file from a newer program is refused legibly.
    try:
        doc = migrate_document(doc)
    except SchemaTooNew as exc:
        return JSONResponse({"error": str(exc)}, status_code=400)
    globals_ = doc.get("globals", {}) or {}
    steps = doc.get("steps", []) or []
    try:
        config_settings.validate_base_steps(steps)
    except ValueError as exc:
        return JSONResponse({"error": str(exc)}, status_code=400)
    store = get_config_store()
    current_version = await run_in_threadpool(lambda: store.get_base()["version"])
    await run_in_threadpool(store.save_base, globals_, steps, current_version, session["username"])
    data = await run_in_threadpool(config_settings.build_base_settings)
    logger.info("Base config imported by %s", session["username"])
    return JSONResponse(data)


@app.get("/api/settings/{code}")
async def get_settings(code: str, request: Request):
    _, err = _require(request, permissions.CONFIG_COMPANY, code)
    if err:
        return err
    data = await run_in_threadpool(config_settings.build_settings, code)
    return JSONResponse(data)


@app.put("/api/settings/{code}")
async def put_settings(code: str, request: Request):
    session, err = _require(request, permissions.CONFIG_COMPANY, code)
    if err:
        return err
    body = await request.json()
    override = body.get("override", {}) or {}
    run_context = body.get("run_context")
    version = body.get("version", 0)
    try:
        config_settings.validate_override(override)
    except ValueError as exc:
        return JSONResponse({"error": str(exc)}, status_code=400)
    try:
        await run_in_threadpool(
            get_config_store().save_override,
            code, override, run_context, version, session["username"],
        )
    except VersionConflict as exc:
        return JSONResponse(
            {"error": "Settings changed elsewhere — reload and retry", "detail": str(exc)},
            status_code=409,
        )
    data = await run_in_threadpool(config_settings.build_settings, code)
    logger.info("Settings saved for %s by %s", code, session["username"])
    return JSONResponse(data)


@app.get("/api/settings/{code}/history")
async def settings_history(code: str, request: Request):
    session, err = _require(request, permissions.CONFIG_COMPANY, code)
    if err:
        return err
    try:
        rows = await run_in_threadpool(get_config_store().history, code)
    except Exception as exc:
        logger.exception("Failed to load history for %s", code)
        return JSONResponse({"error": str(exc)}, status_code=500)
    return JSONResponse(rows)


@app.delete("/api/settings/{code}/history/oldest")
async def delete_settings_history_oldest(code: str, request: Request):
    session, err = _require(request, permissions.CONFIG_COMPANY, code)
    if err:
        return err
    try:
        result = await run_in_threadpool(get_config_store().delete_oldest_history, code)
    except ValueError as exc:
        return JSONResponse({"error": str(exc)}, status_code=400)
    logger.info("History v%s for %s pruned by %s",
                result["deleted_version"], code, session["username"])
    return JSONResponse(result)


@app.get("/api/settings/{code}/export")
async def export_settings(code: str, request: Request):
    """Download a company's override + run_context as a YAML file."""
    _, err = _require(request, permissions.CONFIG_COMPANY, code)
    if err:
        return err
    state = await run_in_threadpool(get_config_store().get_override, code)
    text = yaml.safe_dump({"override": state["override"], "run_context": state["run_context"]},
                          allow_unicode=True, sort_keys=False)
    return PlainTextResponse(text, media_type="application/x-yaml",
        headers={"Content-Disposition": f'attachment; filename="{code}.yaml"'})


@app.post("/api/settings/{code}/import")
async def import_settings(code: str, request: Request):
    """Replace a company's override + run_context with an uploaded YAML file (same shape
    as the export). Always writes against the current version — the overwrite is
    confirmed client-side, not optimistic-locked."""
    session, err = _require(request, permissions.CONFIG_COMPANY, code)
    if err:
        return err
    raw = (await request.body()).decode("utf-8")
    try:
        doc = yaml.safe_load(raw) or {}
    except yaml.YAMLError as exc:
        return JSONResponse({"error": f"Invalid YAML: {exc}"}, status_code=400)
    override = doc.get("override", {}) or {}
    run_context = doc.get("run_context", {}) or {}
    try:
        config_settings.validate_override(override)
    except ValueError as exc:
        return JSONResponse({"error": str(exc)}, status_code=400)
    store = get_config_store()
    current_version = await run_in_threadpool(lambda: store.get_override(code)["version"])
    await run_in_threadpool(store.save_override, code, override, run_context, current_version, session["username"])
    data = await run_in_threadpool(config_settings.build_settings, code)
    logger.info("Settings for %s imported by %s", code, session["username"])
    return JSONResponse(data)


@app.post("/api/settings/{code}/reset")
async def reset_settings(code: str, request: Request):
    session, err = _require(request, permissions.CONFIG_COMPANY, code)
    if err:
        return err
    body = await request.json()
    version = body.get("version")
    store = get_config_store()
    try:
        if body.get("scope") == "all":
            await run_in_threadpool(store.reset_company, code, version, session["username"])
        elif body.get("step_id"):
            await run_in_threadpool(store.reset_step, code, body["step_id"], version, session["username"])
        else:
            return JSONResponse({"error": "Provide scope:'all' or step_id"}, status_code=400)
    except VersionConflict as exc:
        return JSONResponse(
            {"error": "Settings changed elsewhere — reload and retry", "detail": str(exc)},
            status_code=409,
        )
    data = await run_in_threadpool(config_settings.build_settings, code)
    return JSONResponse(data)


# ---------------------------------------------------------------------------
# Company registry (create / rename / delete companies) — sys_admin only
# ---------------------------------------------------------------------------
# No company scope on these: the registry defines what companies *are*, so it sits above
# per-company scope and takes MANAGE_REGISTRY, which only sys_admin carries.

@app.get("/api/registry/companies")
async def registry_list(request: Request):
    _, err = _require(request, permissions.MANAGE_REGISTRY)
    if err:
        return err
    return JSONResponse(await run_in_threadpool(get_config_store().list_registry))


@app.post("/api/registry/companies")
async def registry_create(request: Request):
    session, err = _require(request, permissions.MANAGE_REGISTRY)
    if err:
        return err
    body = await request.json()
    code = (body.get("code") or "").strip()
    display_name = (body.get("display_name") or code).strip()
    run_context = body.get("run_context") or {}
    if not code:
        return JSONResponse({"error": "code is required"}, status_code=400)
    try:
        await run_in_threadpool(
            get_config_store().create_company, code, display_name, run_context, session["username"],
        )
    except ValueError as exc:
        return JSONResponse({"error": str(exc)}, status_code=400)
    logger.info("Company %s created by %s", code, session["username"])
    return JSONResponse(await run_in_threadpool(get_config_store().list_registry))


@app.put("/api/registry/companies/{code}")
async def registry_update(code: str, request: Request):
    _, err = _require(request, permissions.MANAGE_REGISTRY)
    if err:
        return err
    body = await request.json()
    display_name = body.get("display_name")
    if display_name is None:
        return JSONResponse({"error": "display_name is required"}, status_code=400)
    try:
        await run_in_threadpool(get_config_store().rename_company, code, display_name)
    except ValueError as exc:
        return JSONResponse({"error": str(exc)}, status_code=400)
    return JSONResponse(await run_in_threadpool(get_config_store().list_registry))


@app.delete("/api/registry/companies/{code}")
async def registry_delete(code: str, request: Request):
    _, err = _require(request, permissions.MANAGE_REGISTRY)
    if err:
        return err
    await run_in_threadpool(get_config_store().delete_company, code)
    logger.info("Company %s deleted", code)
    return JSONResponse(await run_in_threadpool(get_config_store().list_registry))


# ---------------------------------------------------------------------------
# User registry (create / edit / delete users) — DB-backed
# ---------------------------------------------------------------------------
# MANAGE_USERS (sys_admin only), like the company registry: whoever can mint users can
# mint a sys_admin, so this cannot sit below the top role. Every response is the
# hash-free user list.
#
# A sys_admin can hand out a password but never *knows* one. Two moments produce a
# password here — creating the account (the initial one, typed or generated) and
# resetting it (always generated) — and both mark the account `must_change_password`, so
# the password an administrator saw stops working the moment its owner signs in. Editing
# a user therefore has no password field at all: there is nothing an admin could set that
# would survive the user's first login, and a settable one would only invite the habit of
# choosing passwords for people.

_VALID_ROLES = permissions.VALID_ROLES


def _scope_conflict_error(role: str, company_codes: list[str],
                          exclude_username: str | None = None):
    """400 when this assignment would take a company off another operator, else None."""
    clashes = permissions.scope_conflicts(
        role, company_codes, auth.list_users(), exclude_username=exclude_username)
    if not clashes:
        return None
    detail = ", ".join(f"{code} (held by {who})" for code, who in sorted(clashes.items()))
    return JSONResponse(
        {"error": f"Each company is closed by exactly one operator — already assigned: {detail}"},
        status_code=400)


@app.get("/api/registry/users")
async def users_list(request: Request):
    _, err = _require(request, permissions.MANAGE_USERS)
    if err:
        return err
    return JSONResponse(await run_in_threadpool(auth.list_users))


@app.get("/api/registry/company-scope")
async def users_company_scope(request: Request, role: str = Query(...),
                              username: str | None = Query(None)):
    """Which companies the scope picker may offer for a user in `role`.

    `all` is the registry; `free` drops the companies another operator already owns, so
    the picker cannot build a clash the server would then refuse. `username` is the user
    being edited — their own codes stay free, since keeping them is not a clash."""
    _, err = _require(request, permissions.MANAGE_USERS)
    if err:
        return err
    registry = await run_in_threadpool(get_config_store().list_registry)
    all_codes = [c["code"] for c in registry]
    users = await run_in_threadpool(auth.list_users)
    return JSONResponse({
        "all": all_codes,
        "free": permissions.free_codes(role, users, all_codes, exclude_username=username),
        "exclusive": role in permissions.EXCLUSIVE_SCOPE_ROLES,
        "global_scope": role in permissions.GLOBAL_SCOPE_ROLES,
    })


@app.post("/api/registry/users")
async def users_create(request: Request):
    session, err = _require(request, permissions.MANAGE_USERS)
    if err:
        return err
    body = await request.json()
    username = (body.get("username") or "").strip()
    password = body.get("password") or ""
    display_name = (body.get("display_name") or username).strip()
    email = (body.get("email") or "").strip()
    role = (body.get("role") or permissions.DEFAULT_ROLE).strip()
    company_codes = body.get("company_codes") or []
    if not username:
        return JSONResponse({"error": "username is required"}, status_code=400)
    if role not in _VALID_ROLES:
        return JSONResponse({"error": f"role must be one of {sorted(_VALID_ROLES)}"},
                            status_code=400)
    # Either the admin types an initial password, or (with an address on file) we
    # generate one and mail it — but an account is never created without one.
    generated = False
    if not password:
        if not email:
            return JSONResponse(
                {"error": "Set an initial password, or give an e-mail address to send "
                          "a generated one to"}, status_code=400)
        password, generated = auth.generate_password(), True
    err_resp = await run_in_threadpool(_scope_conflict_error, role, company_codes)
    if err_resp:
        return err_resp
    try:
        await run_in_threadpool(
            lambda: get_config_store().create_user(
                username, auth.hash_password(password), display_name, role,
                company_codes, session["username"], email=email,
                must_change_password=True)
        )
    except ValueError as exc:
        return JSONResponse({"error": str(exc)}, status_code=400)
    logger.info("User %s created by %s", username, session["username"])
    delivery = await _deliver_password(username, email, password, reset=False,
                                       generated=generated)
    return JSONResponse({"users": await run_in_threadpool(auth.list_users), **delivery})


@app.put("/api/registry/users/{username}")
async def users_update(username: str, request: Request):
    session, err = _require(request, permissions.MANAGE_USERS)
    if err:
        return err
    body = await request.json()
    kwargs: dict = {}
    if "display_name" in body:
        kwargs["display_name"] = (body.get("display_name") or username).strip()
    if "email" in body:
        kwargs["email"] = (body.get("email") or "").strip()
    if "role" in body:
        role = (body.get("role") or "").strip()
        if role not in _VALID_ROLES:
            return JSONResponse({"error": f"role must be one of {sorted(_VALID_ROLES)}"},
                                status_code=400)
        kwargs["role"] = role
    if "company_codes" in body:
        kwargs["company_codes"] = body.get("company_codes") or []
    # Resolve the pair the exclusivity rule needs against what the user will *become*:
    # either field may be absent from a partial edit.
    if "role" in kwargs or "company_codes" in kwargs:
        existing = await run_in_threadpool(auth.get_user, username)
        if existing is None:
            return JSONResponse({"error": f"User '{username}' not found"}, status_code=400)
        err_resp = await run_in_threadpool(
            _scope_conflict_error,
            kwargs.get("role", existing.get("role")),
            kwargs.get("company_codes", existing.get("company_codes") or []),
            username)
        if err_resp:
            return err_resp
    try:
        await run_in_threadpool(
            lambda: get_config_store().update_user(
                username, user=session["username"], **kwargs)
        )
    except ValueError as exc:
        return JSONResponse({"error": str(exc)}, status_code=400)
    logger.info("User %s updated by %s", username, session["username"])
    return JSONResponse(await run_in_threadpool(auth.list_users))


async def _deliver_password(username: str, email: str, password: str, *,
                            reset: bool, generated: bool = True) -> dict:
    """Mail a one-time password to its owner; report what actually happened.

    The plaintext comes back to the caller only when it could *not* be delivered — an
    unconfigured mail server or a user with no address on file. That is a deliberate
    fallback, not a convenience: without it a deployment with no SMTP has no way to
    onboard anyone. When the mail goes out, nobody sees the password but its owner."""
    if not generated:
        return {"emailed": False, "password": None,
                "note": f"Give {username} their initial password. They will be asked to "
                        f"change it at first sign-in."}
    if not email:
        return {"emailed": False, "password": password,
                "note": "No e-mail address on file — hand this one-time password over "
                        "directly. It must be changed at the next sign-in."}
    if not mailer.is_configured():
        return {"emailed": False, "password": password,
                "note": "E-mail is not configured on this deployment (SMTP_HOST) — hand "
                        "this one-time password over directly."}
    try:
        await run_in_threadpool(mailer.send_password_email, email, username, password,
                                reset=reset)
    except mailer.MailError as exc:
        return {"emailed": False, "password": password,
                "note": f"{exc}. Hand this one-time password over directly."}
    return {"emailed": True, "password": None,
            "note": f"A one-time password was sent to {email}."}


@app.post("/api/registry/users/{username}/reset-password")
async def users_reset_password(username: str, request: Request):
    """Generate a new one-time password for someone else's account and mail it to them.

    Rare in practice — users reset their own from the login screen — but it is the way
    back in for an account whose address is wrong or whose owner cannot reach mail."""
    session, err = _require(request, permissions.MANAGE_USERS)
    if err:
        return err
    user = await run_in_threadpool(auth.get_user, username)
    if user is None:
        return JSONResponse({"error": f"User '{username}' not found"}, status_code=400)
    password = auth.generate_password()
    email = (user.get("email") or "").strip()
    delivery = await _deliver_password(username, email, password, reset=True)
    await run_in_threadpool(
        lambda: get_config_store().update_user(
            username, password_hash=auth.hash_password(password),
            must_change_password=True, user=session["username"])
    )
    logger.info("Password reset for %s by %s (emailed=%s)",
                username, session["username"], delivery["emailed"])
    return JSONResponse({"users": await run_in_threadpool(auth.list_users), **delivery})


@app.delete("/api/registry/users/{username}")
async def users_delete(username: str, request: Request):
    session, err = _require(request, permissions.MANAGE_USERS)
    if err:
        return err
    try:
        await run_in_threadpool(get_config_store().delete_user, username)
    except ValueError as exc:
        return JSONResponse({"error": str(exc)}, status_code=400)
    logger.info("User %s deleted by %s", username, session["username"])
    return JSONResponse(await run_in_threadpool(auth.list_users))


# ---------------------------------------------------------------------------
# WebSocket: /ws/dashboard  (aggregated status for all authorized companies)
# ---------------------------------------------------------------------------

@app.websocket("/ws/dashboard")
async def ws_dashboard(ws: WebSocket):
    await ws.accept()

    session = _get_session_ws(ws.cookies)
    if not session:
        await ws.close(code=4001, reason="Unauthenticated")
        return

    role = session.get("role")
    if session.get("must_change_password"):
        await ws.close(code=4003, reason="Password change required")
        return
    if not permissions.has_permission(role, permissions.VIEW):
        await ws.close(code=4003, reason="Not authorized")
        return

    authorized_codes: set[str] = set(
        _scoped_codes(session, [c["code"] for c in load_companies()]))
    # Read-only roles (manager) get the full event stream and no control over it. This
    # is the enforcement point — the UI also hides the buttons, but that is cosmetic.
    may_run = permissions.has_permission(role, permissions.RUN)
    mgr = get_run_manager()

    # Subscribe to all authorized companies
    subs: dict[str, asyncio.Queue] = {
        code: mgr.subscribe(code) for code in authorized_codes
    }

    async def receive_loop():
        try:
            while True:
                data = await ws.receive_json()
                msg_type = data.get("type")
                company_code = data.get("company")

                if msg_type == "start" and not may_run:
                    await ws.send_json({
                        "type": "error", "company": company_code,
                        "message": "Your role is read-only — runs are started by an operator",
                    })
                elif msg_type == "start" and company_code in authorized_codes:
                    company = get_company(company_code)
                    if not company:
                        await ws.send_json({"type": "error", "message": f"Unknown company {company_code}"})
                        continue
                    ok = await mgr.start(
                        company_code, company["config_file"],
                        period=data.get("period"),
                        fiscal_year=data.get("fiscal_year"),
                    )
                    if not ok:
                        await ws.send_json({
                            "type": "error",
                            "company": company_code,
                            "message": "Already running",
                        })

        except (WebSocketDisconnect, RuntimeError):
            pass

    async def relay_loop():
        """Fan events from all subscribed companies to the dashboard WS."""
        # Build a combined queue by merging all company queues
        merged: asyncio.Queue = asyncio.Queue()

        async def _pump(code: str, q: asyncio.Queue):
            while True:
                event = await q.get()
                # Do NOT stop on _sentinel: the subscriber queue is reused
                # across runs (rollback+restart, or a fresh run in test mode).
                # Mirror the company WS, which keeps its relay alive across
                # sentinels — otherwise the dashboard freezes a company's card
                # after its first run ends and only F5 refreshes it.
                if event.get("type") == "_sentinel":
                    continue
                tagged = dict(event, company=code)
                await merged.put(tagged)

        pumps = [asyncio.create_task(_pump(code, q)) for code, q in subs.items()]
        try:
            while True:
                event = await merged.get()
                if event.get("type") == "_sentinel":
                    continue
                try:
                    await ws.send_json(event)
                except Exception:
                    break
        finally:
            for t in pumps:
                t.cancel()

    recv_task  = asyncio.create_task(receive_loop())
    relay_task = asyncio.create_task(relay_loop())
    try:
        await asyncio.gather(recv_task, relay_task)
    except Exception:
        pass
    finally:
        recv_task.cancel()
        relay_task.cancel()
        for code, q in subs.items():
            mgr.unsubscribe(code, q)
        try:
            await ws.close()
        except Exception:
            pass
        logger.info("Dashboard WS disconnected (%s)", session.get("username"))


# ---------------------------------------------------------------------------
# WebSocket: /ws?company=RU06  (full event stream for one company)
# ---------------------------------------------------------------------------

@app.websocket("/ws")
async def ws_company(ws: WebSocket, company: str = Query(...)):
    await ws.accept()

    session = _get_session_ws(ws.cookies)
    if not session:
        await ws.close(code=4001, reason="Unauthenticated")
        return

    role = session.get("role")
    if session.get("must_change_password"):
        await ws.close(code=4003, reason="Password change required")
        return
    if (not permissions.has_permission(role, permissions.VIEW)
            or not permissions.in_scope(role, session.get("company_codes", []), company)):
        await ws.close(code=4003, reason="Not authorized for this company")
        return

    # Read-only roles (manager) may watch every event but drive nothing: start,
    # rollback_and_restart and decision are all refused below. Server-side, because
    # hiding the buttons in the UI is not access control.
    may_run = permissions.has_permission(role, permissions.RUN)
    mgr = get_run_manager()
    sub_q = mgr.subscribe(company)

    async def send(event: dict) -> None:
        try:
            await ws.send_json(event)
        except Exception:
            pass

    async def receive_loop():
        try:
            while True:
                data = await ws.receive_json()
                msg_type = data.get("type")

                if (msg_type in ("start", "rollback_and_restart", "decision")
                        and not may_run):
                    await send({"type": "error",
                                "message": "Your role is read-only — "
                                           "this run is driven by an operator"})
                elif msg_type == "start":
                    company_info = get_company(company)
                    if company_info:
                        ok = await mgr.start(
                            company, company_info["config_file"],
                            period=data.get("period"),
                            fiscal_year=data.get("fiscal_year"),
                        )
                        if not ok:
                            await send({"type": "error", "message": "Already running"})
                elif msg_type == "rollback_and_restart":
                    start_from = data.get("start_from_step", "")
                    if not start_from:
                        await send({"type": "error", "message": "No start_from_step specified"})
                    else:
                        company_info = get_company(company)
                        if company_info:
                            ok = await mgr.rollback_start(
                                company, company_info["config_file"], start_from
                            )
                            if not ok:
                                await send({"type": "error",
                                            "message": "Failed to start rollback"})
                elif msg_type == "decision":
                    await mgr.forward(company, data)
                elif msg_type == "disconnect":
                    break

        except (WebSocketDisconnect, RuntimeError):
            pass

    async def event_relay():
        """Forward events from the company's fan-out queue to this WS."""
        try:
            while True:
                event = await sub_q.get()
                if event.get("type") == "_sentinel":
                    # Keep the WebSocket open: a rollback+restart will start a new
                    # fan-out task that continues writing to this same subscriber queue.
                    continue
                await send(event)
        except Exception:
            pass

    # Emit current status + replay completed events for late joiners
    status = mgr.get_status(company)
    await send({"type": "company_status", "company": company, **status})
    for event in mgr.get_catchup_events(company):
        await send(event)

    recv_task  = asyncio.create_task(receive_loop())
    relay_task = asyncio.create_task(event_relay())
    try:
        await asyncio.gather(recv_task, relay_task)
    except Exception:
        pass
    finally:
        recv_task.cancel()
        relay_task.cancel()
        mgr.unsubscribe(company, sub_q)
        try:
            await ws.close()
        except Exception:
            pass
        logger.info("Company WS disconnected: %s (%s)", company, session.get("username"))
