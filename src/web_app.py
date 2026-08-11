"""
web_app.py — FastAPI server for SAP Period Close web UI.

Endpoints
---------
  GET  /               → index.html (login overlay shown by JS if unauthenticated)
  POST /api/login      → validate credentials, set session cookie
  GET  /api/me         → current user info (401 if not logged in)
  POST /api/logout     → clear session cookie
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


def _companies_for_user(session: dict) -> list[dict]:
    codes = session.get("company_codes", [])
    all_companies = load_companies()
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
    companies = _companies_for_user(
        {"company_codes": user.get("company_codes", [])}
    )

    response = JSONResponse({
        "username": user["username"],
        "display_name": user.get("display_name", username),
        "role": user.get("role", "operator"),
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
    return JSONResponse({
        "username": session["username"],
        "display_name": session.get("display_name", session["username"]),
        "role": session.get("role", "operator"),
        "companies": companies,
    })


@app.post("/api/logout")
async def logout(response: Response):
    response = JSONResponse({"ok": True})
    response.delete_cookie(auth.COOKIE_NAME, path="/")
    return response


@app.get("/api/companies")
async def companies(request: Request):
    session = _get_session(request)
    if not session:
        return JSONResponse({"error": "Not authenticated"}, status_code=401)
    return JSONResponse(_companies_for_user(session))


# ---------------------------------------------------------------------------
# Admin Settings REST endpoints (per-company config deltas)
# ---------------------------------------------------------------------------

def _require_admin(request: Request, code: str | None = None):
    """Return (session, None) when the caller is an admin authorized for `code`,
    else (None, JSONResponse) with the right status.

    An admin may configure any company in the registry — registry management is already
    role-only, so a company an admin created must also be one they can set up. Companies
    listed in their users.yaml `company_codes` stay reachable too (that list still gates
    running/monitoring, which is a separate, operator-level concern)."""
    session = _get_session(request)
    if not session:
        return None, JSONResponse({"error": "Not authenticated"}, status_code=401)
    if session.get("role") != "admin":
        return None, JSONResponse({"error": "Admin role required"}, status_code=403)
    if code is not None and code not in set(session.get("company_codes", [])):
        registered = {c["code"] for c in get_config_store().list_registry()}
        if code not in registered:
            return None, JSONResponse({"error": "Not authorized for this company"},
                                      status_code=403)
    return session, None


@app.get("/api/settings/schema")
async def settings_schema(request: Request):
    _, err = _require_admin(request)
    if err:
        return err
    return JSONResponse(step_json_schema())


@app.get("/api/settings/form-schema")
async def settings_form_schema(request: Request):
    """UI form descriptor (sections, fields, conditional visibility, logical enums)."""
    _, err = _require_admin(request)
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

@app.get("/api/settings/base")
async def get_base_settings(request: Request):
    _, err = _require_admin(request)
    if err:
        return err
    data = await run_in_threadpool(config_settings.build_base_settings)
    return JSONResponse(data)


@app.put("/api/settings/base")
async def put_base_settings(request: Request):
    session, err = _require_admin(request)
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
    _, err = _require_admin(request)
    if err:
        return err
    rows = await run_in_threadpool(get_config_store().base_history)
    return JSONResponse(rows)


@app.delete("/api/settings/base/history/oldest")
async def delete_base_history_oldest(request: Request):
    session, err = _require_admin(request)
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
    _, err = _require_admin(request)
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
    session, err = _require_admin(request)
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
    _, err = _require_admin(request, code)
    if err:
        return err
    data = await run_in_threadpool(config_settings.build_settings, code)
    return JSONResponse(data)


@app.put("/api/settings/{code}")
async def put_settings(code: str, request: Request):
    session, err = _require_admin(request, code)
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
    session, err = _require_admin(request, code)
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
    session, err = _require_admin(request, code)
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
    _, err = _require_admin(request, code)
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
    session, err = _require_admin(request, code)
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
    session, err = _require_admin(request, code)
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
# Admin company registry (create / rename / delete companies)
# ---------------------------------------------------------------------------
# Role-only admin check: registry management spans all companies. Editing a company's
# per-company delta still requires that code in the admin's users.yaml company_codes.

@app.get("/api/registry/companies")
async def registry_list(request: Request):
    _, err = _require_admin(request)
    if err:
        return err
    return JSONResponse(await run_in_threadpool(get_config_store().list_registry))


@app.post("/api/registry/companies")
async def registry_create(request: Request):
    session, err = _require_admin(request)
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
    _, err = _require_admin(request)
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
    _, err = _require_admin(request)
    if err:
        return err
    await run_in_threadpool(get_config_store().delete_company, code)
    logger.info("Company %s deleted", code)
    return JSONResponse(await run_in_threadpool(get_config_store().list_registry))


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

    authorized_codes: set[str] = set(session.get("company_codes", []))
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

                if msg_type == "start" and company_code in authorized_codes:
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

    authorized_codes: set[str] = set(session.get("company_codes", []))
    if company not in authorized_codes:
        await ws.close(code=4003, reason="Not authorized for this company")
        return

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

                if msg_type == "start":
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
