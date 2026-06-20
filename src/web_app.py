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

from dotenv import load_dotenv
from fastapi import FastAPI, Query, Request, Response, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

load_dotenv(Path(__file__).parent / ".env", override=True)

import auth
from config import load_config, reset_each_run_enabled
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
                cfg = load_config(c["config_file"])
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
                tagged = dict(event, company=code)
                await merged.put(tagged)
                if event.get("type") == "_sentinel":
                    break

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
