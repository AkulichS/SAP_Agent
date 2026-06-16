"""
run_manager.py — concurrent company run management for SAP Period Close.

One CompanyRun per company code; multiple WS subscribers receive the same event stream
via fan-out. RunManager is a process-level singleton.

Design:
  CompanyRun.msg_queue   — messages FROM browser → graph (start, decision)
  CompanyRun.event_queue — raw events FROM graph → fan-out task
  CompanyRun.subscribers — per-WS queues; fan-out task copies every event into each

Usage (from web_app.py):
    mgr = get_run_manager()
    ok  = await mgr.start(company_code, config_path)   # False if already running
    sub = mgr.subscribe(company_code)                  # AsyncQueue; caller reads events
    mgr.forward(company_code, msg)                     # send browser msg to graph
    mgr.unsubscribe(company_code, sub)
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from pathlib import Path

import yaml

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# CompanyRun
# ---------------------------------------------------------------------------

@dataclass
class CompanyRun:
    company_code: str
    msg_queue: asyncio.Queue = field(default_factory=asyncio.Queue)
    event_queue: asyncio.Queue = field(default_factory=asyncio.Queue)
    subscribers: list[asyncio.Queue] = field(default_factory=list)
    status: str = "idle"              # idle|running|interrupted|completed|failed
    current_step: str | None = None
    step_progress: tuple[int, int] = (0, 0)
    task: asyncio.Task | None = None
    _fan_task: asyncio.Task | None = None
    period: str | None = None         # fiscal period chosen at start (e.g. "11")
    fiscal_year: str | None = None    # fiscal year chosen at start (e.g. "2025")
    # Late-join replay state
    run_init_snapshot: dict | None = None   # stored run_init event for replay
    step_results: dict = field(default_factory=dict)   # step_id → final status
    step_action_log: dict = field(default_factory=dict) # step_id → [action events]
    last_run_end: dict | None = None                    # stored run_end for error replay
    rollback_log: list = field(default_factory=list)   # rollback_start/step/end events
    interrupt_event: dict | None = None                 # stored interrupt event for replay
    # Pre-rollback history for rolled-back steps: {step_id: {result, actions}}
    pre_rollback_step_data: dict = field(default_factory=dict)


# ---------------------------------------------------------------------------
# RunManager
# ---------------------------------------------------------------------------

class RunManager:
    def __init__(self) -> None:
        self._runs: dict[str, CompanyRun] = {}
        self._lock = asyncio.Lock()

    # ------------------------------------------------------------------
    # Start a run
    # ------------------------------------------------------------------

    async def start(self, company_code: str, config_path: str,
                    period: str | None = None, fiscal_year: str | None = None) -> bool:
        """Start period-close for company_code. Returns False if already running."""
        async with self._lock:
            run = self._runs.setdefault(company_code, CompanyRun(company_code))
            if run.status == "running":
                return False

            # Reset per-run state
            run.msg_queue        = asyncio.Queue()
            run.event_queue      = asyncio.Queue()
            run.status           = "running"
            run.current_step     = None
            run.step_progress    = (0, 0)
            run.period           = period
            run.fiscal_year      = fiscal_year
            run.run_init_snapshot     = None
            run.step_results          = {}
            run.step_action_log       = {}
            run.last_run_end          = None
            run.rollback_log          = []
            run.interrupt_event       = None
            run.pre_rollback_step_data = {}

            # Seed the msg_queue with the start signal so the graph
            # does not wait for an additional browser message.
            await run.msg_queue.put({"type": "start"})

            # Fan-out task
            if run._fan_task and not run._fan_task.done():
                run._fan_task.cancel()
            run._fan_task = asyncio.create_task(
                self._fan_out(run), name=f"fanout-{company_code}"
            )

            # Graph task
            if run.task and not run.task.done():
                run.task.cancel()
            run.task = asyncio.create_task(
                self._run_graph(run, config_path), name=f"graph-{company_code}"
            )

            logger.info("RunManager: started run for %s (config: %s)", company_code, config_path)
            return True

    async def _run_graph(self, run: CompanyRun, config_path: str) -> None:
        import graph_builder

        async def send(event: dict) -> None:
            t = event.get("type")
            sid = event.get("step_id")
            if t == "run_init":
                run.run_init_snapshot = event
                run.step_results      = {}
                run.step_action_log   = {}
            elif t == "step_start":
                run.current_step  = event.get("step_id")
                run.step_progress = (event.get("step_index", 0), event.get("total", 0))
            elif t == "step_end":
                run.step_results[sid] = event.get("status", "ok")
            elif t == "action_start":
                if sid:
                    run.step_action_log.setdefault(sid, []).append(dict(event))
            elif t == "action_update":
                # Bake the latest poll message into the stored action_start so
                # replay shows the final message without needing action_update events.
                if sid and sid in run.step_action_log:
                    for e in reversed(run.step_action_log[sid]):
                        if e.get("type") == "action_start" and e.get("action") == "poll":
                            e["message"] = event.get("message", e.get("message"))
                            break
            elif t == "action_end":
                if sid:
                    run.step_action_log.setdefault(sid, []).append(dict(event))
            elif t == "interrupt":
                run.status = "interrupted"
                run.interrupt_event = dict(event)
            elif t == "run_end":
                s = event.get("status", "completed")
                run.status = "completed" if s == "completed" else "failed"
                run.current_step = None
                run.last_run_end = dict(event)
            await run.event_queue.put(event)

        try:
            await graph_builder.run_period_close_web(
                send=send,
                msg_queue=run.msg_queue,
                period_config_path=config_path,
                mcp_config_path="mcp_config.yaml",
                period=run.period,
                fiscal_year=run.fiscal_year,
            )
        except asyncio.CancelledError:
            run.status = "idle"
            raise
        except Exception as exc:
            logger.error("RunManager [%s]: %s", run.company_code, exc, exc_info=True)
            run.status = "failed"
            await run.event_queue.put(
                {"type": "run_end", "status": "error", "message": str(exc)}
            )
        finally:
            if run.status == "running":
                run.status = "completed"
            await run.event_queue.put({"type": "_sentinel"})

    # ------------------------------------------------------------------
    # Rollback + restart
    # ------------------------------------------------------------------

    async def rollback_start(self, company_code: str, config_path: str,
                              start_from_step: str) -> bool:
        """Cancel any running/interrupted run, roll back completed steps from
        start_from_step onwards, then restart the graph from that step."""
        async with self._lock:
            run = self._runs.setdefault(company_code, CompanyRun(company_code))

            # Compute rollback list from server-side step_results BEFORE reset
            try:
                from config import load_config as _load_config
                _cfg = _load_config(config_path, period=run.period, fiscal_year=run.fiscal_year)
                steps_order = [s["step_id"] for s in _cfg.get("steps", [])]
            except Exception as exc:
                logger.error("rollback_start: cannot load config: %s", exc)
                return False

            if start_from_step not in steps_order:
                logger.warning("rollback_start: step %r not found in config", start_from_step)
                return False

            target_idx       = steps_order.index(start_from_step)
            steps_to_rollback = [
                sid for sid in steps_order[target_idx:]
                if run.step_results.get(sid) in ("ok", "skipped")
            ]

            # Save completed steps before the rollback target so catchup replay
            # still shows them as done after the restart.
            pre_rollback_results = {
                sid: run.step_results[sid]
                for sid in steps_order[:target_idx]
                if sid in run.step_results
            }
            pre_rollback_action_log = {
                sid: list(run.step_action_log.get(sid, []))
                for sid in steps_order[:target_idx]
                if sid in run.step_results
            }
            # Save original action history for each rolled-back step so
            # get_catchup_events() can replay Execute/Poll/Validate above Rollback ✓.
            pre_rollback_step_data = {
                sid: {
                    "result": run.step_results.get(sid, "ok"),
                    "actions": list(run.step_action_log.get(sid, [])),
                }
                for sid in steps_to_rollback
                if run.step_results.get(sid) in ("ok", "skipped")
            }

            # Cancel current task (interrupted graph waits at user_node)
            for task in (run.task, run._fan_task):
                if task and not task.done():
                    task.cancel()
                    try:
                        await asyncio.wait_for(asyncio.shield(task), timeout=2.0)
                    except (asyncio.CancelledError, asyncio.TimeoutError, Exception):
                        pass

            # Reset per-run state
            run.msg_queue              = asyncio.Queue()
            run.event_queue            = asyncio.Queue()
            run.status                 = "running"
            run.current_step           = None
            run.step_progress          = (0, 0)
            run.run_init_snapshot      = None
            run.step_results           = {}
            run.step_action_log        = {}
            run.last_run_end           = None
            run.rollback_log           = []
            run.interrupt_event        = None
            run.pre_rollback_step_data = {}

            run._fan_task = asyncio.create_task(
                self._fan_out(run), name=f"fanout-{company_code}"
            )
            run.task = asyncio.create_task(
                self._run_rollback(run, config_path, start_from_step, steps_to_rollback,
                                   pre_rollback_results, pre_rollback_action_log,
                                   pre_rollback_step_data),
                name=f"rollback-{company_code}",
            )
            logger.info("RunManager: rollback %s from %s, rolling back: %s",
                        company_code, start_from_step, steps_to_rollback)
            return True

    async def _run_rollback(self, run: CompanyRun, config_path: str,
                             start_from_step: str, steps_to_rollback: list[str],
                             pre_rollback_results: dict | None = None,
                             pre_rollback_action_log: dict | None = None,
                             pre_rollback_step_data: dict | None = None) -> None:
        import graph_builder

        async def send(event: dict) -> None:
            t   = event.get("type")
            sid = event.get("step_id")
            if t == "run_init":
                run.run_init_snapshot = event
                # Seed with pre-rollback completions so get_catchup_events
                # replays steps before the rollback target as already done.
                run.step_results       = dict(pre_rollback_results or {})
                run.step_action_log    = {k: list(v)
                                          for k, v in (pre_rollback_action_log or {}).items()}
                run.rollback_log       = []
                run.pre_rollback_step_data = dict(pre_rollback_step_data or {})
            elif t in ("rollback_start", "rollback_step_start",
                       "rollback_step_end", "rollback_end"):
                run.rollback_log.append(dict(event))
            elif t == "step_start":
                run.current_step  = sid
                run.step_progress = (event.get("step_index", 0), event.get("total", 0))
            elif t == "step_end":
                run.step_results[sid] = event.get("status", "ok")
            elif t == "action_start":
                if sid:
                    run.step_action_log.setdefault(sid, []).append(dict(event))
            elif t == "action_update":
                if sid and sid in run.step_action_log:
                    for e in reversed(run.step_action_log[sid]):
                        if e.get("type") == "action_start" and e.get("action") == "poll":
                            e["message"] = event.get("message", e.get("message"))
                            break
            elif t == "action_end":
                if sid:
                    run.step_action_log.setdefault(sid, []).append(dict(event))
            elif t == "interrupt":
                run.status = "interrupted"
                run.interrupt_event = dict(event)
            elif t == "run_end":
                s = event.get("status", "completed")
                run.status       = "completed" if s == "completed" else "failed"
                run.current_step = None
                run.last_run_end = dict(event)
            await run.event_queue.put(event)

        try:
            await graph_builder.run_rollback_and_restart_web(
                send=send,
                msg_queue=run.msg_queue,
                period_config_path=config_path,
                mcp_config_path="mcp_config.yaml",
                start_from_step=start_from_step,
                steps_to_rollback=steps_to_rollback,
                period=run.period,
                fiscal_year=run.fiscal_year,
            )
        except asyncio.CancelledError:
            run.status = "idle"
            raise
        except Exception as exc:
            logger.error("RunManager rollback [%s]: %s", run.company_code, exc, exc_info=True)
            run.status = "failed"
            await run.event_queue.put(
                {"type": "run_end", "status": "error", "message": str(exc)}
            )
        finally:
            if run.status == "running":
                run.status = "completed"
            await run.event_queue.put({"type": "_sentinel"})

    async def _fan_out(self, run: CompanyRun) -> None:
        while True:
            event = await run.event_queue.get()
            is_sentinel = event.get("type") == "_sentinel"
            for q in list(run.subscribers):
                await q.put(dict(event))
            if is_sentinel:
                break

    # ------------------------------------------------------------------
    # Subscribe / unsubscribe
    # ------------------------------------------------------------------

    def subscribe(self, company_code: str) -> asyncio.Queue:
        run = self._runs.setdefault(company_code, CompanyRun(company_code))
        q: asyncio.Queue = asyncio.Queue()
        run.subscribers.append(q)
        return q

    def unsubscribe(self, company_code: str, q: asyncio.Queue) -> None:
        run = self._runs.get(company_code)
        if run and q in run.subscribers:
            run.subscribers.remove(q)

    # ------------------------------------------------------------------
    # Forward browser message to the running graph
    # ------------------------------------------------------------------

    async def forward(self, company_code: str, msg: dict) -> None:
        run = self._runs.get(company_code)
        if run:
            await run.msg_queue.put(msg)

    # ------------------------------------------------------------------
    # Status queries (for dashboard)
    # ------------------------------------------------------------------

    def get_status(self, company_code: str) -> dict:
        run = self._runs.get(company_code)
        if run is None:
            return {"status": "idle", "current_step": None, "step_progress": [0, 0]}
        return {
            "status": run.status,
            "current_step": run.current_step,
            "step_progress": list(run.step_progress),
        }

    def get_all_statuses(self) -> dict[str, dict]:
        return {code: self.get_status(code) for code in self._runs}

    def get_catchup_events(self, company_code: str) -> list[dict]:
        """Return replay events for a subscriber that joined after the run started."""
        run = self._runs.get(company_code)
        if run is None:
            return []

        # Run crashed before run_init (e.g., bad config) — replay only the error
        if run.run_init_snapshot is None:
            return [run.last_run_end] if run.last_run_end else []

        # Split rollback_log into global (start/end) and per-step events so we
        # can interleave the per-step ones with pre-rollback action history.
        rollback_by_step: dict[str, list[dict]] = {}
        rb_start_event: dict | None = None
        rb_end_event:   dict | None = None
        for ev in run.rollback_log:
            t   = ev.get("type")
            sid = ev.get("step_id")
            if t == "rollback_start":
                rb_start_event = ev
            elif t == "rollback_end":
                rb_end_event = ev
            elif sid and t in ("rollback_step_start", "rollback_step_end"):
                rollback_by_step.setdefault(sid, []).append(ev)

        pre_rb = run.pre_rollback_step_data  # {step_id: {result, actions}}

        events: list[dict] = [run.run_init_snapshot]

        if rb_start_event:
            events.append(rb_start_event)

        steps = run.run_init_snapshot.get("steps", [])
        total = len(steps)
        done  = False

        for i, step in enumerate(steps):
            if done:
                break
            sid = step["step_id"]
            has_pre_rb = sid in pre_rb and sid in rollback_by_step

            if has_pre_rb:
                # 1) Original Execute/Poll/Validate before the rollback
                pre = pre_rb[sid]
                events.append({"type": "step_start", "step_id": sid,
                                "step_index": i, "total": total})
                events.extend(pre.get("actions", []))
                events.append({"type": "step_end", "step_id": sid,
                                "status": pre.get("result", "ok")})
                # 2) Rollback ✓ entry for this step
                events.extend(rollback_by_step[sid])
                # 3) New run phase (↺ Restart divider + new actions added by onStepStart)
                if sid in run.step_results:
                    events.append({"type": "step_start", "step_id": sid,
                                    "step_index": i, "total": total})
                    events.extend(run.step_action_log.get(sid, []))
                    events.append({"type": "step_end", "step_id": sid,
                                   "status": run.step_results[sid]})
                elif sid == run.current_step:
                    events.append({"type": "step_start", "step_id": sid,
                                    "step_index": i, "total": total})
                    events.extend(run.step_action_log.get(sid, []))
                    done = True
                # else: new run hasn't reached this step yet — leave as pending
            elif sid in run.step_results:
                # Normal completed step
                events.append({"type": "step_start", "step_id": sid,
                                "step_index": i, "total": total})
                events.extend(run.step_action_log.get(sid, []))
                events.append({"type": "step_end", "step_id": sid,
                                "status": run.step_results[sid]})
            elif sid == run.current_step:
                # Currently running step
                events.append({"type": "step_start", "step_id": sid,
                                "step_index": i, "total": total})
                events.extend(run.step_action_log.get(sid, []))
                done = True

        if rb_end_event:
            events.append(rb_end_event)

        # If paused at operator interrupt, replay the interrupt event so the modal appears
        if run.status == "interrupted" and run.interrupt_event:
            events.append(run.interrupt_event)

        # If run finished, replay run_end with original status/message
        if run.status in ("completed", "failed") and run.last_run_end:
            events.append(run.last_run_end)

        return events


# ---------------------------------------------------------------------------
# Module-level singleton
# ---------------------------------------------------------------------------

_manager: RunManager | None = None


def get_run_manager() -> RunManager:
    global _manager
    if _manager is None:
        _manager = RunManager()
    return _manager


# ---------------------------------------------------------------------------
# Company registry helper
# ---------------------------------------------------------------------------

_COMPANIES_PATH = Path(__file__).parent / "companies.yaml"
_companies_cache: list[dict] | None = None


def load_companies(force_reload: bool = False) -> list[dict]:
    global _companies_cache
    if _companies_cache is None or force_reload:
        with open(_COMPANIES_PATH, encoding="utf-8") as f:
            data = yaml.safe_load(f)
        _companies_cache = data.get("companies", [])
    return _companies_cache


def get_company(code: str) -> dict | None:
    return next((c for c in load_companies() if c["code"] == code), None)
