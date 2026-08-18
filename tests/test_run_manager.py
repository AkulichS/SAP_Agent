"""RunManager.get_catchup_events — late-join replay reconstruction.

Built directly from a populated CompanyRun (no graph), so these are fast and
deterministic. Async only so CompanyRun's asyncio.Queue fields construct under a
running loop.
"""


from run_manager import CompanyRun, RunManager


def _mgr_with(run: CompanyRun) -> RunManager:
    mgr = RunManager()
    mgr._runs[run.company_code] = run
    return mgr


async def test_catchup_unknown_company_is_empty():
    assert RunManager().get_catchup_events("NOPE") == []


async def test_catchup_crash_before_run_init_replays_only_error():
    run = CompanyRun("RU06")
    run.run_init_snapshot = None
    run.last_run_end = {"type": "run_end", "status": "error", "message": "bad config"}
    events = _mgr_with(run).get_catchup_events("RU06")
    assert events == [run.last_run_end]


async def test_catchup_completed_run_replays_steps_and_run_end():
    run = CompanyRun("RU06")
    run.run_init_snapshot = {"type": "run_init",
                             "steps": [{"step_id": "A"}, {"step_id": "B"}]}
    run.step_results = {"A": "ok", "B": "failed"}
    run.step_action_log = {"A": [{"type": "action_start", "action": "execute",
                                  "step_id": "A"}]}
    run.status = "completed"
    run.last_run_end = {"type": "run_end", "status": "completed"}

    events = _mgr_with(run).get_catchup_events("RU06")
    types = [e["type"] for e in events]

    assert types[0] == "run_init"
    assert types[-1] == "run_end"
    assert types.count("step_start") == 2
    assert types.count("step_end") == 2
    assert any(e.get("action") == "execute" for e in events)   # action replayed
    b_end = next(e for e in events if e["type"] == "step_end" and e["step_id"] == "B")
    assert b_end["status"] == "failed"


async def test_catchup_interrupted_run_replays_interrupt_last():
    run = CompanyRun("RU06")
    run.run_init_snapshot = {"type": "run_init",
                             "steps": [{"step_id": "A"}, {"step_id": "B"}]}
    run.step_results = {"A": "ok"}
    run.step_action_log = {"B": [{"type": "action_start", "action": "analysis",
                                  "step_id": "B"}]}
    run.current_step = "B"
    run.status = "interrupted"
    run.interrupt_event = {"type": "interrupt", "step_id": "B",
                           "diagnosis": "d", "user_instructions": "u"}

    events = _mgr_with(run).get_catchup_events("RU06")
    types = [e["type"] for e in events]

    assert types[0] == "run_init"
    assert types[-1] == "interrupt"          # modal re-appears for late joiner
    assert "run_end" not in types            # still paused
    assert any(e["type"] == "step_end" and e["step_id"] == "A" for e in events)
