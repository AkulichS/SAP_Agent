"""End-to-end graph runs over small inline step lists.

Covers the key application scenarios: happy path, validation-failure → analysis →
retry → success, retry-exhausted → escalation interrupt, pre_check skip, and a
parallel group. Stub-backed runs exercise graph → mcp_server → SAP stub together;
canned-session runs drive the failure/retry branches deterministically.
"""

import pytest
from langgraph.checkpoint.memory import InMemorySaver
from langgraph.types import Command

from graph_builder import _build_initial_state, build_graph


def _initial(steps, defaults=None):
    cfg = {"company_config": {"fiscal_year": "2025", "period": "11"},
           "defaults": defaults or {}, "steps": steps}
    return _build_initial_state(cfg, steps)


def _run_cfg(thread_id):
    return {"configurable": {"thread_id": thread_id}}


_SPOOL_VALIDATE = {"mode": "keyword", "keyword": {
    "source": "spool", "ok_patterns": ["settled"], "error_patterns": ["FATAL"]}}


# ---------------------------------------------------------------------------
# 1. Happy path — pre_check (none) → execute → validate → finalize → END
# ---------------------------------------------------------------------------

async def test_happy_path(stub_session, make_llms):
    steps = [{"step_id": "S1", "action_type": "SUBMIT", "object_name": "RKO7KO8G",
              "async": False, "validate": _SPOOL_VALIDATE}]
    graph = build_graph(stub_session, make_llms(), InMemorySaver())
    final = await graph.ainvoke(_initial(steps), _run_cfg("t-happy"))
    # A sequential last step finalizes straight to END (bypassing router), so the
    # `completed` flag isn't set on this path — step_records is the finish signal.
    assert len(final["step_records"]) == 1
    assert final["step_records"][0]["final_status"] == "ok"


# ---------------------------------------------------------------------------
# 2. Validation failure → analysis → retry → success
# ---------------------------------------------------------------------------

async def test_validation_failure_then_retry_succeeds(make_session, make_llms, patch_react_agent):
    patch_react_agent('{"action":"retry","corrected_params":null,'
                      '"diagnosis":"transient","user_instructions":null}')
    session = make_session(sap_execute_step=[
        {"status": "ok", "requires_poll": False, "job_name": "J", "job_id": "1",
         "messages": [], "result_json": {"spool": ["bad error here"]}},
        {"status": "ok", "requires_poll": False, "job_name": "J", "job_id": "1",
         "messages": [], "result_json": {"spool": ["all done clean"]}},
    ])
    steps = [{"step_id": "S1", "action_type": "SUBMIT", "object_name": "RKO", "async": False,
              "validate": {"mode": "keyword", "keyword": {
                  "source": "spool", "ok_patterns": ["done"], "error_patterns": ["error"]}}}]
    graph = build_graph(session, make_llms(), InMemorySaver())
    final = await graph.ainvoke(_initial(steps), _run_cfg("t-retry"))

    assert len(final["step_records"]) == 1
    assert final["step_records"][0]["final_status"] == "ok"
    # Step executed twice: original attempt + one retry.
    assert sum(1 for c in session.calls if c[0] == "sap_execute_step") == 2


# ---------------------------------------------------------------------------
# 3. Retry budget exhausted → escalation → user interrupt → resume abort
# ---------------------------------------------------------------------------

async def test_escalation_interrupt_then_abort(make_session, make_llms, patch_react_agent):
    patch_react_agent('{"action":"retry","corrected_params":null,'
                      '"diagnosis":"stuck","user_instructions":"fix manually"}')
    session = make_session(sap_execute_step=lambda _a: {
        "status": "ok", "requires_poll": False, "job_name": "J", "job_id": "1",
        "messages": [], "result_json": {"spool": ["bad error here"]}})
    steps = [{"step_id": "S1", "action_type": "SUBMIT", "object_name": "RKO", "async": False,
              "max_retries": 1,
              "validate": {"mode": "keyword", "keyword": {
                  "source": "spool", "ok_patterns": ["NEVER"], "error_patterns": ["error"]}}}]
    graph = build_graph(session, make_llms(), InMemorySaver())
    cfg = _run_cfg("t-escalate")

    paused = await graph.ainvoke(_initial(steps), cfg)
    assert "__interrupt__" in paused          # graph suspended at user_node

    final = await graph.ainvoke(Command(resume={"action": "abort"}), cfg)
    assert final.get("aborted") is True


# ---------------------------------------------------------------------------
# 4. pre_check decides to skip the step
# ---------------------------------------------------------------------------

async def test_precheck_skips_step(stub_session, make_llms):
    steps = [{"step_id": "S1", "action_type": "SUBMIT", "object_name": "RKO7KO8G",
              "async": False,
              "pre_check": {"enabled": True, "mode": "comparison", "action_type": "TOOLS",
                            "object_name": "COKP", "params": {"table": "COKP"},
                            "comparison": {"select": "non_empty", "on_pass": "skip"}},
              "validate": _SPOOL_VALIDATE}]
    graph = build_graph(stub_session, make_llms(), InMemorySaver())
    final = await graph.ainvoke(_initial(steps), _run_cfg("t-skip"))

    rec = final["step_records"][0]
    assert rec["final_status"] == "skipped"
    assert rec["execute"] is None             # step body never ran


# ---------------------------------------------------------------------------
# 5. Parallel group — router → Send×N → parallel_step_runner → fan_in
# ---------------------------------------------------------------------------

async def test_parallel_group(stub_session, make_llms):
    val = {"mode": "keyword", "keyword": {
        "source": "spool",
        "ok_patterns": ["settled", "processed", "successfully"],
        "error_patterns": ["FATAL"]}}
    steps = [
        {"step_id": "A", "group": "G", "action_type": "SUBMIT",
         "object_name": "RKO7KO8G", "async": False, "validate": val},
        {"step_id": "B", "group": "G", "action_type": "SUBMIT",
         "object_name": "RKABL000", "async": False, "validate": val},
    ]
    graph = build_graph(stub_session, make_llms(), InMemorySaver())
    final = await graph.ainvoke(_initial(steps), _run_cfg("t-parallel"))

    assert final["completed"] is True
    statuses = {r["step_id"]: r["final_status"] for r in final["step_records"]}
    assert statuses == {"A": "ok", "B": "ok"}
