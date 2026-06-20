"""Routing / conditional-edge functions — pure decision logic.

These pin every branch of the step lifecycle and are the highest-ROI guard
against accidental flow changes.
"""

import pytest
from langgraph.graph import END
from langgraph.types import Send

from graph_builder import (route_after_analysis, route_after_execute,
                           route_after_fanin, route_after_finalize,
                           route_after_poll, route_after_precheck,
                           route_after_router, route_after_user,
                           route_after_validate)


# ---------------------------------------------------------------------------
# route_after_router
# ---------------------------------------------------------------------------

def test_router_completed_goes_to_end():
    assert route_after_router({"completed": True}) == END


def test_router_sequential_goes_to_precheck():
    state = {"completed": False, "current_group": None,
             "steps": [{"step_id": "A"}]}
    assert route_after_router(state) == "pre_check"


def test_router_group_fans_out_with_send():
    steps = [{"step_id": "A", "group": "G"}, {"step_id": "B", "group": "G"},
             {"step_id": "C", "group": None}]
    state = {"completed": False, "current_group": "G", "steps": steps}
    result = route_after_router(state)
    assert isinstance(result, list) and len(result) == 2
    assert all(isinstance(s, Send) for s in result)
    assert all(s.node == "parallel_step_runner" for s in result)


# ---------------------------------------------------------------------------
# route_after_precheck
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("pc,expected", [
    ({"skip_step": True}, "finalize_step"),
    ({"skip_step": False, "error": "boom"}, "analysis"),
    ({"skip_step": False, "error": None}, "execute"),
])
def test_route_after_precheck(pc, expected):
    assert route_after_precheck({"current_pre_check": pc}) == expected


# ---------------------------------------------------------------------------
# route_after_execute
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("ex,expected", [
    ({"status": "error"}, "analysis"),
    ({"status": "ok", "requires_poll": True}, "poll"),
    ({"status": "ok", "requires_poll": False}, "validate"),
])
def test_route_after_execute(ex, expected):
    assert route_after_execute({"current_execute": ex}) == expected


# ---------------------------------------------------------------------------
# route_after_poll
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("status,expected", [
    ("RUNNING", "poll"),
    ("FINISHED", "validate"),
    ("ABORTED", "analysis"),
])
def test_route_after_poll(status, expected):
    assert route_after_poll({"current_poll": {"sap_status": status}}) == expected


def test_route_after_poll_defaults_to_running():
    assert route_after_poll({}) == "poll"


# ---------------------------------------------------------------------------
# route_after_validate
# ---------------------------------------------------------------------------

def test_route_after_validate_ok():
    assert route_after_validate({"current_validate": {"verdict": "ok"}}) == "finalize_step"


def test_route_after_validate_not_ok():
    assert route_after_validate({"current_validate": {"verdict": "retry"}}) == "analysis"


# ---------------------------------------------------------------------------
# route_after_analysis  (retry budget aware)
# ---------------------------------------------------------------------------

def _analysis_state(action, retry_count, max_retries=3):
    return {
        "current_analysis": {"action": action},
        "current_step_id": "A",
        "steps": [{"step_id": "A", "max_retries": max_retries}],
        "retry_count": retry_count,
    }


def test_analysis_retry_within_budget_goes_to_execute():
    assert route_after_analysis(_analysis_state("retry", retry_count=1)) == "execute"


def test_analysis_retry_over_budget_escalates_to_user():
    assert route_after_analysis(_analysis_state("retry", retry_count=4)) == "user"


def test_analysis_skip_goes_to_finalize():
    assert route_after_analysis(_analysis_state("skip", retry_count=1)) == "finalize_step"


def test_analysis_user_input_goes_to_user():
    assert route_after_analysis(_analysis_state("user_input", retry_count=0)) == "user"


# ---------------------------------------------------------------------------
# route_after_user
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("restart_from,expected", [
    ("pre_check", "pre_check"),
    ("execute", "execute"),
    ("next_step", "finalize_step"),
    ("abort", END),
    ("anything_else", END),
])
def test_route_after_user(restart_from, expected):
    assert route_after_user({"restart_from": restart_from}) == expected


# ---------------------------------------------------------------------------
# route_after_fanin / route_after_finalize
# ---------------------------------------------------------------------------

def test_route_after_fanin_always_router():
    assert route_after_fanin({}) == "router"


def test_route_after_finalize_more_steps():
    state = {"step_index": 1, "steps": [{"step_id": "A"}, {"step_id": "B"}]}
    assert route_after_finalize(state) == "router"


def test_route_after_finalize_done():
    state = {"step_index": 2, "steps": [{"step_id": "A"}, {"step_id": "B"}]}
    assert route_after_finalize(state) == END
