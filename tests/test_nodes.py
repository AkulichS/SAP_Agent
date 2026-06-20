"""Per-node tests for the graph factory nodes, using fake MCP session + fake LLMs."""

import pytest

from graph_builder import (make_analysis_node, make_execute_node, make_poll_node,
                           make_pre_check_node, make_validate_node)


# ===========================================================================
# pre_check_node
# ===========================================================================

async def test_precheck_disabled_passes(make_session, make_llms, make_state):
    node = make_pre_check_node(make_session(), make_llms())
    out = await node(make_state([{"step_id": "A"}]))
    pc = out["current_pre_check"]
    assert pc["passed"] is True and pc["skip_step"] is False


async def test_precheck_skip_mode(make_session, make_llms, make_state):
    node = make_pre_check_node(make_session(), make_llms())
    step = {"step_id": "A", "pre_check": {"enabled": True, "mode": "skip"}}
    out = await node(make_state([step]))
    assert out["current_pre_check"]["passed"] is True


def _cmp_step(comparison):
    return {"step_id": "A", "pre_check": {
        "enabled": True, "mode": "comparison", "action_type": "TOOLS",
        "object_name": "COKP", "params": {"table": "COKP"},
        "comparison": comparison}}


async def test_precheck_comparison_empty_pass_executes(make_session, make_llms, make_state):
    session = make_session(sap_read_table={"rows": [], "status": "S"})
    node = make_pre_check_node(session, make_llms())
    step = _cmp_step({"select": "empty", "on_pass": "execute", "on_fail": "skip"})
    out = await node(make_state([step]))
    pc = out["current_pre_check"]
    assert pc["passed"] is True and pc["skip_step"] is False


async def test_precheck_comparison_non_empty_on_pass_skip(make_session, make_llms, make_state):
    session = make_session(sap_read_table={"rows": [{"X": "1"}], "status": "S"})
    node = make_pre_check_node(session, make_llms())
    step = _cmp_step({"select": "non_empty", "on_pass": "skip", "on_fail": "execute"})
    out = await node(make_state([step]))
    assert out["current_pre_check"]["skip_step"] is True


async def test_precheck_comparison_field_mismatch_on_fail_skip(make_session, make_llms, make_state):
    session = make_session(sap_read_table={"rows": [{"SPERRE": "X"}], "status": "S"})
    node = make_pre_check_node(session, make_llms())
    step = _cmp_step({"select": "first", "field": "SPERRE", "operator": "eq",
                      "value": "", "on_fail": "skip"})
    out = await node(make_state([step]))
    pc = out["current_pre_check"]
    assert pc["skip_step"] is True and pc["passed"] is True


async def test_precheck_comparison_on_fail_error(make_session, make_llms, make_state):
    session = make_session(sap_read_table={"rows": [{"SPERRE": "X"}], "status": "S"})
    node = make_pre_check_node(session, make_llms())
    step = _cmp_step({"select": "first", "field": "SPERRE", "operator": "eq",
                      "value": "", "on_fail": "error"})
    out = await node(make_state([step]))
    pc = out["current_pre_check"]
    assert pc["passed"] is False and pc["error"]


async def test_precheck_llm_pass(make_session, make_llms, make_state):
    session = make_session(sap_read_table={"rows": [{"A": "1"}], "status": "S"})
    node = make_pre_check_node(session, make_llms(validation="yes"))
    step = {"step_id": "A", "pre_check": {
        "enabled": True, "mode": "llm", "action_type": "TOOLS", "object_name": "COKP",
        "params": {"table": "COKP"}, "llm": {"prompt": "ok?", "pass_values": ["yes"]}}}
    out = await node(make_state([step]))
    pc = out["current_pre_check"]
    assert pc["passed"] is True and pc["skip_step"] is False


async def test_precheck_llm_fail_skips(make_session, make_llms, make_state):
    session = make_session(sap_read_table={"rows": [{"A": "1"}], "status": "S"})
    node = make_pre_check_node(session, make_llms(validation="no"))
    step = {"step_id": "A", "pre_check": {
        "enabled": True, "mode": "llm", "action_type": "TOOLS", "object_name": "COKP",
        "params": {"table": "COKP"}, "llm": {"prompt": "ok?", "pass_values": ["yes"]}}}
    out = await node(make_state([step]))
    assert out["current_pre_check"]["skip_step"] is True


# ===========================================================================
# execute_node
# ===========================================================================

async def test_execute_tools(make_session, make_state):
    session = make_session(sap_read_table={"rows": [{"K": "1"}], "count": 1, "status": "S"})
    node = make_execute_node(session)
    step = {"step_id": "A", "action_type": "TOOLS", "object_name": "COKP",
            "params": {"table": "COKP"}}
    out = await node(make_state([step]))
    ex = out["current_execute"]
    assert ex["status"] == "ok" and ex["action_type"] == "TOOLS"
    assert ex["requires_poll"] is False


async def test_execute_submit_async(make_session, make_state):
    session = make_session(sap_execute_step={
        "status": "submitted", "requires_poll": True, "job_name": "J",
        "job_id": "7", "messages": [], "result_json": {}})
    node = make_execute_node(session)
    step = {"step_id": "A", "action_type": "SUBMIT", "object_name": "RKO", "async": True}
    out = await node(make_state([step]))
    ex = out["current_execute"]
    assert ex["requires_poll"] is True and ex["job_name"] == "J"


async def test_execute_submit_inline(make_session, make_state):
    session = make_session(sap_execute_step={
        "status": "ok", "requires_poll": False, "job_name": "J", "job_id": "7",
        "messages": [], "result_json": {"spool": ["ok line"]}, "spool_text": "ok line"})
    node = make_execute_node(session)
    step = {"step_id": "A", "action_type": "SUBMIT", "object_name": "RKO", "async": False}
    out = await node(make_state([step]))
    ex = out["current_execute"]
    assert ex["status"] == "ok" and ex["requires_poll"] is False


async def test_execute_fm_error(make_session, make_state):
    session = make_session(sap_execute_step={
        "status": "error", "requires_poll": False, "job_name": "", "job_id": "",
        "messages": [{"MESSAGE": "boom"}], "result_json": {}})
    node = make_execute_node(session)
    step = {"step_id": "A", "action_type": "FM", "object_name": "BAPI_X"}
    out = await node(make_state([step]))
    assert out["current_execute"]["status"] == "error"


# ===========================================================================
# poll_node
# ===========================================================================

_EXEC = {"job_name": "J", "job_id": "7", "requires_poll": True, "status": "ok",
         "action_type": "SUBMIT", "messages": [], "result_json": {}}


async def test_poll_finished(make_session, make_state):
    session = make_session(sap_job_status={"status": "FINISHED"})
    node = make_poll_node(session)
    state = make_state([{"step_id": "A"}], current_execute=_EXEC)
    out = await node(state)
    assert out["current_poll"]["sap_status"] == "FINISHED"
    assert out["current_poll"]["poll_count"] == 1


async def test_poll_running_then_finished(make_session, make_state, no_sleep):
    session = make_session(sap_job_status=[{"status": "RUNNING"}, {"status": "FINISHED"}])
    node = make_poll_node(session)
    state = make_state([{"step_id": "A"}], current_execute=_EXEC)

    out1 = await node(state)
    assert out1["current_poll"]["sap_status"] == "RUNNING"

    state["current_poll"] = out1["current_poll"]
    out2 = await node(state)
    assert out2["current_poll"]["sap_status"] == "FINISHED"
    assert out2["current_poll"]["poll_count"] == 2


async def test_poll_aborted(make_session, make_state):
    session = make_session(sap_job_status={"status": "ABORTED"})
    node = make_poll_node(session)
    state = make_state([{"step_id": "A"}], current_execute=_EXEC)
    out = await node(state)
    assert out["current_poll"]["sap_status"] == "ABORTED"


async def test_poll_timeout(make_session, make_state, no_sleep):
    session = make_session(sap_job_status={"status": "RUNNING"})
    node = make_poll_node(session)
    step = {"step_id": "A", "poll_timeout_sec": 0}
    state = make_state([step], current_execute=_EXEC)
    out = await node(state)
    assert out["current_poll"]["timed_out"] is True
    assert out["current_poll"]["sap_status"] == "ABORTED"


# ===========================================================================
# validate_node
# ===========================================================================

def _vstep(validate):
    return {"step_id": "A", "validate": validate}


async def test_validate_keyword_spool_ok(make_session, make_llms, make_state):
    node = make_validate_node(make_session(), make_llms())
    step = _vstep({"mode": "keyword", "keyword": {
        "source": "spool", "ok_patterns": ["done"], "error_patterns": ["FATAL"]}})
    state = make_state([step], current_execute={
        "requires_poll": False, "messages": [], "result_json": {"spool": ["operation done"]}})
    out = await node(state)
    assert out["current_validate"]["verdict"] == "ok"


async def test_validate_keyword_spool_error_retries(make_session, make_llms, make_state):
    node = make_validate_node(make_session(), make_llms())
    step = _vstep({"mode": "keyword", "keyword": {
        "source": "spool", "ok_patterns": ["done"], "error_patterns": ["error"]}})
    state = make_state([step], current_execute={
        "requires_poll": False, "messages": [], "result_json": {"spool": ["bad error here"]}})
    out = await node(state)
    assert out["current_validate"]["verdict"] == "retry"
    assert out["current_validate"]["error_count"] == 1


async def test_validate_keyword_rows(make_session, make_llms, make_state):
    node = make_validate_node(make_session(), make_llms())
    step = _vstep({"mode": "keyword", "keyword": {
        "source": "rows", "ok_patterns": ["x500"], "error_patterns": ["FATAL"]}})
    state = make_state([step], current_execute={
        "requires_poll": False, "messages": [], "result_json": {"rows": [{"KOKRS": "X500"}]}})
    out = await node(state)
    assert out["current_validate"]["verdict"] == "ok"


async def test_validate_keyword_messages(make_session, make_llms, make_state):
    node = make_validate_node(make_session(), make_llms())
    step = _vstep({"mode": "keyword", "keyword": {
        "source": "messages", "ok_patterns": ["good"], "error_patterns": ["FATAL"]}})
    state = make_state([step], current_execute={
        "requires_poll": False, "messages": [{"MESSAGE": "all good"}], "result_json": {}})
    out = await node(state)
    assert out["current_validate"]["verdict"] == "ok"


async def test_validate_llm_ok(make_session, make_llms, make_state):
    llms = make_llms(validation='{"verdict":"ok","error_count":0,"reasoning":"fine"}')
    node = make_validate_node(make_session(), llms)
    step = _vstep({"mode": "llm", "llm": {"prompt": "ok?"}})
    state = make_state([step], current_execute={
        "requires_poll": False, "messages": [], "result_json": {"spool": ["x"]}})
    out = await node(state)
    assert out["current_validate"]["verdict"] == "ok"
    assert out["current_validate"]["error_count"] == 0


async def test_validate_llm_retry(make_session, make_llms, make_state):
    llms = make_llms(validation='{"verdict":"retry","error_count":2,"reasoning":"bad"}')
    node = make_validate_node(make_session(), llms)
    step = _vstep({"mode": "llm", "llm": {"prompt": "ok?"}})
    state = make_state([step], current_execute={
        "requires_poll": False, "messages": [], "result_json": {"spool": ["x"]}})
    out = await node(state)
    assert out["current_validate"]["verdict"] == "retry"
    assert out["current_validate"]["error_count"] == 2


async def test_validate_llm_non_json_escalates(make_session, make_llms, make_state):
    node = make_validate_node(make_session(), make_llms(validation="totally not json"))
    step = _vstep({"mode": "llm", "llm": {"prompt": "ok?"}})
    state = make_state([step], current_execute={
        "requires_poll": False, "messages": [], "result_json": {"spool": ["x"]}})
    out = await node(state)
    assert out["current_validate"]["verdict"] == "escalate"


async def test_validate_run_verification_action(make_session, make_llms, make_state):
    # validate.run executes a TOOLS read and validates ITS rows.
    session = make_session(sap_read_table={"rows": [{"KOKRS": "X500"}], "status": "S"})
    node = make_validate_node(session, make_llms())
    step = _vstep({"mode": "keyword",
                   "run": {"action_type": "TOOLS", "object_name": "COKP",
                           "params": {"table": "COKP"}},
                   "keyword": {"source": "rows", "ok_patterns": ["x500"],
                               "error_patterns": ["FATAL"]}})
    out = await node(make_state([step]))
    assert out["current_validate"]["verdict"] == "ok"
    assert ("sap_read_table", {"table": "COKP", "where": "", "fields": "",
                               "max_rows": 100}) in session.calls


# ===========================================================================
# analysis_node  (create_react_agent patched at the seam)
# ===========================================================================

_VALIDATE = {"step_id": "A", "verdict": "retry", "spool_text": "boom",
             "messages": [], "error_count": 1, "reasoning": "bad"}


async def test_analysis_retry_decision(make_session, make_llms, make_state, patch_react_agent):
    patch_react_agent('{"action":"retry","corrected_params":null,'
                      '"diagnosis":"transient","user_instructions":null}')
    node = make_analysis_node(make_session(), make_llms())
    state = make_state([{"step_id": "A"}], retry_count=0, current_validate=_VALIDATE)
    out = await node(state)
    assert out["current_analysis"]["action"] == "retry"
    assert out["retry_count"] == 1


async def test_analysis_skip_decision(make_session, make_llms, make_state, patch_react_agent):
    patch_react_agent('{"action":"skip","corrected_params":null,'
                      '"diagnosis":"benign","user_instructions":null}')
    node = make_analysis_node(make_session(), make_llms())
    state = make_state([{"step_id": "A"}], retry_count=0, current_validate=_VALIDATE)
    out = await node(state)
    assert out["current_analysis"]["action"] == "skip"


async def test_analysis_non_json_requires_user_input(make_session, make_llms, make_state,
                                                     patch_react_agent):
    patch_react_agent("I could not parse this")
    node = make_analysis_node(make_session(), make_llms())
    state = make_state([{"step_id": "A"}], retry_count=0, current_validate=_VALIDATE)
    out = await node(state)
    assert out["current_analysis"]["action"] == "user_input"
