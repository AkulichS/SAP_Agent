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


def _table_result(rows, *, status="ok"):
    """Build a fake sap_read_table envelope {status, messages, meta, data}."""
    return {"status": status, "messages": [],
            "meta": {"table": "COKP", "row_count": len(rows)},
            "data": {"rows": rows}}


def _exec_env(*, data=None, messages=None, status="ok",
              requires_poll=False, job_name="", job_id=""):
    """Build a fake sap_execute_step / ExecuteResult envelope."""
    return {"status": status, "messages": messages or [],
            "meta": {"action_type": "SUBMIT", "requires_poll": requires_poll,
                     "job_name": job_name, "job_id": job_id},
            "data": data or {}}


def _spool_data(text, *, available=True, lines=None):
    """data payload carrying a normalised spool {available, line_count, text}."""
    return {"spool": {"available": available,
                      "line_count": lines if lines is not None else len(text.splitlines()),
                      "text": text}}


def _job(state):
    """Fake sap_job_status envelope reporting a job state."""
    return {"status": "ok", "messages": [], "meta": {}, "data": {"state": state}}


def _cmp_step(comparison):
    return {"step_id": "A", "pre_check": {
        "enabled": True, "mode": "comparison", "action_type": "TOOLS",
        "object_name": "COKP", "params": {"table": "COKP"},
        "comparison": comparison}}


async def test_precheck_comparison_empty_pass_executes(make_session, make_llms, make_state):
    session = make_session(sap_read_table=_table_result([]))
    node = make_pre_check_node(session, make_llms())
    step = _cmp_step({"select": "empty", "on_pass": "execute", "on_fail": "skip"})
    out = await node(make_state([step]))
    pc = out["current_pre_check"]
    assert pc["passed"] is True and pc["skip_step"] is False


async def test_precheck_comparison_non_empty_on_pass_skip(make_session, make_llms, make_state):
    session = make_session(sap_read_table=_table_result([{"X": "1"}]))
    node = make_pre_check_node(session, make_llms())
    step = _cmp_step({"select": "non_empty", "on_pass": "skip", "on_fail": "execute"})
    out = await node(make_state([step]))
    assert out["current_pre_check"]["skip_step"] is True


async def test_precheck_comparison_field_mismatch_on_fail_skip(make_session, make_llms, make_state):
    session = make_session(sap_read_table=_table_result([{"SPERRE": "X"}]))
    node = make_pre_check_node(session, make_llms())
    step = _cmp_step({"select": "first", "field": "SPERRE", "operator": "eq",
                      "value": "", "on_fail": "skip"})
    out = await node(make_state([step]))
    pc = out["current_pre_check"]
    assert pc["skip_step"] is True and pc["passed"] is True


async def test_precheck_comparison_on_fail_error(make_session, make_llms, make_state):
    # "error" is a backward-compatible alias of "analyse": failure → passed=False and a
    # uniform ErrorContext handed to the source-aware analysis_node (no inline current_analysis).
    session = make_session(sap_read_table=_table_result([{"SPERRE": "X"}]))
    node = make_pre_check_node(session, make_llms())
    step = _cmp_step({"select": "first", "field": "SPERRE", "operator": "eq",
                      "value": "", "on_fail": "error"})
    out = await node(make_state([step]))
    pc = out["current_pre_check"]
    assert pc["passed"] is False and pc["error"]
    assert out["current_error_context"]["source"] == "pre_check"
    assert out["current_error_context"]["default_depth"] == "explain"
    assert "current_analysis" not in out


async def test_precheck_llm_pass(make_session, make_llms, make_state):
    session = make_session(sap_read_table=_table_result([{"A": "1"}]))
    node = make_pre_check_node(session, make_llms(validation="yes"))
    step = {"step_id": "A", "pre_check": {
        "enabled": True, "mode": "llm", "action_type": "TOOLS", "object_name": "COKP",
        "params": {"table": "COKP"}, "llm": {"prompt": "ok?", "pass_values": ["yes"]}}}
    out = await node(make_state([step]))
    pc = out["current_pre_check"]
    assert pc["passed"] is True and pc["skip_step"] is False


async def test_precheck_llm_fail_skips(make_session, make_llms, make_state):
    session = make_session(sap_read_table=_table_result([{"A": "1"}]))
    node = make_pre_check_node(session, make_llms(validation="no"))
    step = {"step_id": "A", "pre_check": {
        "enabled": True, "mode": "llm", "action_type": "TOOLS", "object_name": "COKP",
        "params": {"table": "COKP"}, "llm": {"prompt": "ok?", "pass_values": ["yes"]}}}
    out = await node(make_state([step]))
    assert out["current_pre_check"]["skip_step"] is True


# --- SUBMIT pre-check: emptiness decided from the ABAP-provided spool row count ---

def _submit_cmp_step(comparison, llm=None):
    pc = {"step_id": "A", "pre_check": {
        "enabled": True, "mode": "comparison", "action_type": "SUBMIT",
        "object_name": "RKASELRULES_OR", "async": False, "comparison": comparison}}
    if llm is not None:
        pc["pre_check"]["llm"] = llm
    return pc


def _submit_result(*, status="ok", spool_rows=None, spool_text="", spool_status="S"):
    """Build a fake sap_execute_step envelope for a SUBMIT inline-wait.

    spool_rows=None → no spool key in data.
    spool_status "S" = the report ran and its spool was read (available=True); anything
    else (e.g. "E") = job finished but spool unreadable → available=False.
    """
    data = {"status": "completed"}
    if spool_rows is not None:
        data["spool"] = {"available": spool_status == "S",
                         "line_count": spool_rows, "text": spool_text}
    return _exec_env(status=status, data=data)


async def test_precheck_submit_empty_spool_executes(make_session, make_llms, make_state):
    session = make_session(sap_execute_step=_submit_result(spool_rows=0))
    node = make_pre_check_node(session, make_llms())
    step = _submit_cmp_step({"select": "empty", "on_pass": "execute", "on_fail": "analyse"})
    out = await node(make_state([step]))
    pc = out["current_pre_check"]
    assert pc["passed"] is True and pc["skip_step"] is False
    assert out.get("current_error_context") is None


async def test_precheck_submit_nonempty_spool_to_analysis(make_session, make_llms, make_state):
    # Non-empty spool → pre_check fails and hands the offending spool to analysis_node
    # via a uniform ErrorContext (explain depth). No inline LLM call / current_analysis.
    session = make_session(
        sap_execute_step=_submit_result(spool_rows=24, spool_text="order 1\norder 2"))
    llms = make_llms()
    node = make_pre_check_node(session, llms)
    step = _submit_cmp_step({"select": "empty", "on_pass": "execute", "on_fail": "analyse"})
    out = await node(make_state([step]))
    pc = out["current_pre_check"]
    assert pc["passed"] is False
    ec = out["current_error_context"]
    assert ec["source"] == "pre_check" and ec["default_depth"] == "explain"
    assert "order 1" in ec["spool_text"]
    assert "current_analysis" not in out
    assert llms["validation"].calls == []  # explanation is deferred to analysis_node


async def test_precheck_submit_no_spool_not_verified(make_session, make_llms, make_state):
    session = make_session(sap_execute_step=_submit_result(status="error", spool_rows=None))
    node = make_pre_check_node(session, make_llms())
    step = _submit_cmp_step({"select": "empty", "on_pass": "execute", "on_fail": "analyse"})
    out = await node(make_state([step]))
    pc = out["current_pre_check"]
    assert pc["passed"] is False
    ec = out["current_error_context"]
    assert ec["source"] == "pre_check" and "spool" in ec["summary"].lower()


async def test_precheck_submit_finished_but_spool_unreadable_not_verified(make_session, make_llms, make_state):
    # Job FINISHED (status ok) but the spool was unavailable — ABAP flags the spool
    # object's own status as "E" → must be treated as "not verified", not "empty → ok".
    payload = _submit_result(status="ok", spool_rows=0, spool_status="E")
    payload["messages"] = [{"TYPE": "W", "MESSAGE": "Job finished, spool unavailable: read error"}]
    session = make_session(sap_execute_step=payload)
    node = make_pre_check_node(session, make_llms())
    step = _submit_cmp_step({"select": "empty", "on_pass": "execute", "on_fail": "analyse"})
    out = await node(make_state([step]))
    assert out["current_pre_check"]["passed"] is False
    assert out["current_error_context"]["source"] == "pre_check"


async def test_precheck_submit_completed_without_spool_not_verified(make_session, make_llms, make_state):
    session = make_session(sap_execute_step=_submit_result(status="ok", spool_rows=None))
    node = make_pre_check_node(session, make_llms())
    step = _submit_cmp_step({"select": "empty", "on_pass": "execute", "on_fail": "analyse"})
    out = await node(make_state([step]))
    assert out["current_pre_check"]["passed"] is False
    assert out["current_error_context"]["source"] == "pre_check"


# ===========================================================================
# execute_node
# ===========================================================================

async def test_execute_tools(make_session, make_state):
    session = make_session(sap_read_table=_table_result([{"K": "1"}]))
    node = make_execute_node(session)
    step = {"step_id": "A", "action_type": "TOOLS", "object_name": "COKP",
            "params": {"table": "COKP"}}
    out = await node(make_state([step]))
    ex = out["current_execute"]
    assert ex["status"] == "ok"
    assert ex["data"]["rows"] == [{"K": "1"}]
    assert ex["meta"].get("requires_poll") in (False, None)


async def test_execute_submit_async(make_session, make_state):
    session = make_session(sap_execute_step=_exec_env(
        status="submitted", requires_poll=True, job_name="J", job_id="7"))
    node = make_execute_node(session)
    step = {"step_id": "A", "action_type": "SUBMIT", "object_name": "RKO", "async": True}
    out = await node(make_state([step]))
    ex = out["current_execute"]
    assert ex["meta"]["requires_poll"] is True and ex["meta"]["job_name"] == "J"


async def test_execute_submit_inline(make_session, make_state):
    session = make_session(sap_execute_step=_exec_env(
        job_name="J", job_id="7", data=_spool_data("ok line")))
    node = make_execute_node(session)
    step = {"step_id": "A", "action_type": "SUBMIT", "object_name": "RKO", "async": False}
    out = await node(make_state([step]))
    ex = out["current_execute"]
    assert ex["status"] == "ok" and ex["meta"]["requires_poll"] is False
    assert ex["data"]["spool"]["text"] == "ok line"


async def test_execute_fm_error(make_session, make_state):
    session = make_session(sap_execute_step=_exec_env(
        status="error", messages=[{"MESSAGE": "boom"}]))
    node = make_execute_node(session)
    step = {"step_id": "A", "action_type": "FM", "object_name": "BAPI_X"}
    out = await node(make_state([step]))
    assert out["current_execute"]["status"] == "error"


# ===========================================================================
# poll_node
# ===========================================================================

_EXEC = _exec_env(requires_poll=True, job_name="J", job_id="7")


async def test_poll_finished(make_session, make_state):
    session = make_session(sap_job_status=_job("FINISHED"))
    node = make_poll_node(session)
    state = make_state([{"step_id": "A"}], current_execute=_EXEC)
    out = await node(state)
    assert out["current_poll"]["sap_status"] == "FINISHED"
    assert out["current_poll"]["poll_count"] == 1


async def test_poll_running_then_finished(make_session, make_state, no_sleep):
    session = make_session(sap_job_status=[_job("RUNNING"), _job("FINISHED")])
    node = make_poll_node(session)
    state = make_state([{"step_id": "A"}], current_execute=_EXEC)

    out1 = await node(state)
    assert out1["current_poll"]["sap_status"] == "RUNNING"

    state["current_poll"] = out1["current_poll"]
    out2 = await node(state)
    assert out2["current_poll"]["sap_status"] == "FINISHED"
    assert out2["current_poll"]["poll_count"] == 2


async def test_poll_aborted(make_session, make_state):
    session = make_session(sap_job_status=_job("ABORTED"))
    node = make_poll_node(session)
    state = make_state([{"step_id": "A"}], current_execute=_EXEC)
    out = await node(state)
    assert out["current_poll"]["sap_status"] == "ABORTED"


async def test_poll_timeout(make_session, make_state, no_sleep):
    session = make_session(sap_job_status=_job("RUNNING"))
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
    state = make_state([step], current_execute=_exec_env(data=_spool_data("operation done")))
    out = await node(state)
    assert out["current_validate"]["verdict"] == "ok"


async def test_validate_keyword_spool_error_retries(make_session, make_llms, make_state):
    node = make_validate_node(make_session(), make_llms())
    step = _vstep({"mode": "keyword", "keyword": {
        "source": "spool", "ok_patterns": ["done"], "error_patterns": ["error"]}})
    state = make_state([step], current_execute=_exec_env(data=_spool_data("bad error here")))
    out = await node(state)
    assert out["current_validate"]["verdict"] == "retry"
    assert out["current_validate"]["error_count"] == 1


async def test_validate_keyword_rows(make_session, make_llms, make_state):
    node = make_validate_node(make_session(), make_llms())
    step = _vstep({"mode": "keyword", "keyword": {
        "source": "rows", "ok_patterns": ["x500"], "error_patterns": ["FATAL"]}})
    state = make_state([step], current_execute=_exec_env(data={"rows": [{"KOKRS": "X500"}]}))
    out = await node(state)
    assert out["current_validate"]["verdict"] == "ok"


async def test_validate_keyword_messages(make_session, make_llms, make_state):
    node = make_validate_node(make_session(), make_llms())
    step = _vstep({"mode": "keyword", "keyword": {
        "source": "messages", "ok_patterns": ["good"], "error_patterns": ["FATAL"]}})
    state = make_state([step], current_execute=_exec_env(messages=[{"MESSAGE": "all good"}]))
    out = await node(state)
    assert out["current_validate"]["verdict"] == "ok"


async def test_validate_llm_ok(make_session, make_llms, make_state):
    llms = make_llms(validation='{"verdict":"ok","error_count":0,"reasoning":"fine"}')
    node = make_validate_node(make_session(), llms)
    step = _vstep({"mode": "llm", "llm": {"prompt": "ok?"}})
    state = make_state([step], current_execute=_exec_env(data=_spool_data("x")))
    out = await node(state)
    assert out["current_validate"]["verdict"] == "ok"
    assert out["current_validate"]["error_count"] == 0


async def test_validate_llm_retry(make_session, make_llms, make_state):
    llms = make_llms(validation='{"verdict":"retry","error_count":2,"reasoning":"bad"}')
    node = make_validate_node(make_session(), llms)
    step = _vstep({"mode": "llm", "llm": {"prompt": "ok?"}})
    state = make_state([step], current_execute=_exec_env(data=_spool_data("x")))
    out = await node(state)
    assert out["current_validate"]["verdict"] == "retry"
    assert out["current_validate"]["error_count"] == 2
    ec = out["current_error_context"]
    assert ec["source"] == "validate" and ec["default_depth"] == "diagnose"


async def test_validate_llm_non_json_escalates(make_session, make_llms, make_state):
    node = make_validate_node(make_session(), make_llms(validation="totally not json"))
    step = _vstep({"mode": "llm", "llm": {"prompt": "ok?"}})
    state = make_state([step], current_execute=_exec_env(data=_spool_data("x")))
    out = await node(state)
    assert out["current_validate"]["verdict"] == "escalate"


async def test_validate_run_verification_action(make_session, make_llms, make_state):
    # validate.run executes a TOOLS read and validates ITS rows.
    session = make_session(sap_read_table=_table_result([{"KOKRS": "X500"}]))
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
# analysis_node  (source-aware dispatcher: explain vs diagnose)
# ===========================================================================

# A validate-sourced failure → diagnose depth (heavy ReAct path).
_DIAGNOSE_CTX = {"source": "validate", "summary": "bad", "spool_text": "boom",
                 "error_count": 1, "default_depth": "diagnose"}


async def test_analysis_retry_decision(make_session, make_llms, make_state, patch_react_agent):
    patch_react_agent('{"action":"retry","corrected_params":null,'
                      '"diagnosis":"transient","user_instructions":null}')
    node = make_analysis_node(make_session(), make_llms())
    state = make_state([{"step_id": "A"}], retry_count=0, current_error_context=_DIAGNOSE_CTX)
    out = await node(state)
    assert out["current_analysis"]["action"] == "retry"
    assert out["retry_count"] == 1


async def test_analysis_skip_decision(make_session, make_llms, make_state, patch_react_agent):
    patch_react_agent('{"action":"skip","corrected_params":null,'
                      '"diagnosis":"benign","user_instructions":null}')
    node = make_analysis_node(make_session(), make_llms())
    state = make_state([{"step_id": "A"}], retry_count=0, current_error_context=_DIAGNOSE_CTX)
    out = await node(state)
    assert out["current_analysis"]["action"] == "skip"


async def test_analysis_non_json_requires_user_input(make_session, make_llms, make_state,
                                                     patch_react_agent):
    patch_react_agent("I could not parse this")
    node = make_analysis_node(make_session(), make_llms())
    state = make_state([{"step_id": "A"}], retry_count=0, current_error_context=_DIAGNOSE_CTX)
    out = await node(state)
    assert out["current_analysis"]["action"] == "user_input"


async def test_analysis_explain_depth_lists_rows_no_react(make_session, make_llms, make_state):
    # A pre_check-sourced failure resolves to the cheap "explain" depth: one validation
    # call over the offending rows, action=user_input, and the text-based ReAct is NOT invoked.
    llms = make_llms(validation="Orders 1,2 lack settlement rules; maintain KO02.")
    node = make_analysis_node(make_session(), llms)
    ec = {"source": "pre_check", "summary": "Pre-check failed",
          "rows": [{"AUFNR": "1"}, {"AUFNR": "2"}], "default_depth": "explain"}
    state = make_state([{"step_id": "A"}], retry_count=0, current_error_context=ec)
    out = await node(state)
    assert out["current_analysis"]["action"] == "user_input"
    assert "KO02" in out["current_analysis"]["diagnosis"]
    assert len(llms["validation"].calls) == 1
    assert "analysis_messages" not in out   # explain path is stateless


async def test_analysis_explain_depth_via_on_error_mode_map(make_session, make_llms, make_state):
    # on_error.mode as a {source: depth} map overrides the context default_depth.
    llms = make_llms(validation="explanation")
    node = make_analysis_node(make_session(), llms)
    step = {"step_id": "A", "on_error": {"mode": {"validate": "explain"}}}
    ec = {"source": "validate", "summary": "x", "default_depth": "diagnose"}
    state = make_state([step], retry_count=0, current_error_context=ec)
    out = await node(state)
    assert out["current_analysis"]["action"] == "user_input"
