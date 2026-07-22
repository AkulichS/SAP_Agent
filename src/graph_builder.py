"""
SAP Period Close Agent — LangGraph orchestration layer.

Architecture (DOE principle)
-----------------------------
  Directive    : config_store.db (base ⊕ company delta) — what to do
  Orchestration: graph_builder.py          — how to coordinate
  Execution    : mcp_server.py             — actual SAP operations via RFC

Sequential flow (one step):
  START → router → pre_check → execute → [poll →] validate
                                               └─ ok    → finalize_step → router → …
                                               └─ error → analysis → [user →] execute/finalize
Parallel flow (group of steps):
  router → [Send ×N] → parallel_step_runner → fan_in → router → …

Event bus
---------
  All nodes call _emit(event_dict) which puts events into _event_queue.
  run_period_close_web() relays this queue over WebSocket.
  run_period_close() (CLI) ignores the queue.
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
import sys, os
from pathlib import Path
from typing import Annotated, Any, Awaitable, Callable, TypedDict
from uuid import uuid4

import yaml
from dotenv import load_dotenv
from langchain_core.messages import (
    BaseMessage, HumanMessage, SystemMessage, RemoveMessage,
)

load_dotenv(Path(__file__).parent / ".env", override=True)
from langgraph.graph.message import add_messages, REMOVE_ALL_MESSAGES
from langchain_openai import ChatOpenAI
from langgraph.graph import END, StateGraph
from langgraph.graph.state import CompiledStateGraph
from langgraph.types import Command, Send, interrupt
from mcp import ClientSession
from mcp.client.sse import sse_client
from mcp.client.stdio import StdioServerParameters, stdio_client

from config import (build_async_checkpointer, build_checkpointer, build_llms_from_config,
                    load_config, reset_each_run_enabled)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Event bus  (module-level; set once per run by run_period_close_web)
# ---------------------------------------------------------------------------

_event_queue: asyncio.Queue | None = None


async def _emit(event: dict) -> None:
    if _event_queue is not None:
        await _event_queue.put(event)


# ---------------------------------------------------------------------------
# Placeholder resolution
# ---------------------------------------------------------------------------

def _resolve(obj: Any, ctx: dict) -> Any:
    if isinstance(obj, str):
        for k, v in ctx.items():
            obj = obj.replace(f"{{{{{k}}}}}", str(v))
        return obj
    if isinstance(obj, dict):
        return {k: _resolve(v, ctx) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_resolve(i, ctx) for i in obj]
    return obj


# ---------------------------------------------------------------------------
# Structured analysis result helpers (diagnose depth)
# ---------------------------------------------------------------------------

def _flatten_result(result: dict) -> tuple[str, str]:
    """Derive back-compat (diagnosis, user_instructions) text from a structured
    {status, errors[], resolutions[]} result, for the CLI and legacy UI fallback."""
    errors      = result.get("errors") or []
    resolutions = result.get("resolutions") or []
    if errors:
        lines = []
        for e in errors:
            line = f"- {e.get('object_id', '?')}: {e.get('error', '')}"
            if e.get("cause"):
                line += f" — {e['cause']}"
            lines.append(line)
        diagnosis = "\n".join(lines)
    else:
        diagnosis = "No specific errors identified."
    instr = "\n".join(r.get("recommendation", "")
                      for r in resolutions if r.get("recommendation"))
    return diagnosis or "No diagnosis.", instr or "No specific resolution provided."


def _extract_json_object(text: str) -> dict | None:
    """Best-effort recovery of a single JSON object embedded in otherwise
    free-form text (e.g. a model that narrates its plan before/around the
    JSON it was told to emit exclusively). Returns None if no balanced
    ``{...}`` span parses cleanly."""
    start = text.find("{")
    if start == -1:
        return None
    depth = 0
    in_str = False
    escape = False
    for i in range(start, len(text)):
        ch = text[i]
        if in_str:
            if escape:
                escape = False
            elif ch == "\\":
                escape = True
            elif ch == '"':
                in_str = False
            continue
        if ch == '"':
            in_str = True
        elif ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                try:
                    return json.loads(text[start:i + 1])
                except json.JSONDecodeError:
                    return None
    return None


def _synthesize_result(diagnosis: str, status: str = "completed",
                       recommendation: str | None = None) -> dict:
    """Wrap a flat diagnosis string into the structured result shape so the UI can
    always render the two-table view, even for legacy / fallback answers."""
    return {
        "status": status,
        "errors": [{"object_id": "", "error": diagnosis, "cause": ""}],
        "analysis": [],
        "resolutions": [{"recommendation": recommendation or diagnosis,
                         "affected_objects": []}],
    }


def _normalize_analysis(result: dict) -> None:
    """Populate ``result['analysis']`` (the per-object evidence the UI's Analysis
    table renders) from each error's model-supplied ``analysis`` field. Prefers a
    standalone ``analysis`` array if the model already gave one. Never fabricates
    from ``cause`` — if the model provided no evidence, the table stays empty."""
    existing = result.get("analysis")
    if isinstance(existing, list) and existing:
        return
    rows: list[dict] = []
    for e in result.get("errors") or []:
        finding = str(e.get("analysis") or "").strip()
        if finding:
            rows.append({"object_id": e.get("object_id") or "", "finding": finding})
    result["analysis"] = rows


def _summarize_tool_args(args: dict | None) -> str:
    """Pick a short, human-readable label for a diagnostic tool call so the UI
    can show e.g. "sap_read_table · AUFK" rather than an opaque arg blob."""
    if not isinstance(args, dict) or not args:
        return ""
    for key in ("table", "job_name", "spool_id", "spool_no", "object_name", "name"):
        val = args.get(key)
        if val:
            return str(val)
    # Fall back to the first scalar value present.
    for val in args.values():
        if isinstance(val, (str, int)) and str(val).strip():
            return str(val)
    return ""


# ---------------------------------------------------------------------------
# State TypedDicts
# ---------------------------------------------------------------------------

class PreCheckResult(TypedDict, total=False):
    step_id: str; passed: bool; skip_step: bool; raw_data: dict; error: str | None


class ErrorContext(TypedDict, total=False):
    """Uniform failure payload handed to the source-aware analysis_node.

    Every failing node (pre_check / validate / execute / poll) fills this in and
    routes to ``analysis``; the node reads ``source`` + ``default_depth`` to pick
    the cheap ``explain`` path or the heavy ``diagnose`` (ReAct) path.
    """
    source: str            # "pre_check" | "validate" | "execute" | "poll"
    summary: str           # one-line reason
    rows: list[dict]       # offending rows already in hand
    spool_text: str
    messages: list[dict]
    error_count: int
    errors: list[dict]     # structured errors[] from _llm_verdict (e.g. KO8G shape)
    default_depth: str     # "explain" | "diagnose"
    job_name: str
    job_id: str


class ExecuteResult(TypedDict, total=False):
    step_id:  str
    status:   str            # ok | error | submitted
    messages: list[dict]
    meta:     dict           # {action_type, requires_poll, job_name, job_id}
    data:     dict           # raw RFC payload (spool/rows live here, once)


class PollStatus(TypedDict):
    job_name: str; job_id: str; sap_status: str
    poll_count: int; elapsed_sec: float; timed_out: bool


class ValidateResult(TypedDict):
    step_id: str; verdict: str      # ok | retry | escalate
    spool_text: str; messages: list[dict]; error_count: int; reasoning: str


class AnalysisResult(TypedDict, total=False):
    step_id: str; action: str       # retry | user_input | skip
    corrected_params: list[dict] | None   # operator param overrides (not LLM-set)
    diagnosis: str; user_instructions: str | None; tools_used: list[str]
    status: str                     # completed | partial | undetermined (diagnose only)
    result: dict | None             # structured {status, errors[], resolutions[]}


class StepRecord(TypedDict):
    step_id: str; group: str | None
    pre_check: PreCheckResult | None; execute: ExecuteResult | None
    poll: PollStatus | None; validate: ValidateResult | None
    analysis: AnalysisResult | None; final_status: str


def _merge_parallel(a: dict, b: dict) -> dict:
    return {**a, **b}


class PeriodCloseState(TypedDict):
    company_config: dict; steps: list[dict]; global_defaults: dict
    analysis_defaults: dict
    step_index: int; current_group: str | None; parallel_step_ids: list[str]
    current_step_id: str
    current_pre_check: PreCheckResult | None
    current_execute: ExecuteResult | None
    current_poll: PollStatus | None
    current_validate: ValidateResult | None
    current_analysis: AnalysisResult | None
    current_error_context: ErrorContext | None
    retry_count: int
    parallel_results: Annotated[dict[str, dict], _merge_parallel]
    step_records: list[StepRecord]
    analysis_messages: Annotated[list[BaseMessage], add_messages]
    user_decision: dict | None; restart_from: str | None
    completed: bool; escalated: bool; aborted: bool; run_error: str | None


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------

def _parse_tool_result(result: Any) -> dict:
    for block in result.content:
        if block.type == "text":
            try:
                return json.loads(block.text)
            except json.JSONDecodeError:
                return {"raw": block.text}
    return {}


# ---------------------------------------------------------------------------
# Envelope accessors
#
# Every MCP tool / ExecuteResult carries the {status, messages, meta, data}
# envelope. These read it; they never restructure it. ``data.spool`` is
# legitimately None when a step produced no spool — guard for it once here.
# ---------------------------------------------------------------------------

def _spool(env: dict) -> dict:
    """Spool object {available, line_count, text} from an envelope, or {}."""
    return (env.get("data") or {}).get("spool") or {}


def _rows(env: dict) -> list:
    """Row list from an envelope's data payload, or []."""
    return (env.get("data") or {}).get("rows") or []


def _meta(env: dict, key: str, default: Any = None) -> Any:
    """One field from an envelope's meta dict."""
    return (env.get("meta") or {}).get(key, default)


def _get_step(state: PeriodCloseState) -> dict:
    step_id = state["current_step_id"]
    for s in state["steps"]:
        if s["step_id"] == step_id:
            return s
    raise KeyError(f"Step not found: {step_id!r}")


def _defaults(state: PeriodCloseState) -> dict:
    return state.get("global_defaults", {})


def _as_number(x: Any) -> float | None:
    """Best-effort numeric parse; tolerates thousands separators and SAP sign suffix."""
    try:
        s = str(x).strip().replace(" ", "").replace(",", "")
        if s.endswith("-"):           # SAP trailing-minus notation, e.g. "100.00-"
            s = "-" + s[:-1]
        return float(s)
    except (ValueError, TypeError):
        return None


def _compare(actual: Any, op: str, expected: Any) -> bool:
    """Compare actual vs expected. Numeric when both parse as numbers, else string.

    Operators (case-insensitive): eq, ne, gt, lt, ge, le, contains.
    """
    op = (op or "eq").lower()
    na, nb = _as_number(actual), _as_number(expected)
    if op in ("eq", "ne", "gt", "lt", "ge", "le") and na is not None and nb is not None:
        a, b = na, nb
    else:
        a, b = str(actual), str(expected)
    ops = {
        "eq": lambda a, b: a == b, "ne": lambda a, b: a != b,
        "gt": lambda a, b: a > b,  "lt": lambda a, b: a < b,
        "ge": lambda a, b: a >= b, "le": lambda a, b: a <= b,
        "contains": lambda a, b: str(b) in str(a),
    }
    return ops.get(op, lambda a, b: False)(a, b)


async def _run_action(session: ClientSession, action_type: str, object_name: str,
                      params: Any, async_mode: bool = False, test_run: bool = False) -> dict:
    """Route one SAP action to the right MCP tool and return its result."""
    
    at = (action_type or "").upper()
    if at == "TOOLS":
        # Generic tool call: object_name selects the ABAP handler, params travel
        # verbatim as iv_params_json. Legacy TOOLS steps set object_name to a *table*
        # (the tool was always TOOL_READ_TABLE, hardcoded in sap_read_table) — keep
        # them working by defaulting non-TOOL_* names to a TOOL_READ_TABLE read.
        p = dict(params) if isinstance(params, dict) else {}
        tool = (object_name or "").strip().upper()
        if not tool.startswith("TOOL_"):
            p.setdefault("table", object_name)
            tool = "TOOL_READ_TABLE"
        r = await session.call_tool("sap_run_tool", arguments={
            "object_name": tool,
            "params_json": json.dumps(p),
            "test_run":    test_run,
        })
    else:
        r = await session.call_tool("sap_execute_step", arguments={
            "action_type": at,
            "object_name": object_name,
            "params_json": json.dumps(params),
            "async_mode":  async_mode,
            "test_run":    test_run,
        })
    return _parse_tool_result(r)


def _analysis_depth(step: dict, ec: dict) -> str:
    """Resolve the analysis depth for a failure: cheap "explain" vs heavy "diagnose".

    Precedence: on_error.mode (a string, or a {source: depth} map) →
    error_context.default_depth → "diagnose".
    """
    src = ec.get("source", "")
    oem = (step.get("on_error", {}) or {}).get("mode")
    depth = (oem.get(src) if isinstance(oem, dict) else oem) \
            or ec.get("default_depth") or "diagnose"
    return depth


# ---------------------------------------------------------------------------
# Shared LLM job: the verdict / judge gate
# ---------------------------------------------------------------------------

# Reasoning models wrap their out-loud deliberation in one of these tags. We
# match the whole family rather than just <think>.
_REASONING_TAGS = "think|thinking|reason|reasoning"
_THINK_RE = re.compile(
    rf"<({_REASONING_TAGS})\b[^>]*>.*?</\1\s*>", re.DOTALL | re.IGNORECASE)
_THINK_CLOSE_RE = re.compile(rf"</(?:{_REASONING_TAGS})\s*>", re.IGNORECASE)


def _strip_reasoning(text: str) -> str:
    """Drop a reasoning model's ``<think>…</think>`` deliberation, keeping only
    its final answer.
    """
    text = _THINK_RE.sub("", text)
    closes = list(_THINK_CLOSE_RE.finditer(text))
    if closes:
        text = text[closes[-1].end():]
    return text.strip()


async def _llm_verdict(
    llm,
    *,
    prompt: str,
    system_prompt: str | None = None,
    spool_text: str = "",
    rows: list | None = None,
    messages: list | None = None,
    pass_values: list[str] | None = None,
    llm_fail_verdict: str = "ok",
) -> dict:
    """Single LLM gate shared by pre_check(mode:llm) and validate(mode:llm).

    Returns ``{"verdict","error_count","errors","reasoning"}`` where verdict is
    ``ok`` | ``retry`` | ``escalate``.
    """
    rows     = rows or []
    messages = messages or []
    messages_text = " ".join(m.get("MESSAGE", m.get("text", "")) for m in messages)
    rows_text     = json.dumps(rows, ensure_ascii=False) if rows else ""

    # _resolve only replaces {{token}}; literal single braces (e.g. a JSON schema
    # written into the prompt) pass through unchanged.
    ctx = {"spool_text": spool_text or "", "messages_text": messages_text,
           "rows_text": rows_text}
    resolved_prompt = _resolve(prompt, ctx)

    # If the prompt embeds the output itself, use it verbatim; otherwise append it.
    if any(tok in prompt for tok in ("{{spool_text}}", "{{messages_text}}", "{{rows_text}}")):
        human = resolved_prompt
    else:
        content = spool_text or rows_text or messages_text
        human   = f"Task: {resolved_prompt}\n\nOutput:\n{content}"

    if pass_values is not None:
        # Push the model toward a clean, single-word final line so the last-line
        # verdict parse below is trivial. This only shrinks the failure surface —
        # correctness does not depend on the model obeying it (reasoning is
        # stripped and only the final line is judged regardless).
        default_system = (
            "Analyse this SAP data and answer the question. "
            "Your reply MUST end with a final line containing exactly one word: "
            "the verdict and nothing else.")
    else:
        default_system = (
            "You are an SAP job result analyser. Respond ONLY with JSON: "
            '{"verdict":"ok"|"retry"|"escalate","error_count":int,"reasoning":"one sentence"}'
        )
    system = _resolve(system_prompt or default_system, ctx)

    try:
        response = await llm.ainvoke([
            SystemMessage(content=system),
            HumanMessage(content=human),
        ])
        raw = re.sub(r"```\w*\n?", "", (response.content or "").strip()).strip()
    except Exception as exc:
        logger.warning("_llm_verdict LLM not available: %s", exc)
        return {"verdict": llm_fail_verdict, "error_count": 0, "errors": [],
                "reasoning": f"LLM not available — skipping LLM check ({str(exc)[:120]})"}

    # Reasoning models emit a <think>…</think> block before the answer; its
    # deliberation is full of pass words even when the verdict is NO, and it also
    # breaks json.loads below. Strip it so only the final answer is evaluated.
    raw = _strip_reasoning(raw)

    if pass_values is not None:
        # Judge the *final answer*, not the whole reply: take the last non-empty line
        # (the prompt asks for a single-word verdict) and match pass words on
        # word boundaries so "no" never matches inside "cannot", etc.
        answer = next((ln for ln in reversed(raw.splitlines()) if ln.strip()), raw).lower()
        passed = any(re.search(rf"\b{re.escape(v.lower())}\b", answer) for v in pass_values)
        return {"verdict": "ok" if passed else "retry",
                "error_count": 0 if passed else 1, "errors": [],
                "reasoning": raw[:300]}

    try:
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError:
            # Model wrapped the JSON in prose — grab the outermost {…} object.
            m = re.search(r"\{.*\}", raw, re.DOTALL)
            if not m:
                raise
            parsed = json.loads(m.group(0))
        error_count = int(parsed.get("error_count", 0))
        errors      = parsed.get("errors") or []
        verdict     = parsed.get("verdict") or ("ok" if error_count == 0 else "retry")
        reasoning   = parsed.get("reasoning") or (
            json.dumps(errors, ensure_ascii=False) if errors else raw[:300])
        return {"verdict": verdict, "error_count": error_count,
                "errors": errors, "reasoning": reasoning}
    except json.JSONDecodeError:
        return {"verdict": "escalate", "error_count": 0, "errors": [],
                "reasoning": f"LLM returned non-JSON: {raw[:150]}"}


# ---------------------------------------------------------------------------
# Node: router_node
# ---------------------------------------------------------------------------

def make_router_node():
    async def router_node(state: PeriodCloseState) -> dict:
        idx   = state["step_index"]
        steps = state["steps"]

        if idx >= len(steps):
            return {"completed": True}

        step  = steps[idx]
        group = step.get("group")
        group_ids = (
            [s["step_id"] for s in steps if s.get("group") == group]
            if group else []
        )

        await _emit({
            "type":       "step_start",
            "step_id":    step["step_id"],
            "step_index": idx,
            "total":      len(steps),
        })

        return {
            "current_step_id":    step["step_id"],
            "current_group":      group,
            "parallel_step_ids":  group_ids,
            "current_pre_check":  None,
            "current_execute":    None,
            "current_poll":       None,
            "current_validate":   None,
            "current_analysis":   None,
            "current_error_context": None,
            "retry_count":        0,
            "user_decision":      None,
            "restart_from":       None,
            "run_error":          None,
        }

    return router_node


# ---------------------------------------------------------------------------
# Node: pre_check_node
# ---------------------------------------------------------------------------

def make_pre_check_node(session: ClientSession, llms: dict[str, ChatOpenAI]):
    async def pre_check_node(state: PeriodCloseState) -> dict:
        step    = _get_step(state)
        step_id = step["step_id"]
        pc      = step.get("pre_check", {})

        if not pc.get("enabled", False):
            return {"current_pre_check": {
                "step_id": step_id, "passed": True, "skip_step": False,
                "raw_data": {}, "error": None,
            }}

        mode = pc.get("mode", "skip")
        await _emit({"type": "action_start", "step_id": step_id, "action": "pre_check",
                     "message": f"Running {mode} pre-check via {pc.get('object_name', '')}…"})

        if mode == "skip":
            await _emit({"type": "action_end", "step_id": step_id, "action": "pre_check",
                         "status": "skipped", "message": "Pre-check skipped"})
            return {"current_pre_check": {
                "step_id": step_id, "passed": True, "skip_step": False,
                "raw_data": {}, "error": None,
            }}

        # Run the pre-check action in any mode (TOOLS/FM/BAPI/BDC/SUBMIT). A SUBMIT
        # report uses inline-wait (async=false) so its spool comes back in one call.
        run = await _run_action(
            session, pc["action_type"], pc["object_name"], pc.get("params", {}),
            async_mode=pc.get("async", False), test_run=pc.get("test_run", True),
        )
        rows            = _rows(run)
        spool           = _spool(run)
        spool_text      = spool.get("text", "")
        spool_row_count = spool.get("line_count", 0)  # only consulted for SUBMIT (rows empty)
        # raw_data is what comparison/LLM/operator inspect: table rows when present,
        # otherwise the SUBMIT spool text.
        raw_data = run["data"] if rows else (spool_text or run["data"])

        # A failed pre-check no longer renders its own explanation. It marks the
        # pre-check failed and hands a uniform ErrorContext to the source-aware
        # analysis_node, which (cheap "explain" depth by default for pre_check)
        # explains the finding to the operator through user_node.
        def _fail_to_analysis(summary: str, detail: dict, emit_msg: str) -> dict:
            return {
                "current_pre_check": {
                    "step_id": step_id, "passed": False, "skip_step": False,
                    "raw_data": raw_data, "error": summary,
                },
                "current_error_context": {
                    "source": "pre_check", "summary": summary,
                    "rows": (rows or [])[:50], "spool_text": spool_text or "",
                    "default_depth": "explain",
                },
            }

        if mode == "comparison":
            cmp = pc["comparison"]
            if not isinstance(rows, list):
                rows = []
            select = cmp.get("select", "first")
            on_pass, on_fail = cmp.get("on_pass", "execute"), cmp.get("on_fail", "skip")
            pc_action = (pc.get("action_type") or "").upper()
            obj       = pc.get("object_name", "")

            # --- No verification artifact: a SUBMIT pre-check whose report errored or
            #     returned no spool cannot judge the precondition. Route to analysis so
            #     the operator is told the step was not verified (fix-and-retry or skip).
            if pc_action == "SUBMIT" and (run["status"] == "error" or not spool.get("available", False)):
                summary = (f"Step {step_id} could not be verified: pre-check report "
                           f"{obj} returned no spool. Ensure the report runs and produces a "
                           f"spool, then retry the pre-check — or skip the step.")
                await _emit({"type": "action_end", "step_id": step_id, "action": "pre_check",
                             "status": "failed", "message": f"not verified — no spool from {obj}",
                             "detail": {"object_name": obj, "status": run["status"],
                                        "spool_present": spool.get("available", False)}})
                return _fail_to_analysis(
                    summary,
                    {"object_name": obj, "status": run["status"]},
                    f"not verified — no spool from {obj}")

            # Determine the comparison outcome.
            if select in ("empty", "non_empty"):
                # Use the ABAP-provided spool row count for SUBMIT pre-checks; fall
                # back to len(rows) for TOOLS (table reads) where rows is a list.
                count  = spool_row_count if not rows else len(rows)
                passed = (count == 0) if select == "empty" else (count > 0)
                actual = f"{count} row(s)"
                cond   = f"table is {select.replace('_', '-')}"
            else:
                row    = rows[-1] if (select == "last" and rows) else (rows[0] if rows else {})
                actual = row.get(cmp["field"], "") if row else ""
                passed = _compare(actual, cmp.get("operator", "eq"), cmp.get("value", ""))
                cond   = (f"{cmp.get('field', '')}={actual!r} "
                          f"{cmp.get('operator', 'eq')} {cmp.get('value', '')!r}")

            params   = pc.get("params")
            table_id = params.get("table", "") if isinstance(params, dict) else pc.get("object_name", "")
            _det = {
                "table":    table_id,
                "rows":     rows[:5],
                "select":   select,
                "field":    cmp.get("field", ""),
                "actual":   actual,
                "operator": cmp.get("operator", ""),
                "expected": cmp.get("value", ""),
                "passed":   passed,
            }
            if passed:
                result: PreCheckResult = {"step_id": step_id, "passed": True,
                    "skip_step": on_pass == "skip", "raw_data": raw_data, "error": None}
                await _emit({"type": "action_end", "step_id": step_id, "action": "pre_check",
                             "status": "ok", "message": f"{cond} ✓", "detail": _det})
            elif on_fail == "skip":
                result = {"step_id": step_id, "passed": True, "skip_step": True,
                          "raw_data": raw_data, "error": None}
                await _emit({"type": "action_end", "step_id": step_id, "action": "pre_check",
                             "status": "skipped", "message": f"{cond} ✗ → skip step", "detail": _det})
            elif on_fail == "execute":
                result = {"step_id": step_id, "passed": True, "skip_step": False,
                          "raw_data": raw_data, "error": None}
                await _emit({"type": "action_end", "step_id": step_id, "action": "pre_check",
                             "status": "ok", "message": f"{cond} ✗ → execute anyway", "detail": _det})
            else:
                # Condition violated → hand the offending output to the source-aware
                # analysis_node. Canonical on_fail is "analyse"; "llm"/"error" are kept
                # as backward-compatible aliases (any other value also routes here).
                summary = f"Pre-check failed: {cond} ({actual})"
                await _emit({"type": "action_end", "step_id": step_id, "action": "pre_check",
                             "status": "failed", "message": f"{cond} ✗ → analysis", "detail": _det})
                return _fail_to_analysis(summary, _det, f"{cond} ✗ → analysis")
            return {"current_pre_check": result}

        if mode == "llm":
            # Boolean gate via the shared verdict helper: pass → execute, fail → skip.
            # llm_fail_verdict="escalate" ensures an LLM connection error routes to the
            # operator (via _fail_to_analysis) rather than silently passing the pre-check.
            lcfg      = pc.get("llm", {})
            pass_vals = lcfg.get("pass_values", ["yes", "true", "pass", "ok"])
            v = await _llm_verdict(
                llms["validation"],
                prompt=lcfg.get("prompt", "Is this valid?"),
                # Feed the SUBMIT spool verbatim when present; otherwise the table rows.
                spool_text=spool_text if (spool_text and not rows) else "",
                rows=rows,
                pass_values=pass_vals,
                llm_fail_verdict="escalate",
            )
            if v["verdict"] == "escalate":
                summary = f"Pre-check LLM unavailable for step {step_id}: {v['reasoning']}"
                await _emit({"type": "action_end", "step_id": step_id, "action": "pre_check",
                             "status": "failed", "message": summary[:120]})
                return _fail_to_analysis(summary, {"llm_error": v["reasoning"]}, summary)
            passed = v["verdict"] == "ok"
            st = "ok" if passed else "skipped"
            await _emit({"type": "action_end", "step_id": step_id, "action": "pre_check",
                         "status": st, "message": v["reasoning"][:120],
                         "detail": {"raw_data": raw_data, "llm_response": v["reasoning"][:300],
                                    "passed": passed}})
            return {"current_pre_check": {
                "step_id": step_id, "passed": passed, "skip_step": not passed,
                "raw_data": raw_data, "error": None if passed else v["reasoning"],
            }}

        await _emit({"type": "action_end", "step_id": step_id, "action": "pre_check",
                     "status": "ok", "message": "Passed (unknown mode)"})
        return {"current_pre_check": {
            "step_id": step_id, "passed": True, "skip_step": False, "raw_data": {}, "error": None,
        }}

    return pre_check_node


# ---------------------------------------------------------------------------
# Node: execute_node
# ---------------------------------------------------------------------------

def make_execute_node(session: ClientSession):
    async def execute_node(state: PeriodCloseState) -> dict:
        step    = _get_step(state)
        step_id = step["step_id"]
        defs    = _defaults(state)
        analysis = state.get("current_analysis")
        params   = (analysis["corrected_params"]
                    if analysis and analysis.get("corrected_params")
                    else step.get("params", []))
        async_mode = step.get("async", defs.get("async", False))
        test_run   = step.get("test_run", defs.get("test_run", True))

        await _emit({"type": "action_start", "step_id": step_id, "action": "execute",
                     "message": f"Executing {step['action_type']}/{step['object_name']}"
                                f" (async={async_mode}, test={test_run})"})

        # _run_action routes TOOLS → sap_read_table and FM/BAPI/BDC/SUBMIT →
        # sap_execute_step, returning the unified {status, messages, meta, data} envelope.
        env = await _run_action(
            session, step["action_type"], step["object_name"], params,
            async_mode=async_mode, test_run=test_run,
        )
        execute_result: ExecuteResult = {"step_id": step_id, **env}
        has_error = env["status"] == "error"

        if step["action_type"] == "TOOLS":
            table = _meta(env, "table", step["object_name"])
            count = _meta(env, "row_count", len(_rows(env)))
            msg   = f"error reading {table}" if has_error else f"{count} row(s) from {table}"
            await _emit({"type": "action_end", "step_id": step_id, "action": "execute",
                         "status": "failed" if has_error else "ok",
                         "message": msg,
                         "detail": {"table": table, "count": count,
                                    "rows": _rows(env)[:5], "status": env["status"]}})
            out = {"current_execute": execute_result}
            if has_error:
                out["current_error_context"] = {
                    "source": "execute", "summary": msg,
                    "rows": _rows(env)[:50], "messages": [{"MESSAGE": msg}],
                    "default_depth": "diagnose",
                }
            return out

        # FM / BAPI / BDC / SUBMIT
        requires_poll = _meta(env, "requires_poll", False)
        job_name      = _meta(env, "job_name", "")
        job_id        = _meta(env, "job_id", "")
        if requires_poll:
            msg = f"Job submitted: {job_name}/{job_id}"
        elif has_error:
            msg = "; ".join(m.get("MESSAGE", "") for m in env["messages"])[:200] or "Error"
        else:
            msg = "Executed synchronously"

        await _emit({"type": "action_end", "step_id": step_id, "action": "execute",
                     "status": "failed" if has_error else "ok",
                     "message": msg,
                     "detail": {
                         "job_name":      job_name,
                         "job_id":        job_id,
                         "status":        env["status"],
                         "requires_poll": requires_poll,
                         "messages":      [m.get("MESSAGE", str(m)) for m in env["messages"]],
                     }})
        out = {"current_execute": execute_result}
        if has_error:
            out["current_error_context"] = {
                "source": "execute", "summary": msg,
                "messages": env["messages"],
                "default_depth": "diagnose",
            }
        return out

    return execute_node


# ---------------------------------------------------------------------------
# Node: poll_node
# ---------------------------------------------------------------------------

def make_poll_node(session: ClientSession):
    async def poll_node(state: PeriodCloseState) -> dict:
        execute  = state["current_execute"]
        step     = _get_step(state)
        defs     = _defaults(state)
        job_name      = _meta(execute, "job_name", "")
        job_id        = _meta(execute, "job_id", "")
        poll_interval = step.get("poll_interval_sec", defs.get("poll_interval_sec", 30))
        poll_timeout  = step.get("poll_timeout_sec",  defs.get("poll_timeout_sec", 14400))

        prev       = state.get("current_poll")
        elapsed    = prev["elapsed_sec"] + poll_interval if prev else 0.0
        poll_count = (prev["poll_count"] + 1) if prev else 1

        if prev is None:
            await _emit({"type": "action_start", "step_id": step["step_id"], "action": "poll",
                         "message": f"Polling {job_name}/{job_id}…"})

        tool_result = await session.call_tool("sap_job_status",
                                              arguments={"job_name": job_name, "job_id": job_id})
        data      = _parse_tool_result(tool_result)
        raw_st    = (data.get("data") or {}).get("state", "SCHEDULED")

        if raw_st == "FINISHED":
            sap_status = "FINISHED"
        elif raw_st == "ABORTED":
            sap_status = "ABORTED"
        else:
            sap_status = "RUNNING"

        # Skip the sleep when the job is already done (avoids 30 s delay in stub/test mode)
        if sap_status == "RUNNING":
            await asyncio.sleep(poll_interval)
        elapsed  += poll_interval
        timed_out = elapsed >= poll_timeout

        final_st = "ABORTED" if timed_out else sap_status
        if final_st in ("FINISHED", "ABORTED"):
            ui_status = "ok" if final_st == "FINISHED" else "failed"
            await _emit({"type": "action_end", "step_id": step["step_id"], "action": "poll",
                         "status": ui_status,
                         "message": f"Job {final_st} — {poll_count} polls / {elapsed:.0f}s elapsed"})
        else:
            await _emit({"type": "action_update", "step_id": step["step_id"], "action": "poll",
                         "message": f"Poll #{poll_count} — {elapsed:.0f}s / {poll_timeout}s — RUNNING"})

        out = {"current_poll": {
            "job_name": job_name, "job_id": job_id, "sap_status": final_st,
            "poll_count": poll_count, "elapsed_sec": elapsed, "timed_out": timed_out,
        }}
        # ABORTED / timed-out → route_after_poll sends us to analysis. Hand it a
        # uniform ErrorContext (heavy diagnose depth: the job log needs investigating).
        if final_st == "ABORTED":
            summary = (f"Job {job_name}/{job_id} timed out after {elapsed:.0f}s"
                       if timed_out else f"Job {job_name}/{job_id} aborted")
            out["current_error_context"] = {
                "source": "poll", "summary": summary,
                "default_depth": "diagnose", "job_name": job_name, "job_id": job_id,
            }
        return out

    return poll_node


# ---------------------------------------------------------------------------
# Node: validate_node
# ---------------------------------------------------------------------------

def make_validate_node(session: ClientSession, llms: dict[str, ChatOpenAI]):
    async def validate_node(state: PeriodCloseState) -> dict:
        step    = _get_step(state)
        step_id = step["step_id"]
        execute = state.get("current_execute", {})
        poll    = state.get("current_poll")
        vcfg    = step.get("validate", {})
        mode    = vcfg.get("mode", "keyword")

        await _emit({"type": "action_start", "step_id": step_id, "action": "validate",
                     "message": f"Validating result ({mode} mode)…"})

        spool_text  = ""
        rows        = []
        messages    = execute.get("messages", []) if execute else []
        error_count = 0
        reasoning   = ""
        errors: list[dict] = []

        run_cfg = vcfg.get("run")
        if run_cfg:
            # Run a dedicated verification action and validate ITS output. Useful when
            # the main step executed async with no useful spool — confirm the result by
            # reading a table, calling an FM, or running a check report (any mode).
            await _emit({"type": "action_update", "step_id": step_id, "action": "validate",
                         "message": f"Running verification "
                                    f"{run_cfg['action_type']}/{run_cfg['object_name']}…"})
            # Runtime-only placeholders: this step's job identifiers, resolved the same
            # way whether it ran async (job in `poll`) or inline (job in `execute` meta).
            # Load-time tokens ({{period}}, {{company_code}}, …) are already substituted.
            job_ctx = {
                "job_name": (poll or {}).get("job_name") or _meta(execute or {}, "job_name", ""),
                "job_id":   (poll or {}).get("job_id")   or _meta(execute or {}, "job_id", ""),
            }
            run_params = _resolve(run_cfg.get("params", {}) or {}, job_ctx)
            run = await _run_action(
                session, run_cfg["action_type"], run_cfg["object_name"], run_params,
                async_mode=run_cfg.get("async", False), test_run=run_cfg.get("test_run", True),
            )
            spool_text = _spool(run).get("text", "")
            rows       = _rows(run)
            if run["messages"]:
                messages = run["messages"]
        elif (execute and _meta(execute, "requires_poll") and poll and poll.get("sap_status") == "FINISHED"):
            max_lines = vcfg.get("llm", {}).get("max_spool_lines", 500)
            spool_res = await session.call_tool("sap_read_spool", arguments={
                "job_name":  poll["job_name"],
                "job_id":    poll["job_id"],
                "max_lines": max_lines,
            })
            spool_text = _spool(_parse_tool_result(spool_res)).get("text", "")
        elif execute:
            # SUBMIT that ran inline-wait (async=false) carries its spool in data;
            # TOOLS/FM steps carry rows there. Pick up whatever is present.
            spool_text = _spool(execute).get("text", "")
            rows       = _rows(execute)

        if mode == "keyword":
            kw       = vcfg.get("keyword", {})
            ok_pats  = kw.get("ok_patterns", [])
            err_pats = kw.get("error_patterns", ["error", "Error"])
            src      = kw.get("source", "messages")
            if src == "spool":
                haystack = spool_text
            elif src == "rows":
                haystack = json.dumps(rows, ensure_ascii=False)
            else:
                haystack = " ".join(m.get("MESSAGE", m.get("text", "")) for m in messages)
            haystack = haystack.lower()
            hits        = [p for p in err_pats if p.lower() in haystack]
            error_count = len(hits)
            if error_count > 0:
                verdict   = "retry"
                reasoning = f"Error patterns matched: {hits}"
            elif ok_pats and any(p.lower() in haystack for p in ok_pats):
                verdict   = "ok"
                reasoning = "OK patterns matched"
            elif not ok_pats:
                verdict   = "ok"
                reasoning = "No error patterns found"
            else:
                verdict   = "retry"
                reasoning = "No OK patterns matched"

        elif mode == "llm":
            lcfg = vcfg.get("llm", {})
            await _emit({"type": "action_update", "step_id": step_id, "action": "validate",
                         "message": "Sending spool to LLM for analysis…"})
            v = await _llm_verdict(
                llms["validation"],
                prompt=lcfg.get("prompt", "Was this SAP step successful?"),
                system_prompt=lcfg.get("system_prompt"),
                spool_text=spool_text, rows=rows, messages=messages,
            )
            verdict     = v["verdict"]
            error_count = v["error_count"]
            errors      = v["errors"]
            reasoning   = v["reasoning"]
        else:
            verdict   = "ok"
            reasoning = "No validation configured"

        logger.info("validate step=%s verdict=%s errors=%d", step_id, verdict, error_count)
        await _emit({"type": "action_end", "step_id": step_id, "action": "validate",
                     "status": verdict,
                     "message": f"{error_count} error(s) — {reasoning}",
                     "detail": {
                         "verdict":       verdict,
                         "error_count":   error_count,
                         "errors":        errors,
                         "reasoning":     reasoning,
                         "spool_preview": "\n".join(spool_text.splitlines()[:15]) if spool_text else "",
                         "rows":          rows[:5] if rows else [],
                         "messages":      [m.get("MESSAGE", str(m)) for m in (messages or [])],
                     }})

        out: dict = {"current_validate": {
            "step_id": step_id, "verdict": verdict, "spool_text": spool_text,
            "messages": messages, "error_count": error_count, "reasoning": reasoning,
        }}
        if verdict != "ok":
            # Hand a uniform ErrorContext to the source-aware analysis_node. Post-step
            # failures default to the heavy "diagnose" (ReAct + SAP tools) depth.
            out["current_error_context"] = {
                "source": "validate", "summary": reasoning,
                "spool_text": spool_text or "", "rows": (rows or [])[:50],
                "messages": messages, "error_count": error_count, "errors": errors,
                "default_depth": "diagnose",
                "job_name": (poll or {}).get("job_name", "") if poll else "",
                "job_id":   (poll or {}).get("job_id", "")   if poll else "",
            }
        return out

    return validate_node


# ---------------------------------------------------------------------------
# Node: analysis_node  (ReAct loop)
# ---------------------------------------------------------------------------

def make_analysis_node(session: ClientSession, llms: dict[str, ChatOpenAI]):

    _tool_schema_cache: list[dict] | None = None

    async def _call_tool(name: str, args: dict) -> dict:
        """Generic MCP dispatcher — routes any tool call by name, no hardcoding."""
        r = await session.call_tool(name, arguments=args)
        return _parse_tool_result(r)

    async def _get_tool_schema() -> list[dict]:
        """Fetch tool list from MCP once and cache for the process lifetime."""
        nonlocal _tool_schema_cache
        if _tool_schema_cache is None:
            resp = await session.list_tools()
            _tool_schema_cache = [
                {
                    "name": t.name,
                    "description": t.description or "",
                    "args": t.inputSchema.get("properties", {}),
                }
                for t in resp.tools
            ]
        return _tool_schema_cache

    async def _run_text_react(
        llm: ChatOpenAI,
        call_tool: Callable[[str, dict], Awaitable[dict]],
        tool_schema: list[dict],
        system_prompt: str,
        prior_messages: list,
        emit: Callable,
        step_id: str,
        max_steps: int = 8,
    ) -> tuple[list, list[str], bool]:
        """Text-based ReAct loop: plain JSON I/O, no native API tool-calling.

        The LLM must output one of two JSON objects per turn:
          {"type":"tool_call",   "tool":"<name>","args":{...}}
          {"type":"final_answer","action":"retry|user_input|skip",
           "result":{"status":...,"errors":[...],"resolutions":[...]}}

        Tool errors are fed back as HumanMessages so the model can self-correct.
        Returns (new_messages_added, tool_names_used, budget_exhausted) where
        budget_exhausted is True when ``max_steps`` ran out before a final_answer.
        """
        tools_block = json.dumps(tool_schema, ensure_ascii=False, indent=2)
        full_system = (
            f"{system_prompt}\n\n"
            "=== AVAILABLE TOOLS ===\n"
            f"{tools_block}\n\n"
            "=== RESPONSE FORMAT (strict) ===\n"
            "Every response MUST be a single JSON object — no other text.\n"
            "To call a tool:\n"
            '  {"type":"tool_call","tool":"<name>","args":{<args>}}\n'
            "To give your final answer:\n"
            '  {"type":"final_answer","action":"retry|user_input|skip",'
            '"result":{"status":"completed|partial|undetermined",'
            '"errors":[{"object_id":"...","error":"...","cause":"...","analysis":"..."}],'
            '"resolutions":[{"recommendation":"...","affected_objects":["..."]}]}}\n'
            "- action: retry (re-run the step), skip (skip it), or user_input "
            "(operator must intervene).\n"
            "- Group identical fixes under one resolution to avoid duplicates; "
            "list every object a resolution covers in its affected_objects.\n"
            "- Each error's \"analysis\" field is REQUIRED: a short summary of the "
            "concrete facts you read from the tools for THAT object (statuses, "
            "flags, percentages, amounts, or that a table returned no rows). "
            "Fill it whenever you called any tool.\n"
            "- status: completed (all diagnosed), partial (some unresolved), "
            "undetermined (no root cause found).\n"
            f"- You may make at most {max_steps} tool calls. When every reported "
            "object is covered, output the final_answer immediately.\n"
            "If a tool returns status=error, correct the arguments and call it again."
        )

        conv: list = [SystemMessage(content=full_system)] + list(prior_messages)
        new_msgs: list = []
        tools_used: list[str] = []
        budget_exhausted = True

        async def _substep(sid_: str, kind: str, label: str, status: str, message: str) -> None:
            await emit({"type": "action_update", "step_id": step_id, "action": "analysis",
                        "message": message,
                        "substep": {"id": sid_, "kind": kind, "label": label,
                                    "status": status}})

        reformat_tries = 0
        MAX_REFORMAT   = 2
        for i in range(max_steps):
            llm_sub = f"llm-{i}"
            await _substep(llm_sub, "llm", "Reasoning…", "running", "Reasoning…")
            response = await llm.ainvoke(conv)
            new_msgs.append(response)
            conv.append(response)

            raw = re.sub(r"```\w*\n?", "", response.content.strip()).strip()
            try:
                parsed = json.loads(raw)
            except json.JSONDecodeError:
                parsed = _extract_json_object(raw)
            if parsed is None:
                # Model emitted non-JSON. One malformed turn shouldn't abort the
                # whole diagnosis — nudge it to reformat a bounded number of
                # times before giving up.
                reformat_tries += 1
                if reformat_tries > MAX_REFORMAT:
                    await _substep(llm_sub, "llm", "Unparsable reply",
                                   "error", "Analysis reply could not be parsed")
                    budget_exhausted = False
                    break  # model won't produce valid JSON — treat as terminal
                await _substep(llm_sub, "llm", "Reply not JSON — asking to reformat",
                               "error", "Analysis reply was not valid JSON; retrying")
                correction = HumanMessage(content=(
                    "Your last reply was not valid JSON. Reply with ONLY a single "
                    "JSON object in the required format — a tool_call or a "
                    "final_answer — with no prose, explanation, or markdown fences."))
                new_msgs.append(correction)
                conv.append(correction)
                continue

            if parsed.get("type") != "tool_call":
                await _substep(llm_sub, "llm", "Reached conclusion", "ok",
                               "Analysis concluded")
                budget_exhausted = False
                break  # final_answer or unrecognised — exit loop

            tool_name = parsed.get("tool", "")
            tool_args = parsed.get("args", {})
            arg_hint  = _summarize_tool_args(tool_args)
            tool_label = tool_name + (f" · {arg_hint}" if arg_hint else "")

            await _substep(llm_sub, "llm", f"Requested {tool_name}", "ok",
                           f"Calling SAP: {tool_name}…")
            tool_sub = f"tool-{i}"
            await _substep(tool_sub, "tool", tool_label, "running",
                           f"Calling SAP: {tool_name}…")
            try:
                result = await call_tool(tool_name, tool_args)
                tools_used.append(tool_name)
            except Exception as exc:
                result = {"status": "error",
                          "messages": [{"TYPE": "E", "MESSAGE": str(exc)}], "data": {}}

            tool_status = "ok" if result.get("status") == "ok" else "error"
            await _substep(tool_sub, "tool", tool_label, tool_status,
                           f"Tool returned ({tool_name}): {result.get('status')}")

            feedback = json.dumps(
                {"tool": tool_name, "status": result.get("status"),
                 "messages": result.get("messages", []), "data": result.get("data", {})},
                ensure_ascii=False,
            )
            fb_msg = HumanMessage(content=f"Tool result:\n{feedback}")
            new_msgs.append(fb_msg)
            conv.append(fb_msg)

        return new_msgs, tools_used, budget_exhausted

    def _ctx_text(ec: dict) -> str:
        """Render the uniform ErrorContext into a prompt-ready block for either depth."""
        parts: list[str] = []
        if ec.get("summary"):
            parts.append(str(ec["summary"]))
        if ec.get("error_count"):
            parts.append(f"error_count={ec['error_count']}")
        if ec.get("errors"):
            parts.append("Errors:\n" + json.dumps(ec["errors"][:50], ensure_ascii=False))
        if ec.get("rows"):
            parts.append("Offending rows (the condition that must be resolved):\n"
                         + json.dumps(ec["rows"][:50], ensure_ascii=False))
        if ec.get("spool_text"):
            parts.append(f"Spool (first 2000 chars):\n{ec['spool_text'][:2000]}")
        if ec.get("messages"):
            msgs = " ".join(m.get("MESSAGE", m.get("text", "")) for m in ec["messages"])
            if msgs.strip():
                parts.append(f"Messages: {msgs}")
        return "\n\n".join(parts) or "No detailed error context."

    async def _analyse_explain(state: PeriodCloseState, ec: dict, step: dict) -> dict:
        """Cheap depth: one validation-model call over data already in hand. No tools,
        no SAP re-fetch, no analysis_messages. Always returns user_input → operator."""
        step_id = step["step_id"]
        await _emit({"type": "action_start", "step_id": step_id, "action": "analysis",
                     "message": "Explaining finding to operator…"})
        oe       = step.get("on_error", {})
        guidance = oe.get("analysis_guidance", "")
        # Prompt precedence: on_error.explain_prompt → legacy pre_check.llm.prompt → default.
        prompt = (oe.get("explain_prompt")
                  or (step.get("pre_check", {}) or {}).get("llm", {}).get("prompt")
                  or "Explain this SAP finding to the operator and suggest how to correct it.")
        system = ("You are an SAP period-close pre-check analyst. In 2-4 short sentences, "
                  "state the cause(s) of the finding and how to correct it, in plain language. "
                  "Do not include your reasoning or restate the raw data — give only the "
                  "conclusion. Be concise."
                  + (f"\n\nDomain guidance:\n{guidance}" if guidance else ""))
        try:
            resp = await llms["validation"].ainvoke([
                SystemMessage(content=system),
                HumanMessage(content=f"Data:\n{_ctx_text(ec)}\n\n{prompt}"),
            ])
            # Reasoning models (e.g. qwen3-32b) prepend a verbose <think>…</think>
            # deliberation; keep only the final operator-facing explanation.
            diagnosis = _strip_reasoning((resp.content or "").strip()) \
                or ec.get("summary", "Pre-check failed.")
        except Exception as exc:
            logger.warning("analysis explain LLM error step=%s: %s", step_id, exc)
            diagnosis = ec.get("summary", "Pre-check failed; manual review required.")
        instructions = ("Correct the listed items in SAP, then retry the step from the "
                        "pre-check — or skip the step if the check is not required.")
        await _emit({"type": "action_end", "step_id": step_id, "action": "analysis",
                     "status": "user_input", "message": diagnosis[:200]})
        return {"current_analysis": {
            "step_id": step_id, "action": "user_input", "corrected_params": None,
            "diagnosis": diagnosis, "user_instructions": instructions, "tools_used": [],
        }}

    def _build_diagnose_system_prompt(
        state: PeriodCloseState, step: dict, ec: dict,
    ) -> tuple[str, int]:
        """Build the diagnose system prompt + tool-call budget.

        Preferred: section-based prompt assembled from ``on_error.analysis`` with
        per-section fallback to top-level ``analysis_defaults``. Legacy fallback:
        the single ``on_error.analysis_guidance`` blob (so un-migrated steps keep
        working). Returns (system_prompt, max_tool_calls).
        """
        oe   = step.get("on_error", {}) or {}
        ad   = state.get("analysis_defaults", {}) or {}
        ctx  = dict(state.get("company_config", {}))
        defs = _defaults(state)
        default_budget = int(ad.get("max_tool_calls", 8) or 8)
        an   = oe.get("analysis")

        if isinstance(an, dict) and an:
            def sec(key: str) -> str:
                return an.get(key) or ad.get(key) or ""

            parts: list[str] = []
            role = sec("role")
            if role:
                parts.append(_resolve(role, ctx))
            ctmpl = sec("context_template")
            context = (_resolve(ctmpl, {**ctx, "errors": _ctx_text(ec)})
                       if ctmpl else _ctx_text(ec))
            parts.append("## Context\n" + context)
            for label, key in (("Goal", "goal"), ("Tools", "tools"),
                               ("Instructions", "instructions"),
                               ("Output Format", "output_format"), ("Rules", "rules")):
                val = sec(key)
                if val:
                    parts.append(f"## {label}\n" + _resolve(val, ctx))
            system_prompt  = "\n\n".join(parts)
            max_tool_calls = int(an.get("max_tool_calls", default_budget) or default_budget)
            return system_prompt, max_tool_calls

        # Legacy path — single guidance blob
        guidance = oe.get("analysis_guidance", "")
        retry    = state.get("retry_count", 0)
        max_ret  = step.get("max_retries", defs.get("max_retries", 3))
        system_prompt = (
            f"You are an SAP period-close error analyst.\n"
            f"Step: {step['step_id']} | {step.get('action_type')}/{step.get('object_name')}\n"
            f"Company config: {json.dumps(state.get('company_config', {}))}\n"
            f"Retry: {retry}/{max_ret}\n\nDomain guidance:\n{guidance}"
        )
        return system_prompt, default_budget

    async def _analyse_diagnose(state: PeriodCloseState, ec: dict, step: dict) -> dict:
        step_id = step["step_id"]
        defs    = _defaults(state)
        max_ret = step.get("max_retries", defs.get("max_retries", 3))
        retry   = state.get("retry_count", 0)

        await _emit({"type": "action_start", "step_id": step_id, "action": "analysis",
                     "message": f"Analysing error (attempt {retry + 1}/{max_ret})…"})

        error_ctx = _ctx_text(ec)
        system_prompt, max_tool_calls = _build_diagnose_system_prompt(state, step, ec)

        llm   = llms["analysis"]
        prior = list(state.get("analysis_messages", []))
        new_messages: list = []
        tools_used: list[str] = []
        try:
            input_msgs   = prior + [HumanMessage(content=f"Error context:\n{error_ctx}")]
            tool_schema  = await _get_tool_schema()
            await _emit({"type": "action_update", "step_id": step_id, "action": "analysis",
                         "message": "Sending request to LLM…"})
            new_messages, tools_used, budget_exhausted = await _run_text_react(
                llm, _call_tool, tool_schema, system_prompt,
                input_msgs, _emit, step_id, max_tool_calls,
            )
            last_msg = new_messages[-1] if new_messages else None
            raw      = re.sub(r"```\w*\n?", "",
                              (last_msg.content if last_msg else "").strip()).strip()
            try:
                parsed = json.loads(raw)
            except json.JSONDecodeError:
                # Only trust a recovered fragment if it looks like a genuine
                # final_answer (has "action") — otherwise it may be trailing
                # tool-feedback text, which must fall through to the
                # unparsable-response branch below.
                candidate = _extract_json_object(raw)
                parsed = candidate if isinstance(candidate, dict) and "action" in candidate else None

            if isinstance(parsed, dict) and parsed.get("type") != "tool_call":
                action       = parsed.get("action", "user_input")
                result_block = parsed.get("result")
                if isinstance(result_block, dict):
                    status                  = result_block.get("status") or "completed"
                    diagnosis, instructions = _flatten_result(result_block)
                else:
                    # legacy flat shape — synthesize a structured result for the UI
                    diagnosis    = parsed.get("diagnosis") or raw
                    instructions = parsed.get("user_instructions")
                    status       = "completed"
                    result_block = _synthesize_result(diagnosis, status, instructions)
            else:
                # No clean final answer: budget ran out, deviated, or empty response
                action = "user_input"
                if budget_exhausted:
                    status    = "partial"
                    diagnosis = ("Analysis stopped after reaching the tool-call limit "
                                 "without a complete diagnosis.")
                else:
                    status = "undetermined"
                    logger.warning("analysis step=%s: unparsable model response: %.500s",
                                    step_id, raw)
                    diagnosis = "Analysis could not produce a structured diagnosis; manual review required."
                instructions = "Manual review required."
                result_block = _synthesize_result(diagnosis, status, instructions)
        except Exception as exc:
            logger.warning("analysis LLM not available step=%s: %s", step_id, exc)
            action       = "skip"
            status       = "undetermined"
            diagnosis    = f"Analysis LLM not available ({str(exc)[:150]}) — skipping step"
            instructions = None
            result_block = None
            tools_used   = []

        # Lift the model's per-error `analysis` evidence into result['analysis']
        # so the operator's Analysis-result table renders it.
        if isinstance(result_block, dict):
            _normalize_analysis(result_block)

        logger.info("analysis step=%s action=%s status=%s", step_id, action, status)
        await _emit({"type": "action_end", "step_id": step_id, "action": "analysis",
                     "status": action, "message": diagnosis[:200]})

        return {
            "current_analysis": {
                "step_id": step_id, "action": action, "corrected_params": None,
                "diagnosis": diagnosis, "user_instructions": instructions,
                "tools_used": tools_used, "status": status, "result": result_block,
            },
            "analysis_messages": new_messages,
            "retry_count": retry + 1,
        }

    async def analysis_node(state: PeriodCloseState) -> dict:
        """Source-aware dispatcher. Reads the uniform ErrorContext set by the failing
        node and picks the depth: cheap `explain` (operator-facing, no tools) or heavy
        `diagnose` (ReAct + SAP tools)."""
        step = _get_step(state)
        ec   = state.get("current_error_context") or {}
        if _analysis_depth(step, ec) == "explain":
            return await _analyse_explain(state, ec, step)
        return await _analyse_diagnose(state, ec, step)

    analysis_node._explain = _analyse_explain   # exposed for parallel_step_runner
    return analysis_node


# ---------------------------------------------------------------------------
# Node: user_node  (CLI/web interrupt)
# ---------------------------------------------------------------------------

def make_user_node():
    async def user_node(state: PeriodCloseState) -> dict:
        step    = _get_step(state)
        step_id = step["step_id"]
        an      = state.get("current_analysis", {})

        await _emit({"type": "action_start", "step_id": step_id, "action": "user_input",
                     "message": "Waiting for operator decision…"})

        # Pass structured dict so web handler can populate the interrupt panel
        interrupt_value = {
            "step_id":          step_id,
            "diagnosis":        an.get("diagnosis", "N/A"),
            "user_instructions": an.get("user_instructions", "N/A"),
            "status":           an.get("status"),
            "result":           an.get("result"),
        }

        # CLI fallback summary (shown when interrupt() prints to stdout)
        _ = (
            f"\n{'='*60}\n"
            f"Step    : {step_id}\n"
            f"Diagnosis: {interrupt_value['diagnosis']}\n"
            f"Fix     : {interrupt_value['user_instructions']}\n"
            f"Options : [1] retry_from_precheck  [2] retry_from_execute  "
            f"[3] skip_step  [4] abort\n{'='*60}\n"
        )

        decision = interrupt(interrupt_value)

        action = decision.get("action", "abort")
        restart_map = {
            "retry_from_precheck": "pre_check",
            "retry_from_execute":  "execute",
            "skip_step":           "next_step",
            "abort":               "abort",
        }
        restart_from = restart_map.get(action, "abort")

        overrides = decision.get("param_overrides")
        updated_analysis = state.get("current_analysis")
        if overrides and updated_analysis:
            updated_analysis = {**updated_analysis, "corrected_params": overrides}

        await _emit({"type": "action_end", "step_id": step_id, "action": "user_input",
                     "status": action, "message": f"Operator chose: {action}"})

        return {
            "user_decision":    decision,
            "restart_from":     restart_from,
            "retry_count":      0,
            "current_analysis": updated_analysis,
            "aborted":          restart_from == "abort",
        }

    return user_node


# ---------------------------------------------------------------------------
# Node: finalize_step_node  (sequential steps only)
# ---------------------------------------------------------------------------

def make_finalize_step_node():
    async def finalize_step_node(state: PeriodCloseState) -> dict:
        step    = _get_step(state)
        step_id = step["step_id"]
        pc = state.get("current_pre_check")
        vr = state.get("current_validate")

        if pc and pc.get("skip_step"):
            final_status = "skipped"
        elif vr:
            final_status = "ok" if vr["verdict"] == "ok" else "failed"
        else:
            final_status = "ok"

        record: StepRecord = {
            "step_id": step_id, "group": step.get("group"),
            "pre_check": pc, "execute": state.get("current_execute"),
            "poll": state.get("current_poll"), "validate": vr,
            "analysis": state.get("current_analysis"), "final_status": final_status,
        }

        logger.info("finalize step=%s status=%s", step_id, final_status)
        await _emit({"type": "step_end", "step_id": step_id, "status": final_status})

        return {
            "step_records":      list(state.get("step_records", [])) + [record],
            "step_index":        state["step_index"] + 1,
            "current_pre_check":  None, "current_execute":  None,
            "current_poll":       None, "current_validate": None,
            "current_analysis":   None, "user_decision":    None, "restart_from": None,
            "current_error_context": None,
            # Clear ReAct history so the next step's diagnose path starts fresh
            # (RemoveMessage(REMOVE_ALL) is the add_messages reducer's reset signal).
            "analysis_messages": [RemoveMessage(id=REMOVE_ALL_MESSAGES)],
        }

    return finalize_step_node


# ---------------------------------------------------------------------------
# Node: parallel_step_runner
# ---------------------------------------------------------------------------

def make_parallel_step_runner(session: ClientSession, llms: dict[str, ChatOpenAI]):
    _pre_check = make_pre_check_node(session, llms)
    _execute   = make_execute_node(session)
    _poll      = make_poll_node(session)
    _validate  = make_validate_node(session, llms)
    _analysis  = make_analysis_node(session, llms)

    async def _maybe_explain(s: dict):
        """Parallel branches have no node routing, so the heavy diagnose+interrupt path
        is unavailable. For explain-depth failures we still run the cheap one-call
        explanation inline and attach it to the result for the operator (post fan_in)."""
        ec = s.get("current_error_context") or {}
        if not ec:
            return None
        step_def = next((st for st in s["steps"] if st["step_id"] == s["current_step_id"]), {})
        if _analysis_depth(step_def, ec) != "explain":
            return None
        out = await _analysis._explain(s, ec, step_def)
        return out.get("current_analysis")

    async def parallel_step_runner(state: PeriodCloseState) -> dict:
        step_id = state["current_step_id"]
        logger.info("parallel_step_runner step=%s", step_id)

        s = {**state, **(await _pre_check(state))}
        pc = s.get("current_pre_check", {})
        if pc.get("skip_step") or pc.get("error"):
            return {"parallel_results": {step_id: {
                "final_status": "skipped" if pc.get("skip_step") else "failed",
                "pre_check": pc, "execute": None, "poll": None, "validate": None,
                "analysis": (await _maybe_explain(s)) if pc.get("error") else None,
            }}}

        s = {**s, **(await _execute(s))}
        execute = s.get("current_execute", {})

        if _meta(execute, "requires_poll"):
            step_def     = next(st for st in s["steps"] if st["step_id"] == step_id)
            poll_timeout = step_def.get("poll_timeout_sec",
                           s.get("global_defaults", {}).get("poll_timeout_sec", 14400))
            while True:
                s = {**s, **(await _poll(s))}
                poll = s.get("current_poll", {})
                if poll.get("sap_status") in ("FINISHED", "ABORTED") or poll.get("timed_out"):
                    break
                if poll.get("elapsed_sec", 0) >= poll_timeout:
                    break

            poll = s.get("current_poll", {})
            if poll.get("sap_status") != "FINISHED":
                return {"parallel_results": {step_id: {
                    "final_status": "failed",
                    "pre_check": pc, "execute": execute, "poll": poll, "validate": None,
                }}}

        s = {**s, **(await _validate(s))}
        vr = s.get("current_validate", {})

        return {"parallel_results": {step_id: {
            "final_status": "ok" if vr.get("verdict") == "ok" else "failed",
            "pre_check": pc, "execute": execute,
            "poll": s.get("current_poll"), "validate": vr,
            "analysis": (await _maybe_explain(s)) if vr.get("verdict") != "ok" else None,
        }}}

    return parallel_step_runner


# ---------------------------------------------------------------------------
# Node: fan_in_node
# ---------------------------------------------------------------------------

def make_fan_in_node():
    async def fan_in_node(state: PeriodCloseState) -> dict:
        parallel_ids = state.get("parallel_step_ids", [])
        results      = state.get("parallel_results", {})
        new_records  = list(state.get("step_records", []))

        for step_id in parallel_ids:
            res    = results.get(step_id, {})
            status = res.get("final_status", "unknown")
            group  = next((s.get("group") for s in state["steps"] if s["step_id"] == step_id), None)
            new_records.append({
                "step_id": step_id, "group": group,
                "pre_check": res.get("pre_check"), "execute": res.get("execute"),
                "poll": res.get("poll"), "validate": res.get("validate"),
                "analysis": res.get("analysis"), "final_status": status,
            })
            await _emit({"type": "step_end", "step_id": step_id, "status": status})

        idx   = state["step_index"]
        steps = state["steps"]
        group = state.get("current_group")
        if group:
            while idx < len(steps) and steps[idx].get("group") == group:
                idx += 1

        logger.info("fan_in group=%s next_idx=%d", group, idx)
        return {
            "step_records": new_records, "step_index": idx,
            "parallel_results": {}, "current_group": None, "parallel_step_ids": [],
            "run_error": None,
        }

    return fan_in_node


# ---------------------------------------------------------------------------
# Routing functions
# ---------------------------------------------------------------------------

def route_after_router(state: PeriodCloseState):
    if state.get("completed"):
        return END
    if state.get("current_group"):
        group = state["current_group"]
        return [Send("parallel_step_runner", {**state, "current_step_id": s["step_id"]})
                for s in state["steps"] if s.get("group") == group]
    return "pre_check"


def route_after_precheck(state: PeriodCloseState) -> str:
    pc = state.get("current_pre_check", {})
    if pc.get("skip_step"):    return "finalize_step"
    if pc.get("passed", True): return "execute"
    return "analysis"          # failed → source-aware analysis_node (explain by default)


def route_after_execute(state: PeriodCloseState) -> str:
    ex = state.get("current_execute", {})
    if ex.get("status") == "error":    return "analysis"
    if _meta(ex, "requires_poll"):     return "poll"
    return "validate"


def route_after_poll(state: PeriodCloseState) -> str:
    st = state.get("current_poll", {}).get("sap_status", "RUNNING")
    if st == "RUNNING":   return "poll"
    if st == "FINISHED":  return "validate"
    return "analysis"


def route_after_validate(state: PeriodCloseState) -> str:
    return "finalize_step" if state.get("current_validate", {}).get("verdict") == "ok" else "analysis"


def route_after_analysis(state: PeriodCloseState) -> str:
    an     = state.get("current_analysis", {})
    action = an.get("action", "user_input")
    defs   = _defaults(state)
    step   = next((s for s in state.get("steps", [])
                   if s["step_id"] == state.get("current_step_id", "")), {})
    max_ret = step.get("max_retries", defs.get("max_retries", 3))
    if action == "retry" and state.get("retry_count", 0) <= max_ret: return "execute"
    if action == "skip": return "finalize_step"
    return "user"


def route_after_user(state: PeriodCloseState) -> str:
    r = state.get("restart_from", "abort")
    if r == "pre_check": return "pre_check"
    if r == "execute":   return "execute"
    if r == "next_step": return "finalize_step"
    return END


def route_after_fanin(state: PeriodCloseState) -> str:
    return "router"


def route_after_finalize(state: PeriodCloseState) -> str:
    return END if state.get("step_index", 0) >= len(state.get("steps", [])) else "router"


# ---------------------------------------------------------------------------
# Graph builder
# ---------------------------------------------------------------------------

def build_graph(
    session: ClientSession,
    llms:    dict[str, ChatOpenAI],
    checkpointer = None,
) -> CompiledStateGraph:
    g = StateGraph(PeriodCloseState)

    g.add_node("router",               make_router_node())
    g.add_node("pre_check",            make_pre_check_node(session, llms))
    g.add_node("execute",              make_execute_node(session))
    g.add_node("poll",                 make_poll_node(session))
    g.add_node("validate",             make_validate_node(session, llms))
    g.add_node("analysis",             make_analysis_node(session, llms))
    g.add_node("user",                 make_user_node())
    g.add_node("finalize_step",        make_finalize_step_node())
    g.add_node("parallel_step_runner", make_parallel_step_runner(session, llms))
    g.add_node("fan_in",               make_fan_in_node())

    g.set_entry_point("router")

    g.add_conditional_edges("router", route_after_router)
    g.add_edge("parallel_step_runner", "fan_in")
    g.add_conditional_edges("fan_in", route_after_fanin, {"router": "router"})

    g.add_conditional_edges("pre_check", route_after_precheck,
        {"execute": "execute", "finalize_step": "finalize_step",
         "analysis": "analysis"})
    g.add_conditional_edges("execute", route_after_execute,
        {"poll": "poll", "validate": "validate", "analysis": "analysis"})
    g.add_conditional_edges("poll", route_after_poll,
        {"poll": "poll", "validate": "validate", "analysis": "analysis"})
    g.add_conditional_edges("validate", route_after_validate,
        {"finalize_step": "finalize_step", "analysis": "analysis"})
    g.add_conditional_edges("analysis", route_after_analysis,
        {"execute": "execute", "finalize_step": "finalize_step", "user": "user"})
    g.add_conditional_edges("user", route_after_user,
        {"pre_check": "pre_check", "execute": "execute",
         "finalize_step": "finalize_step", END: END})
    g.add_conditional_edges("finalize_step", route_after_finalize,
        {"router": "router", END: END})

    return g.compile(checkpointer=checkpointer)


# ---------------------------------------------------------------------------
# Shared run helpers
# ---------------------------------------------------------------------------

def _make_thread_id(company_config: dict, defaults: dict) -> str:
    """Checkpoint thread id for a run.

    Production (default): deterministic `period_close_<year>_<period>` so a crashed
    run can be resumed by re-running with the same id, and a finished close cannot be
    re-run by accident.

    Dev/testing: set `reset_each_run: true` in defaults (base config or a company
    delta, or env RESET_EACH_RUN=1) to append a random suffix, giving every run a
    clean checkpoint so the same period can be re-run as many times as needed.
    """
    base = (f"period_close_"
            f"{company_config.get('fiscal_year', 'X')}_"
            f"{company_config.get('period', 'X')}")
    return f"{base}_{uuid4().hex[:8]}" if reset_each_run_enabled(defaults) else base


def _build_initial_state(period_cfg: dict, steps: list[dict],
                         start_step_index: int = 0) -> PeriodCloseState:
    company_config = period_cfg.get("company_config", {})
    return {
        "company_config":    company_config,
        "steps":             steps,
        "global_defaults":   period_cfg.get("defaults", {}),
        "analysis_defaults": period_cfg.get("analysis_defaults", {}),
        "step_index":        max(0, start_step_index),
        "current_group":     None,
        "parallel_step_ids": [],
        "current_step_id":   "",
        "current_pre_check":  None,
        "current_execute":    None,
        "current_poll":       None,
        "current_validate":   None,
        "current_analysis":   None,
        "current_error_context": None,
        "retry_count":        0,
        "parallel_results":   {},
        "step_records":       [],
        "analysis_messages":  [],
        "user_decision":      None,
        "restart_from":       None,
        "completed":          False,
        "escalated":          False,
        "aborted":            False,
        "run_error":          None,
    }


def _build_cm(mcp_cfg: dict):
    server_cfg = mcp_cfg.get("server", {})
    command    = server_cfg.get("command", "python")
    if command == "sys.executable":
        command = sys.executable
    transport = server_cfg.get("transport", "stdio")
    if transport == "stdio":
        env = dict(os.environ) | (server_cfg.get("env") or {})
        # Spawn the MCP server with its working directory pinned to this package
        # (src/), so a relative `mcp_server.py` arg resolves regardless of where
        # uvicorn/CLI was launched from.
        cwd = server_cfg.get("cwd") or str(Path(__file__).parent)
        return stdio_client(StdioServerParameters(
            command=command,
            args=server_cfg.get("args", []),
            env=env,
            cwd=cwd,
        ))
    return sse_client(server_cfg["sse_url"])


async def _run_with_interrupts(
    graph: CompiledStateGraph,
    initial_state: dict,
    thread_config: dict,
    get_decision,          # async callable () -> dict
) -> dict:
    """Run graph; call get_decision() at each interrupt and resume."""
    await graph.ainvoke(initial_state, config=thread_config)
    while True:
        snapshot = await graph.aget_state(thread_config)
        if not snapshot.next:
            break
        for task in snapshot.tasks:
            for intr in getattr(task, "interrupts", []):
                iv = intr.value
                if isinstance(iv, dict):
                    await _emit({"type": "interrupt",
                                 "step_id":          iv.get("step_id", ""),
                                 "diagnosis":         iv.get("diagnosis", ""),
                                 "user_instructions": iv.get("user_instructions", ""),
                                 "status":            iv.get("status"),
                                 "result":            iv.get("result")})
                else:
                    await _emit({"type": "interrupt", "message": str(iv)})
        decision = await get_decision()
        await graph.ainvoke(Command(resume=decision), config=thread_config)
    snapshot = await graph.aget_state(thread_config)
    return snapshot.values if snapshot else {}


# ---------------------------------------------------------------------------
# Rollback helpers
# ---------------------------------------------------------------------------

async def execute_rollback_step(
    session: ClientSession,
    step_cfg: dict,
    company_config: dict,
    defaults: dict,
) -> dict:
    """Execute the rollback action for one step. Returns {status}."""
    step_id = step_cfg["step_id"]
    rb = step_cfg.get("rollback", {})

    if not rb.get("enabled", False):
        await _emit({"type": "rollback_step_end", "step_id": step_id,
                     "status": "skipped", "message": "No rollback configured"})
        return {"status": "skipped"}

    action_type  = rb.get("action_type", step_cfg.get("action_type", "SUBMIT"))
    object_name  = rb.get("object_name", step_cfg.get("object_name", ""))
    params       = _resolve(rb.get("params", []), company_config)
    async_mode   = rb.get("async", False)
    test_run     = rb.get("test_run", defaults.get("test_run", True))
    poll_interval = rb.get("poll_interval_sec", defaults.get("poll_interval_sec", 30))
    poll_timeout  = rb.get("poll_timeout_sec",  defaults.get("poll_timeout_sec", 3600))

    await _emit({"type": "rollback_step_start", "step_id": step_id,
                 "message": f"Rolling back {action_type}/{object_name}…"})
    try:
        env = await _run_action(session, action_type, object_name, params,
                                async_mode=async_mode, test_run=test_run)

        if async_mode and _meta(env, "requires_poll"):
            job_name = _meta(env, "job_name", "")
            job_id   = _meta(env, "job_id", "")
            elapsed  = 0.0
            while elapsed < poll_timeout:
                # Poll first so stub/already-finished jobs don't wait a full interval.
                pr = await session.call_tool("sap_job_status",
                                             arguments={"job_name": job_name, "job_id": job_id})
                pd = _parse_tool_result(pr)
                sap_st = (pd.get("data") or {}).get("state", "RUNNING")
                if sap_st == "FINISHED":
                    env["status"] = "ok"
                    break
                elif sap_st == "ABORTED":
                    env["status"] = "error"
                    break
                await asyncio.sleep(poll_interval)
                elapsed += poll_interval

        has_error = env["status"] == "error"
        status    = "error" if has_error else "ok"
        msg = ("; ".join(m.get("MESSAGE", "") for m in env["messages"])[:200]
               or ("Rollback failed" if has_error else "Rollback completed"))
        await _emit({"type": "rollback_step_end", "step_id": step_id,
                     "status": status, "message": msg})
        return {"status": status}

    except Exception as exc:
        msg = str(exc)[:200]
        logger.error("Rollback error step=%s: %s", step_id, exc)
        await _emit({"type": "rollback_step_end", "step_id": step_id,
                     "status": "error", "message": msg})
        return {"status": "error"}


def _resolve_period_cfg(period_config_path, company_code, period, fiscal_year):
    """Effective config keyed by company code (base ⊕ SQLite delta) when a code is given;
    legacy file-based load_config otherwise (only for callers that still pass a real
    base+company YAML pair, e.g. tests with tmp_path fixtures)."""
    if company_code:
        from config_store import load_effective_config
        return load_effective_config(company_code, period=period, fiscal_year=fiscal_year)
    if not period_config_path:
        raise ValueError("run requires either company_code (DB-backed) or an explicit period_config_path")
    return load_config(period_config_path, period=period, fiscal_year=fiscal_year)


async def run_rollback_and_restart_web(
    send,
    msg_queue:          asyncio.Queue,
    period_config_path: str | None = None,
    mcp_config_path:    str = "configs/mcp_config.yaml",
    start_from_step:    str = "",
    steps_to_rollback:  list[str] | None = None,
    thread_id:          str | None = None,
    period:             str | None = None,
    fiscal_year:        str | None = None,
    company_code:       str | None = None,
) -> None:
    """Roll back listed steps (in reverse order) then restart the graph from start_from_step."""
    global _event_queue
    _event_queue = asyncio.Queue()

    async def _relay():
        while True:
            event = await _event_queue.get()
            if event is None:
                break
            await send(event)

    relay_task = asyncio.create_task(_relay())

    try:
        period_cfg  = _resolve_period_cfg(period_config_path, company_code, period, fiscal_year)
        mcp_cfg     = _load_yaml(mcp_config_path)
        company_config = period_cfg["company_config"]
        steps_raw      = period_cfg["steps"]
        steps          = _resolve(steps_raw, company_config)
        defaults       = period_cfg.get("defaults", {})

        steps_meta = [
            {"step_id": s["step_id"], "description": s.get("description", s["step_id"]),
             "group": s.get("group"), "action_type": s.get("action_type", ""),
             "async": s.get("async", False)}
            for s in steps
        ]
        await _emit({"type": "run_init", "steps": steps_meta, "company_config": company_config,
                     "is_rollback": True, "test_mode": reset_each_run_enabled(defaults)})

        steps_to_rb = steps_to_rollback or []
        step_map    = {s["step_id"]: s for s in steps}

        await _emit({"type": "rollback_start", "steps": steps_to_rb,
                     "start_from": start_from_step,
                     "total": len(steps_to_rb)})

        async with _build_cm(mcp_cfg) as (read, write):
            async with ClientSession(read, write) as session:
                await session.initialize()

                for step_id in reversed(steps_to_rb):
                    step_cfg = step_map.get(step_id)
                    if step_cfg:
                        await execute_rollback_step(session, step_cfg, company_config, defaults)

                await _emit({"type": "rollback_end", "status": "ok",
                             "message": f"{len(steps_to_rb)} step(s) rolled back"})

                start_step_index = next(
                    (i for i, s in enumerate(steps) if s["step_id"] == start_from_step), 0
                )
                llms         = build_llms_from_config(period_cfg)
                checkpointer = await build_async_checkpointer()
                if thread_id is None:
                    thread_id = _make_thread_id(company_config, period_cfg.get("defaults", {}))
                thread_config = {"configurable": {"thread_id": thread_id}}

                await _emit({"type": "run_start", "company_config": company_config,
                             "total": len(steps),
                             "start_from_step": start_from_step})

                graph         = build_graph(session, llms, checkpointer)
                initial_state = _build_initial_state(period_cfg, steps, start_step_index)

                async def web_decision() -> dict:
                    while True:
                        msg = await msg_queue.get()
                        if msg.get("type") == "decision":
                            return {"action": msg.get("action", "abort"),
                                    "param_overrides": msg.get("param_overrides")}

                final_state = await _run_with_interrupts(graph, initial_state, thread_config, web_decision)

        run_status = "aborted" if final_state.get("aborted") else "completed"
        await _emit({"type": "run_end", "status": run_status})

    except Exception as exc:
        logger.error("Rollback+restart failed: %s", exc, exc_info=True)
        await _emit({"type": "run_end", "status": "error", "message": str(exc)})
    finally:
        await _event_queue.put(None)
        await relay_task
        _event_queue = None


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

async def run_period_close(
    company_code:       str | None = None,
    period_config_path: str | None = None,
    mcp_config_path:    str = "configs/mcp_config.yaml",
    thread_id:          str | None = None,
    period:             str | None = None,
    fiscal_year:        str | None = None,
) -> dict:
    """CLI run. Pass `company_code` to load base ⊕ delta from config_store.db (the normal
    case — companies are managed in the admin UI), or `period_config_path` to load a
    standalone base+company YAML pair (tests / one-off files)."""
    period_cfg  = _resolve_period_cfg(period_config_path, company_code, period, fiscal_year)
    mcp_cfg     = _load_yaml(mcp_config_path)
    company_config = period_cfg["company_config"]
    steps          = _resolve(period_cfg["steps"], company_config)
    llms           = build_llms_from_config(period_cfg)
    checkpointer   = await build_async_checkpointer()
    if thread_id is None:
        thread_id = _make_thread_id(company_config, period_cfg.get("defaults", {}))
    thread_config = {"configurable": {"thread_id": thread_id}}

    async with _build_cm(mcp_cfg) as (read, write):
        async with ClientSession(read, write) as session:
            await session.initialize()
            graph = build_graph(session, llms, checkpointer)
            initial_state = _build_initial_state(period_cfg, steps)
            logger.info("Starting period close CLI run — %d steps", len(steps))

            async def cli_decision() -> dict:
                choice_map = {"1": "retry_from_precheck", "2": "retry_from_execute",
                              "3": "skip_step",           "4": "abort"}
                choice = input("Choice [1-4]: ").strip()
                action = choice_map.get(choice, "abort")
                param_overrides = None
                if action in ("retry_from_precheck", "retry_from_execute"):
                    raw = input("Param overrides JSON (or Enter): ").strip()
                    if raw:
                        try: param_overrides = json.loads(raw)
                        except json.JSONDecodeError: pass
                return {"action": action, "param_overrides": param_overrides}

            final_state = await _run_with_interrupts(graph, initial_state, thread_config, cli_decision)

    _log_summary(final_state)
    return final_state


# ---------------------------------------------------------------------------
# Web entry point  (called by web_app.py)
# ---------------------------------------------------------------------------

async def run_period_close_web(
    send,                        # async callable: send(event_dict) → WebSocket
    msg_queue:  asyncio.Queue,   # messages from browser: start / decision
    period_config_path: str | None = None,
    mcp_config_path:    str = "configs/mcp_config.yaml",
    thread_id:  str | None = None,
    period:     str | None = None,
    fiscal_year: str | None = None,
    company_code: str | None = None,
) -> None:
    global _event_queue
    _event_queue = asyncio.Queue()

    async def _relay():
        while True:
            event = await _event_queue.get()
            if event is None:
                break
            await send(event)

    relay_task = asyncio.create_task(_relay())

    try:
        period_cfg  = _resolve_period_cfg(period_config_path, company_code, period, fiscal_year)
        logger.info("effective config for %s keys: %s", company_code or period_config_path, list(period_cfg.keys()))
        mcp_cfg     = _load_yaml(mcp_config_path)
        company_config = period_cfg["company_config"]
        steps_raw      = period_cfg["steps"]
        steps          = _resolve(steps_raw, company_config)

        # Send step list to browser immediately (before run starts)
        steps_meta = [
            {
                "step_id":     s["step_id"],
                "description": s.get("description", s["step_id"]),
                "group":       s.get("group"),
                "action_type": s.get("action_type", ""),
                "async":       s.get("async", False),
            }
            for s in steps
        ]
        await _emit({"type": "run_init", "steps": steps_meta, "company_config": company_config,
                     "test_mode": reset_each_run_enabled(period_cfg.get("defaults", {}))})

        # Wait for start signal — optionally includes start_from_step
        start_msg: dict = {}
        while True:
            msg = await msg_queue.get()
            if msg.get("type") == "start":
                start_msg = msg
                break

        # Resolve start_from_step → step_index
        start_step_index = 0
        start_from = start_msg.get("start_from_step")
        if start_from:
            ids = [s["step_id"] for s in steps]
            if start_from in ids:
                start_step_index = ids.index(start_from)
                logger.info("Starting from step %s (index %d)", start_from, start_step_index)
            else:
                logger.warning("start_from_step %r not found — starting from step 0", start_from)

        llms         = build_llms_from_config(period_cfg)
        checkpointer = await build_async_checkpointer()
        if thread_id is None:
            thread_id = _make_thread_id(company_config, period_cfg.get("defaults", {}))
        logger.info("Web run thread_id=%s", thread_id)
        thread_config = {"configurable": {"thread_id": thread_id}}

        await _emit({"type": "run_start", "company_config": company_config, "total": len(steps),
                     "start_from_step": start_from or steps[0]["step_id"]})

        async with _build_cm(mcp_cfg) as (read, write):
            async with ClientSession(read, write) as session:
                await session.initialize()
                graph         = build_graph(session, llms, checkpointer)
                initial_state = _build_initial_state(period_cfg, steps, start_step_index)

                async def web_decision() -> dict:
                    while True:
                        msg = await msg_queue.get()
                        if msg.get("type") == "decision":
                            return {
                                "action":          msg.get("action", "abort"),
                                "param_overrides": msg.get("param_overrides"),
                            }

                final_state = await _run_with_interrupts(graph, initial_state, thread_config, web_decision)

        run_status = "aborted" if final_state.get("aborted") else "completed"
        await _emit({"type": "run_end", "status": run_status})

    except Exception as exc:
        logger.error("Web run failed: %s", exc, exc_info=True)
        await _emit({"type": "run_end", "status": "error", "message": str(exc)})
    finally:
        await _event_queue.put(None)
        await relay_task
        _event_queue = None


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------

def _log_summary(state: dict) -> None:
    logger.info("=" * 60)
    status = ("COMPLETED" if state.get("completed")
              else "ESCALATED" if state.get("escalated")
              else "ABORTED"   if state.get("aborted")
              else "ERROR")
    logger.info("Period close %s", status)
    for r in state.get("step_records", []):
        logger.info("  [%s] %s", r["final_status"].upper(), r["step_id"])
    logger.info("=" * 60)


def _load_yaml(path: str) -> dict:
    p = Path(path)
    if not p.is_absolute():
        p = Path(__file__).parent / p
    if not p.exists():
        raise FileNotFoundError(f"Config not found: {path}")
    with open(p) as f:
        return yaml.safe_load(f)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import asyncio
    import sys
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    if len(sys.argv) < 2:
        sys.exit("Usage: python graph_builder.py <COMPANY_CODE>  "
                  "(companies are managed in the admin UI → Settings → Manage companies)")
    asyncio.run(run_period_close(company_code=sys.argv[1]))
