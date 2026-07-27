"""
MCP server for the SAP Period Close Agent.

Exposes SAP tools to the LangGraph agent via MCP protocol.
Run standalone (stdio transport): python mcp_server.py
Run with SSE transport:           MCP_TRANSPORT=sse python mcp_server.py

All SAP calls go through ZFI_AI_PERIOD_CLOSE_RFC — the agent process
never imports pyrfc or SAPConnectionManager directly.
"""

from __future__ import annotations

import json
import os
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

import yaml
from dotenv import load_dotenv
from mcp.server.fastmcp import FastMCP

load_dotenv(Path(__file__).parent / ".env", override=True)

from logging_config import init_logging
from sap_connection_manager import SAPConnectionManager, SAPConnectionParams

import logging

RFC = "ZFI_AI_PERIOD_CLOSE_RFC"

init_logging()
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Config and connection bootstrap (done once at server startup)
# ---------------------------------------------------------------------------

def _load_config() -> dict:
    p = Path(__file__).parent / "configs" / "mcp_config.yaml"
    if not p.exists():
        raise FileNotFoundError(f"MCP config not found: {p}")
    with open(p) as f:
        return yaml.safe_load(f)


def _get_sap_params() -> SAPConnectionParams:
    return SAPConnectionParams(
        ashost          = os.environ.get("SAP_HOST"),
        sysnr           = os.environ.get("SAP_SYSNR"),
        client          = os.environ.get("SAP_CLIENT"),
        user            = os.environ.get("SAP_USER"),
        passwd          = os.environ.get("SAP_PASSWORD"),
        lang            = os.environ.get("SAP_LANG", "EN"),
        snc_mode        = os.environ.get("SAP_SNC_MODE"),
        snc_myname      = os.environ.get("SAP_SNC_MYNAME"),
        snc_partnername = os.environ.get("SAP_SNC_PARTNERNAME"),
    )


cfg = _load_config()

conn_mgr = SAPConnectionManager()
conn_mgr.configure(_get_sap_params())

logger.info("MCP server starting — SAP host=%s", os.environ.get("SAP_HOST"))


# ---------------------------------------------------------------------------
# Lifespan
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(server: FastMCP):
    logger.info("MCP server lifespan: startup")
    yield
    logger.info("MCP server lifespan: shutdown — closing SAP connection")
    conn_mgr.close()


mcp = FastMCP(name="sap-period-close", lifespan=lifespan)


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------

def _rfc(conn, action_type: str, object_name: str, params: dict,
         async_mode: bool = False, test_run: bool = False) -> dict:
    """Call ZFI_AI_PERIOD_CLOSE_RFC and return a normalised result dict."""
    rfc_result = conn.call(
        RFC,
        IV_ACTION_TYPE = action_type,
        IV_OBJECT_NAME = object_name,
        IV_PARAMS_JSON = json.dumps(params, separators=(',', ':')),
        IV_ASYNC       = "X" if async_mode else "",
        IV_TEST_RUN    = "X" if test_run else "",
    )
    return {
        "status":      rfc_result.get("EV_STATUS", ""),
        "result_json": json.loads(rfc_result.get("EV_RESULT_JSON", "{}")),
        "messages":    rfc_result.get("ET_MESSAGES", []),
    }


# ---------------------------------------------------------------------------
# Unified response envelope
#
# Every MCP tool returns exactly four top-level keys:
#   status   — outcome of THIS call: "ok" | "error" | "submitted"
#   messages — SAP messages (BAPIRET-style: TYPE/MESSAGE/…)
#   meta     — derived / control fields (job ids, requires_poll, echoes)
#   data     — verbatim RFC EV_RESULT_JSON payload (spool/rows live here, once)
# ---------------------------------------------------------------------------

def _envelope(status: str, messages: list | None = None,
              meta: dict | None = None, data: dict | None = None) -> dict:
    """Build the canonical {status, messages, meta, data} response."""
    return {
        "status":   status,
        "messages": messages or [],
        "meta":     meta or {},
        "data":     data or {},
    }


def _norm_spool(raw: Any) -> dict | None:
    """Normalise an ABAP spool object to {available, line_count, text}.

    ABAP returns {"status": "S"/"E", "text": "..."}. Returns None when no spool
    object is present (e.g. async submit, FM/BAPI). ``available`` means the report
    ran and its spool was readable (status == "S").
    """
    if not isinstance(raw, dict):
        return None
    text = raw.get("text", "")
    return {
        "available":  raw.get("status") == "S",
        "line_count": len(text.splitlines()),
        "text":       text,
    }


# ---------------------------------------------------------------------------
# Tool: sap_check_period
# Pre-check RFC call — used by pre_check_node for all modes
# ---------------------------------------------------------------------------

@mcp.tool()
def sap_check_period(
    action_type: str,
    object_name: str,
    params_json: str,
    async_mode:  bool = False,
    test_run:    bool = True
) -> dict:
    """
    Run a period-close pre-check RFC call and return the raw result.

    Parameters
    ----------
    action_type : RFC action type, e.g. "TOOLS"
    object_name : RFC object, e.g. "TOOL_READ_TABLE"
    params_json : JSON-encoded params dict specific to the action_type

    Returns
    -------
    {"status": "ok"|"error", "messages": list,
     "meta": {"action_type", "object_name"}, "data": <raw RFC payload>}
    """
    conn = conn_mgr.get_connection()
    params = json.loads(params_json) if isinstance(params_json, str) else params_json
    result = _rfc(conn, action_type, object_name, params, async_mode, test_run)
    has_errors = any(m.get("TYPE") in ("E", "A") for m in result["messages"]) \
                 or result["status"] == "E"
    status = "error" if has_errors else "ok"
    logger.info("sap_check_period action=%s object=%s status=%s",
                action_type, object_name, status)
    return _envelope(
        status,
        messages = result["messages"],
        meta     = {"action_type": action_type, "object_name": object_name},
        data     = result["result_json"],
    )


# ---------------------------------------------------------------------------
# Tool: sap_execute_step
# Submit / run a single period-close step — returns immediately for async
# ---------------------------------------------------------------------------

@mcp.tool()
def sap_execute_step(
    action_type: str,
    object_name: str,
    params_json: str,
    async_mode:  bool = False,
    test_run:    bool = False,
) -> dict:
    """
    Execute one period-close step via ZFI_AI_PERIOD_CLOSE_RFC.

    For SUBMIT with async_mode=True the job is started and the call returns
    immediately with meta.requires_poll=True so the graph can poll via sap_job_status.

    Returns
    -------
    The unified envelope {status, messages, meta, data} where
        status — "ok" | "error" | "submitted"
        meta   — {"action_type", "requires_poll", "job_name", "job_id"}
        data   — raw RFC payload; for an inline-wait SUBMIT the spool is normalised
                 in place as data["spool"] = {available, line_count, text}
    """
    conn = conn_mgr.get_connection()
    params = json.loads(params_json) if isinstance(params_json, str) else params_json

    result = _rfc(conn, action_type, object_name, params, async_mode=async_mode, test_run=test_run)
    messages    = result["messages"]
    result_json = result["result_json"]

    logger.info("sap_execute_step action=%s object=%s async=%s test=%s",
                action_type, object_name, async_mode, test_run)

    def _meta(requires_poll: bool = False, job_name: str = "", job_id: str = "") -> dict:
        return {"action_type": action_type, "requires_poll": requires_poll,
                "job_name": job_name, "job_id": job_id}

    if action_type in ("FM", "BAPI", "BDC"):
        has_errors = any(m.get("TYPE") in ("E", "A") for m in messages)
        return _envelope("error" if has_errors else "ok",
                         messages=messages, meta=_meta(), data=result_json)

    if action_type == "SUBMIT":
        job_name = result_json.get("jobname", "")
        job_id   = result_json.get("jobcount", "")

        # ABAP runs SUBMIT as a background job either way. With async_mode=True it
        # returns EV_STATUS='A' (lc_async) and the caller polls. With async_mode=False
        # it waits inline and returns EV_STATUS='S'/'E' plus the spool inline under
        # result_json["spool"] (mode="sync_wait") — no separate sap_read_spool needed.
        is_async = (result["status"] == "A")

        if is_async:
            if not job_id:
                return _envelope(
                    "error",
                    messages=messages + [{"TYPE": "E", "MESSAGE": "No job ID returned"}],
                    meta=_meta())
            logger.info("SUBMIT submitted (async): job=%s/%s", job_name, job_id)
            return _envelope(
                "submitted",
                messages=messages,
                meta=_meta(requires_poll=True, job_name=job_name, job_id=job_id),
                data=result_json)

        # Inline-wait outcome: normalise the spool in place inside data.
        spool = _norm_spool(result_json.get("spool"))
        if spool is not None:
            result_json["spool"] = spool
        has_errors = (result["status"] == "E") or any(m.get("TYPE") in ("E", "A") for m in messages)
        logger.info("SUBMIT completed (inline-wait): job=%s/%s lines=%d status=%s",
                    job_name, job_id, (spool or {}).get("line_count", 0), result["status"])
        return _envelope(
            "error" if has_errors else "ok",
            messages=messages,
            meta=_meta(job_name=job_name, job_id=job_id),
            data=result_json)

    # Unknown action_type
    return _envelope(
        "error",
        messages=[{"TYPE": "E", "MESSAGE": f"Unknown action_type: {action_type}"}],
        meta=_meta())


# ---------------------------------------------------------------------------
# Tool: sap_run_tool
# Generic passthrough for the config-driven action path (execute / pre_check /
# validate.run). object_name selects the ABAP WHEN branch; params travel verbatim
# as iv_params_json. Adding a new client tool needs only the ABAP handler — no
# change here. (The typed sap_read_table / sap_read_spool / sap_job_status tools
# stay: they are the diagnostic tools the analysis ReAct agent calls by name.)
# ---------------------------------------------------------------------------

@mcp.tool()
def sap_run_tool(object_name: str, params_json: str, test_run: bool = False) -> dict:
    """
    Run any TOOLS-type SAP tool and normalise its result generically.

    The raw RFC payload is normalised so downstream row/spool consumers work for
    any tool without per-tool knowledge:
        data.rows  — present when the tool returns {"data": [ ... ]}   (table-like)
        data.spool — present when the tool returns {"status","text"}   (spool-like)
        data.raw   — the verbatim RFC payload, always (custom tools stay reachable)

    Returns the unified envelope {status, messages, meta, data} where
        meta — {"object_name", "row_count"}
    """
    conn = conn_mgr.get_connection()
    params = json.loads(params_json) if isinstance(params_json, str) else (params_json or {})
    result = _rfc(conn, "TOOLS", object_name, params, test_run=test_run)
    raw = result["result_json"]

    data: dict = {"raw": raw}
    if isinstance(raw, dict):
        if isinstance(raw.get("data"), list):
            data["rows"] = raw["data"]
        if "text" in raw:
            data["spool"] = _norm_spool(raw)

    has_errors = any(m.get("TYPE") in ("E", "A") for m in result["messages"]) \
                 or result["status"] == "E"
    logger.info("sap_run_tool object=%s status=%s rows=%d spool=%s",
                object_name, result["status"],
                len(data.get("rows", [])), "spool" in data)
    return _envelope(
        "error" if has_errors else "ok",
        messages = result["messages"],
        meta     = {"object_name": object_name,
                    "row_count": len(data["rows"]) if "rows" in data else 0},
        data     = data,
    )


# ---------------------------------------------------------------------------
# Tool: sap_read_table
# Direct diagnostic table read — used by analysis_node
# ---------------------------------------------------------------------------

@mcp.tool()
def sap_read_table(table: str, where: str, fields: str, max_rows: int = 100) -> dict:
    """
    Read rows from any SAP transparent table via RFC_READ_TABLE.

    Returns
    -------
    The unified envelope {status, messages, meta, data} where
        meta — {"table", "row_count"}
        data — {"rows": [{"FIELD": "value"}, ...]}
    """
    conn = conn_mgr.get_connection()
    result = _rfc(conn, "TOOLS", "TOOL_READ_TABLE", {
        "table":    table,
        "where":    where,
        "fields":   fields,
        "max_rows": max_rows,
    })
    rows = result["result_json"].get("data", [])
    logger.info("sap_read_table table=%s rows=%d", table, len(rows))
    return _envelope(
        "error" if result["status"] == "E" else "ok",
        messages = result["messages"],
        meta     = {"table": table, "row_count": len(rows)},
        data     = {"rows": rows},
    )


# ---------------------------------------------------------------------------
# Tool: sap_job_status
# Single-shot job status check — polling loop lives in graph's poll_node
# ---------------------------------------------------------------------------

@mcp.tool()
def sap_job_status(job_name: str, job_id: str) -> dict:
    """
    Check the current status of a SAP background job (single shot, non-blocking).

    SAP states returned by SHOW_JOBSTATE:
      SCHEDULED | READY | RUNNING → job still in progress
      FINISHED                    → completed successfully
      ABORTED                     → failed

    Returns
    -------
    The unified envelope {status, messages, meta, data} where
        status — "ok" | "error" (outcome of the status check itself)
        meta   — {"job_name", "job_id"}
        data   — {"state", "is_running", "is_finished", "is_aborted"}
    """
    conn = conn_mgr.get_connection()
    result = _rfc(conn, "TOOLS", "TOOL_JOB_STATUS", {
        "jobname":  job_name,
        "jobcount": job_id,
    })
    state = result["result_json"].get("state", "SCHEDULED")

    logger.info("sap_job_status job=%s/%s state=%s", job_name, job_id, state)
    return _envelope(
        "error" if result["status"] == "E" else "ok",
        messages = result["messages"],
        meta     = {"job_name": job_name, "job_id": job_id},
        data     = {
            "state":       state,
            "is_running":  state in ("SCHEDULED", "READY", "RUNNING"),
            "is_finished": state == "FINISHED",
            "is_aborted":  state == "ABORTED",
        },
    )


# ---------------------------------------------------------------------------
# Tool: sap_read_spool
# Read spool output of a completed background job
# ---------------------------------------------------------------------------

@mcp.tool()
def sap_read_spool(job_name: str, job_id: str, max_lines: int = 500) -> dict:
    """
    Read the spool output of a completed SAP background job.

    Returns
    -------
    The unified envelope {status, messages, meta, data} where
        meta — {"job_name", "job_id", "line_count", "truncated"}
        data — {"spool": {available, line_count, text}}
    """
    conn = conn_mgr.get_connection()
    result = _rfc(conn, "TOOLS", "TOOL_READ_JOB_SPOOL", {
        "jobname":  job_name,
        "jobcount": job_id,
    })

    # ABAP (ztool_read_job_spool) returns {"status","text"} where text is the
    # newline-joined spool text. Normalise it, then trim to max_lines.
    spool = _norm_spool(result["result_json"]) or {"available": False, "line_count": 0, "text": ""}
    lines     = spool["text"].splitlines()
    truncated = len(lines) > max_lines
    trimmed   = lines[:max_lines]
    spool     = {"available": spool["available"], "line_count": len(trimmed),
                 "text": "\n".join(trimmed)}

    logger.info("sap_read_spool job=%s/%s lines=%d truncated=%s",
                job_name, job_id, len(trimmed), truncated)
    return _envelope(
        "error" if result["status"] == "E" else "ok",
        messages = result["messages"],
        meta     = {"job_name": job_name, "job_id": job_id,
                    "line_count": len(trimmed), "truncated": truncated},
        data     = {"spool": spool},
    )


# ---------------------------------------------------------------------------
# Tool: sap_read_job_log
# Read the SM37 runtime job log of a background job (abort reason lives here)
# ---------------------------------------------------------------------------

@mcp.tool()
def sap_read_job_log(job_name: str, job_id: str, max_lines: int = 500) -> dict:
    """
    Read the runtime job log (SM37 "Job log") of a SAP background job.

    Unlike the spool (the report's list output), the job log holds the job's
    own runtime messages — including the abort reason and the last messages
    before an ABORTED job terminated — so it is the primary artefact for
    diagnosing a job that aborted or timed out during polling. Keyed only by
    job_name/job_id, which the caller already holds; no log-name lookup needed.

    Returns
    -------
    The unified envelope {status, messages, meta, data} where
        meta — {"job_name", "job_id", "line_count", "truncated"}
        data — {"joblog": {available, line_count, text}}
    """
    conn = conn_mgr.get_connection()
    result = _rfc(conn, "TOOLS", "TOOL_READ_JOB_LOG", {
        "jobname":  job_name,
        "jobcount": job_id,
    })

    # ABAP (ztool_read_job_log) returns the same {"status","text"} shape as the
    # spool handler, so it normalises identically. Then trim to max_lines.
    joblog    = _norm_spool(result["result_json"]) or {"available": False, "line_count": 0, "text": ""}
    lines     = joblog["text"].splitlines()
    truncated = len(lines) > max_lines
    trimmed   = lines[:max_lines]
    joblog    = {"available": joblog["available"], "line_count": len(trimmed),
                 "text": "\n".join(trimmed)}

    logger.info("sap_read_job_log job=%s/%s lines=%d truncated=%s",
                job_name, job_id, len(trimmed), truncated)
    return _envelope(
        "error" if result["status"] == "E" else "ok",
        messages = result["messages"],
        meta     = {"job_name": job_name, "job_id": job_id,
                    "line_count": len(trimmed), "truncated": truncated},
        data     = {"joblog": joblog},
    )


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    transport = cfg.get("server", {}).get("transport", "stdio")
    logger.info("Starting MCP server transport=%s", transport)
    mcp.run(transport=transport)
