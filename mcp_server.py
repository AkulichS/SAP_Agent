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

import yaml
from mcp.server.fastmcp import FastMCP

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
    p = Path("mcp_config.yaml")
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
        IV_PARAMS_JSON = json.dumps(params),
        IV_ASYNC       = "X" if async_mode else "",
        IV_TEST_RUN    = "X" if test_run else "",
    )
    return {
        "status":      rfc_result.get("EV_STATUS", ""),
        "result_json": json.loads(rfc_result.get("EV_RESULT_JSON", "{}")),
        "messages":    rfc_result.get("ET_MESSAGES", []),
    }


# ---------------------------------------------------------------------------
# Tool: sap_check_period
# Pre-check RFC call — used by pre_check_node for all modes
# ---------------------------------------------------------------------------

@mcp.tool()
def sap_check_period(action_type: str, object_name: str, params_json: str) -> dict:
    """
    Run a period-close pre-check RFC call and return the raw result.

    Parameters
    ----------
    action_type : RFC action type, e.g. "TOOLS"
    object_name : RFC object, e.g. "TOOL_READ_TABLE"
    params_json : JSON-encoded params dict specific to the action_type

    Returns
    -------
    {"status": "S"|"E"|"W", "result_json": dict, "messages": list}
    """
    conn = conn_mgr.get_connection()
    params = json.loads(params_json) if isinstance(params_json, str) else params_json
    result = _rfc(conn, action_type, object_name, params)
    has_errors = any(m.get("TYPE") in ("E", "A") for m in result["messages"])
    result["status"] = "E" if has_errors else result["status"] or "S"
    logger.info("sap_check_period action=%s object=%s status=%s",
                action_type, object_name, result["status"])
    return result


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
    {"table": str, "count": int, "rows": [{"FIELD": "value"}], "status": str}
    """
    conn = conn_mgr.get_connection()
    result = _rfc(conn, "TOOLS", "TOOL_READ_TABLE", {
        "table":    table,
        "where":    where,
        "fields":   fields,
        "max_rows": max_rows,
    })
    rj = result["result_json"]
    rows = rj.get("rows", [])
    logger.info("sap_read_table table=%s rows=%d", table, len(rows))
    return {
        "table":  table,
        "count":  len(rows),
        "rows":   rows,
        "status": result["status"],
    }


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
    test_run:    bool = True,
) -> dict:
    """
    Execute one period-close step via ZFI_AI_PERIOD_CLOSE_RFC.

    For SUBMIT with async_mode=True the job is started and the call returns
    immediately with requires_poll=True so the graph can poll via sap_job_status.

    Returns
    -------
    {
        "status":        "ok"|"error"|"submitted",
        "requires_poll": bool,
        "job_name":      str,
        "job_id":        str,
        "messages":      list,
        "result_json":   dict,
    }
    """
    conn = conn_mgr.get_connection()
    params = json.loads(params_json) if isinstance(params_json, str) else params_json

    result = _rfc(conn, action_type, object_name, params,
                  async_mode=async_mode, test_run=test_run)
    messages    = result["messages"]
    result_json = result["result_json"]

    logger.info("sap_execute_step action=%s object=%s async=%s test=%s",
                action_type, object_name, async_mode, test_run)

    if action_type in ("FM", "BAPI", "BDC"):
        has_errors = any(m.get("TYPE") in ("E", "A") for m in messages)
        return {
            "status":        "error" if has_errors else "ok",
            "requires_poll": False,
            "job_name":      "",
            "job_id":        "",
            "messages":      messages,
            "result_json":   result_json,
        }

    if action_type == "SUBMIT":
        job_name = result_json.get("jobname", "")
        job_id   = result_json.get("jobcount", "")

        if not job_id:
            return {
                "status":        "error",
                "requires_poll": False,
                "job_name":      "",
                "job_id":        "",
                "messages":      messages + [{"TYPE": "E", "MESSAGE": "No job ID returned"}],
                "result_json":   {},
            }

        logger.info("SUBMIT submitted: job=%s/%s", job_name, job_id)
        return {
            "status":        "submitted",
            "requires_poll": True,
            "job_name":      job_name,
            "job_id":        job_id,
            "messages":      messages,
            "result_json":   result_json,
        }

    # Unknown action_type
    return {
        "status":        "error",
        "requires_poll": False,
        "job_name":      "",
        "job_id":        "",
        "messages":      [{"TYPE": "E", "MESSAGE": f"Unknown action_type: {action_type}"}],
        "result_json":   {},
    }


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
    {"job_name": str, "job_id": str, "status": str,
     "is_running": bool, "is_finished": bool, "is_aborted": bool}
    """
    conn = conn_mgr.get_connection()
    result = _rfc(conn, "TOOLS", "TOOL_JOB_STATUS", {
        "jobname":  job_name,
        "jobcount": job_id,
    })
    rj = result["result_json"]
    state = rj.get("state", "SCHEDULED")

    logger.info("sap_job_status job=%s/%s state=%s", job_name, job_id, state)
    return {
        "job_name":    job_name,
        "job_id":      job_id,
        "status":      state,
        "is_running":  state in ("SCHEDULED", "READY", "RUNNING"),
        "is_finished": state == "FINISHED",
        "is_aborted":  state == "ABORTED",
    }


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
    {"job_id": str, "spool_text": str, "line_count": int, "truncated": bool}
    """
    conn = conn_mgr.get_connection()
    result = _rfc(conn, "TOOLS", "TOOL_READ_JOB_SPOOL", {
        "jobname":  job_name,
        "jobcount": job_id,
    })

    raw = result["result_json"]
    # ABAP serialises the spool as a JSON array of lines
    if isinstance(raw, list):
        lines = raw
    elif isinstance(raw, dict):
        text = raw.get("SPOOL_TEXT", "")
        lines = text.splitlines()
    else:
        lines = []

    truncated    = len(lines) > max_lines
    trimmed      = lines[:max_lines]
    logger.info("sap_read_spool job=%s/%s lines=%d truncated=%s",
                job_name, job_id, len(trimmed), truncated)
    return {
        "job_id":     job_id,
        "spool_text": "\n".join(trimmed),
        "line_count": len(trimmed),
        "truncated":  truncated,
    }


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    transport = cfg.get("server", {}).get("transport", "stdio")
    logger.info("Starting MCP server transport=%s", transport)
    mcp.run(transport=transport)
