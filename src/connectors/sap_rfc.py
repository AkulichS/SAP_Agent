"""
SAP-RFC connector — the default device on the bus.

Wraps the three MCP tools the engine used to call inline
(``sap_run_tool`` / ``sap_execute_step`` → ``run``, ``sap_job_status`` →
``poll``, ``sap_read_spool`` → ``read_output``). All SAP-specific vocabulary —
the SUBMIT/FM/BAPI/BDC/TOOLS action types, job identifiers — lives here, behind
the backend-neutral :class:`~connectors.base.Connector` contract. The engine sees
none of it.
"""

from __future__ import annotations

import json
import logging
from typing import Any

from mcp import ClientSession

from .base import (POLL_ABORTED, POLL_FINISHED, POLL_RUNNING, BaseConnector,
                   parse_tool_result, warn_foreign_data_keys)

logger = logging.getLogger(__name__)

# Native SAP job states (SHOW_JOBSTATE) → normalized lifecycle.
_STATE_MAP = {"FINISHED": POLL_FINISHED, "ABORTED": POLL_ABORTED}

# The two MCP tools that MUTATE SAP — never offered to, nor callable by, the
# read-only diagnosis agent. Every other tool on the server is read-only.
MUTATING_TOOLS = {"sap_execute_step", "sap_run_tool"}


class SapRfcConnector(BaseConnector):
    """Talks to ``ZFI_AI_PERIOD_CLOSE_RFC`` via the MCP execution layer."""

    name = "sap_rfc"

    def __init__(self, session: ClientSession):
        super().__init__(session)
        self._diag_schema_cache: list[dict] | None = None

    async def run(
        self,
        action_type: str,
        object_name: str,
        params: Any,
        *,
        async_mode: bool = False,
        test_run: bool = False,
    ) -> dict:
        """Route one SAP action to the right MCP tool and return its envelope.

        (Formerly ``graph_builder._run_action``.)
        """
        at = (action_type or "").upper()
        if at == "TOOLS":
            # Generic tool call: object_name IS the ABAP tool handler name; params
            # travel verbatim as iv_params_json. The server dispatches by name, so
            # nothing here is hardcoded — a new handler needs only its ABAP branch.
            r = await self.session.call_tool("sap_run_tool", arguments={
                "object_name": (object_name or "").strip().upper(),
                "params_json": json.dumps(params if isinstance(params, dict) else {}),
                "test_run":    test_run,
            })
        else:
            r = await self.session.call_tool("sap_execute_step", arguments={
                "action_type": at,
                "object_name": object_name,
                "params_json": json.dumps(params),
                "async_mode":  async_mode,
                "test_run":    test_run,
            })
        env = parse_tool_result(r)

        # Publish the opaque async handle the engine hands back to poll(). Emitted
        # whenever the action produced a job (async OR inline SUBMIT), so validate can
        # both poll it and derive placeholders from it. SAP job identity stays inside
        # the handle; the flat job_name/job_id remain on meta as SAP-published values.
        meta = env.get("meta") or {}
        if "handle" not in meta and (meta.get("requires_poll")
                                     or meta.get("job_name") or meta.get("job_id")):
            meta["handle"] = {"job_name": meta.get("job_name", ""),
                              "job_id":   meta.get("job_id", "")}
            env["meta"] = meta
        # The RFC already speaks the neutral data vocabulary (text/rows/raw) — no
        # translation here anymore. Guard the boundary in case a handler slips.
        return warn_foreign_data_keys(env, where="sap_rfc.run")

    async def poll(self, handle: dict) -> dict:
        """Single-shot SAP job-status check → normalized lifecycle state."""
        handle = handle or {}
        r = await self.session.call_tool("sap_job_status", arguments={
            "job_name": handle.get("job_name", ""),
            "job_id":   handle.get("job_id", ""),
        })
        data = parse_tool_result(r)
        raw_state = (data.get("data") or {}).get("state", "SCHEDULED")
        return {"state": _STATE_MAP.get(raw_state, POLL_RUNNING), "raw": data}

    async def read_output(self, handle: dict, *, max_lines: int = 500) -> dict:
        """Read a finished job's spool as an envelope (``data.text``)."""
        handle = handle or {}
        r = await self.session.call_tool("sap_read_spool", arguments={
            "job_name":  handle.get("job_name", ""),
            "job_id":    handle.get("job_id", ""),
            "max_lines": max_lines,
        })
        return warn_foreign_data_keys(parse_tool_result(r), where="sap_rfc.read_output")

    def placeholders(self, handle: dict) -> dict:
        """SAP publishes the job identifiers as ``{{job_name}}`` / ``{{job_id}}``."""
        handle = handle or {}
        return {"job_name": handle.get("job_name", ""),
                "job_id":   handle.get("job_id", "")}

    async def system_identity(self) -> dict | None:
        """Read the connected SAP system's logical identity (SID + installation
        number) via the ``sap_system_identity`` MCP tool → ``TOOL_SYSTEM_IDENTITY``.

        Returns the neutral ``{"sid", "installation_number"}`` mapping the license
        binding anchors to. Any read failure (tool absent, RFC error) degrades to
        ``None`` rather than raising — the binding is warn-only, so a failed identity
        read must never break a run; the caller then falls back to its own default."""
        try:
            r = await self.session.call_tool("sap_system_identity", arguments={})
        except Exception as exc:  # noqa: BLE001 — a failed identity read must never break a run
            logger.warning("sap_rfc.system_identity read failed: %s", exc)
            return None
        env = parse_tool_result(r)
        meta = env.get("meta") or {}
        return {"sid":                 meta.get("sid") or "",
                "installation_number": meta.get("installation_number") or ""}

    async def list_diagnostic_tools(self) -> list[dict]:
        """Read-only SAP tool schemas (OpenAI function shape) for the analysis agent.

        Discovered live from the MCP session, minus the mutating tools; cached on
        the instance so a single diagnosis doesn't re-list."""
        if self._diag_schema_cache is None:
            listed = await self.session.list_tools()
            schemas: list[dict] = []
            for t in getattr(listed, "tools", None) or []:
                if t.name in MUTATING_TOOLS:
                    continue
                schemas.append({
                    "type": "function",
                    "function": {
                        "name": t.name,
                        "description": (getattr(t, "description", "") or "")[:1024],
                        "parameters": getattr(t, "inputSchema", None)
                        or {"type": "object", "properties": {}},
                    },
                })
            self._diag_schema_cache = schemas
        return self._diag_schema_cache

    async def call_diagnostic(self, name: str, args: dict) -> dict:
        """Dispatch a read-only SAP diagnostic tool by name → envelope.

        Mutating tools are refused (diagnosis must never change SAP); an unknown
        name simply errors at the MCP boundary and the agent self-corrects."""
        if name in MUTATING_TOOLS:
            return {"status": "error", "data": {}, "messages": [{"TYPE": "E", "MESSAGE": (
                f"Tool '{name}' cannot be used during diagnosis — it modifies SAP. "
                "Use a read-only diagnostic tool instead.")}]}
        r = await self.session.call_tool(name, arguments=args)
        return parse_tool_result(r)
