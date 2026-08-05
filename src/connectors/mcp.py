"""
MCP connector — plug ANOTHER MCP server into the bus.

This is the "USB" payoff: a second device that is not SAP at all. ``run`` calls a
tool on a *different* MCP server and normalizes whatever it returns into the
unified ``{status, messages, meta, data}`` envelope, so the engine consumes it
exactly like a SAP result — with zero engine edits.

It is deliberately **synchronous-only**: generic MCP tools return their result in
one call, so ``poll`` / ``read_output`` are left unimplemented (they raise via
``BaseConnector``). That is the point — a device implements only what its backend
supports, and the engine never assumes async exists.

Wiring it up
------------
The connector talks to the other server through a ``target`` you supply — either a
live ``ClientSession`` or an async provider ``() -> ClientSession`` (memoize the
session inside the provider so it is opened once per run, not per call). The host
app opens that session as part of its startup and registers the device::

    register_mcp_connector("tax_engine", provider=open_tax_session)

after which any step can target it with ``connector: tax_engine``. Opening the
session (stdio / SSE transport) is the host's responsibility — the same place it
already opens the SAP session — so this module stays transport-agnostic and fully
unit-testable with an injected fake session.
"""

from __future__ import annotations

from typing import Any, Awaitable, Callable

from .base import BaseConnector, envelope_data, parse_tool_result
from .registry import register_connector

# A target is a live session (has ``call_tool``) or an async provider returning one.
Target = Any  # ClientSession | Callable[[], Awaitable[ClientSession]]


def _norm_text(raw: dict) -> dict:
    """Coerce a tool's text output into the neutral {available, line_count, text}."""
    txt = raw.get("text")
    if isinstance(txt, dict):          # already structured — pass through
        return txt
    txt = txt or ""
    return {"available": bool(txt), "line_count": len(txt.splitlines()), "text": txt}


def _to_envelope(raw: Any, object_name: str) -> dict:
    """Wrap an arbitrary MCP tool payload in the unified envelope.

    Generic by shape (same rules as the SAP ``sap_run_tool`` passthrough): a
    ``{"data": [...]}`` payload surfaces as ``data.rows``; a payload carrying
    ``text`` surfaces as ``data.text``; the verbatim payload is always kept under
    ``data.raw`` so bespoke tools stay reachable."""
    messages = raw.get("messages") if isinstance(raw, dict) else None
    messages = messages if isinstance(messages, list) else []

    rows = raw["data"] if isinstance(raw, dict) and isinstance(raw.get("data"), list) else None
    text = _norm_text(raw) if isinstance(raw, dict) and "text" in raw else None
    data = envelope_data(text=text, rows=rows, raw=raw)

    has_error = any(m.get("TYPE") in ("E", "A") for m in messages) \
                or (isinstance(raw, dict) and raw.get("status") == "E")
    return {"status": "error" if has_error else "ok",
            "messages": messages,
            "meta": {"object_name": object_name, "connector": "mcp"},
            "data": data}


class McpConnector(BaseConnector):
    """Proxy device for another MCP server (synchronous)."""

    def __init__(self, session: Any = None, *, target: Target, name: str = "mcp"):
        super().__init__(session)
        self.name = name
        self._target = target

    async def _session(self):
        """Resolve the target to a live session (await the provider if needed)."""
        t = self._target
        return t if hasattr(t, "call_tool") else await t()

    async def run(self, action_type: str, object_name: str, params: Any,
                  *, async_mode: bool = False, test_run: bool = False) -> dict:
        """Call ``object_name`` on the target server; normalize its result.

        ``action_type`` is not needed to dispatch (the tool name is the address);
        it is accepted for signature parity and may guide the target's own logic.
        ``params`` travels verbatim as the tool arguments."""
        sess = await self._session()
        args = params if isinstance(params, dict) else {}
        result = await sess.call_tool(object_name, arguments=args)
        return _to_envelope(parse_tool_result(result), object_name)

    async def list_diagnostic_tools(self) -> list[dict]:
        """Offer the target server's tools to the analysis agent (function shape).

        The target server owns its own read/write safety; this proxy surfaces what
        it advertises so a failed step on this device is diagnosed with ITS tools."""
        sess = await self._session()
        listed = await sess.list_tools()
        return [{
            "type": "function",
            "function": {
                "name": t.name,
                "description": (getattr(t, "description", "") or "")[:1024],
                "parameters": getattr(t, "inputSchema", None)
                or {"type": "object", "properties": {}},
            },
        } for t in getattr(listed, "tools", None) or []]

    async def call_diagnostic(self, name: str, args: dict) -> dict:
        """Dispatch a tool on the target server → normalized envelope."""
        sess = await self._session()
        result = await sess.call_tool(name, arguments=args or {})
        return _to_envelope(parse_tool_result(result), name)


def register_mcp_connector(name: str, *, target: Target) -> None:
    """Register an :class:`McpConnector` under ``name``.

    ``target`` is a live session or an async provider ``() -> ClientSession`` for
    the other server. The SAP session the registry passes to the factory is
    irrelevant here and ignored."""
    register_connector(name, lambda _session: McpConnector(target=target, name=name))
