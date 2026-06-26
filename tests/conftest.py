"""
Shared fixtures for the SAP Period Close test suite.

The whole suite runs fully offline:
  * `pyrfc` is absent, so `sap_connection_manager` falls back to `_StubConnection`
    (canned table/spool data) — no real SAP system.
  * LLM-dependent nodes get `FakeLLM` objects injected via the `llms` dict — no
    API key, no network.

Three reusable fakes are exposed as fixtures:
  * `make_session`   — factory for a `FakeMCPSession` with canned per-tool responses.
  * `stub_session`   — a session that dispatches to the real `mcp_server` tool
                       functions (which run against the SAP stub) for true
                       graph → mcp_server → stub integration.
  * `make_llms`      — factory for a `{"analysis", "validation"}` dict of `FakeLLM`s.

Plus helpers: `make_state` (initial graph state) and `patch_react_agent`
(replaces `create_agent` so `analysis_node` is deterministic).
"""

from __future__ import annotations

import json
import os
from types import SimpleNamespace

import pytest
from langchain_core.messages import AIMessage

# Defensive: keep everything offline/deterministic even if a real key is exported.
os.environ.setdefault("GROQ_API_KEY", "test-key")
os.environ.setdefault("OPENROUTER_API_KEY", "test-key")
os.environ.setdefault("RESET_EACH_RUN", "1")

import graph_builder
from graph_builder import _build_initial_state


# ---------------------------------------------------------------------------
# Fake MCP tool result — mimics mcp.types.CallToolResult that the graph reads
# via _parse_tool_result (iterates `.content`, finds a `.type == "text"` block,
# json.loads its `.text`).
# ---------------------------------------------------------------------------

def make_tool_result(payload: dict):
    block = SimpleNamespace(type="text", text=json.dumps(payload))
    return SimpleNamespace(content=[block])


# ---------------------------------------------------------------------------
# FakeMCPSession — canned responses keyed by tool name
# ---------------------------------------------------------------------------

class FakeMCPSession:
    """Drop-in for mcp.ClientSession.

    `handlers` maps a tool name to one of:
      * a dict           — returned for every call to that tool,
      * a callable(args) — computes the payload from the call arguments,
      * a list of dicts  — a FIFO queue (last entry repeats once exhausted),
                           handy for sequencing e.g. RUNNING → FINISHED polls.
    """

    def __init__(self, handlers: dict | None = None):
        self.handlers = handlers or {}
        self.calls: list[tuple[str, dict]] = []

    async def call_tool(self, name: str, arguments: dict | None = None):
        args = arguments or {}
        self.calls.append((name, args))
        h = self.handlers.get(name)
        if h is None:
            payload: dict = {}
        elif callable(h):
            payload = h(args)
        elif isinstance(h, list):
            payload = h.pop(0) if len(h) > 1 else h[0]
        else:
            payload = h
        return make_tool_result(payload)

    async def list_tools(self):
        return SimpleNamespace(tools=[])


class StubBackedSession:
    """Session that dispatches to the real mcp_server tool functions.

    FastMCP's @mcp.tool() leaves the functions as plain callables, so we can call
    them directly; they execute against the offline SAP stub. This exercises the
    full graph → mcp_server → _StubConnection path without a real SAP system.
    """

    async def call_tool(self, name: str, arguments: dict | None = None):
        import mcp_server
        fn = getattr(mcp_server, name)
        payload = fn(**(arguments or {}))
        return make_tool_result(payload)

    async def list_tools(self):
        return SimpleNamespace(tools=[])


# ---------------------------------------------------------------------------
# FakeLLM — async ainvoke returning canned content
# ---------------------------------------------------------------------------

class FakeLLM:
    """Minimal stand-in for ChatOpenAI.ainvoke.

    `content` is either a fixed string or a callable(messages) -> str.
    """

    def __init__(self, content: str = ""):
        self.content = content
        self.calls: list = []

    async def ainvoke(self, messages):
        self.calls.append(messages)
        text = self.content(messages) if callable(self.content) else self.content
        return AIMessage(content=text)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def make_session():
    def _make(**handlers) -> FakeMCPSession:
        return FakeMCPSession(handlers)
    return _make


@pytest.fixture
def stub_session() -> StubBackedSession:
    return StubBackedSession()


@pytest.fixture
def make_llms():
    def _make(validation: str = "", analysis: str = "") -> dict:
        return {"validation": FakeLLM(validation), "analysis": FakeLLM(analysis)}
    return _make


@pytest.fixture
def make_state():
    """Build a PeriodCloseState for a list of steps.

    `current_step_id` defaults to the step at `step_index`. Any extra keyword
    overrides (e.g. current_execute=..., current_validate=...) are applied last.
    """
    def _make(steps, *, step_index=0, current_step_id=None,
              defaults=None, company_config=None, **overrides):
        cfg = {"company_config": company_config or {}, "defaults": defaults or {}}
        state = _build_initial_state(cfg, steps, step_index)
        if current_step_id is None and steps:
            current_step_id = steps[step_index]["step_id"]
        state["current_step_id"] = current_step_id or ""
        state.update(overrides)
        return state
    return _make


@pytest.fixture
def patch_react_agent(monkeypatch):
    """Make the analysis FakeLLM return a deterministic JSON decision.

    Patches FakeLLM.ainvoke so _run_text_react immediately receives `content`
    as the model response, parses it as a final_answer, and exits the loop.
    """
    def _patch(content: str):
        async def _fake_ainvoke(self, messages):
            return AIMessage(content=content)
        monkeypatch.setattr(FakeLLM, "ainvoke", _fake_ainvoke)
    return _patch


@pytest.fixture
def no_sleep(monkeypatch):
    """Make poll_node's inter-poll sleep instant."""
    async def _noop(*_a, **_k):
        return None
    monkeypatch.setattr(graph_builder.asyncio, "sleep", _noop)
