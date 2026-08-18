"""The shared-server transport: MCP over SSE.

`stdio` gives every run its own MCP server subprocess — fine for the CLI, ruinous
for a web deployment running many companies at once. `sse` is the deployment
answer: one server process, connections pooled across all runs. These cover the
switch (env over YAML, both sides reading the same variables) and then actually
launch the server and drive two concurrent sessions through it.
"""

import asyncio
import os
import socket
import subprocess
import sys
from pathlib import Path

import pytest
import yaml
from mcp import ClientSession
from mcp.client.sse import sse_client

import graph_builder
from connectors.base import parse_tool_result

SRC = Path(graph_builder.__file__).parent


# ---------------------------------------------------------------------------
# Transport selection
# ---------------------------------------------------------------------------

def test_env_transport_overrides_yaml(monkeypatch):
    """A deployment flips to the shared server in .env — no config edit needed."""
    captured = {}
    monkeypatch.setattr(graph_builder, "sse_client", lambda url: captured.setdefault("url", url))
    monkeypatch.setenv("MCP_TRANSPORT", "sse")
    monkeypatch.setenv("MCP_SSE_URL", "http://127.0.0.1:9999/sse")

    graph_builder._build_cm({"server": {"transport": "stdio", "command": "sys.executable"}})
    assert captured["url"] == "http://127.0.0.1:9999/sse"


def test_sse_url_falls_back_to_yaml(monkeypatch):
    captured = {}
    monkeypatch.setattr(graph_builder, "sse_client", lambda url: captured.setdefault("url", url))
    monkeypatch.delenv("MCP_SSE_URL", raising=False)
    monkeypatch.delenv("MCP_TRANSPORT", raising=False)

    graph_builder._build_cm({"server": {"transport": "sse", "sse_url": "http://cfg:8100/sse"}})
    assert captured["url"] == "http://cfg:8100/sse"


def test_sse_without_url_fails_loudly(monkeypatch):
    monkeypatch.delenv("MCP_SSE_URL", raising=False)
    monkeypatch.delenv("MCP_TRANSPORT", raising=False)
    with pytest.raises(ValueError, match="MCP_SSE_URL"):
        graph_builder._build_cm({"server": {"transport": "sse"}})


def test_default_stays_stdio(monkeypatch):
    """No env, shipped config: dev and the CLI keep working with zero setup."""
    monkeypatch.delenv("MCP_TRANSPORT", raising=False)
    captured = {}
    monkeypatch.setattr(graph_builder, "stdio_client",
                        lambda params: captured.setdefault("params", params))

    cfg = yaml.safe_load((SRC / "configs" / "mcp_config.yaml").read_text(encoding="utf-8"))
    assert cfg["server"]["transport"] == "stdio"

    graph_builder._build_cm(cfg)
    assert captured["params"].args == ["mcp_server.py"]
    assert captured["params"].cwd == str(SRC)      # cwd stays pinned to src/


# ---------------------------------------------------------------------------
# Integration: one server, two concurrent sessions
# ---------------------------------------------------------------------------

def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _wait_until_listening(port: int, proc: subprocess.Popen, timeout: float = 60.0) -> None:
    import time
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            raise RuntimeError(f"MCP server exited early with code {proc.returncode}")
        with socket.socket() as s:
            s.settimeout(0.5)
            if s.connect_ex(("127.0.0.1", port)) == 0:
                return
        time.sleep(0.25)
    raise TimeoutError(f"MCP SSE server did not start listening on {port}")


@pytest.fixture(scope="module")
def sse_server():
    port = _free_port()
    env  = dict(os.environ,
                MCP_TRANSPORT="sse", MCP_SSE_HOST="127.0.0.1", MCP_SSE_PORT=str(port))
    proc = subprocess.Popen([sys.executable, "mcp_server.py"], cwd=str(SRC), env=env,
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    try:
        _wait_until_listening(port, proc)
        yield f"http://127.0.0.1:{port}/sse"
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:      # pragma: no cover - defensive
            proc.kill()


async def _read_table(url: str, table: str) -> dict:
    """One session's worth of work — decoded exactly the way a connector decodes it."""
    async with sse_client(url) as (read, write):
        async with ClientSession(read, write) as session:
            await session.initialize()
            result = await session.call_tool(
                "sap_read_table", {"table": table, "where": "", "fields": ""})
            return parse_tool_result(result)


async def test_runs_share_one_server(sse_server):
    """Two sessions — the shape of two company runs — served by a single process."""
    first, second = await asyncio.wait_for(
        asyncio.gather(_read_table(sse_server, "COKP"),
                       _read_table(sse_server, "BKPF")),
        timeout=90,
    )
    assert first["data"]["rows"][0]["KOKRS"] == "X500"
    assert second["data"]["rows"][0]["BUKRS"] == "RU06"


async def test_graph_step_runs_over_the_shared_server(sse_server, make_llms):
    """The real production path end to end: graph → connector bus → MCP over SSE →
    pooled connection → SAP (stub). Nothing here knows the transport changed."""
    from langgraph.checkpoint.memory import InMemorySaver

    from graph_builder import _build_initial_state, build_graph

    steps = [{"step_id": "KSW5", "action_type": "SUBMIT", "object_name": "RKABL000",
              "async": False,
              "validate": {"mode": "keyword",
                           "keyword": {"source": "spool",
                                       "ok_patterns": ["processed"],
                                       "error_patterns": ["FATAL"]}}}]
    cfg = {"company_config": {"fiscal_year": "2025", "period": "11"},
           "defaults": {}, "steps": steps}

    async with sse_client(sse_server) as (read, write):
        async with ClientSession(read, write) as session:
            await session.initialize()
            graph = build_graph(session, make_llms(), InMemorySaver())
            final = await asyncio.wait_for(
                graph.ainvoke(_build_initial_state(cfg, steps),
                              {"configurable": {"thread_id": "sse-e2e"}}),
                timeout=60)

    assert [r["final_status"] for r in final["step_records"]] == ["ok"]


async def test_shared_server_lists_its_tools(sse_server):
    async with sse_client(sse_server) as (read, write):
        async with ClientSession(read, write) as session:
            await session.initialize()
            names = {t.name for t in (await session.list_tools()).tools}
    assert {"sap_check_period", "sap_execute_step", "sap_read_table",
            "sap_job_status", "sap_read_spool"} <= names
