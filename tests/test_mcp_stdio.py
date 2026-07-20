"""Integration smoke: the graph can actually launch the MCP server over stdio
and connect, regardless of the current working directory.

This guards the exact failure that a `src/` layout change can reintroduce: the
server is spawned as `python mcp_server.py`, so its working directory must be
pinned to the package dir or the subprocess can't find the script and the whole
run dies with anyio's "unhandled errors in a TaskGroup". Unit tests with a fake
session cannot catch this — only a real subprocess launch can.
"""

import asyncio
from pathlib import Path

import pytest
import yaml
from mcp import ClientSession

import graph_builder

EXPECTED_TOOLS = {"sap_check_period", "sap_execute_step", "sap_read_table",
                  "sap_job_status", "sap_read_spool"}


async def test_mcp_server_launches_and_lists_tools(monkeypatch, tmp_path):
    # Run from an unrelated CWD to prove the launch doesn't depend on it.
    monkeypatch.chdir(tmp_path)
    cfg = yaml.safe_load(
        (Path(graph_builder.__file__).parent / "configs" / "mcp_config.yaml").read_text())

    async def _connect():
        async with graph_builder._build_cm(cfg) as (read, write):
            async with ClientSession(read, write) as session:
                await session.initialize()
                return {t.name for t in (await session.list_tools()).tools}

    names = await asyncio.wait_for(_connect(), timeout=30)
    assert EXPECTED_TOOLS <= names
