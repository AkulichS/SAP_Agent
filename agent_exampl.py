#!/usr/bin/env python3
"""
LangGraph ReAct Agent — CLI entry point
════════════════════════════════════════════════════════════════
Shared config (prompt, LLM, MCP servers) lives in config.py.
This file only contains CLI-specific logic: demo runner, REPL, main().
════════════════════════════════════════════════════════════════
"""

import asyncio
import logging

from dotenv import load_dotenv
from langchain_core.messages import HumanMessage
from langchain_mcp_adapters.client import MultiServerMCPClient
from langchain.agents import create_agent
from config import SYSTEM_PROMPT, build_llm, build_mcp_config

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


# ── Query runner ───────────────────────────────────────────────────────────────
async def run_query(query: str, agent) -> str:
    """Invoke the agent with a single query and return the final response text."""
    result = await agent.ainvoke({"messages": [HumanMessage(content=query)]})
    return result["messages"][-1].content


# ── Demo test suite ────────────────────────────────────────────────────────────
DEMO_QUERIES = [
    ("MATH",    "What is sqrt(1764) + factorial(6) - 2**8?"),
    ("WEATHER", "What is the current weather in Rome, Italy?"),
    ("WEATHER", "Give me a 3-day forecast for Tokyo."),
    ("SEARCH",  "What is LangGraph and how does the ReAct agent loop work?"),
    ("NEWS",    "What are the latest news about artificial intelligence?"),
    ("MULTI",   "What is the current temperature in Paris? "
                "If it rises by 12 degrees, what will it be? "
                "Use the calculator for the addition."),
]


async def run_demo(agent) -> None:
    print("\n" + "═" * 68)
    print("  LANGGRAPH + MCP AGENT  —  FULL PIPELINE DEMO")
    print("═" * 68)
    for i, (category, query) in enumerate(DEMO_QUERIES, 1):
        print(f"\n{'─' * 68}")
        print(f"  Test {i} [{category}]: {query}")
        print("─" * 68)
        try:
            print(f"\n{await run_query(query, agent)}")
        except Exception as exc:
            logger.error("Query failed: %s", exc, exc_info=True)
    print("\n" + "═" * 68)


async def run_interactive(agent) -> None:
    print("\n" + "═" * 68)
    print("  INTERACTIVE MODE  —  type 'quit' to exit")
    print("═" * 68 + "\n")
    while True:
        try:
            user_input = input("You: ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\nGoodbye! 👋")
            break
        if not user_input:
            continue
        if user_input.lower() in {"quit", "exit", "q", "bye"}:
            print("Goodbye! 👋")
            break
        try:
            print(f"\nAgent: {await run_query(user_input, agent)}\n")
        except Exception as exc:
            logger.error("Error: %s", exc, exc_info=True)


# ── Entry point ────────────────────────────────────────────────────────────────
async def main() -> None:
    llm    = build_llm('OPENROUTER_API_KEY')
    client = MultiServerMCPClient(build_mcp_config())

    logger.info("Loading tools from MCP servers…")
    tools = await client.get_tools()
    logger.info("Loaded %d tools: %s", len(tools), [t.name for t in tools])

    agent = create_agent(llm, tools, system_prompt=SYSTEM_PROMPT)

    await run_demo(agent)
    await run_interactive(agent)


if __name__ == "__main__":
    asyncio.run(main())
