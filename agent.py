import asyncio
import logging

from sap_connections import connect_to_sap
from dotenv import load_dotenv
from langchain_core.messages import HumanMessage
from langchain_mcp_adapters.client import MultiServerMCPClient
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



# async def run_interactive(agent) -> None:
#     print("\n" + "═" * 68)
#     print("  INTERACTIVE MODE  —  type 'quit' to exit")
#     print("═" * 68 + "\n")
#     while True:
#         try:
#             user_input = input("You: ").strip()
#         except (EOFError, KeyboardInterrupt):
#             print("\nGoodbye! 👋")
#             break
#         if not user_input:
#             continue
#         if user_input.lower() in {"quit", "exit", "q", "bye"}:
#             print("Goodbye! 👋")
#             break
#         try:
#             print(f"\nAgent: {await run_query(user_input, agent)}\n")
#         except Exception as exc:
#             logger.error("Error: %s", exc, exc_info=True)


# ── Entry point ────────────────────────────────────────────────────────────────
async def main() -> None:
    sap_conn = connect_to_sap()
    llm    = build_llm('OPENROUTER_API_KEY')
    client = MultiServerMCPClient(build_mcp_config(sap_conn, job_result))

    logger.info("Loading tools from MCP servers…")
    tools = await client.get_tools()
    logger.info("Loaded %d tools: %s", len(tools), [t.name for t in tools])

    # agent = create_agent(llm, tools, system_prompt=SYSTEM_PROMPT)


    # await run_interactive(agent)


if __name__ == "__main__":
    asyncio.run(main())
