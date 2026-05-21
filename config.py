"""
config.py — single source of truth for the LangGraph MCP agent
───────────────────────────────────────────────────────────────
Imported by both agent.py (CLI) and chat_app.py (Streamlit UI).
Edit this file to change prompts, models, servers, or tool icons.
"""

import os
import sys
from pathlib import Path
import httpx

from langchain_openai import ChatOpenAI

# ── Paths ──────────────────────────────────────────────────────────────────────
SERVERS_DIR = Path(__file__).parent / "tools"


# ── LLM factory ───────────────────────────────────────────────────────────────
def build_llm(api_key_name: str | None = None) -> ChatOpenAI:
    """
    Return a configured LLM.  Falls back to env vars when keys are not passed.
    Priority: Groq (free, fast) → OpenAI → EnvironmentError.
    """

    if api_key_name == 'GROQ_API_KEY':
        return ChatOpenAI(
            model="llama-3.3-70b-versatile",
            base_url="https://api.groq.com/openai/v1",
            api_key=os.getenv("GROQ_API_KEY"),
            temperature=0,
            max_retries=3,
            http_client=httpx.Client(verify=False),
        )

    elif api_key_name == 'OPENROUTER_API_KEY':
        api_key = os.getenv("OPENROUTER_API_KEY")
        return ChatOpenAI(
            model="openrouter/free",                  # selects a suitable available free model,
            base_url="https://openrouter.ai/api/v1",
            api_key=os.getenv("OPENROUTER_API_KEY"),   
            temperature=0,
            http_client=httpx.Client(verify=False),
        )
        
    raise EnvironmentError("No LLM API key found. Set API_KEY in .env")


# ── MCP server configuration ───────────────────────────────────────────────────
def build_mcp_config(sap_conn, job_result) -> dict:
    """
    Config dict for MultiServerMCPClient.
    Each entry launches a server subprocess over stdio transport.
    """
    return {
        "read_job_spool": {
            "command":   sys.executable,
            "args":      [str(SERVERS_DIR / "tool_read_job_spool.py"), sap_conn, job_result],
            "transport": "stdio",
        },
    }


# ── System prompt ──────────────────────────────────────────────────────────────
# Edit this to change how the agent behaves, which tools it prefers, or its tone.
SYSTEM_PROMPT = """You are a highly capable SAP assistant with access to real SAP tools.

Available tools:

• read_job_spool    — Read SAP Spool when job is complited.


Decision rules:

1. Use tool read_job_spool() only after the submitted job was complited.
2. For complex questions → chain multiple tools in order.
3. Always base your final answer on actual tool results, not assumptions.
4. For running tools use only proper JSON format, don't use XML
"""


# ── Tool display metadata (used by the Streamlit UI) ──────────────────────────
# Maps tool name → emoji icon shown in the chat interface.
# Add an entry here whenever you add a new MCP tool.
TOOL_ICONS: dict[str, str] = {
    "calculate":           "🔢",
    "get_current_weather": "🌤️",
    "get_forecast":        "📅",
    "web_search":          "🔍",
    "news_search":         "📰",
}
