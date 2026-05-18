"""
config.py — single source of truth for the LangGraph MCP agent
───────────────────────────────────────────────────────────────
Imported by both agent.py (CLI) and chat_app.py (Streamlit UI).
Edit this file to change prompts, models, servers, or tool icons.
"""

import os
import sys
from pathlib import Path

from langchain_openai import ChatOpenAI

# ── Paths ──────────────────────────────────────────────────────────────────────
SERVERS_DIR = Path(__file__).parent / "mcp_servers"


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
        )

    elif api_key_name == 'OPENROUTER_API_KEY':
        api_key = os.getenv("OPENROUTER_API_KEY")
        return ChatOpenAI(
            model="openrouter/free",                  # selects a suitable available free model,
            base_url="https://openrouter.ai/api/v1",
            api_key=os.getenv("OPENROUTER_API_KEY"),   
            temperature=0
        )
        
    raise EnvironmentError("No LLM API key found. Set API_KEY in .env")


# ── MCP server configuration ───────────────────────────────────────────────────
def build_mcp_config() -> dict:
    """
    Config dict for MultiServerMCPClient.
    Each entry launches a server subprocess over stdio transport.
    Add new servers here — both agent.py and chat_app.py pick them up automatically.
    """
    return {
        "calculator": {
            "command":   sys.executable,
            "args":      [str(SERVERS_DIR / "calculator_server.py")],
            "transport": "stdio",
        },
        "weather": {
            "command":   sys.executable,
            "args":      [str(SERVERS_DIR / "weather_server.py")],
            "transport": "stdio",
        },
        "search": {
            "command":   sys.executable,
            "args":      [str(SERVERS_DIR / "search_server.py")],
            "transport": "stdio",
        },
    }


# ── System prompt ──────────────────────────────────────────────────────────────
# Edit this to change how the agent behaves, which tools it prefers, or its tone.
SYSTEM_PROMPT = """You are a highly capable research assistant with access to real tools.

Available tools:

• calculate(expression)           — Evaluate any math expression safely.

• get_current_weather(city)       — Live weather for any city worldwide.
                                    Returns: temperature, humidity, wind,
                                    precipitation, UV index.

• get_forecast(city, days)        — Daily forecast up to 16 days ahead.

• web_search(query, max_results)  — General web search via DuckDuckGo.
                                    Use for facts, definitions, how-to, etc.

• news_search(query, max_results) — Recent news articles on any topic.

Decision rules:

1. For ANY arithmetic, algebra, or trigonometry → always use calculate().
2. For weather queries → use get_current_weather() or get_forecast().
3. For current events, facts, research → use web_search() or news_search().
4. For complex questions → chain multiple tools in order.
5. Always base your final answer on actual tool results, not assumptions.
6. For running tools use only proper JSON format, don't use XML
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
