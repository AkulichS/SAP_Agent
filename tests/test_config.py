"""config.py — YAML merge, placeholder injection, checkpointer fallbacks."""

import textwrap

import config
from config import (_build_steps, _merge_params, build_async_checkpointer,
                    load_config, reset_each_run_enabled)


# ---------------------------------------------------------------------------
# load_config — base + company merge, runtime period/fiscal_year
# ---------------------------------------------------------------------------

def test_load_config_merges_base_and_company(tmp_path):
    (tmp_path / "base.yaml").write_text(textwrap.dedent("""
        llm_profiles:
          analysis: {provider: groq, model: llama-3.3-70b}
        steps:
          - step_id: KO8G
            action_type: SUBMIT
    """), encoding="utf-8")
    (tmp_path / "period_close_RU06.yaml").write_text(textwrap.dedent("""
        company_config:
          company_code: "RU06"
    """), encoding="utf-8")
    cfg = load_config(str(tmp_path / "period_close_RU06.yaml"), period="11", fiscal_year="2025")
    rc = cfg["company_config"]
    assert rc["company_code"] == "RU06"
    assert rc["period"] == "11"
    assert rc["fiscal_year"] == "2025"
    assert len(cfg["steps"]) > 0          # steps pulled from base library
    assert "llm_profiles" in cfg


def test_load_config_legacy_single_file(tmp_path):
    solo = tmp_path / "solo.yaml"
    solo.write_text(textwrap.dedent("""
        company_config:
          company_code: "TST"
        steps:
          - step_id: ONLY
            action_type: SUBMIT
    """), encoding="utf-8")
    # No base.yaml sibling in tmp_path -> legacy single-file branch.
    cfg = load_config(str(solo), period="01", fiscal_year="2030")
    assert cfg["company_config"]["company_code"] == "TST"
    assert cfg["company_config"]["period"] == "01"
    assert cfg["steps"][0]["step_id"] == "ONLY"


def test_analysis_defaults_and_section_merge(tmp_path):
    (tmp_path / "base.yaml").write_text(textwrap.dedent("""
        analysis_defaults:
          role: "base role"
          max_tool_calls: 8
        steps:
          - step_id: KO8G
            action_type: SUBMIT
            on_error:
              analysis:
                goal: "base goal"
                instructions: "base instr"
    """), encoding="utf-8")
    (tmp_path / "company.yaml").write_text(textwrap.dedent("""
        company_config:
          company_code: "RU06"
        analysis_defaults:
          role: "company role"
        steps:
          - step_id: KO8G
            on_error:
              analysis:
                goal: "company goal"
    """), encoding="utf-8")
    cfg = load_config(str(tmp_path / "company.yaml"), period="11", fiscal_year="2025")
    # Shared analysis_defaults: company wins per key, base-only keys preserved.
    assert cfg["analysis_defaults"]["role"] == "company role"
    assert cfg["analysis_defaults"]["max_tool_calls"] == 8
    # on_error.analysis deep-merges: company goal overrides, base instructions kept.
    ko8g = next(s for s in cfg["steps"] if s["step_id"] == "KO8G")
    an = ko8g["on_error"]["analysis"]
    assert an["goal"] == "company goal"
    assert an["instructions"] == "base instr"


# ---------------------------------------------------------------------------
# _build_steps — company controls which steps run and in what order
# ---------------------------------------------------------------------------

def test_build_steps_selects_orders_and_merges():
    base = [
        {"step_id": "A", "action_type": "SUBMIT", "object_name": "PA"},
        {"step_id": "B", "action_type": "SUBMIT", "object_name": "PB"},
        {"step_id": "C", "action_type": "SUBMIT", "object_name": "PC"},
    ]
    company = [
        {"step_id": "C"},                              # base as-is (len 1)
        {"step_id": "A", "object_name": "OVERRIDE"},   # merged override
        {"step_id": "D", "action_type": "FM"},         # company-only step
    ]
    result = _build_steps(base, company)
    ids = [s["step_id"] for s in result]
    assert ids == ["C", "A", "D"]              # B omitted, company order kept
    merged_a = next(s for s in result if s["step_id"] == "A")
    assert merged_a["object_name"] == "OVERRIDE"
    assert merged_a["action_type"] == "SUBMIT"  # untouched base field preserved


def test_build_steps_none_returns_base():
    base = [{"step_id": "A"}]
    assert _build_steps(base, None) == base


def test_merge_params_by_selname():
    base = [{"selname": "P_BUKRS", "value": "RU06"},
            {"selname": "P_GJAHR", "value": "2024"}]
    override = [{"selname": "P_GJAHR", "value": "2025"},
                {"selname": "P_NEW", "value": "X"}]
    merged = _merge_params(base, override)
    by_name = {p["selname"]: p["value"] for p in merged}
    assert by_name == {"P_BUKRS": "RU06", "P_GJAHR": "2025", "P_NEW": "X"}
    # Order: base entries first (in place), appended new last.
    assert [p["selname"] for p in merged] == ["P_BUKRS", "P_GJAHR", "P_NEW"]


# ---------------------------------------------------------------------------
# reset_each_run_enabled
# ---------------------------------------------------------------------------

def test_reset_each_run_from_defaults(monkeypatch):
    monkeypatch.delenv("RESET_EACH_RUN", raising=False)
    assert reset_each_run_enabled({"reset_each_run": True}) is True
    assert reset_each_run_enabled({}) is False


def test_reset_each_run_from_env(monkeypatch):
    monkeypatch.setenv("RESET_EACH_RUN", "1")
    assert reset_each_run_enabled({}) is True


# ---------------------------------------------------------------------------
# Checkpointer — must always return a usable saver
# ---------------------------------------------------------------------------

async def test_build_async_checkpointer_returns_saver(tmp_path):
    saver = await build_async_checkpointer(str(tmp_path / "cp_async.db"))
    try:
        assert type(saver).__name__ in ("AsyncSqliteSaver", "InMemorySaver")
    finally:
        await config.close_checkpointers()   # also avoids an aiosqlite "loop closed" warning


# ---------------------------------------------------------------------------
# Shared per-process resources
#
# A checkpointer owns an aiosqlite connection (and therefore a thread); an LLM
# owns two httpx clients (and therefore connection pools). Built per run, N
# concurrent company closes leak N of each for the life of the process — so both
# factories cache. These tests are the guard on that.
# ---------------------------------------------------------------------------

async def test_checkpointer_is_shared_per_db_path(tmp_path):
    path = str(tmp_path / "shared.db")
    try:
        first  = await build_async_checkpointer(path)
        second = await build_async_checkpointer(path)
        assert first is second

        other = await build_async_checkpointer(str(tmp_path / "other.db"))
        assert other is not first                  # a different DB is a different saver
    finally:
        await config.close_checkpointers()


async def test_concurrent_starts_share_one_checkpointer(tmp_path):
    """Two runs starting at the same moment must not open two connections."""
    import asyncio
    path  = str(tmp_path / "race.db")
    opens = 0
    original = config._open_checkpointer

    async def _counting(p):
        nonlocal opens
        opens += 1
        return await original(p)

    config._open_checkpointer = _counting
    try:
        savers = await asyncio.gather(*[build_async_checkpointer(path) for _ in range(5)])
        assert opens == 1
        assert all(s is savers[0] for s in savers)
    finally:
        config._open_checkpointer = original
        await config.close_checkpointers()


async def test_close_checkpointers_releases_the_cache(tmp_path):
    path  = str(tmp_path / "closed.db")
    first = await build_async_checkpointer(path)
    await config.close_checkpointers()
    try:
        assert await build_async_checkpointer(path) is not first
    finally:
        await config.close_checkpointers()


async def test_llm_clients_are_shared_across_runs(monkeypatch):
    monkeypatch.setenv("GROQ_API_KEY", "test-key")
    profile = {"provider": "groq", "model": "llama-3.3-70b", "temperature": 0}
    try:
        first  = config._llm_from_profile(dict(profile))
        second = config._llm_from_profile(dict(profile))
        assert first is second                     # same profile → one client, reused

        other = config._llm_from_profile({**profile, "model": "gpt-oss-120b"})
        assert other is not first
        # …but both ride the SAME http connection pool
        assert other.http_async_client is first.http_async_client
        assert other.http_client is first.http_client
        assert first.http_async_client in config._http_async.values()
    finally:
        await config.close_http_clients()


async def test_close_http_clients_clears_the_cache(monkeypatch):
    monkeypatch.setenv("GROQ_API_KEY", "test-key")
    profile = {"provider": "groq", "model": "llama-3.3-70b", "temperature": 0}
    first = config._llm_from_profile(dict(profile))
    await config.close_http_clients()
    try:
        assert config._llm_from_profile(dict(profile)) is not first
    finally:
        await config.close_http_clients()
