"""
config.py — config loader, LLM factory and checkpointer for the SAP Period Close agent.
"""

from __future__ import annotations

import copy
import os
from pathlib import Path

import httpx
import yaml
from langchain_openai import ChatOpenAI

SERVERS_DIR = Path(__file__).parent / "tools"
CONFIGS_DIR = Path(__file__).parent / "configs"
DB_DIR = Path(__file__).parent / "db"
DEFAULT_CHECKPOINT_DB = DB_DIR / "period_close_checkpoints.db"

# ---------------------------------------------------------------------------
# Provider registry
# ---------------------------------------------------------------------------

_PROVIDERS: dict[str, dict] = {
    "groq": {
        "base_url":    "https://api.groq.com/openai/v1",
        "api_key_env": "GROQ_API_KEY",
    },
    "openrouter": {
        "base_url":    "https://openrouter.ai/api/v1",
        "api_key_env": "OPENROUTER_API_KEY",
    },
}

# Default profiles used when llm_profiles is absent
_DEFAULT_PROFILES: dict[str, dict] = {
    "analysis": {
        "provider":    "groq",
        "model":       "llama-3.3-70b-versatile",
        "temperature": 0,
    },
    "validation": {
        "provider":    "groq",
        "model":       "llama-3.1-8b-instant",
        "temperature": 0,
    },
}


# ---------------------------------------------------------------------------
# Step merge helpers
# ---------------------------------------------------------------------------

def _merge_params(base_params: list, override_params: list) -> list:
    """Merge params lists by selname key.

    Override replaces a matching base entry in-place (preserving order).
    Override entries with no matching selname are appended at the end.
    """
    override_map = {p["selname"]: dict(p) for p in override_params}
    result: list = []
    used: set = set()
    for p in base_params:
        sn = p["selname"]
        result.append(override_map[sn] if sn in override_map else dict(p))
        used.add(sn)
    for p in override_params:
        if p["selname"] not in used:
            result.append(dict(p))
    return result


def _merge_params_field(base_params, override_params):
    """Merge a params field that may be a list (selname-keyed) or dict (TOOLS action)."""
    if isinstance(base_params, list) and isinstance(override_params, list):
        return _merge_params(base_params, override_params)
    if isinstance(base_params, dict) and isinstance(override_params, dict):
        return {**base_params, **override_params}
    return override_params


def _merge_section(base: dict, override: dict) -> dict:
    """Shallow-merge a nested step section (pre_check/validate/rollback/on_error),
    but merge a nested `params` field by selname like top-level params so a company
    override adds/overrides individual params instead of replacing the whole list."""
    out = dict(base)
    for key, val in override.items():
        if key == "params":
            out["params"] = _merge_params_field(out.get("params"), val)
        elif isinstance(val, dict) and isinstance(out.get(key), dict):
            out[key] = {**out[key], **val}
        else:
            out[key] = val
    return out


def _merge_step(base_step: dict, override: dict) -> dict:
    """Deep-merge a company step override onto the base step template.

    - Scalar fields: company value wins.
    - params (list): merged by selname via _merge_params.
    - params (dict, TOOLS action): shallow dict merge.
    - Nested dicts (pre_check, validate, rollback, on_error): merged via _merge_section,
      which also merges their nested `params` by selname.
    """
    merged = copy.deepcopy(base_step)
    for key, val in override.items():
        if key == "step_id":
            continue
        if key == "params":
            merged["params"] = _merge_params_field(merged.get("params"), val)
        elif isinstance(val, dict) and isinstance(merged.get(key), dict):
            merged[key] = _merge_section(merged[key], val)
        else:
            merged[key] = val
    return merged


def _build_steps(base_steps: list[dict], company_steps: list[dict] | None) -> list[dict]:
    """Build the final ordered step list for a company.

    Rules:
    - company_steps is None → all base steps used as-is (backwards compat).
    - step present in company only → included as-is (company-specific step).
    - step present in both → merged (company overrides win on conflict).
    - step present in base only → omitted.
    """
    if company_steps is None:
        return base_steps

    base_map = {s["step_id"]: s for s in base_steps}
    result: list[dict] = []
    for entry in company_steps:
        step_id = entry["step_id"]
        if step_id not in base_map:
            result.append(entry)
        elif len(entry) == 1:
            result.append(base_map[step_id])
        else:
            result.append(_merge_step(base_map[step_id], entry))
    return result


# ---------------------------------------------------------------------------
# Base layer: base.yaml globals + steps/*.yaml + externalized prompts/*.md
# ---------------------------------------------------------------------------

def _inline_prompt_files(node, base_dir: Path) -> None:
    """Recursively replace any '<name>_file' string value with the referenced file's
    text under '<name>', dropping the '_file' pointer. Paths are resolved relative to
    the configs dir, so a step's `instructions_file: prompts/KO8G/diagnose.md` becomes
    `instructions: <file contents>` — downstream code sees a plain string prompt."""
    if isinstance(node, dict):
        for key in list(node.keys()):
            val = node[key]
            if key.endswith("_file") and isinstance(val, str):
                node[key[: -len("_file")]] = (base_dir / val).read_text(encoding="utf-8")
                del node[key]
            else:
                _inline_prompt_files(val, base_dir)
    elif isinstance(node, list):
        for item in node:
            _inline_prompt_files(item, base_dir)


def _assemble_steps_from_dir(steps_dir: Path, step_order, base_dir: Path) -> list[dict]:
    """Load steps/*.yaml, ordered by step_order (alphabetical fallback). A step_id in
    step_order with no matching file is skipped; files not listed in step_order are
    appended after the ordered ones so nothing is silently lost."""
    files = {p.stem: p for p in steps_dir.glob("*.yaml")}
    order = list(step_order) if step_order else sorted(files)
    order += [sid for sid in sorted(files) if sid not in order]
    steps: list[dict] = []
    for sid in order:
        path = files.get(sid)
        if path is None:
            continue
        step = yaml.safe_load(path.read_text(encoding="utf-8"))
        _inline_prompt_files(step, base_dir)
        steps.append(step)
    return steps


def _config_signature(base_path: Path) -> tuple:
    """mtime fingerprint of base.yaml + steps/*.yaml + prompts/**/*.md for cache invalidation."""
    base_dir = base_path.parent
    paths = [base_path]
    steps_dir, prompts_dir = base_dir / "steps", base_dir / "prompts"
    if steps_dir.is_dir():
        paths += steps_dir.glob("*.yaml")
    if prompts_dir.is_dir():
        paths += prompts_dir.rglob("*.md")
    return tuple(sorted((str(p), p.stat().st_mtime_ns) for p in paths))


_base_cache: dict = {"sig": None, "data": None}


def load_base_config(base_path: str | Path | None = None) -> dict:
    """Assemble the base (product) config: base.yaml globals (defaults, llm_profiles,
    analysis_defaults, step_order) plus the step library.

    Steps come from `steps/*.yaml` (ordered by `step_order`) when that directory exists;
    otherwise from an inline `steps:` list in base.yaml (legacy / test fixtures). Any
    `*_file` prompt references are inlined to text. Result is cached by file mtimes."""
    base_path = Path(base_path) if base_path else CONFIGS_DIR / "base.yaml"
    sig = _config_signature(base_path)
    if _base_cache["sig"] == sig and _base_cache["data"] is not None:
        return copy.deepcopy(_base_cache["data"])

    base = yaml.safe_load(base_path.read_text(encoding="utf-8")) or {}
    base_dir = base_path.parent
    steps = base.get("steps")
    if steps:
        _inline_prompt_files(steps, base_dir)
    else:
        steps_dir = base_dir / "steps"
        steps = _assemble_steps_from_dir(steps_dir, base.get("step_order"), base_dir) \
            if steps_dir.is_dir() else []
    base["steps"] = steps

    _base_cache["sig"] = sig
    _base_cache["data"] = base
    return copy.deepcopy(base)


# ---------------------------------------------------------------------------
# Config loader: base.yaml + company override + runtime period/fiscal_year
# ---------------------------------------------------------------------------

def load_config(
    company_config_path: str,
    period: str | None = None,
    fiscal_year: str | None = None,
) -> dict:
    """Load merged config: configs/base.yaml + company file + runtime period/fiscal_year.

    Company file provides company_config (company_code, controlling_area, currency)
    and an optional steps list that controls which steps run and in what order.
    Falls back to single-file loading when base.yaml is not present (legacy CLI use).
    """
    company_path = Path(company_config_path)
    if not company_path.is_absolute():
        company_path = Path(__file__).parent / company_config_path
    base_path = company_path.parent / "base.yaml"

    if base_path.exists() and company_path.name != "base.yaml":
        result: dict = load_base_config(base_path)
        with open(company_path, encoding="utf-8") as f:
            company_cfg: dict = yaml.safe_load(f)

        # company_config: company wins on conflict
        run_ctx = {
            **result.get("company_config", {}),
            **company_cfg.get("company_config", {}),
        }

        # Allow company to partially override defaults, llm_profiles or analysis_defaults
        for key in ("defaults", "llm_profiles", "analysis_defaults"):
            if key in company_cfg:
                result[key] = {**result.get(key, {}), **company_cfg[key]}

        # Build final steps from base library + company step declarations
        result["steps"] = _build_steps(
            result.get("steps", []),
            company_cfg.get("steps"),
        )
    else:
        with open(company_path, encoding="utf-8") as f:
            result = yaml.safe_load(f)
        run_ctx = result.get("company_config", {})

    if period is not None:
        run_ctx["period"] = period
    if fiscal_year is not None:
        run_ctx["fiscal_year"] = fiscal_year
    result["company_config"] = run_ctx
    return result


# ---------------------------------------------------------------------------
# Public factories
# ---------------------------------------------------------------------------

def reset_each_run_enabled(defaults: dict) -> bool:
    """True in dev/testing mode: each run gets a fresh checkpoint and a finished run
    can be re-run. Driven by defaults.reset_each_run or env RESET_EACH_RUN=1."""
    return (bool(defaults.get("reset_each_run", False))
            or os.getenv("RESET_EACH_RUN") in ("1", "true", "True"))


def build_llm(profile_name: str = "analysis") -> ChatOpenAI:
    """Return an LLM from a named default profile ('analysis' | 'validation')."""
    profile = _DEFAULT_PROFILES.get(profile_name, _DEFAULT_PROFILES["analysis"])
    return _llm_from_profile(profile)


def build_llms_from_config(period_cfg: dict) -> dict[str, ChatOpenAI]:
    """
    Return {"analysis": LLM, "validation": LLM} resolved from config.
    Falls back to _DEFAULT_PROFILES when a profile is absent.
    """
    llm_profiles = period_cfg.get("llm_profiles", {})
    result: dict[str, ChatOpenAI] = {}
    for name in ("analysis", "validation"):
        profile = llm_profiles.get(name, _DEFAULT_PROFILES[name])
        result[name] = _llm_from_profile(profile)
    return result


def build_checkpointer(db_path: str | None = None):
    """Sync checkpointer for CLI use. Falls back to InMemorySaver."""
    import logging as _log
    path = db_path or os.getenv("CHECKPOINT_DB", str(DEFAULT_CHECKPOINT_DB))
    try:
        import sqlite3
        from langgraph.checkpoint.sqlite import SqliteSaver
        DB_DIR.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(path, check_same_thread=False)
        saver = SqliteSaver(conn)
        _log.getLogger(__name__).info("Using SqliteSaver at %s", path)
        return saver
    except Exception as exc:
        _log.getLogger(__name__).warning(
            "SqliteSaver failed (%s) — using InMemorySaver", exc
        )
    from langgraph.checkpoint.memory import InMemorySaver
    return InMemorySaver()


async def build_async_checkpointer(db_path: str | None = None):
    """
    Async checkpointer for use with graph.ainvoke / astream.
    Creates an AsyncSqliteSaver by opening an aiosqlite connection directly.
    Falls back to InMemorySaver when aiosqlite is absent.
    """
    import logging as _log
    path = db_path or os.getenv("CHECKPOINT_DB", str(DEFAULT_CHECKPOINT_DB))
    try:
        import aiosqlite
        from langgraph.checkpoint.sqlite.aio import AsyncSqliteSaver
        DB_DIR.mkdir(parents=True, exist_ok=True)
        conn = await aiosqlite.connect(path)
        saver = AsyncSqliteSaver(conn)
        await saver.setup()
        _log.getLogger(__name__).info("Using AsyncSqliteSaver at %s", path)
        return saver
    except Exception as exc:
        _log.getLogger(__name__).warning(
            "AsyncSqliteSaver failed (%s) — using InMemorySaver", exc
        )
    from langgraph.checkpoint.memory import InMemorySaver
    return InMemorySaver()


# ---------------------------------------------------------------------------
# Internal helper
# ---------------------------------------------------------------------------

def _llm_from_profile(profile: dict) -> ChatOpenAI:
    provider_name = profile.get("provider", "groq")
    provider = _PROVIDERS.get(provider_name)
    if provider is None:
        raise ValueError(
            f"Unknown LLM provider '{provider_name}'. "
            f"Supported: {list(_PROVIDERS)}"
        )
    api_key = os.getenv(provider["api_key_env"])
    return ChatOpenAI(
        model=profile["model"],
        base_url=provider["base_url"],
        api_key=api_key,
        temperature=profile.get("temperature", 0),
        max_retries=3,
        http_client=httpx.Client(verify=False),
        http_async_client=httpx.AsyncClient(verify=False),
    )
