"""
config_settings.py — settings field-tree + override validation for the admin UI.

Turns a company's (base step ⊕ delta) into a flat, schema-annotated field list the
Settings form renders, marking each leaf inherited (from base) or overridden. Also
validates a proposed override by checking that every merged step is schema-valid.

Field path syntax mirrors the step structure; a param leaf is `params.@<selname>.<attr>`
(the `@` marks a selname-keyed list entry), everything else is plain dotted keys.
"""

from __future__ import annotations

from typing import Any

import config
import config_schema
import config_store
from config_store import ConfigStore, get_config_store

SECTION_KEYS = ("params", "pre_check", "validate", "rollback", "on_error")

ENUMS: dict[str, list[str]] = {
    "action_type": ["SUBMIT", "FM", "BAPI", "BDC", "TOOLS"],
    "source": ["spool", "rows", "messages"],
    "operator": ["eq", "ne", "gt", "lt", "ge", "le", "contains"],
    "select": ["first", "last", "empty", "non_empty"],
    "on_pass": ["execute", "skip"],
    "on_fail": ["analyse", "skip", "execute"],
}
TEXTAREA_KEYS = {
    "prompt", "instructions", "tech_instructions", "analysis_guidance",
    "context_template", "explain_prompt",
    "system_prompt", "role", "tools", "rules", "goal",
}


# ---------------------------------------------------------------------------
# Flatten / field building
# ---------------------------------------------------------------------------

def _flatten(obj: Any, prefix: tuple = ()) -> dict[tuple, Any]:
    """Flatten a step (or delta) to {path_tuple: leaf_value}. selname-keyed param lists
    become `(..., 'params', '@SEL', attr)`; lists of scalars (e.g. ok_patterns) are a
    single leaf."""
    out: dict[tuple, Any] = {}
    if isinstance(obj, dict):
        for k, v in obj.items():
            out.update(_flatten(v, prefix + (k,)))
    elif isinstance(obj, list):
        if obj and all(isinstance(x, dict) and "selname" in x for x in obj):
            for entry in obj:
                sel = entry["selname"]
                for k, v in entry.items():
                    if k == "selname":
                        continue
                    out.update(_flatten(v, prefix + (f"@{sel}", k)))
        else:
            out[prefix] = list(obj)
    else:
        out[prefix] = obj
    return out


def _group_of(segs: tuple) -> str:
    return segs[0] if segs and segs[0] in SECTION_KEYS else "step"


def _label(segs: tuple) -> str:
    return " › ".join(s.lstrip("@") for s in segs)


def _widget(key: str, value: Any, enum: list[str] | None) -> str:
    if enum:
        return "select"
    if isinstance(value, bool):
        return "bool"
    if isinstance(value, list):
        return "list"
    if key in TEXTAREA_KEYS:
        return "textarea"
    return "text"


def build_step_fields(base_step: dict, delta: dict | None) -> list[dict]:
    """Flat field list for one step: base leaves first, delta-only leaves appended."""
    base_flat = _flatten(base_step)
    base_flat.setdefault(("enabled",), True)   # always surface the enable/disable toggle
    delta_flat = _flatten(delta or {})

    paths = [p for p in base_flat if p != ("step_id",)]
    paths += [p for p in delta_flat if p not in base_flat and p != ("step_id",)]

    fields: list[dict] = []
    for segs in paths:
        raw_key = segs[-1]
        key = raw_key.lstrip("@") if isinstance(raw_key, str) and raw_key.startswith("@") else raw_key
        enum = ENUMS.get(key)
        base_val = base_flat.get(segs)
        overridden = segs in delta_flat
        value = delta_flat.get(segs, base_val)
        fields.append({
            "path": ".".join(segs),
            "label": _label(segs),
            "group": _group_of(segs),
            "widget": _widget(key, value if value is not None else base_val, enum),
            "enum": enum,
            "base_value": base_val,
            "value": value,
            "overridden": overridden,
        })
    return fields


# ---------------------------------------------------------------------------
# Public API for the web layer
# ---------------------------------------------------------------------------

def _base_steps_by_id(store: ConfigStore | None = None, base_path=None) -> tuple[dict[str, dict], dict]:
    steps = config_store.get_base_steps(store=store, base_path=base_path)
    return {s["step_id"]: s for s in steps}, {"steps": steps}


def build_settings(company_code: str, store: ConfigStore | None = None, base_path=None) -> dict:
    """Full settings payload for the UI: version, run_context fields, and per-step
    field trees (inherited vs overridden)."""
    by_id, base = _base_steps_by_id(store, base_path)
    state = (store or get_config_store()).get_override(company_code)
    override = state.get("override") or {}

    steps_out = []
    for step in base["steps"]:
        sid = step["step_id"]
        delta = override.get(sid) or {}
        steps_out.append({
            "step_id": sid,
            "description": step.get("description", sid),
            "enabled": delta.get("enabled", True),
            "overridden": bool(delta),
            # `base` lets the UI render the same descriptor-driven form as the base editor,
            # merging the delta over it client-side; `fields` is the flat inherited/overridden
            # view of the same data.
            "base": step,
            "fields": build_step_fields(step, delta),
        })

    run_ctx = state.get("run_context") or {}
    run_ctx_fields = [
        {"path": k, "label": k, "group": "run_context", "widget": "text",
         "enum": None, "base_value": None, "value": v, "overridden": True}
        for k, v in run_ctx.items()
    ]

    # Base analysis_defaults travel with the company payload so the step form can
    # prefill an added Role/Tools/Rules override from the Base value (the same value
    # the step inherits at runtime when the override is absent).
    base_globals = config_store.get_base_config(store=store, base_path=base_path)
    return {
        "company_code": company_code,
        "version": state["version"],
        "run_context": run_ctx,
        "run_context_fields": run_ctx_fields,
        "override": override,      # raw delta, so the UI edits it directly
        "steps": steps_out,
        "analysis_defaults": base_globals.get("analysis_defaults") or {},
    }


def validate_override(override: dict, base_path=None) -> None:
    """Validate that every overridden step yields a schema-valid merged step.
    Raises ValueError(message) on the first problem (unknown step or bad field)."""
    by_id, _ = _base_steps_by_id(base_path=base_path)
    for sid, delta in (override or {}).items():
        if sid not in by_id:
            raise ValueError(f"Unknown step_id '{sid}' — steps are defined in the base config.")
        merged = config._merge_step(by_id[sid], {"step_id": sid, **(delta or {})})
        try:
            config_schema.validate_step(merged)
        except Exception as exc:  # pydantic ValidationError
            raise ValueError(f"Step '{sid}': {exc}") from exc


# ---------------------------------------------------------------------------
# Base-tier settings (full step editing) + run_context
# ---------------------------------------------------------------------------

def build_base_settings(store: ConfigStore | None = None, base_path=None) -> dict:
    """Payload for the 'Company Base' editor: the versioned globals + full ordered steps.
    Unlike build_settings (company delta), steps are returned verbatim for full editing.
    When the store is unseeded (version 0) and base_path names a real file, that file
    populates the editor (tests / one-off runs). Otherwise — the normal state for a fresh
    deployment before an admin uploads a base.yaml — this returns an empty scaffold so the
    panel loads (empty) instead of erroring; the first save/import then writes into the DB
    (version 1)."""
    base = (store or get_config_store()).get_base()
    if base["version"] == 0:
        b = config_store.get_base_config(store=store, base_path=base_path)
        globals_ = {k: b[k] for k in ("defaults", "llm_profiles", "analysis_defaults") if k in b}
        return {"version": 0, "globals": globals_, "steps": b.get("steps") or []}
    return {
        "version": base["version"],
        "globals": base.get("globals") or {},
        "steps": base.get("steps") or [],
    }


def validate_base_steps(steps: list[dict]) -> None:
    """Validate every base step against the schema; raise ValueError on the first problem
    (unknown/duplicate step_id or bad field)."""
    seen: set[str] = set()
    for step in steps or []:
        sid = (step or {}).get("step_id")
        if not sid:
            raise ValueError("Every step needs a step_id.")
        if sid in seen:
            raise ValueError(f"Duplicate step_id '{sid}'.")
        seen.add(sid)
        try:
            config_schema.validate_step(step)
        except Exception as exc:  # pydantic ValidationError
            raise ValueError(f"Step '{sid}': {exc}") from exc


# ---------------------------------------------------------------------------
# Form descriptor — drives the schema-driven step form in the admin UI
# ---------------------------------------------------------------------------
#
# Each field: {path, label, widget, enum?, auto?, show_when?, action_type_path?}
#   - path            dotted path relative to the step root (e.g. "validate.llm.prompt")
#   - widget          text | textarea | bool | number | select | list | params
#   - enum            allowed values for a select
#   - auto            true → revealed automatically when show_when matches (primary field);
#                     false/absent → offered in the "+ Add field" picker (secondary)
#   - show_when       {controlling_path: [allowed_values]} — field is relevant only then
#   - action_type_path (params widget only) sibling action_type that selects list vs dict

_ACTION = ["SUBMIT", "FM", "BAPI", "BDC", "TOOLS"]

# Advisory catalog of TOOLS object names, offered as datalist suggestions on any
# object_name field whose action_type is TOOLS. Purely advisory — an object_name not
# in the catalog still runs (a client adds a tool with only an ABAP WHEN branch). The
# effective catalog is this default merged with any `tools` list saved in base globals.
DEFAULT_TOOLS = ["TOOL_READ_TABLE", "TOOL_READ_JOB_SPOOL", "TOOL_READ_JOB_LOG", "TOOL_JOB_STATUS"]


def tool_catalog(globals_: dict | None = None) -> list[str]:
    """Default tools ⊕ admin-added `tools` from base globals, de-duplicated, order-stable."""
    extra = (globals_ or {}).get("tools") or []
    seen, out = set(), []
    for name in [*DEFAULT_TOOLS, *extra]:
        name = str(name).strip()
        if name and name not in seen:
            seen.add(name)
            out.append(name)
    return out


def _f(path, label, widget, *, enum=None, auto=False, show_when=None, action_type_path=None):
    d = {"path": path, "label": label, "widget": widget, "auto": auto}
    if enum is not None:
        d["enum"] = enum
    if show_when is not None:
        d["show_when"] = show_when
    if action_type_path is not None:
        d["action_type_path"] = action_type_path
    return d


def _label(text, *, show_when=None):
    """A plain, non-editable gray sub-heading inside a section (e.g. 'Step params')."""
    d = {"path": None, "label": text, "widget": "label"}
    if show_when is not None:
        d["show_when"] = show_when
    return d


def form_descriptor(globals_: dict | None = None) -> dict:
    """A UI-oriented description of a step's five sections and their fields, including the
    conditional visibility and logical enums the raw JSON Schema does not encode.

    `globals_` (the base globals) is consulted only for the TOOLS object-name catalog."""
    pc_on  = {"pre_check.enabled": [True]}
    pc_cmp = {"pre_check.enabled": [True], "pre_check.mode": ["comparison"]}
    pc_llm = {"pre_check.enabled": [True], "pre_check.mode": ["llm"]}
    v_run  = {"validate.run": ["__truthy__"]}    # sentinel: object present, not a value match
    # Async is a poll flag that only applies to background-job modes; TOOLS calls never poll,
    # so the Async field is hidden (and cleared client-side) whenever action_type is TOOLS.
    # "!TOOLS" is a negation match handled by showWhenMatches.
    no_tools    = {"action_type": ["!TOOLS"]}
    pc_no_tools = {"pre_check.enabled": [True], "pre_check.action_type": ["!TOOLS"]}
    v_no_tools  = {"validate.run": ["__truthy__"], "validate.run.action_type": ["!TOOLS"]}
    rb_no_tools = {"rollback.enabled": [True], "rollback.action_type": ["!TOOLS"]}
    v_kw   = {"validate.run": ["__truthy__"], "validate.mode": ["keyword"]}
    v_llm  = {"validate.run": ["__truthy__"], "validate.mode": ["llm"]}
    rb_on  = {"rollback.enabled": [True]}

    sections = [
        {"key": "step", "title": "Step", "fields": [
            _f("enabled", "Enabled", "bool", auto=True),
            _f("description", "Description", "text", auto=True),
            _f("action_type", "Action type", "select", enum=_ACTION, auto=True),
            _f("object_name", "Object name", "text", auto=True, action_type_path="action_type"),
            _f("async", "Async (poll)", "bool", auto=True, show_when=no_tools),
            _f("test_run", "Test run", "bool", auto=True),
            _label("Params"),
            _f("params", "Parameters", "params", auto=True, action_type_path="action_type"),
            _f("group", "Parallel group", "text"),
            _f("poll_timeout_sec", "Poll timeout (sec)", "number"),
        ]},
        {"key": "pre_check", "title": "Pre-check", "fields": [
            _f("pre_check.enabled", "Enabled", "bool", auto=True),
            _f("pre_check.action_type", "Action type", "select", enum=_ACTION,
               auto=True, show_when=pc_on),
            _f("pre_check.object_name", "Object name", "text", auto=True, show_when=pc_on,
               action_type_path="pre_check.action_type"),
            _f("pre_check.async", "Async", "bool", auto=True, show_when=pc_no_tools),
            _label("Params", show_when=pc_on),
            _f("pre_check.params", "Parameters", "params", auto=True, show_when=pc_on,
               action_type_path="pre_check.action_type"),
            _label("llm params", show_when=pc_on),
            _f("pre_check.mode", "Mode", "select",
               enum=["comparison", "llm"], auto=True, show_when=pc_on),
            _f("pre_check.llm.system_prompt", "LLM · system prompt", "textarea",
               auto=True, show_when=pc_llm),
            _f("pre_check.llm.prompt", "LLM · prompt", "textarea", auto=True, show_when=pc_llm),
            _f("pre_check.llm.pass_values", "LLM · pass values", "list",
               auto=True, show_when=pc_llm),
            _f("pre_check.llm.max_spool_lines", "LLM · max spool lines", "number",
               auto=True, show_when=pc_llm),
            _f("pre_check.comparison.field", "Compare · field", "text",
               auto=True, show_when=pc_cmp),
            _f("pre_check.comparison.operator", "Compare · operator", "select",
               enum=ENUMS["operator"], auto=True, show_when=pc_cmp),
            _f("pre_check.comparison.value", "Compare · value", "text",
               auto=True, show_when=pc_cmp),
            _f("pre_check.comparison.select", "Compare · select", "select",
               enum=ENUMS["select"], auto=True, show_when=pc_cmp),
            _f("pre_check.comparison.on_pass", "Compare · on pass", "select",
               enum=ENUMS["on_pass"], show_when=pc_cmp),
            _f("pre_check.comparison.on_fail", "Compare · on fail", "select",
               enum=ENUMS["on_fail"], show_when=pc_cmp),
        ]},
        {"key": "validate", "title": "Validate", "fields": [
            _f("validate.run", "Enabled", "block_toggle", auto=True),
            _f("validate.run.action_type", "Action type", "select", enum=_ACTION,
               auto=True, show_when=v_run),
            _f("validate.run.object_name", "Object name", "text", auto=True, show_when=v_run,
               action_type_path="validate.run.action_type"),
            _f("validate.run.async", "Async", "bool", auto=True, show_when=v_no_tools),
            _label("Params", show_when=v_run),
            _f("validate.run.params", "Parameters", "params", auto=True, show_when=v_run,
               action_type_path="validate.run.action_type"),
            _label("llm params", show_when=v_run),
            _f("validate.mode", "Mode", "select", enum=["keyword", "llm"], auto=True, show_when=v_run),
            _f("validate.keyword.source", "Keyword · source", "select",
               enum=ENUMS["source"], auto=True, show_when=v_kw),
            _f("validate.keyword.ok_patterns", "Keyword · ok patterns", "list",
               auto=True, show_when=v_kw),
            _f("validate.keyword.error_patterns", "Keyword · error patterns", "list",
               auto=True, show_when=v_kw),
            _f("validate.llm.system_prompt", "LLM · system prompt", "textarea",
               auto=True, show_when=v_llm),
            _f("validate.llm.prompt", "LLM · prompt", "textarea", auto=True, show_when=v_llm),
            _f("validate.llm.max_spool_lines", "LLM · max spool lines", "number",
               auto=True, show_when=v_llm),
            _f("validate.llm.pass_values", "LLM · pass values", "list", show_when=v_llm),
        ]},
        {"key": "rollback", "title": "Rollback", "fields": [
            _f("rollback.enabled", "Enabled", "bool", auto=True),
            _f("rollback.action_type", "Action type", "select", enum=_ACTION,
               auto=True, show_when=rb_on),
            _f("rollback.object_name", "Object name", "text", auto=True, show_when=rb_on,
               action_type_path="rollback.action_type"),
            _f("rollback.async", "Async", "bool", auto=True, show_when=rb_no_tools),
            _f("rollback.test_run", "Test run", "bool", auto=True, show_when=rb_on),
            _label("Params", show_when=rb_on),
            _f("rollback.params", "Parameters", "params", auto=True, show_when=rb_on,
               action_type_path="rollback.action_type"),
            _f("rollback.poll_timeout_sec", "Poll timeout (sec)", "number", show_when=rb_on),
        ]},
        {"key": "on_error", "title": "On error", "fields": [
            _f("on_error.mode", "Mode", "on_error_mode", enum=["explain", "diagnose"], auto=True),
            _f("on_error.explain_prompt", "Explain prompt", "textarea"),
            _f("on_error.analysis.role", "Analysis · role", "textarea"),
            _f("on_error.analysis.context_template", "Analysis · context", "textarea"),
            _f("on_error.analysis.goal", "Analysis · goal", "textarea"),
            _f("on_error.analysis.tools", "Analysis · tools", "textarea"),
            _f("on_error.analysis.instructions", "Analysis · instructions", "textarea"),
            _f("on_error.analysis.tech_instructions",
               "Analysis · technical playbook", "textarea"),
            _f("on_error.analysis.rules", "Analysis · rules", "textarea"),
            _f("on_error.analysis.max_tool_calls", "Analysis · max tool calls", "number"),
        ]},
    ]
    return {"sections": sections, "action_types": _ACTION,
            "tools": tool_catalog(globals_),
            "globals": globals_descriptor()}


def globals_descriptor() -> list[dict]:
    """Field groups for the base 'Global settings' editor (defaults / llm_profiles /
    analysis_defaults). Fixed shape — not part of a step."""
    profile = lambda p: [  # noqa: E731
        _f(f"llm_profiles.{p}.provider", "Provider", "select",
           enum=["groq", "openrouter"], auto=True),
        _f(f"llm_profiles.{p}.model", "Model", "text", auto=True),
        _f(f"llm_profiles.{p}.temperature", "Temperature", "number", auto=True),
    ]
    return [
        {"key": "defaults", "title": "Defaults", "fields": [
            _f("defaults.max_retries", "Max retries", "number", auto=True),
            _f("defaults.poll_interval_sec", "Poll interval (sec)", "number", auto=True),
            _f("defaults.poll_timeout_sec", "Poll timeout (sec)", "number", auto=True),
            _f("defaults.test_run", "Test run", "bool", auto=True),
            _f("defaults.reset_each_run", "Reset each run", "bool", auto=True),
        ]},
        {"key": "llm_analysis", "title": "LLM profile · analysis", "fields": profile("analysis")},
        {"key": "llm_validation", "title": "LLM profile · validation", "fields": profile("validation")},
        {"key": "analysis_defaults", "title": "Analysis defaults", "fields": [
            _f("analysis_defaults.max_tool_calls", "Max tool calls", "number", auto=True),
            _f("analysis_defaults.role", "Role", "textarea", auto=True),
            _f("analysis_defaults.tools", "Tools", "textarea", auto=True),
            # Optional (auto=False) → hidden behind the section's "+ Add field" until
            # an admin overrides the built-in TECH_DIAGNOSIS_PLAYBOOK.
            _f("analysis_defaults.tech_instructions", "Technical playbook", "textarea"),
            _f("analysis_defaults.rules", "Rules", "textarea", auto=True),
        ]},
    ]
