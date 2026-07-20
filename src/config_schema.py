"""
config_schema.py — Pydantic models for a period-close step.

Single source of truth for the step shape. It powers three things:
  1. load-time / save-time validation (`validate_step`) — fail fast on bad keys/enums,
  2. the JSON Schema emitted to `configs/step.schema.json` (editor autocomplete in the
     per-step YAML files, via the `# yaml-language-server: $schema=...` header),
  3. field metadata (label / widget / group) the admin Settings form renders from.

Validation policy: structured sections forbid unknown keys (typos surface immediately);
the free-form `analysis` prose block allows extra keys (it carries many optional prompt
sections). Enums are pinned only where the value set is stable; softer fields stay `str`.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Literal, Optional, Union

from pydantic import BaseModel, ConfigDict, Field

ActionType = Literal["SUBMIT", "FM", "BAPI", "BDC", "TOOLS"]

# Params are either a selname-keyed list (SUBMIT/FM/BAPI/BDC) or a single dict (TOOLS).
ParamsField = Union[list["Param"], dict[str, Any]]


class _Strict(BaseModel):
    """Base for structured sections: unknown keys are rejected, aliases accepted by name."""
    model_config = ConfigDict(extra="forbid", populate_by_name=True)


class _Open(BaseModel):
    """Base for free-form prose sections: unknown keys allowed."""
    model_config = ConfigDict(extra="allow", populate_by_name=True)


# ---------------------------------------------------------------------------
# Leaf sections
# ---------------------------------------------------------------------------

class Param(_Strict):
    selname: str = Field(json_schema_extra={"label": "Select name", "widget": "text"})
    kind: str = Field("P", json_schema_extra={"label": "Kind", "widget": "text"})
    low: str = Field("", json_schema_extra={"label": "Low", "widget": "text"})
    high: Optional[str] = Field(None, json_schema_extra={"label": "High", "widget": "text"})


class Comparison(_Strict):
    field: Optional[str] = None
    operator: Optional[str] = None          # eq | ne | gt | lt | ... (kept as str)
    value: Optional[Any] = None
    select: Optional[str] = None            # empty | non_empty | first | last
    on_pass: str = "execute"
    on_fail: str = "analyse"                # execute | skip | analyse (llm/error aliases)


class LlmCheck(_Open):
    prompt: str = Field("", json_schema_extra={"widget": "textarea"})
    pass_values: Optional[list[str]] = None
    system_prompt: Optional[str] = Field(None, json_schema_extra={"widget": "textarea"})
    max_spool_lines: Optional[int] = None


class KeywordCheck(_Strict):
    ok_patterns: list[str] = Field(default_factory=list)
    error_patterns: list[str] = Field(default_factory=list)
    source: Literal["spool", "rows", "messages"] = "spool"


class ActionBlock(_Strict):
    """A runnable action used by pre_check and validate.run."""
    action_type: ActionType = "TOOLS"
    object_name: Optional[str] = None
    async_: bool = Field(False, alias="async")
    params: Optional[ParamsField] = None


class PreCheck(_Strict):
    enabled: bool = Field(False, json_schema_extra={"group": "pre_check"})
    mode: Literal["skip", "comparison", "llm"] = "skip"
    action_type: Optional[ActionType] = None
    object_name: Optional[str] = None
    async_: bool = Field(False, alias="async")
    params: Optional[ParamsField] = None
    comparison: Optional[Comparison] = None
    llm: Optional[LlmCheck] = None


class Validate(_Strict):
    mode: Literal["keyword", "llm"] = "keyword"
    run: Optional[ActionBlock] = None
    keyword: Optional[KeywordCheck] = None
    llm: Optional[LlmCheck] = None


class Rollback(_Strict):
    enabled: bool = False
    action_type: Optional[ActionType] = None
    object_name: Optional[str] = None
    async_: bool = Field(False, alias="async")
    test_run: Optional[bool] = None
    poll_timeout_sec: Optional[int] = None
    params: Optional[ParamsField] = None


class Analysis(_Open):
    """Structured diagnose prompt; free-form (role/tools/goal/context_template/instructions/…)."""
    goal: Optional[str] = Field(None, json_schema_extra={"widget": "textarea"})
    context_template: Optional[str] = Field(None, json_schema_extra={"widget": "textarea"})
    instructions: Optional[str] = Field(None, json_schema_extra={"widget": "textarea"})


class OnError(_Strict):
    # mode is "explain" | "diagnose" | {pre_check: ..., validate: ...}
    mode: Optional[Union[str, dict[str, str]]] = None
    explain_prompt: Optional[str] = Field(None, json_schema_extra={"widget": "textarea"})
    analysis: Optional[Analysis] = None
    analysis_guidance: Optional[str] = Field(None, json_schema_extra={"widget": "textarea"})


# ---------------------------------------------------------------------------
# Step
# ---------------------------------------------------------------------------

class Step(_Strict):
    step_id: str
    description: Optional[str] = Field(None, json_schema_extra={"group": "step"})
    group: Optional[str] = Field(None, json_schema_extra={"group": "step"})
    action_type: ActionType = Field("SUBMIT", json_schema_extra={"group": "step"})
    object_name: Optional[str] = Field(None, json_schema_extra={"group": "step"})
    async_: bool = Field(True, alias="async", json_schema_extra={"group": "step"})
    test_run: Optional[bool] = Field(None, json_schema_extra={"group": "step"})
    poll_timeout_sec: Optional[int] = Field(None, json_schema_extra={"group": "step"})
    enabled: bool = Field(True, json_schema_extra={"group": "step"})   # company delta toggle
    params: Optional[ParamsField] = Field(None, json_schema_extra={"group": "params"})
    pre_check: Optional[PreCheck] = Field(None, json_schema_extra={"group": "pre_check"})
    validate_: Optional[Validate] = Field(None, alias="validate", json_schema_extra={"group": "validate"})
    rollback: Optional[Rollback] = Field(None, json_schema_extra={"group": "rollback"})
    on_error: Optional[OnError] = Field(None, json_schema_extra={"group": "on_error"})


# ---------------------------------------------------------------------------
# Public helpers
# ---------------------------------------------------------------------------

def validate_step(data: dict) -> Step:
    """Validate a (merged, effective) step dict against the schema. Raises ValidationError."""
    return Step.model_validate(data)


def step_json_schema() -> dict:
    return Step.model_json_schema(by_alias=True)


def write_schema_file(path: str | Path | None = None) -> Path:
    """Write configs/step.schema.json (used by the YAML language server + the UI form)."""
    dest = Path(path) if path else Path(__file__).parent / "configs" / "step.schema.json"
    dest.write_text(json.dumps(step_json_schema(), indent=2), encoding="utf-8")
    return dest


if __name__ == "__main__":
    out = write_schema_file()
    print(f"Wrote {out}")
