"""Pure helper functions in graph_builder — no mocks, no I/O."""

from types import SimpleNamespace

import pytest

from graph_builder import _as_number, _compare, _get_step, _parse_tool_result, _resolve


# ---------------------------------------------------------------------------
# _resolve — {{token}} substitution
# ---------------------------------------------------------------------------

def test_resolve_simple_string():
    assert _resolve("period={{period}}", {"period": "11"}) == "period=11"


def test_resolve_multiple_tokens():
    ctx = {"period": "11", "fiscal_year": "2025"}
    assert _resolve("{{fiscal_year}}/{{period}}", ctx) == "2025/11"


def test_resolve_missing_token_left_untouched():
    assert _resolve("{{unknown}}", {"period": "11"}) == "{{unknown}}"


def test_resolve_recurses_into_dict_and_list():
    ctx = {"period": "11"}
    obj = {"where": "PERAB = {{period}}", "vals": ["{{period}}", "x"]}
    assert _resolve(obj, ctx) == {"where": "PERAB = 11", "vals": ["11", "x"]}


def test_resolve_non_string_passthrough():
    assert _resolve(42, {"period": "11"}) == 42
    assert _resolve(None, {"period": "11"}) is None


# ---------------------------------------------------------------------------
# _as_number — tolerant numeric parsing
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("raw,expected", [
    ("100", 100.0),
    ("1,000.00", 1000.0),       # thousands separator
    ("100.00-", -100.0),        # SAP trailing-minus
    ("  5 ", 5.0),
    ("abc", None),
    ("", None),
    (None, None),
])
def test_as_number(raw, expected):
    assert _as_number(raw) == expected


# ---------------------------------------------------------------------------
# _compare — numeric when both parse, else string
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("actual,op,expected,result", [
    ("100.00", "eq", "100", True),     # numeric equality despite formatting
    ("5", "gt", "3", True),
    ("3", "gt", "5", False),
    ("3", "lt", "5", True),
    ("5", "ge", "5", True),
    ("5", "le", "4", False),
    ("5", "ne", "4", True),
    ("hello world", "contains", "world", True),
    ("hello", "contains", "zzz", False),
    ("AB", "eq", "AB", True),          # string equality (non-numeric)
    ("AB", "eq", "CD", False),
    ("5", "bogus", "5", False),        # unknown operator -> False
])
def test_compare(actual, op, expected, result):
    assert _compare(actual, op, expected) is result


def test_compare_defaults_to_eq():
    assert _compare("7", None, "7") is True


# ---------------------------------------------------------------------------
# _get_step — lookup by current_step_id
# ---------------------------------------------------------------------------

def test_get_step_found():
    state = {"current_step_id": "B", "steps": [{"step_id": "A"}, {"step_id": "B"}]}
    assert _get_step(state) == {"step_id": "B"}


def test_get_step_missing_raises():
    state = {"current_step_id": "Z", "steps": [{"step_id": "A"}]}
    with pytest.raises(KeyError):
        _get_step(state)


# ---------------------------------------------------------------------------
# _parse_tool_result — extract JSON from an MCP CallToolResult
# ---------------------------------------------------------------------------

def _result(text):
    return SimpleNamespace(content=[SimpleNamespace(type="text", text=text)])


def test_parse_tool_result_json():
    assert _parse_tool_result(_result('{"a": 1}')) == {"a": 1}


def test_parse_tool_result_non_json_falls_back_to_raw():
    assert _parse_tool_result(_result("not json")) == {"raw": "not json"}


def test_parse_tool_result_empty_content():
    assert _parse_tool_result(SimpleNamespace(content=[])) == {}
