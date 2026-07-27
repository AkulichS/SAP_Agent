"""Tests for the per-company delta store, effective-config builder, and settings tree."""

import textwrap

import pytest

import config
import config_settings
import config_store
from config_store import ConfigStore, VersionConflict, load_effective_config


@pytest.fixture
def store(tmp_path):
    return ConfigStore(tmp_path / "cfg.db")


# Minimal base step library for tests that need real base content (KO8G/CO88) — the
# repo no longer ships configs/base.yaml as a seed file, so tests seed the DB tier
# directly instead of relying on config.load_base_config()'s file fallback.
BASE_GLOBALS = {"defaults": {"max_retries": 3}, "llm_profiles": {}}
BASE_STEPS = [
    {
        "step_id": "KO8G",
        "action_type": "SUBMIT",
        "async": True,
        "test_run": True,
        "params": [{"selname": "VAART", "kind": "P", "low": "1"}],
        "validate": {"keyword": {"source": "rows"}},
    },
    {
        "step_id": "CO88",
        "action_type": "SUBMIT",
        "async": True,
        "test_run": True,
    },
]


@pytest.fixture
def seeded_store(store):
    """A store pre-seeded with BASE_STEPS, for tests that exercise override/settings
    logic against real step content rather than the (deleted) file-fallback path."""
    store.save_base(BASE_GLOBALS, BASE_STEPS, expected_version=0, user="fixture")
    return store


# ---------------------------------------------------------------------------
# Store: save / get / version locking / history / reset
# ---------------------------------------------------------------------------

def test_empty_company_is_base(store):
    state = store.get_override("RU06")
    assert state == {"override": {}, "run_context": {}, "version": 0}


def test_save_get_and_version_bump(store):
    st = store.save_override("RU06", {"KO8G": {"enabled": False}},
                             {"currency": "RUB"}, expected_version=0, user="admin")
    assert st["version"] == 1
    got = store.get_override("RU06")
    assert got["override"] == {"KO8G": {"enabled": False}}
    assert got["run_context"] == {"currency": "RUB"}
    assert got["version"] == 1


def test_optimistic_version_conflict(store):
    store.save_override("RU06", {}, {}, expected_version=0, user="a")
    with pytest.raises(VersionConflict):
        store.save_override("RU06", {}, {}, expected_version=0, user="b")


def test_history_records_each_version(store):
    store.save_override("RU06", {"KO8G": {"enabled": False}}, {}, expected_version=0, user="a")
    store.save_override("RU06", {"KO8G": {"test_run": True}}, {}, expected_version=1, user="b")
    hist = store.history("RU06")
    assert [h["version"] for h in hist] == [2, 1]
    assert hist[0]["updated_by"] == "b"


def test_reset_step_and_company(store):
    store.save_override("RU06", {"KO8G": {"enabled": False}, "CO88": {"test_run": True}},
                        {"currency": "RUB"}, expected_version=0, user="a")
    store.reset_step("RU06", "KO8G", expected_version=1, user="a")
    assert "KO8G" not in store.get_override("RU06")["override"]
    assert "CO88" in store.get_override("RU06")["override"]
    store.reset_company("RU06", expected_version=2, user="a")
    state = store.get_override("RU06")
    assert state["override"] == {}
    assert state["run_context"] == {"currency": "RUB"}   # run_context preserved


def test_delete_oldest_history_prunes_oldest_first(store):
    for i in range(4):
        store.save_override("RU06", {"KO8G": {"idx": i}}, {}, expected_version=i, user="a")
    assert [h["version"] for h in store.history("RU06")] == [4, 3, 2, 1]

    assert store.delete_oldest_history("RU06") == {"deleted_version": 1, "remaining": 3}
    assert [h["version"] for h in store.history("RU06")] == [4, 3, 2]
    assert store.delete_oldest_history("RU06")["deleted_version"] == 2
    assert [h["version"] for h in store.history("RU06")] == [4, 3]

    # live config + version counter are untouched by pruning history
    assert store.get_override("RU06")["version"] == 4


def test_delete_oldest_history_keeps_last_remaining(store):
    store.save_override("RU06", {}, {}, expected_version=0, user="a")
    with pytest.raises(ValueError):
        store.delete_oldest_history("RU06")   # only the current version's row remains


def test_delete_oldest_base_history_prunes_oldest_first(store):
    for i in range(3):
        store.save_base({"defaults": {"n": i}}, [], expected_version=i, user="a")
    assert [h["version"] for h in store.base_history()] == [3, 2, 1]
    assert store.delete_oldest_base_history() == {"deleted_version": 1, "remaining": 2}
    assert [h["version"] for h in store.base_history()] == [3, 2]
    assert store.get_base()["version"] == 3
    store.delete_oldest_base_history()
    with pytest.raises(ValueError):
        store.delete_oldest_base_history()    # only the current version's row remains


# ---------------------------------------------------------------------------
# Effective config: base ⊕ delta
# ---------------------------------------------------------------------------

def test_effective_applies_delta_and_run_context(seeded_store):
    seeded_store.save_override("RU06", {"KO8G": {"test_run": False}},
                        {"controlling_area": "X500"}, expected_version=0, user="a")
    cfg = load_effective_config("RU06", period="11", fiscal_year="2025", store=seeded_store)
    rc = cfg["company_config"]
    assert rc["company_code"] == "RU06" and rc["period"] == "11" and rc["controlling_area"] == "X500"
    ko8g = next(s for s in cfg["steps"] if s["step_id"] == "KO8G")
    assert ko8g["test_run"] is False


def test_enabled_false_drops_step(seeded_store):
    base_ids = {s["step_id"] for s in config_store.get_base_steps(store=seeded_store)}
    assert "CO88" in base_ids
    seeded_store.save_override("RU06", {"CO88": {"enabled": False}}, {}, expected_version=0, user="a")
    cfg = load_effective_config("RU06", store=seeded_store)
    assert "CO88" not in {s["step_id"] for s in cfg["steps"]}


def test_param_override_merges_by_selname(seeded_store):
    seeded_store.save_override("RU06", {"KO8G": {"params": [{"selname": "VAART", "low": "2"}]}},
                        {}, expected_version=0, user="a")
    cfg = load_effective_config("RU06", store=seeded_store)
    ko8g = next(s for s in cfg["steps"] if s["step_id"] == "KO8G")
    vaart = next(p for p in ko8g["params"] if p["selname"] == "VAART")
    assert vaart["low"] == "2"


# ---------------------------------------------------------------------------
# Settings field tree + validation
# ---------------------------------------------------------------------------

def test_build_settings_marks_inherited_vs_overridden(seeded_store):
    seeded_store.save_override("RU06", {"KO8G": {"test_run": False}}, {}, expected_version=0, user="a")
    data = config_settings.build_settings("RU06", store=seeded_store)
    ko8g = next(s for s in data["steps"] if s["step_id"] == "KO8G")
    by_path = {f["path"]: f for f in ko8g["fields"]}
    assert by_path["test_run"]["overridden"] is True
    assert by_path["test_run"]["value"] is False
    assert by_path["action_type"]["overridden"] is False          # inherited
    assert by_path["action_type"]["base_value"] == "SUBMIT"
    assert by_path["action_type"]["widget"] == "select"


def test_build_settings_carries_raw_override_and_base_step(seeded_store):
    """The settings form renders from the base step + the stored delta, so both ship —
    and `base` stays the untouched template (the UI merges the delta over it)."""
    seeded_store.save_override("RU06", {"KO8G": {"test_run": False}}, {}, expected_version=0, user="a")
    data = config_settings.build_settings("RU06", store=seeded_store)
    assert data["override"] == {"KO8G": {"test_run": False}}

    base_ko8g = next(s for s in config_store.get_base_steps(seeded_store) if s["step_id"] == "KO8G")
    ko8g = next(s for s in data["steps"] if s["step_id"] == "KO8G")
    assert ko8g["base"] == base_ko8g


def test_validate_override_rejects_bad_enum_and_unknown_step():
    config_settings.validate_override({"KO8G": {"validate": {"keyword": {"source": "rows"}}}})
    with pytest.raises(ValueError):
        config_settings.validate_override({"KO8G": {"validate": {"keyword": {"source": "BOGUS"}}}})
    with pytest.raises(ValueError):
        config_settings.validate_override({"NO_SUCH_STEP": {"x": 1}})


# ---------------------------------------------------------------------------
# Base tier: get / save / version locking / history / file fallback
# ---------------------------------------------------------------------------

def test_base_unseeded_is_version_zero(store):
    assert store.get_base() == {"globals": {}, "steps": [], "version": 0}


def test_get_base_steps_falls_back_to_files_then_db(store, tmp_path):
    # unseeded → file fallback. The repo no longer ships a seed configs/base.yaml,
    # so the test supplies its own file via an explicit base_path.
    base_file = tmp_path / "base.yaml"
    base_file.write_text(textwrap.dedent("""
        steps:
          - step_id: KO8G
            action_type: SUBMIT
          - step_id: CO88
            action_type: SUBMIT
    """), encoding="utf-8")
    file_ids = {s["step_id"] for s in config.load_base_config(base_file)["steps"]}
    assert file_ids == {"KO8G", "CO88"}
    assert {s["step_id"] for s in config_store.get_base_steps(store=store, base_path=base_file)} == file_ids
    # after seeding a smaller base, DB wins (base_path is only consulted while unseeded)
    step = {"step_id": "KO8G", "action_type": "SUBMIT", "async": True}
    store.save_base({"defaults": {"max_retries": 2}}, [step], expected_version=0, user="a")
    assert [s["step_id"] for s in config_store.get_base_steps(store=store, base_path=base_file)] == ["KO8G"]


def test_save_base_bumps_version_and_history(store):
    s1 = {"step_id": "A", "action_type": "SUBMIT", "async": True}
    s2 = {"step_id": "B", "action_type": "TOOLS", "async": False}
    r = store.save_base({"defaults": {}}, [s1, s2], expected_version=0, user="a")
    assert r["version"] == 1
    store.save_base({"defaults": {}}, [s2, s1], expected_version=1, user="b")   # reorder
    got = store.get_base()
    assert [s["step_id"] for s in got["steps"]] == ["B", "A"] and got["version"] == 2
    hist = store.base_history()
    assert [h["version"] for h in hist] == [2, 1]
    assert hist[0]["updated_by"] == "b"


def test_save_base_optimistic_conflict(store):
    step = {"step_id": "A", "action_type": "SUBMIT", "async": True}
    store.save_base({}, [step], expected_version=0, user="a")
    with pytest.raises(VersionConflict):
        store.save_base({}, [step], expected_version=0, user="b")


def test_save_base_rejects_invalid_step(store):
    with pytest.raises(ValueError):
        store.save_base({}, [{"step_id": "A", "action_type": "NOPE"}], expected_version=0)


def test_effective_config_sources_base_from_db(store):
    step = {"step_id": "ONLYSTEP", "action_type": "SUBMIT", "async": True,
            "params": [{"selname": "KOKRS", "kind": "P", "low": "{{controlling_area}}"}]}
    store.save_base({"defaults": {"max_retries": 5}, "llm_profiles": {}}, [step],
                    expected_version=0, user="a")
    store.save_override("RU06", {"ONLYSTEP": {"test_run": False}},
                        {"controlling_area": "X500"}, expected_version=0, user="a")
    cfg = load_effective_config("RU06", period="11", fiscal_year="2025", store=store)
    assert [s["step_id"] for s in cfg["steps"]] == ["ONLYSTEP"]
    assert cfg["steps"][0]["test_run"] is False
    assert cfg["defaults"]["max_retries"] == 5
    assert cfg["company_config"]["controlling_area"] == "X500"


# ---------------------------------------------------------------------------
# Company registry
# ---------------------------------------------------------------------------

def test_registry_create_list_rename_delete(store):
    store.create_company("RU06", "Company A", {"controlling_area": "X500"}, user="a")
    store.create_company("RU47", "Company B", user="a")
    assert {c["code"] for c in store.list_registry()} == {"RU06", "RU47"}
    # run_context was seeded for RU06
    assert store.get_override("RU06")["run_context"] == {"controlling_area": "X500"}
    store.rename_company("RU06", "Renamed")
    assert next(c for c in store.list_registry() if c["code"] == "RU06")["display_name"] == "Renamed"
    store.delete_company("RU06")
    assert {c["code"] for c in store.list_registry()} == {"RU47"}
    assert store.get_override("RU06")["version"] == 0   # overrides gone


def test_registry_duplicate_rejected(store):
    store.create_company("RU06", "A", user="a")
    with pytest.raises(ValueError):
        store.create_company("RU06", "dup", user="a")


# ---------------------------------------------------------------------------
# Form descriptor
# ---------------------------------------------------------------------------

def test_form_descriptor_shape():
    d = config_settings.form_descriptor()
    keys = [s["key"] for s in d["sections"]]
    assert keys == ["step", "pre_check", "validate", "rollback", "on_error"]
    # every field carries the metadata the UI form relies on
    for sec in d["sections"]:
        for f in sec["fields"]:
            assert "path" in f and "label" in f and "widget" in f
    # conditional visibility + logical enums are present
    validate = next(s for s in d["sections"] if s["key"] == "validate")
    llm_prompt = next(f for f in validate["fields"] if f["path"] == "validate.llm.prompt")
    assert llm_prompt["show_when"].get("validate.mode") == ["llm"]
    assert "globals" in d and any(g["key"] == "defaults" for g in d["globals"])


def test_form_descriptor_object_name_has_action_type_path():
    # Every object-name field advertises its sibling action_type so the UI can offer the
    # TOOLS catalog as a datalist only when action_type is TOOLS.
    d = config_settings.form_descriptor()
    obj_fields = [f for s in d["sections"] for f in s["fields"]
                  if f.get("path") and f["path"].endswith("object_name")]
    assert obj_fields and all(f.get("action_type_path") for f in obj_fields)


def test_tool_catalog_merges_defaults_with_globals():
    # Defaults first, admin-added names appended, de-duplicated, order-stable.
    cat = config_settings.tool_catalog({"tools": ["TOOL_NEW", "TOOL_READ_TABLE", " "]})
    assert cat[:len(config_settings.DEFAULT_TOOLS)] == config_settings.DEFAULT_TOOLS
    assert cat[-1] == "TOOL_NEW"
    assert cat.count("TOOL_READ_TABLE") == 1
    assert "" not in cat
    # descriptor surfaces the catalog for the UI datalist
    assert config_settings.form_descriptor({"tools": ["TOOL_NEW"]})["tools"][-1] == "TOOL_NEW"


def test_async_fields_hidden_for_tools():
    # Every Async field is gated so it hides (and is cleared client-side) under action_type TOOLS.
    d = config_settings.form_descriptor()
    async_fields = [f for s in d["sections"] for f in s["fields"]
                    if f.get("path") and f["path"].endswith("async")]
    assert async_fields
    for f in async_fields:
        sw = f.get("show_when") or {}
        at_key = next(k for k in sw if k.endswith("action_type"))
        assert sw[at_key] == ["!TOOLS"]


def test_build_base_settings_file_fallback(store, tmp_path):
    base_file = tmp_path / "base.yaml"
    base_file.write_text(textwrap.dedent("""
        defaults:
          max_retries: 3
        steps:
          - step_id: KO8G
            action_type: SUBMIT
    """), encoding="utf-8")
    bs = config_settings.build_base_settings(store=store, base_path=base_file)
    assert bs["version"] == 0
    assert {s["step_id"] for s in bs["steps"]} == {s["step_id"] for s in config.load_base_config(base_file)["steps"]}
    assert "defaults" in bs["globals"]
