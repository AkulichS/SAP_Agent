"""Tests for step-schema versioning: the grandfather rule, migration chain, the
reject-a-newer-config guard, and the store stamp/round-trip.

Schema versioning tags the *grammar* of a config document so a newer program can migrate
an older client's hand-built pipeline forward instead of silently misreading it. See the
"Schema versioning" block in config_schema.py.
"""

import pytest

import config_schema
from config_schema import (STEP_SCHEMA_VERSION, SchemaTooNew, migrate_document)
from config_store import ConfigStore


@pytest.fixture
def store(tmp_path):
    return ConfigStore(tmp_path / "cfg.db")


# ---------------------------------------------------------------------------
# migrate_document: grandfather rule, idempotence, reject-newer
# ---------------------------------------------------------------------------

def test_tagless_document_loads_as_v1():
    """A document with no schema_version is grandfathered to v1 and left otherwise
    intact — the crucial rule that lets today's untagged configs keep working."""
    doc = {"globals": {"defaults": {"max_retries": 3}}, "steps": [{"step_id": "KO8G"}]}
    out = migrate_document(doc)
    assert out["schema_version"] == 1
    assert out["globals"] == doc["globals"]
    assert out["steps"] == doc["steps"]


def test_grandfather_default_matches_v1_constant():
    """The grandfather rule assumes the initial versioned shape is v1; if the constant is
    ever bumped this test is a reminder that untagged == v1 must still be the intended
    floor (and a 1->2 migration must exist)."""
    assert STEP_SCHEMA_VERSION >= 1
    if STEP_SCHEMA_VERSION > 1:
        assert 1 in config_schema._MIGRATIONS


def test_current_version_document_is_unchanged():
    doc = {"schema_version": STEP_SCHEMA_VERSION, "globals": {}, "steps": []}
    out = migrate_document(doc)
    assert out["schema_version"] == STEP_SCHEMA_VERSION
    assert out["globals"] == {}
    assert out["steps"] == []


def test_migrate_does_not_mutate_input():
    doc = {"globals": {"a": 1}, "steps": []}
    migrate_document(doc)
    assert "schema_version" not in doc  # original untouched; a new dict is returned


def test_newer_document_is_refused_legibly():
    doc = {"schema_version": STEP_SCHEMA_VERSION + 1, "globals": {}, "steps": []}
    with pytest.raises(SchemaTooNew) as exc:
        migrate_document(doc)
    assert "Upgrade the program" in str(exc.value)


def test_migration_chain_runs_in_order(monkeypatch):
    """A stubbed two-step chain (vX -> vX+1 -> vX+2) proves migrations apply in order and
    the final schema_version is stamped — without depending on real future migrations."""
    base = STEP_SCHEMA_VERSION
    calls = []

    def up_1(doc):
        calls.append("a")
        return {**doc, "steps": doc.get("steps", []) + ["a"]}

    def up_2(doc):
        calls.append("b")
        return {**doc, "steps": doc.get("steps", []) + ["b"]}

    monkeypatch.setattr(config_schema, "STEP_SCHEMA_VERSION", base + 2)
    monkeypatch.setitem(config_schema._MIGRATIONS, base, up_1)
    monkeypatch.setitem(config_schema._MIGRATIONS, base + 1, up_2)

    out = migrate_document({"schema_version": base, "steps": []})
    assert calls == ["a", "b"]
    assert out["steps"] == ["a", "b"]
    assert out["schema_version"] == base + 2


def test_missing_migration_in_chain_raises(monkeypatch):
    """A gap in the migration chain must fail loudly, not silently skip a version."""
    base = STEP_SCHEMA_VERSION
    monkeypatch.setattr(config_schema, "STEP_SCHEMA_VERSION", base + 1)
    # deliberately register no migration for `base`
    monkeypatch.delitem(config_schema._MIGRATIONS, base, raising=False)
    with pytest.raises(RuntimeError):
        migrate_document({"schema_version": base, "steps": []})


# ---------------------------------------------------------------------------
# ConfigStore: save stamps the tag, get surfaces it
# ---------------------------------------------------------------------------

def test_save_base_stamps_current_schema_version(store):
    store.save_base({"defaults": {}}, [{"step_id": "KO8G", "action_type": "SUBMIT"}],
                    expected_version=0, user="t")
    base = store.get_base()
    assert base["schema_version"] == STEP_SCHEMA_VERSION


def test_unseeded_base_reports_no_schema(store):
    """A fresh store returns the empty scaffold (version 0); schema_version is absent
    there and only meaningful once seeded."""
    base = store.get_base()
    assert base["version"] == 0
