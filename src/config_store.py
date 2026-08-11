"""
config_store.py — SQLite store: the single source of truth for configuration.

Three tiers, all versioned with optimistic locking so two admins can't silently clobber:

  * base_config   — the product base: {globals, steps} as one versioned document
                    (globals = defaults/llm_profiles/analysis_defaults; steps ordered =
                    the pipeline). Prompts are stored inline in the step textarea fields.
  * company_overrides — each company_code's sparse **delta** over the base + run_context.
  * companies     — the company registry, and the only one: companies are created,
                    renamed and deleted in the admin UI ("Manage companies"), never in a
                    file. A fresh DB simply starts with no companies.

The base tier has no bundled seed file — a fresh deployment seeds it via the admin UI
(Settings → Company Base → Manage companies → upload a base.yaml exported from another
deployment), which round-trips through ConfigStore.save_base(...). If base_config is
unseeded (version 0) and nothing has been uploaded yet, get_base_config/build_base_settings
return an empty scaffold ({defaults:{}, llm_profiles:{}, analysis_defaults:{}, steps:[]})
so the Settings panel loads (empty) instead of erroring.

Company delta shape mirrors a step so it feeds the existing merge engine directly:
    {"KO8G": {"params": [{"selname": "VAART", "low": "2"}],
              "validate": {"keyword": {"source": "rows"}},
              "enabled": false}}

run_context (company_code, controlling_area, currency) is stored alongside the delta.
"""

from __future__ import annotations

import json
import os
import sqlite3
import threading
from datetime import datetime, timezone
from pathlib import Path

DEFAULT_DB = Path(__file__).parent / "db" / "config_store.db"


class VersionConflict(Exception):
    """Raised when a save's expected_version does not match the stored version."""


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


class ConfigStore:
    def __init__(self, db_path: str | Path | None = None):
        self.db_path = str(db_path or os.getenv("CONFIG_DB") or DEFAULT_DB)
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._init_db()

    # -- connection ---------------------------------------------------------

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA foreign_keys=ON")
        return conn

    def _init_db(self) -> None:
        with self._lock, self._connect() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS company_overrides (
                    company_code     TEXT PRIMARY KEY,
                    override_json    TEXT NOT NULL DEFAULT '{}',
                    run_context_json TEXT NOT NULL DEFAULT '{}',
                    version          INTEGER NOT NULL DEFAULT 0,
                    updated_at       TEXT,
                    updated_by       TEXT
                );
                CREATE TABLE IF NOT EXISTS company_overrides_history (
                    company_code     TEXT NOT NULL,
                    version          INTEGER NOT NULL,
                    override_json    TEXT NOT NULL,
                    run_context_json TEXT NOT NULL,
                    updated_at       TEXT,
                    updated_by       TEXT,
                    PRIMARY KEY (company_code, version)
                );
                CREATE TABLE IF NOT EXISTS base_config (
                    id          INTEGER PRIMARY KEY CHECK (id = 1),
                    doc_json    TEXT NOT NULL DEFAULT '{}',
                    version     INTEGER NOT NULL DEFAULT 0,
                    updated_at  TEXT,
                    updated_by  TEXT
                );
                CREATE TABLE IF NOT EXISTS base_config_history (
                    version     INTEGER PRIMARY KEY,
                    doc_json    TEXT NOT NULL,
                    updated_at  TEXT,
                    updated_by  TEXT
                );
                CREATE TABLE IF NOT EXISTS companies (
                    code         TEXT PRIMARY KEY,
                    display_name TEXT,
                    created_at   TEXT,
                    updated_at   TEXT
                );
                CREATE TABLE IF NOT EXISTS runtime_kv (
                    key        TEXT PRIMARY KEY,
                    value_json TEXT NOT NULL DEFAULT '{}',
                    updated_at TEXT
                );
                """
            )

    # -- reads --------------------------------------------------------------

    def get_override(self, company_code: str) -> dict:
        """Return {override, run_context, version}. version 0 means 'no row yet' (== base)."""
        with self._connect() as conn:
            row = conn.execute(
                "SELECT override_json, run_context_json, version "
                "FROM company_overrides WHERE company_code = ?",
                (company_code,),
            ).fetchone()
        if row is None:
            return {"override": {}, "run_context": {}, "version": 0}
        return {
            "override": json.loads(row["override_json"]),
            "run_context": json.loads(row["run_context_json"]),
            "version": row["version"],
        }

    # -- runtime key/value (small process-wide runtime state, not versioned config) --------

    def get_runtime_value(self, key: str) -> dict | None:
        """Return the JSON value stored under `key`, or None if unset. Used for small runtime
        state such as the licensing binding grace-window anchor (see licensing.py)."""
        with self._connect() as conn:
            row = conn.execute(
                "SELECT value_json FROM runtime_kv WHERE key = ?", (key,)
            ).fetchone()
        return json.loads(row["value_json"]) if row else None

    def set_runtime_value(self, key: str, value: dict) -> None:
        """Upsert a small JSON value under `key` (no versioning, no history)."""
        with self._lock, self._connect() as conn:
            conn.execute(
                "INSERT INTO runtime_kv (key, value_json, updated_at) VALUES (?,?,?) "
                "ON CONFLICT(key) DO UPDATE SET "
                "  value_json=excluded.value_json, updated_at=excluded.updated_at",
                (key, json.dumps(value or {}, ensure_ascii=False), _now()),
            )

    def list_companies(self) -> list[dict]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT company_code, version, updated_at, updated_by "
                "FROM company_overrides ORDER BY company_code"
            ).fetchall()
        return [dict(r) for r in rows]

    def history(self, company_code: str) -> list[dict]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT version, updated_at, updated_by, override_json, run_context_json "
                "FROM company_overrides_history "
                "WHERE company_code = ? ORDER BY version DESC",
                (company_code,),
            ).fetchall()
        result = []
        for r in rows:
            d = dict(r)
            d["override"] = json.loads(d.pop("override_json") or "{}")
            d["run_context"] = json.loads(d.pop("run_context_json") or "{}")
            result.append(d)
        return result

    # -- writes -------------------------------------------------------------

    def save_override(
        self,
        company_code: str,
        override: dict,
        run_context: dict | None = None,
        expected_version: int | None = None,
        user: str | None = None,
    ) -> dict:
        """Atomically replace a company's delta + run_context, bump version, snapshot history.

        Optimistic locking: if expected_version is given and differs from the stored
        version, raise VersionConflict (the caller should reload and retry). Pass
        expected_version=None to force-write (used by migration/seeding)."""
        override_json = json.dumps(override or {}, ensure_ascii=False)
        ts = _now()
        with self._lock, self._connect() as conn:
            cur = conn.execute(
                "SELECT run_context_json, version FROM company_overrides WHERE company_code = ?",
                (company_code,),
            ).fetchone()
            current_version = cur["version"] if cur else 0
            if expected_version is not None and expected_version != current_version:
                raise VersionConflict(
                    f"{company_code}: expected v{expected_version}, stored v{current_version}"
                )
            # run_context defaults to the existing one when not supplied
            if run_context is None:
                run_context = json.loads(cur["run_context_json"]) if cur else {}
            run_context_json = json.dumps(run_context, ensure_ascii=False)
            new_version = current_version + 1

            conn.execute(
                "INSERT INTO company_overrides "
                "  (company_code, override_json, run_context_json, version, updated_at, updated_by) "
                "VALUES (?,?,?,?,?,?) "
                "ON CONFLICT(company_code) DO UPDATE SET "
                "  override_json=excluded.override_json, "
                "  run_context_json=excluded.run_context_json, "
                "  version=excluded.version, updated_at=excluded.updated_at, updated_by=excluded.updated_by",
                (company_code, override_json, run_context_json, new_version, ts, user),
            )
            conn.execute(
                "INSERT INTO company_overrides_history "
                "  (company_code, version, override_json, run_context_json, updated_at, updated_by) "
                "VALUES (?,?,?,?,?,?)",
                (company_code, new_version, override_json, run_context_json, ts, user),
            )
        return {"override": override or {}, "run_context": run_context, "version": new_version}

    def delete_oldest_history(self, company_code: str) -> dict:
        """Prune the single oldest (lowest-version) change-history entry for a company.

        Only history rows are touched — the live config, its `version` counter and
        optimistic locking are unaffected. Never deletes the last remaining row (which
        records the provenance of the current config); raises ValueError in that case.
        Returns {deleted_version, remaining}."""
        with self._lock, self._connect() as conn:
            rows = conn.execute(
                "SELECT version FROM company_overrides_history "
                "WHERE company_code = ? ORDER BY version",
                (company_code,),
            ).fetchall()
            if len(rows) <= 1:
                raise ValueError(
                    "Nothing to prune: only the current version's history remains"
                )
            oldest = rows[0]["version"]
            conn.execute(
                "DELETE FROM company_overrides_history WHERE company_code = ? AND version = ?",
                (company_code, oldest),
            )
        return {"deleted_version": oldest, "remaining": len(rows) - 1}

    def reset_company(self, company_code: str, expected_version: int | None = None,
                      user: str | None = None) -> dict:
        """Reset an entire company back to base (empty delta), preserving run_context."""
        return self.save_override(company_code, {}, None, expected_version, user)

    def reset_step(self, company_code: str, step_id: str, expected_version: int | None = None,
                   user: str | None = None) -> dict:
        """Drop one step's delta (revert that step to base)."""
        state = self.get_override(company_code)
        override = state["override"]
        override.pop(step_id, None)
        return self.save_override(company_code, override, state["run_context"], expected_version, user)

    # -- base tier ----------------------------------------------------------
    # The product base itself now lives in the store (globals + ordered steps),
    # one versioned document with its own history — the same optimistic-lock model
    # as company overrides. version 0 means 'no row yet' → callers fall back to files.

    def get_base(self) -> dict:
        """Return {globals, steps, version}. version 0 means unseeded (use file fallback)."""
        with self._connect() as conn:
            row = conn.execute(
                "SELECT doc_json, version FROM base_config WHERE id = 1"
            ).fetchone()
        if row is None:
            return {"globals": {}, "steps": [], "version": 0}
        doc = json.loads(row["doc_json"]) or {}
        return {
            "globals": doc.get("globals", {}),
            "steps": doc.get("steps", []),
            # untagged stored doc == pre-versioning == schema v1 (grandfather rule)
            "schema_version": doc.get("schema_version", 1),
            "version": row["version"],
        }

    def save_base(
        self,
        globals_: dict,
        steps: list[dict],
        expected_version: int | None = None,
        user: str | None = None,
    ) -> dict:
        """Atomically replace the base document (globals + ordered steps), bump version,
        snapshot history. Every step is schema-validated first (raises ValueError on a bad
        step). Step list order IS the pipeline order. Optimistic locking mirrors
        save_override; expected_version=None force-writes (used by seeding)."""
        import config_schema  # local import avoids import-time coupling

        for step in steps or []:
            sid = (step or {}).get("step_id", "?")
            try:
                config_schema.validate_step(step)
            except Exception as exc:  # pydantic ValidationError
                raise ValueError(f"Step '{sid}': {exc}") from exc

        # A successful save is always current-shape: the steps came either from the
        # current-program admin form or from an import that already ran migrate_document.
        # So stamp the running program's schema version onto the stored document.
        doc_json = json.dumps(
            {"schema_version": config_schema.STEP_SCHEMA_VERSION,
             "globals": globals_ or {}, "steps": steps or []},
            ensure_ascii=False,
        )
        ts = _now()
        with self._lock, self._connect() as conn:
            cur = conn.execute("SELECT version FROM base_config WHERE id = 1").fetchone()
            current_version = cur["version"] if cur else 0
            if expected_version is not None and expected_version != current_version:
                raise VersionConflict(
                    f"base: expected v{expected_version}, stored v{current_version}"
                )
            new_version = current_version + 1
            conn.execute(
                "INSERT INTO base_config (id, doc_json, version, updated_at, updated_by) "
                "VALUES (1,?,?,?,?) "
                "ON CONFLICT(id) DO UPDATE SET "
                "  doc_json=excluded.doc_json, version=excluded.version, "
                "  updated_at=excluded.updated_at, updated_by=excluded.updated_by",
                (doc_json, new_version, ts, user),
            )
            conn.execute(
                "INSERT INTO base_config_history (version, doc_json, updated_at, updated_by) "
                "VALUES (?,?,?,?)",
                (new_version, doc_json, ts, user),
            )
        return {"globals": globals_ or {}, "steps": steps or [], "version": new_version}

    def base_history(self) -> list[dict]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT version, updated_at, updated_by, doc_json "
                "FROM base_config_history ORDER BY version DESC"
            ).fetchall()
        result = []
        for r in rows:
            d = dict(r)
            doc = json.loads(d.pop("doc_json") or "{}")
            d["globals"] = doc.get("globals", {})
            d["steps"] = doc.get("steps", [])
            result.append(d)
        return result

    def delete_oldest_base_history(self) -> dict:
        """Prune the single oldest (lowest-version) base change-history entry.

        Mirror of delete_oldest_history for the base tier: only history rows are touched;
        the live base document, its version and optimistic locking are unaffected. Never
        deletes the last remaining row. Returns {deleted_version, remaining}."""
        with self._lock, self._connect() as conn:
            rows = conn.execute(
                "SELECT version FROM base_config_history ORDER BY version"
            ).fetchall()
            if len(rows) <= 1:
                raise ValueError(
                    "Nothing to prune: only the current version's history remains"
                )
            oldest = rows[0]["version"]
            conn.execute("DELETE FROM base_config_history WHERE version = ?", (oldest,))
        return {"deleted_version": oldest, "remaining": len(rows) - 1}

    # -- company registry ---------------------------------------------------
    # The set of companies lives here and nowhere else: admins create/rename/delete them
    # in-app. run_context still lives in company_overrides.

    def list_registry(self) -> list[dict]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT code, display_name, created_at, updated_at "
                "FROM companies ORDER BY code"
            ).fetchall()
        return [dict(r) for r in rows]

    def create_company(self, code: str, display_name: str,
                       run_context: dict | None = None, user: str | None = None) -> dict:
        """Register a new company; optionally seed its run_context. Raises ValueError if
        the code already exists."""
        ts = _now()
        with self._lock, self._connect() as conn:
            exists = conn.execute(
                "SELECT 1 FROM companies WHERE code = ?", (code,)
            ).fetchone()
            if exists:
                raise ValueError(f"Company '{code}' already exists")
            conn.execute(
                "INSERT INTO companies (code, display_name, created_at, updated_at) "
                "VALUES (?,?,?,?)",
                (code, display_name, ts, ts),
            )
        if run_context:
            # seeds a company_overrides row (version 1) carrying the run_context
            self.save_override(code, {}, run_context, expected_version=None, user=user)
        return {"code": code, "display_name": display_name}

    def rename_company(self, code: str, display_name: str) -> dict:
        ts = _now()
        with self._lock, self._connect() as conn:
            cur = conn.execute(
                "UPDATE companies SET display_name = ?, updated_at = ? WHERE code = ?",
                (display_name, ts, code),
            )
            if cur.rowcount == 0:
                raise ValueError(f"Company '{code}' not found")
        return {"code": code, "display_name": display_name}

    def delete_company(self, code: str) -> None:
        """Remove a company from the registry along with its overrides + history."""
        with self._lock, self._connect() as conn:
            conn.execute("DELETE FROM companies WHERE code = ?", (code,))
            conn.execute("DELETE FROM company_overrides WHERE company_code = ?", (code,))
            conn.execute("DELETE FROM company_overrides_history WHERE company_code = ?", (code,))


# ---------------------------------------------------------------------------
# Effective-config builder: base (files) ⊕ company delta (this store)
# ---------------------------------------------------------------------------

def _apply_company_delta(base_steps: list[dict], override: dict) -> list[dict]:
    """Merge each company step delta over the matching base step (reusing the existing
    merge engine). `enabled: false` drops the step. Base step order is preserved; the
    delta cannot add steps (steps are a base/file concern in the two-tier model)."""
    import config  # local import avoids any import-time coupling
    out: list[dict] = []
    for step in base_steps:
        delta = override.get(step["step_id"]) if override else None
        if not delta:
            out.append(step)
            continue
        if delta.get("enabled") is False:
            continue
        out.append(config._merge_step(step, {"step_id": step["step_id"], **delta}))
    return out


_EMPTY_BASE = {"defaults": {}, "llm_profiles": {}, "analysis_defaults": {}, "steps": []}


def get_base_config(store: "ConfigStore | None" = None, base_path=None) -> dict:
    """Return the effective base (product) config dict — the same shape
    config.load_base_config produces ({defaults, llm_profiles, analysis_defaults, steps}).
    Sourced from the DB base tier; falls back to a file when the store is unseeded
    (version 0) and base_path is given (tests / one-off standalone runs). A genuinely
    fresh deployment has no seed file and no base_path — that's not an error, it's the
    normal state before an admin seeds the base via Settings → Company Base → upload, so
    this returns an empty scaffold rather than raising."""
    import config
    base_state = (store or get_config_store()).get_base()
    if base_state["version"] == 0:
        try:
            return config.load_base_config(base_path)
        except FileNotFoundError:
            return dict(_EMPTY_BASE)
    return {**(base_state["globals"] or {}), "steps": base_state["steps"] or []}


def get_base_steps(store: "ConfigStore | None" = None, base_path=None) -> list[dict]:
    """Ordered base step library (DB base tier, file fallback when unseeded)."""
    return get_base_config(store, base_path).get("steps", [])


def load_effective_config(
    company_code: str,
    period: str | None = None,
    fiscal_year: str | None = None,
    store: "ConfigStore | None" = None,
    base_path=None,
) -> dict:
    """Build the runtime config for a company: base (DB, file fallback) ⊕ company delta
    (SQLite), with run_context and runtime period/fiscal_year injected. Returns the same
    dict shape graph_builder / run_manager / mcp_server already consume."""
    store = store or get_config_store()
    base = get_base_config(store, base_path)
    state = store.get_override(company_code)
    base["steps"] = _apply_company_delta(base["steps"], state.get("override") or {})
    run_ctx = {**base.get("company_config", {}), **(state.get("run_context") or {})}
    run_ctx.setdefault("company_code", company_code)
    if period is not None:
        run_ctx["period"] = period
    if fiscal_year is not None:
        run_ctx["fiscal_year"] = fiscal_year
    base["company_config"] = run_ctx
    return base


# ---------------------------------------------------------------------------
# Module-level default singleton
# ---------------------------------------------------------------------------

_store: ConfigStore | None = None


def get_config_store() -> ConfigStore:
    global _store
    if _store is None:
        _store = ConfigStore()
    return _store
