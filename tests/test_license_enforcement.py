"""Engine-side licensing: the config_store runtime-KV seam, the license-resolution/step-gate
helpers, and graph_builder._enforce_license — the run-start gate that is STRICT on module
entitlement (refuses to start) but WARN-ONLY on anti-copy binding (never blocks the close)."""

import json
from datetime import datetime, timedelta, timezone

import pytest

import config_store as config_store_mod
import graph_builder
import licensing
from config_store import ConfigStore
from licensing import GRACE_DAYS, build_payload, generate_keypair, sign_license


# ---------------------------------------------------------------------------
# config_store runtime key/value seam (backs the binding grace window)
# ---------------------------------------------------------------------------

def test_runtime_kv_roundtrip_and_default(tmp_path):
    store = ConfigStore(tmp_path / "cfg.db")
    assert store.get_runtime_value("license_binding") is None      # unset → None
    store.set_runtime_value("license_binding", {"mismatch_since": "2026-08-09", "state": "x"})
    got = store.get_runtime_value("license_binding")
    assert got == {"mismatch_since": "2026-08-09", "state": "x"}
    store.set_runtime_value("license_binding", {"state": "y"})      # upsert overwrites
    assert store.get_runtime_value("license_binding") == {"state": "y"}


# ---------------------------------------------------------------------------
# licensing resolution + step-gate helpers (pure)
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def keypair():
    return generate_keypair()  # (private_pem, public_pem)


def _write_license(tmp_path, private_pem, **kw):
    payload = build_payload(
        licensee=kw.get("licensee", "Acme GmbH"),
        modules=kw.get("modules", ["CO"]),
        installation_numbers=kw.get("installation_numbers"),
        sids=kw.get("sids"),
        support_months=kw.get("support_months"),
    )
    if "support_expires" in kw:
        payload["support_expires"] = kw["support_expires"]
    p = tmp_path / "test.lic"
    p.write_text(json.dumps(sign_license(payload, private_pem)), encoding="utf-8")
    return p


def test_resolve_license_path_precedence(tmp_path, monkeypatch):
    monkeypatch.delenv("LICENSE_FILE", raising=False)
    monkeypatch.setattr(licensing, "_LICENSE_FILE", tmp_path / "absent.lic")
    assert licensing.resolve_license_path() is None                 # nothing → dev mode
    monkeypatch.setenv("LICENSE_FILE", str(tmp_path / "env.lic"))
    assert licensing.resolve_license_path() == tmp_path / "env.lic"  # env wins
    assert licensing.resolve_license_path("explicit.lic") == \
        __import__("pathlib").Path("explicit.lic")                  # explicit wins over env


def test_load_active_license_missing_is_dev_mode(tmp_path, monkeypatch):
    monkeypatch.delenv("LICENSE_FILE", raising=False)
    monkeypatch.setattr(licensing, "_LICENSE_FILE", tmp_path / "absent.lic")
    assert licensing.load_active_license() is None


def test_load_active_license_corrupt_is_hard_error(tmp_path, monkeypatch, keypair):
    _, public_pem = keypair
    monkeypatch.setenv("LICENSE_PUBLIC_KEY_PEM", public_pem.decode())
    bad = tmp_path / "bad.lic"
    bad.write_text("{ not json", encoding="utf-8")
    monkeypatch.setenv("LICENSE_FILE", str(bad))
    with pytest.raises(licensing.LicenseError):
        licensing.load_active_license()


def test_identity_from_tool_rows():
    """The sap_system_identity / TOOL_SYSTEM_IDENTITY rows convert to a SystemIdentity."""
    rows = [{"name": "SID", "value": "PRD"},
            {"name": "CLIENT", "value": "100"},
            {"name": "INSTALLATION_NUMBER", "value": "0020123456"}]
    ident = licensing.identity_from_tool_rows(rows)
    assert ident.sid == "PRD" and ident.installation_number == "0020123456"
    # A blank/absent installation number → None (unbound-ish, tolerated by warn-only binding).
    ident2 = licensing.identity_from_tool_rows([{"name": "SID", "value": "DEV"},
                                                {"name": "INSTALLATION_NUMBER", "value": ""}])
    assert ident2.sid == "DEV" and ident2.installation_number is None
    assert licensing.identity_from_tool_rows([]).sid is None


def test_step_module_and_unlicensed_steps(keypair):
    private_pem, public_pem = keypair
    lic = licensing.license_from_payload(
        licensing.verify_document(sign_license(build_payload("A", ["CO"]), private_pem), public_pem)
    )
    assert licensing.step_module({"step_id": "s", "module": " fi "}) == "FI"
    assert licensing.step_module({"step_id": "s"}) is None           # base = untagged
    steps = [{"step_id": "S1"}, {"step_id": "S2", "module": "CO"}, {"step_id": "S3", "module": "FI"}]
    assert licensing.unlicensed_steps(lic, steps) == [("S3", "FI")]  # only the FI step
    assert licensing.unlicensed_steps(None, steps) == []             # dev mode gates nothing


# ---------------------------------------------------------------------------
# graph_builder._enforce_license — the run-start gate
# ---------------------------------------------------------------------------

@pytest.fixture
def emitted(monkeypatch):
    """Capture events _enforce_license emits instead of pushing to the WebSocket queue."""
    events: list[dict] = []

    async def _fake_emit(ev):
        events.append(ev)

    monkeypatch.setattr(graph_builder, "_emit", _fake_emit)
    return events


@pytest.fixture
def store(tmp_path, monkeypatch):
    """A throwaway config store wired in as the process singleton for the duration."""
    s = ConfigStore(tmp_path / "cfg.db")
    monkeypatch.setattr(config_store_mod, "get_config_store", lambda: s)
    return s


@pytest.fixture
def issue(tmp_path, monkeypatch, keypair):
    """Write a signed license, point $LICENSE_FILE at it, embed the verifying key."""
    private_pem, public_pem = keypair
    monkeypatch.setenv("LICENSE_PUBLIC_KEY_PEM", public_pem.decode())

    def _issue(**kw):
        p = _write_license(tmp_path, private_pem, **kw)
        monkeypatch.setenv("LICENSE_FILE", str(p))
        return p

    return _issue


def _clear_identity(monkeypatch):
    monkeypatch.delenv("SAP_SID", raising=False)
    monkeypatch.delenv("SAP_INSTALLATION_NUMBER", raising=False)


async def _enforce(steps, session=None):
    """Drive the two-phase gate the way the run entry points do: phase 1 (strict module
    gate, may raise) then phase 2 (warn-only binding). Returns the loaded License."""
    lic = await graph_builder._enforce_license_modules(steps)
    await graph_builder._enforce_license_binding(lic, session)
    return lic


# --- a minimal fake MCP session so the SAP connector's system_identity() is exercised ---

class _FakeBlock:
    type = "text"
    def __init__(self, text): self.text = text


class _FakeResult:
    def __init__(self, payload): self.content = [_FakeBlock(json.dumps(payload))]


class _FakeSession:
    """Returns a fixed envelope for every call_tool (only sap_system_identity is used here)."""
    def __init__(self, payload=None, raise_exc=False):
        self._payload = payload or {}
        self._raise = raise_exc

    async def call_tool(self, name, arguments=None):
        if self._raise:
            raise RuntimeError("tool call boom")
        return _FakeResult(self._payload)


def _identity_session(sid="", instno=""):
    return _FakeSession({"meta": {"sid": sid, "client": "100",
                                  "installation_number": instno},
                         "data": {"rows": []}})


async def test_dev_mode_runs_unlicensed(emitted, store, tmp_path, monkeypatch):
    """No license file at all → nothing is enforced, even an FI step, and no events fire."""
    monkeypatch.delenv("LICENSE_FILE", raising=False)
    monkeypatch.setattr(licensing, "_LICENSE_FILE", tmp_path / "absent.lic")
    assert await _enforce([{"step_id": "S1", "module": "FI"}]) is None
    assert emitted == []


async def test_module_gate_refuses_to_start(emitted, store, issue, monkeypatch):
    """Phase 1 alone refuses to start — before any session opens."""
    _clear_identity(monkeypatch)
    issue(modules=["CO"])
    steps = [{"step_id": "S1"}, {"step_id": "S2", "module": "FI"}]
    with pytest.raises(licensing.ModuleNotLicensed):
        await graph_builder._enforce_license_modules(steps)


async def test_licensed_modules_and_unbound_are_silent(emitted, store, issue, monkeypatch):
    _clear_identity(monkeypatch)
    issue(modules=["CO"])                                            # unbound → runs anywhere
    await _enforce([{"step_id": "S1"}, {"step_id": "S2", "module": "CO"}])
    assert emitted == []


async def test_binding_mismatch_warns_and_persists_anchor(emitted, store, issue, monkeypatch):
    issue(modules=["*"], installation_numbers=["0020123456"])
    monkeypatch.setenv("SAP_INSTALLATION_NUMBER", "0099999999")      # a different system (env fallback)
    monkeypatch.delenv("SAP_SID", raising=False)
    await _enforce([{"step_id": "S1"}])
    warns = [e for e in emitted if e["type"] == "license_warning"]
    assert warns and warns[0]["state"] == "mismatch_grace"
    assert warns[0]["updates_allowed"] is True                       # core + updates still fine
    saved = store.get_runtime_value("license_binding")
    assert saved["identity"] == "0099999999|" and saved["mismatch_since"]


async def test_binding_mismatch_past_grace_flags_updates_blocked(emitted, store, issue, monkeypatch):
    issue(modules=["*"], installation_numbers=["0020123456"])
    monkeypatch.setenv("SAP_INSTALLATION_NUMBER", "0099999999")
    monkeypatch.delenv("SAP_SID", raising=False)
    old = (datetime.now(timezone.utc) - timedelta(days=GRACE_DAYS + 1)).isoformat()
    store.set_runtime_value("license_binding", {"identity": "0099999999|", "mismatch_since": old})
    await _enforce([{"step_id": "S1"}])
    warns = [e for e in emitted if e["type"] == "license_warning"]
    assert warns and warns[0]["state"] == "mismatch_expired"
    assert warns[0]["updates_allowed"] is False                      # only updates withheld


async def test_binding_match_clears_stale_anchor(emitted, store, issue, monkeypatch):
    """A prior mismatch anchor is cleared once the system matches again — and no warning."""
    issue(modules=["*"], installation_numbers=["0020123456"], sids=["PRD"])
    monkeypatch.setenv("SAP_INSTALLATION_NUMBER", "0020123456")
    monkeypatch.setenv("SAP_SID", "PRD")
    store.set_runtime_value("license_binding",
                            {"identity": "stale", "mismatch_since": "2026-01-01T00:00:00+00:00"})
    await _enforce([{"step_id": "S1"}])
    assert not [e for e in emitted if e["type"] == "license_warning"]
    assert store.get_runtime_value("license_binding")["mismatch_since"] is None


async def test_support_expired_warns_only(emitted, store, issue, monkeypatch):
    _clear_identity(monkeypatch)
    issue(modules=["*"], support_expires="2020-01-01")               # unbound, but expired support
    await _enforce([{"step_id": "S1"}])
    warns = [e for e in emitted if e["type"] == "license_warning"]
    assert any(w["state"] == "support_expired" and w["updates_allowed"] is False for w in warns)


async def test_corrupt_license_raises_and_emits_invalid(emitted, store, tmp_path, monkeypatch, keypair):
    _, public_pem = keypair
    monkeypatch.setenv("LICENSE_PUBLIC_KEY_PEM", public_pem.decode())
    bad = tmp_path / "bad.lic"
    bad.write_text("{ not json", encoding="utf-8")
    monkeypatch.setenv("LICENSE_FILE", str(bad))
    with pytest.raises(licensing.LicenseError):
        await graph_builder._enforce_license_modules([{"step_id": "S1"}])
    assert any(e.get("state") == "invalid" for e in emitted)


# ---------------------------------------------------------------------------
# Phase 2 identity source: the connector's RFC read is trusted OVER env
# ---------------------------------------------------------------------------

async def test_connector_identity_overrides_env(emitted, store, issue, monkeypatch):
    """The whole anti-copy point: the connector reads the REAL system, so its identity wins
    over env the customer controls. Env is set to MATCH the license; the connector reports a
    DIFFERENT system → we must still warn (i.e. the connector value was used)."""
    issue(modules=["*"], installation_numbers=["0020123456"])
    monkeypatch.setenv("SAP_INSTALLATION_NUMBER", "0020123456")      # env would MATCH (spoof)
    monkeypatch.setenv("SAP_SID", "PRD")
    session = _identity_session(sid="QAS", instno="0099999999")      # RFC: a different box
    await _enforce([{"step_id": "S1"}], session=session)
    warns = [e for e in emitted if e["type"] == "license_warning"]
    assert warns and warns[0]["state"] == "mismatch_grace"           # connector (mismatch) beat env (match)


async def test_binding_falls_back_to_env_when_connector_blank(emitted, store, issue, monkeypatch):
    """A connector that returns a blank identity (stub/unsupported) falls back to env."""
    issue(modules=["*"], installation_numbers=["0020123456"])
    monkeypatch.setenv("SAP_INSTALLATION_NUMBER", "0099999999")      # env mismatch
    monkeypatch.delenv("SAP_SID", raising=False)
    session = _identity_session(sid="", instno="")                   # connector has nothing
    await _enforce([{"step_id": "S1"}], session=session)
    warns = [e for e in emitted if e["type"] == "license_warning"]
    assert warns and warns[0]["state"] == "mismatch_grace"           # fell back to env identity


async def test_binding_connector_failure_falls_back_to_env(emitted, store, issue, monkeypatch):
    """A connector whose identity read raises must not break the run — it falls back to env."""
    issue(modules=["*"], installation_numbers=["0020123456"])
    monkeypatch.setenv("SAP_INSTALLATION_NUMBER", "0020123456")      # env MATCHES
    monkeypatch.setenv("SAP_SID", "PRD")
    session = _FakeSession(raise_exc=True)
    await _enforce([{"step_id": "S1"}], session=session)             # no exception propagates
    assert not [e for e in emitted if e["type"] == "license_warning"]  # env matched → silent


# ---------------------------------------------------------------------------
# Connector method itself
# ---------------------------------------------------------------------------

async def test_sap_connector_system_identity_parses_meta():
    from connectors.sap_rfc import SapRfcConnector
    session = _identity_session(sid="PRD", instno="0020123456")
    ident = await SapRfcConnector(session).system_identity()
    assert ident == {"sid": "PRD", "installation_number": "0020123456"}


async def test_sap_connector_system_identity_read_failure_returns_none():
    from connectors.sap_rfc import SapRfcConnector
    assert await SapRfcConnector(_FakeSession(raise_exc=True)).system_identity() is None


async def test_base_connector_system_identity_default_none():
    from connectors.base import BaseConnector
    assert await BaseConnector().system_identity() is None
