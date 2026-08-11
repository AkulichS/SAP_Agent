"""Tests for offline signed licensing: signature integrity, strict module entitlement,
and the warn-only / grace-windowed anti-copy binding (which must never block the core)."""

import json
from datetime import datetime, timedelta, timezone

import pytest

import licensing
from licensing import (GRACE_DAYS, BindingState, License, LicenseError,
                       ModuleNotLicensed, SystemIdentity, build_payload,
                       generate_keypair, license_from_payload, load_license,
                       read_system_identity, require_module, sign_license,
                       verify_document)

NOW = datetime(2026, 8, 8, tzinfo=timezone.utc)


@pytest.fixture(scope="module")
def keypair():
    return generate_keypair()  # (private_pem, public_pem)


@pytest.fixture
def issue(keypair):
    """Factory: build + sign a license doc with sensible defaults."""
    private_pem, _ = keypair

    def _issue(**kw):
        payload = build_payload(
            licensee=kw.get("licensee", "Acme GmbH"),
            modules=kw.get("modules", ["CO"]),
            installation_numbers=kw.get("installation_numbers"),
            sids=kw.get("sids"),
            support_months=kw.get("support_months"),
        )
        if "support_expires" in kw:  # allow explicit override for expiry tests
            payload["support_expires"] = kw["support_expires"]
        return sign_license(payload, private_pem)

    return _issue


def _license(doc, keypair) -> License:
    _, public_pem = keypair
    return license_from_payload(verify_document(doc, public_pem))


# ---------------------------------------------------------------------------
# Signature integrity (the one hard gate)
# ---------------------------------------------------------------------------

def test_sign_verify_roundtrip(issue, keypair):
    _, public_pem = keypair
    doc = issue(modules=["CO", "FI"])
    payload = verify_document(doc, public_pem)
    assert payload["licensee"] == "Acme GmbH"
    assert set(payload["modules"]) == {"CO", "FI"}


def test_tampered_payload_fails(issue, keypair):
    _, public_pem = keypair
    doc = issue(modules=["CO"])
    doc["payload"]["modules"] = ["CO", "FI"]  # grant yourself FI after signing
    with pytest.raises(LicenseError):
        verify_document(doc, public_pem)


def test_wrong_key_fails(issue):
    _, other_public = generate_keypair()  # a different vendor keypair
    doc = issue()
    with pytest.raises(LicenseError):
        verify_document(doc, other_public)


def test_malformed_document_fails(keypair):
    _, public_pem = keypair
    with pytest.raises(LicenseError):
        verify_document({"not": "a license"}, public_pem)


def test_load_license_file_roundtrip(issue, keypair, tmp_path):
    _, public_pem = keypair
    path = tmp_path / "acme.lic"
    path.write_text(json.dumps(issue(modules=["CO"])), encoding="utf-8")
    lic = load_license(path, public_pem)
    assert lic.licensee == "Acme GmbH"
    assert lic.has_module("CO")


def test_missing_file_raises(keypair, tmp_path):
    _, public_pem = keypair
    with pytest.raises(LicenseError):
        load_license(tmp_path / "nope.lic", public_pem)


# ---------------------------------------------------------------------------
# Module entitlement (job #1, strict — cannot misfire)
# ---------------------------------------------------------------------------

def test_module_gating_strict(issue, keypair):
    lic = _license(issue(modules=["CO"]), keypair)
    assert lic.has_module("CO")
    assert not lic.has_module("FI")
    require_module(lic, "CO")  # no raise
    with pytest.raises(ModuleNotLicensed):
        require_module(lic, "FI")


def test_module_case_insensitive(issue, keypair):
    lic = _license(issue(modules=["co"]), keypair)
    assert lic.has_module("CO")
    assert lic.has_module("co")


def test_wildcard_grants_all(issue, keypair):
    lic = _license(issue(modules=["*"]), keypair)
    assert lic.has_module("ANYTHING")


# ---------------------------------------------------------------------------
# Anti-copy binding (job #2, warn-only + grace) — never blocks the core
# ---------------------------------------------------------------------------

def test_unbound_license_runs_anywhere(issue, keypair):
    lic = _license(issue(), keypair)  # no bind
    r = lic.evaluate_binding(SystemIdentity(sid="PRD", installation_number="0020123456"), now=NOW)
    assert r.state is BindingState.UNBOUND
    assert r.ok_to_run and r.updates_allowed


def test_binding_match(issue, keypair):
    lic = _license(issue(installation_numbers=["0020123456"], sids=["PRD"]), keypair)
    r = lic.evaluate_binding(SystemIdentity(sid="PRD", installation_number="0020123456"), now=NOW)
    assert r.state is BindingState.MATCH
    assert r.ok_to_run and r.updates_allowed


def test_server_move_and_upgrade_do_not_break_match(issue, keypair):
    """A different host/SID casing but the same installation number still matches — the
    binding follows the logical SAP identity, not hardware."""
    lic = _license(issue(installation_numbers=["0020123456"]), keypair)  # instno only
    r = lic.evaluate_binding(SystemIdentity(sid="ANY", installation_number="0020123456"), now=NOW)
    assert r.state is BindingState.MATCH


def test_binding_mismatch_grace_still_runs(issue, keypair):
    lic = _license(issue(installation_numbers=["0020123456"]), keypair)
    r = lic.evaluate_binding(SystemIdentity(installation_number="0099999999"), now=NOW)
    assert r.state is BindingState.MISMATCH_GRACE
    assert r.ok_to_run and r.updates_allowed          # core runs, updates still allowed
    assert r.mismatch_since == NOW                     # first-seen echoed for the caller to persist


def test_binding_mismatch_past_grace_gates_updates_only(issue, keypair):
    lic = _license(issue(installation_numbers=["0020123456"]), keypair)
    since = NOW - timedelta(days=GRACE_DAYS + 1)
    r = lic.evaluate_binding(SystemIdentity(installation_number="0099999999"),
                             now=NOW, mismatch_since=since)
    assert r.state is BindingState.MISMATCH_EXPIRED
    assert r.ok_to_run          # the close is NEVER blocked
    assert not r.updates_allowed  # only updates are withheld


# ---------------------------------------------------------------------------
# Support window (gates updates, not the core)
# ---------------------------------------------------------------------------

def test_support_expiry_flag(issue, keypair):
    lic = _license(issue(support_expires="2025-01-01"), keypair)
    assert lic.support_expired(now=NOW) is True


def test_support_not_expired_when_absent(issue, keypair):
    lic = _license(issue(), keypair)
    assert lic.support_expired(now=NOW) is False


# ---------------------------------------------------------------------------
# System identity read (stub via env)
# ---------------------------------------------------------------------------

def test_read_system_identity_from_env(monkeypatch):
    monkeypatch.setenv("SAP_SID", "prd")
    monkeypatch.setenv("SAP_INSTALLATION_NUMBER", "0020123456")
    ident = read_system_identity()
    assert ident.installation_number == "0020123456"
    assert ident.sid == "prd"


def test_read_system_identity_via_provider():
    ident = read_system_identity(lambda: {"sid": "DEV", "installation_number": "0007"})
    assert ident == SystemIdentity(sid="DEV", installation_number="0007")
