"""
licensing.py — offline, signed licensing for the period-close product.

Two separable jobs, only ONE of which touches system identity:

  1. Module entitlement (CO / FI / MM …) — enforced STRICTLY from a signed license file.
     No system binding, so it cannot misfire. This is also the land-and-expand unlock
     mechanism: selling FI next year = issuing a new signed license that lists FI.
  2. Anti-copy binding — a WARN-ONLY, grace-windowed check that the license's bound SAP
     installation number(s) match the system the engine is actually connected to. It NEVER
     bricks the already-purchased core (a finance close must never stop over a license edge
     case such as a legitimate server move); the EULA is the real enforcement and this is
     the detectable, contractually-actionable tripwire.

Crypto: Ed25519. The vendor holds the private key; the shipped product embeds only the
PUBLIC key, so a licensee can verify a license but cannot forge one. A tampered payload
fails signature verification — the one hard error here.

License file (JSON):
    {"payload": {…}, "signature": "<base64 Ed25519 over the canonical payload>"}
payload = {
    "schema": 1,
    "license_id": "<uuid>",
    "licensee": "Acme GmbH",
    "modules": ["CO"],                 # entitlements (upper-cased); [] or ["*"] = all
    "bind": {                          # optional — absent/empty ⇒ UNBOUND (runs anywhere)
        "installation_number": ["0020123456"],
        "sid": ["PRD"],
    },
    "issued": "2026-08-08",
    "support_expires": "2027-08-08",   # optional; gates UPDATES/support, never core run
}

Vendor CLI (private-key side — this is for you, not shipped to clients):
    python licensing.py keygen --out-dir .
    python licensing.py issue  --key license_signing_key.pem --licensee "Acme GmbH" \\
                               --modules CO --bind-instno 0020123456 --bind-sid PRD \\
                               --support-months 12 --out acme.lic
    python licensing.py verify --pubkey license_pubkey.pem --license acme.lic

Engine integration (customer side), see the two seams marked INTEGRATION below:
    lic = load_license(path)                 # hard-fails on a forged/corrupt file
    require_module(lic, step_module)         # strict gate before running a non-base step
    result = lic.evaluate_binding(identity, mismatch_since=persisted_since)
    # persist result.mismatch_since; show result.message; gate UPDATES on result.updates_allowed
"""

from __future__ import annotations

import base64
import json
import os
import uuid
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from enum import Enum
from pathlib import Path
from typing import Optional

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import (
    Ed25519PrivateKey, Ed25519PublicKey)

LICENSE_SCHEMA = 1
# Days the core keeps running with only a warning after a binding mismatch is first seen,
# before UPDATES (not the close) are gated. Generous on purpose: a real system migration
# must never turn into a close-day lockout.
GRACE_DAYS = 21

# Where the shipped product finds the embedded verifying key: an env override first, then a
# bundled public-key file. The PRIVATE key never ships — it lives only with the vendor.
_PUBKEY_ENV = "LICENSE_PUBLIC_KEY_PEM"
_PUBKEY_FILE = Path(__file__).parent / "configs" / "license_pubkey.pem"

# Where the engine looks for the customer's license file: an env override first, then a
# bundled default path. When neither resolves, the engine runs UNLICENSED (dev mode) — it
# does NOT invent a gate out of a missing file; a real deployment ships license.lic here.
_LICENSE_ENV = "LICENSE_FILE"
_LICENSE_FILE = Path(__file__).parent / "configs" / "license.lic"


class LicenseError(Exception):
    """A license file is missing, malformed, or fails signature verification (hard error)."""


class ModuleNotLicensed(LicenseError):
    """A step's module is not covered by the active license (strict entitlement gate)."""


# ---------------------------------------------------------------------------
# System identity (the anti-copy anchor) — logical SAP identity, NOT hardware/path
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class SystemIdentity:
    """The logical SAP identity the engine is connected to. Stable across server moves and
    support-pack/EHP upgrades; changes only for a genuinely different SAP installation."""
    sid: Optional[str] = None
    installation_number: Optional[str] = None


def read_system_identity(provider=None) -> SystemIdentity:
    """Return the connected SAP system's logical identity.

    INTEGRATION: the real implementation reads these over RFC — the SID from ``sy-sysid``
    and the installation number from SAP's licensing FM (SLIC* family; confirm the exact FM
    on the target system). Pass a ``provider()`` callable returning that dict to wire it in.
    The stub falls back to env vars so the whole scheme is testable with no live SAP.
    """
    if provider is not None:
        raw = provider() or {}
    else:
        raw = {
            "sid": os.getenv("SAP_SID"),
            "installation_number": os.getenv("SAP_INSTALLATION_NUMBER"),
        }
    return SystemIdentity(
        sid=(raw.get("sid") or None),
        installation_number=(raw.get("installation_number") or None),
    )


def identity_from_tool_rows(rows) -> SystemIdentity:
    """Build a SystemIdentity from the ABAP TOOL_SYSTEM_IDENTITY / `sap_system_identity`
    rows ([{"name": "SID"|"INSTALLATION_NUMBER"|..., "value": ...}]). Use it to wire the
    RFC read as a provider:  ``read_system_identity(lambda: {...})`` — or call this directly
    on the tool's ``data.rows``. Unknown/blank names are ignored; a missing installation
    number yields None (→ unbound-ish, which the warn-only binding tolerates)."""
    by_name = {str(r.get("name", "")).strip().upper(): (r.get("value") or None)
               for r in (rows or []) if isinstance(r, dict)}
    return SystemIdentity(
        sid=(by_name.get("SID") or None),
        installation_number=(by_name.get("INSTALLATION_NUMBER") or None),
    )


# ---------------------------------------------------------------------------
# Binding evaluation — warn-only, grace-windowed (job #2)
# ---------------------------------------------------------------------------

class BindingState(str, Enum):
    UNBOUND = "unbound"                  # license carries no binding → runs anywhere
    MATCH = "match"                      # bound identity matches the connected system
    MISMATCH_GRACE = "mismatch_grace"    # mismatch, still inside the grace window
    MISMATCH_EXPIRED = "mismatch_expired"  # mismatch past grace → gate UPDATES (not the core)


@dataclass(frozen=True)
class BindingResult:
    state: BindingState
    message: str
    ok_to_run: bool = True          # ALWAYS true — the purchased core is never blocked here
    updates_allowed: bool = True    # false only when a mismatch has outlived the grace window
    mismatch_since: Optional[datetime] = None  # echo/first-seen; the caller persists this


# ---------------------------------------------------------------------------
# License object
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class License:
    payload: dict
    licensee: str
    modules: frozenset[str]
    bind: dict
    issued: Optional[date]
    support_expires: Optional[date]
    license_id: str

    # -- entitlements (job #1, strict) --
    def has_module(self, module: str) -> bool:
        if "*" in self.modules or not self.modules:
            return True
        return (module or "").strip().upper() in self.modules

    def support_expired(self, now: Optional[datetime] = None) -> bool:
        if self.support_expires is None:
            return False
        now = now or datetime.now(timezone.utc)
        return now.date() > self.support_expires

    # -- anti-copy binding (job #2, warn-only) --
    def evaluate_binding(
        self,
        identity: SystemIdentity,
        now: Optional[datetime] = None,
        mismatch_since: Optional[datetime] = None,
    ) -> BindingResult:
        """Compare the license's bound identity against the connected system. Never raises
        and never blocks the core; on a persistent mismatch it only withholds UPDATES after
        the grace window. Pass the previously-persisted ``mismatch_since`` so the grace
        window survives restarts; when a mismatch is first seen this returns a fresh
        ``mismatch_since`` for the caller to store."""
        now = now or datetime.now(timezone.utc)
        allowed_instno = {str(x) for x in (self.bind.get("installation_number") or [])}
        allowed_sid = {str(x).upper() for x in (self.bind.get("sid") or [])}

        if not allowed_instno and not allowed_sid:
            return BindingResult(BindingState.UNBOUND, "License is not system-bound.")

        instno_ok = (not allowed_instno) or (identity.installation_number in allowed_instno)
        sid_ok = (not allowed_sid) or ((identity.sid or "").upper() in allowed_sid)
        if instno_ok and sid_ok:
            return BindingResult(BindingState.MATCH, "License matches this SAP system.")

        # Mismatch: warn now, escalate only after the grace window.
        since = mismatch_since or now
        expired = (now - since) > timedelta(days=GRACE_DAYS)
        seen = f"installation_number={identity.installation_number!r}, sid={identity.sid!r}"
        if expired:
            return BindingResult(
                BindingState.MISMATCH_EXPIRED,
                f"License is bound to a different SAP system ({seen}); grace window elapsed. "
                f"The close still runs, but updates are withheld — contact your vendor to "
                f"re-issue the license.",
                updates_allowed=False,
                mismatch_since=since,
            )
        return BindingResult(
            BindingState.MISMATCH_GRACE,
            f"License is bound to a different SAP system ({seen}). Running under a "
            f"{GRACE_DAYS}-day grace window — contact your vendor to re-issue the license.",
            mismatch_since=since,
        )


def require_module(license: License, module: str) -> None:
    """INTEGRATION (strict gate): call before running a step whose module isn't the base.
    Raises ModuleNotLicensed when the module isn't entitled."""
    if not license.has_module(module):
        have = ", ".join(sorted(license.modules)) or "(none)"
        raise ModuleNotLicensed(
            f"Module {module!r} is not licensed (this license covers: {have}). "
            f"Contact your vendor to add it."
        )


# ---------------------------------------------------------------------------
# Sign / verify
# ---------------------------------------------------------------------------

def _canonical(payload: dict) -> bytes:
    """Deterministic byte encoding of the payload to sign/verify (stable key order)."""
    return json.dumps(payload, sort_keys=True, separators=(",", ":"),
                      ensure_ascii=True).encode("utf-8")


def generate_keypair() -> tuple[bytes, bytes]:
    """Return (private_pem, public_pem). Keep the private PEM secret — it is the only thing
    that can mint licenses; the public PEM ships inside the product."""
    key = Ed25519PrivateKey.generate()
    private_pem = key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )
    public_pem = key.public_key().public_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PublicFormat.SubjectPublicKeyInfo,
    )
    return private_pem, public_pem


def sign_license(payload: dict, private_pem: bytes) -> dict:
    """Sign a payload dict, returning the {payload, signature} license document."""
    key = serialization.load_pem_private_key(private_pem, password=None)
    if not isinstance(key, Ed25519PrivateKey):
        raise LicenseError("Signing key is not an Ed25519 private key.")
    sig = key.sign(_canonical(payload))
    return {"payload": payload, "signature": base64.b64encode(sig).decode("ascii")}


def _load_public_key(public_pem: Optional[bytes]) -> Ed25519PublicKey:
    if public_pem is None:
        env = os.getenv(_PUBKEY_ENV)
        if env:
            public_pem = env.encode("utf-8")
        elif _PUBKEY_FILE.exists():
            public_pem = _PUBKEY_FILE.read_bytes()
        else:
            raise LicenseError(
                "No verifying public key available (set $%s or bundle %s)."
                % (_PUBKEY_ENV, _PUBKEY_FILE.name)
            )
    key = serialization.load_pem_public_key(public_pem)
    if not isinstance(key, Ed25519PublicKey):
        raise LicenseError("Embedded key is not an Ed25519 public key.")
    return key


def verify_document(doc: dict, public_pem: Optional[bytes] = None) -> dict:
    """Verify a {payload, signature} document against the (embedded) public key. Returns the
    payload on success; raises LicenseError on any tampering or malformed structure."""
    if not isinstance(doc, dict) or "payload" not in doc or "signature" not in doc:
        raise LicenseError("License file is malformed (expected {payload, signature}).")
    key = _load_public_key(public_pem)
    try:
        sig = base64.b64decode(doc["signature"])
    except Exception as exc:  # noqa: BLE001 - any decode failure is an invalid license
        raise LicenseError(f"License signature is not valid base64: {exc}") from exc
    try:
        key.verify(sig, _canonical(doc["payload"]))
    except InvalidSignature as exc:
        raise LicenseError("License signature verification failed (tampered or wrong key).") from exc
    return doc["payload"]


def _parse_date(value) -> Optional[date]:
    return date.fromisoformat(value) if value else None


def license_from_payload(payload: dict) -> License:
    modules = frozenset((m or "").strip().upper() for m in (payload.get("modules") or []))
    return License(
        payload=payload,
        licensee=payload.get("licensee", ""),
        modules=modules,
        bind=payload.get("bind") or {},
        issued=_parse_date(payload.get("issued")),
        support_expires=_parse_date(payload.get("support_expires")),
        license_id=payload.get("license_id", ""),
    )


def load_license(path: str | Path, public_pem: Optional[bytes] = None) -> License:
    """Read, verify, and parse a license file into a License. Raises LicenseError on a
    missing/corrupt/forged file — this is the hard gate the engine relies on."""
    p = Path(path)
    try:
        doc = json.loads(p.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise LicenseError(f"License file not found: {p}") from exc
    except json.JSONDecodeError as exc:
        raise LicenseError(f"License file is not valid JSON: {exc}") from exc
    payload = verify_document(doc, public_pem)
    return license_from_payload(payload)


# ---------------------------------------------------------------------------
# Engine integration — resolve + load the active license, gate steps by module
# ---------------------------------------------------------------------------

def resolve_license_path(explicit: str | Path | None = None) -> Optional[Path]:
    """Resolve which license file to use: the explicit arg, else $LICENSE_FILE, else the
    bundled default IF it exists. Returns None when nothing resolves (→ UNLICENSED dev mode).
    An explicit path is returned as-is even if absent, so a caller that names a file gets a
    clear 'not found' rather than a silent dev-mode fallback."""
    if explicit:
        return Path(explicit)
    env = os.getenv(_LICENSE_ENV)
    if env:
        return Path(env)
    return _LICENSE_FILE if _LICENSE_FILE.exists() else None


def load_active_license(
    path: str | Path | None = None, public_pem: Optional[bytes] = None
) -> Optional[License]:
    """Load the engine's active license, or None when no license file is present (dev mode).
    A present-but-corrupt/forged/wrong-key file raises LicenseError — a tampered license is a
    hard error, only a *missing* one is tolerated (as dev/unlicensed)."""
    resolved = resolve_license_path(path)
    if resolved is None:
        return None
    return load_license(resolved, public_pem)


def step_module(step: dict) -> Optional[str]:
    """The module-pack a step belongs to, upper-cased, or None for the product base (which is
    always licensed). Reads the step's ``module`` field."""
    return ((step or {}).get("module") or "").strip().upper() or None


def unlicensed_steps(license: Optional[License], steps: list[dict]) -> list[tuple[str, str]]:
    """Return [(step_id, module)] for every step whose module tag is not entitled by the
    license. Empty when the license is None (dev mode) or every module is covered. This is the
    pre-flight the engine runs BEFORE a close starts: entitlement is job #1 (strict), and a
    close you can't finish should never begin."""
    if license is None:
        return []
    bad: list[tuple[str, str]] = []
    for st in steps:
        mod = step_module(st)
        if mod and not license.has_module(mod):
            bad.append((st.get("step_id", "?"), mod))
    return bad


# ---------------------------------------------------------------------------
# License minting (vendor-side helper used by the CLI)
# ---------------------------------------------------------------------------

def build_payload(
    licensee: str,
    modules: list[str],
    installation_numbers: Optional[list[str]] = None,
    sids: Optional[list[str]] = None,
    support_months: Optional[int] = None,
    issued: Optional[date] = None,
) -> dict:
    issued = issued or datetime.now(timezone.utc).date()
    bind: dict = {}
    if installation_numbers:
        bind["installation_number"] = [str(x) for x in installation_numbers]
    if sids:
        bind["sid"] = [str(x).upper() for x in sids]
    payload = {
        "schema": LICENSE_SCHEMA,
        "license_id": str(uuid.uuid4()),
        "licensee": licensee,
        "modules": [m.strip().upper() for m in modules],
        "issued": issued.isoformat(),
    }
    if bind:
        payload["bind"] = bind
    if support_months:
        # simple month arithmetic: add ~30-day months, good enough for a support window
        payload["support_expires"] = (issued + timedelta(days=30 * support_months)).isoformat()
    return payload


# ---------------------------------------------------------------------------
# CLI (vendor-side)
# ---------------------------------------------------------------------------

def _cli(argv=None) -> int:
    import argparse

    ap = argparse.ArgumentParser(description="Issue and inspect product licenses (vendor-side).")
    sub = ap.add_subparsers(dest="cmd", required=True)

    kg = sub.add_parser("keygen", help="Generate a signing keypair.")
    kg.add_argument("--out-dir", default=".")

    iss = sub.add_parser("issue", help="Sign a license file.")
    iss.add_argument("--key", required=True, help="Path to the private signing key PEM.")
    iss.add_argument("--licensee", required=True)
    iss.add_argument("--modules", nargs="+", default=["CO"], help="Entitled modules, e.g. CO FI.")
    iss.add_argument("--bind-instno", nargs="*", default=None, help="Bound SAP installation number(s).")
    iss.add_argument("--bind-sid", nargs="*", default=None, help="Bound SAP SID(s).")
    iss.add_argument("--support-months", type=int, default=None)
    iss.add_argument("--out", required=True)

    ver = sub.add_parser("verify", help="Verify and print a license file.")
    ver.add_argument("--pubkey", required=True)
    ver.add_argument("--license", required=True)

    args = ap.parse_args(argv)

    if args.cmd == "keygen":
        priv, pub = generate_keypair()
        out = Path(args.out_dir)
        out.mkdir(parents=True, exist_ok=True)
        (out / "license_signing_key.pem").write_bytes(priv)
        (out / "license_pubkey.pem").write_bytes(pub)
        print(f"Wrote {out/'license_signing_key.pem'} (KEEP SECRET) and {out/'license_pubkey.pem'}")
        return 0

    if args.cmd == "issue":
        payload = build_payload(
            licensee=args.licensee,
            modules=args.modules,
            installation_numbers=args.bind_instno,
            sids=args.bind_sid,
            support_months=args.support_months,
        )
        doc = sign_license(payload, Path(args.key).read_bytes())
        Path(args.out).write_text(json.dumps(doc, indent=2), encoding="utf-8")
        print(f"Issued {args.out} for {args.licensee!r} - modules {payload['modules']}, "
              f"bind {payload.get('bind', {})}, expires {payload.get('support_expires', 'never')}")
        return 0

    if args.cmd == "verify":
        lic = load_license(args.license, Path(args.pubkey).read_bytes())
        print(f"OK  licensee={lic.licensee!r}  modules={sorted(lic.modules)}  "
              f"bind={lic.bind}  support_expires={lic.support_expires}  "
              f"support_expired={lic.support_expired()}")
        return 0

    return 1


if __name__ == "__main__":
    raise SystemExit(_cli())
