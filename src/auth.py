"""
auth.py — session management for the SAP Period Close web UI.

Signed cookies (itsdangerous) carry {username, company_codes}.
Passwords are bcrypt-hashed in the config store's `users` table.
"""

from __future__ import annotations

import os
import secrets

import bcrypt
from itsdangerous import BadSignature, SignatureExpired, URLSafeTimedSerializer

import config_store
import permissions

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

_SECRET_KEY = os.environ.get("SESSION_SECRET", "change-me-in-production-please")
_SESSION_TTL = int(os.environ.get("SESSION_TTL_SEC", 8 * 3600))   # 8 hours
_SALT = "sap-period-close-session"
_COOKIE_NAME = "session"

_serializer = URLSafeTimedSerializer(_SECRET_KEY)

# ---------------------------------------------------------------------------
# User registry — DB-backed (config_store), UI-managed ("Manage users"). A fresh
# deployment gets its first account from the create-admin CLI below.
# ---------------------------------------------------------------------------

def get_user(username: str) -> dict | None:
    """Full user record (incl. password_hash) from the store, or None."""
    return config_store.get_config_store().get_user(username)


def list_users() -> list[dict]:
    """All users without password hashes — the admin-UI shape."""
    return config_store.get_config_store().list_users()


# ---------------------------------------------------------------------------
# Password verification
# ---------------------------------------------------------------------------

def verify_password(plain: str, hashed: str) -> bool:
    return bcrypt.checkpw(plain.encode(), hashed.encode())


# ---------------------------------------------------------------------------
# Session token (signed cookie value)
# ---------------------------------------------------------------------------

def create_session_token(user: dict) -> str:
    payload = {
        "username": user["username"],
        "display_name": user.get("display_name", user["username"]),
        "email": user.get("email"),
        "role": user.get("role", permissions.DEFAULT_ROLE),
        "company_codes": user.get("company_codes", []),
        # Carried in the cookie so every gate can see it without a DB read; cleared by
        # re-issuing the cookie once the user sets a password of their own.
        "must_change_password": bool(user.get("must_change_password")),
    }
    return _serializer.dumps(payload, salt=_SALT)


def decode_session_token(token: str) -> dict | None:
    """Return payload dict or None if token is invalid/expired."""
    try:
        return _serializer.loads(token, salt=_SALT, max_age=_SESSION_TTL)
    except (SignatureExpired, BadSignature, Exception):
        return None


# ---------------------------------------------------------------------------
# Cookie helpers
# ---------------------------------------------------------------------------

COOKIE_NAME = _COOKIE_NAME


def get_session_from_cookies(cookies: dict[str, str]) -> dict | None:
    token = cookies.get(COOKIE_NAME)
    if not token:
        return None
    return decode_session_token(token)


# ---------------------------------------------------------------------------
# Helper: hash a new password (used once during setup)
# ---------------------------------------------------------------------------

def hash_password(plain: str) -> str:
    return bcrypt.hashpw(plain.encode(), bcrypt.gensalt(rounds=12)).decode()


# ---------------------------------------------------------------------------
# Generated (hand-over) passwords
# ---------------------------------------------------------------------------

#: Unambiguous alphabet — no 0/O, 1/l/I. These passwords get read off a screen or
#: retyped out of an e-mail, so a lookalike character is a support call.
_PW_ALPHABET = "ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz23456789"


def generate_password(length: int = 14) -> str:
    """A random one-time password to hand over (initial account, or a reset).

    Never stored or logged in the clear: the caller mails it, marks the account
    `must_change_password`, and keeps only the bcrypt hash."""
    return "".join(secrets.choice(_PW_ALPHABET) for _ in range(length))


# ---------------------------------------------------------------------------
# CLI: password hashing + first-admin bootstrap
# ---------------------------------------------------------------------------

def _cli_create_admin(argv: list[str]) -> int:
    """Bootstrap a system administrator directly into the config store — the way the
    first login is created on a fresh deployment (before any user exists to open the
    UI). Creates the top `sys_admin` role, whose company scope is every registered
    company; `--companies` is accepted for symmetry but is not what grants that reach."""
    import argparse
    import getpass

    p = argparse.ArgumentParser(prog="auth.py create-admin")
    p.add_argument("--username", required=True)
    p.add_argument("--password", help="prompted for (no echo) if omitted")
    p.add_argument("--display", help="display name (defaults to the username)")
    p.add_argument("--email", help="address password resets are sent to")
    p.add_argument("--companies", default="",
                   help="comma-separated company codes (sys_admin already reaches all)")
    a = p.parse_args(argv)

    password = a.password or getpass.getpass("Password: ")
    if not password:
        print("A password is required.")
        return 1
    codes = [c.strip() for c in a.companies.split(",") if c.strip()]
    try:
        config_store.get_config_store().create_user(
            a.username, hash_password(password),
            display_name=a.display or a.username, role=permissions.SYS_ADMIN,
            company_codes=codes, user="cli", email=a.email,
        )
    except ValueError as exc:
        print(f"Error: {exc}")
        return 1
    print(f"System administrator '{a.username}' created (role: {permissions.SYS_ADMIN}).")
    print("Provision a second one — never share this account's password.")
    return 0


if __name__ == "__main__":
    import sys

    args = sys.argv[1:]
    if args and args[0] == "create-admin":
        raise SystemExit(_cli_create_admin(args[1:]))
    if args and args[0] == "hash":
        args = args[1:]
    if len(args) == 1:
        print(hash_password(args[0]))
    else:
        print("Usage:\n"
              "  python auth.py <plain_password>          # print a bcrypt hash\n"
              "  python auth.py hash <plain_password>     # same, explicit\n"
              "  python auth.py create-admin --username U [--password P] "
              "[--display D] [--email E] [--companies A,B]")
