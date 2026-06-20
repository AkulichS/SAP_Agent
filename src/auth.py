"""
auth.py — session management for the SAP Period Close web UI.

Signed cookies (itsdangerous) carry {username, company_codes}.
Passwords are bcrypt-hashed in users.yaml.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import bcrypt
import yaml
from itsdangerous import BadSignature, SignatureExpired, URLSafeTimedSerializer

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

_SECRET_KEY = os.environ.get("SESSION_SECRET", "change-me-in-production-please")
_SESSION_TTL = int(os.environ.get("SESSION_TTL_SEC", 8 * 3600))   # 8 hours
_SALT = "sap-period-close-session"
_COOKIE_NAME = "session"

_serializer = URLSafeTimedSerializer(_SECRET_KEY)

# ---------------------------------------------------------------------------
# User registry
# ---------------------------------------------------------------------------

_USERS_PATH = Path(__file__).parent / "users.yaml"
_users_cache: list[dict] | None = None


def load_users(force_reload: bool = False) -> list[dict]:
    global _users_cache
    if _users_cache is None or force_reload:
        with open(_USERS_PATH, encoding="utf-8") as f:
            data = yaml.safe_load(f)
        _users_cache = data.get("users", [])
    return _users_cache


def get_user(username: str) -> dict | None:
    return next((u for u in load_users() if u["username"] == username), None)


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
        "role": user.get("role", "operator"),
        "company_codes": user.get("company_codes", []),
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


if __name__ == "__main__":
    import sys
    if len(sys.argv) == 2:
        print(hash_password(sys.argv[1]))
    else:
        print("Usage: python auth.py <plain_password>")
