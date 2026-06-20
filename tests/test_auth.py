"""auth.py — password hashing and signed session tokens (pure functions)."""

import auth


# ---------------------------------------------------------------------------
# Password hashing
# ---------------------------------------------------------------------------

def test_hash_then_verify_roundtrip():
    h = auth.hash_password("s3cret")
    assert auth.verify_password("s3cret", h) is True


def test_verify_rejects_wrong_password():
    h = auth.hash_password("s3cret")
    assert auth.verify_password("wrong", h) is False


# ---------------------------------------------------------------------------
# Session tokens
# ---------------------------------------------------------------------------

_USER = {
    "username": "alice",
    "display_name": "Alice A",
    "role": "operator",
    "company_codes": ["RU06", "RU47"],
}


def test_session_token_roundtrip():
    token = auth.create_session_token(_USER)
    payload = auth.decode_session_token(token)
    assert payload is not None
    assert payload["username"] == "alice"
    assert payload["company_codes"] == ["RU06", "RU47"]


def test_tampered_token_rejected():
    token = auth.create_session_token(_USER)
    tampered = token[:-3] + ("aaa" if not token.endswith("aaa") else "bbb")
    assert auth.decode_session_token(tampered) is None


def test_garbage_token_rejected():
    assert auth.decode_session_token("not-a-real-token") is None


def test_expired_token_rejected(monkeypatch):
    token = auth.create_session_token(_USER)
    # Any positive token age now exceeds a negative max-age -> SignatureExpired.
    monkeypatch.setattr(auth, "_SESSION_TTL", -1)
    assert auth.decode_session_token(token) is None


# ---------------------------------------------------------------------------
# Cookie helper
# ---------------------------------------------------------------------------

def test_get_session_from_cookies_valid():
    token = auth.create_session_token(_USER)
    payload = auth.get_session_from_cookies({auth.COOKIE_NAME: token})
    assert payload is not None and payload["username"] == "alice"


def test_get_session_from_cookies_missing():
    assert auth.get_session_from_cookies({}) is None
