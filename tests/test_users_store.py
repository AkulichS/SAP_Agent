"""User registry — DB-backed store CRUD and the last-sys_admin guards."""

import pytest

import auth
from config_store import ConfigStore


@pytest.fixture
def store(tmp_path):
    return ConfigStore(tmp_path / "cfg.db")


def _hash(pw="pw"):
    return auth.hash_password(pw)


# ---------------------------------------------------------------------------
# CRUD
# ---------------------------------------------------------------------------

def test_create_and_get_roundtrip(store):
    store.create_user("ivanov", _hash("secret"), display_name="Ivanov I",
                      role="operator", company_codes=["RU06", "RU72"])
    u = store.get_user("ivanov")
    assert u["username"] == "ivanov"
    assert u["role"] == "operator"
    assert u["company_codes"] == ["RU06", "RU72"]
    assert auth.verify_password("secret", u["password_hash"])          # hash usable
    assert store.get_user("ghost") is None


def test_list_users_hides_password_hash(store):
    store.create_user("admin", _hash(), role="admin")
    rows = store.list_users()
    assert rows and "password_hash" not in rows[0]
    assert rows[0]["username"] == "admin"


def test_create_duplicate_rejected(store):
    store.create_user("dup", _hash(), role="operator")
    with pytest.raises(ValueError):
        store.create_user("dup", _hash(), role="operator")


def test_create_requires_username(store):
    with pytest.raises(ValueError):
        store.create_user("  ", _hash())


def test_update_partial_leaves_other_fields(store):
    store.create_user("u", _hash("old"), display_name="Old", role="operator",
                      company_codes=["RU06"])
    store.update_user("u", display_name="New")
    u = store.get_user("u")
    assert u["display_name"] == "New"
    assert u["company_codes"] == ["RU06"]           # untouched
    assert auth.verify_password("old", u["password_hash"])   # password untouched


def test_update_password_only(store):
    store.create_user("u", _hash("old"), role="operator")
    store.update_user("u", password_hash=_hash("new"))
    u = store.get_user("u")
    assert auth.verify_password("new", u["password_hash"])
    assert not auth.verify_password("old", u["password_hash"])


def test_update_unknown_user_raises(store):
    with pytest.raises(ValueError):
        store.update_user("nobody", display_name="x")


def test_delete_user(store):
    store.create_user("admin", _hash(), role="admin")
    store.create_user("op", _hash(), role="operator")
    store.delete_user("op")
    assert store.get_user("op") is None
    assert [u["username"] for u in store.list_users()] == ["admin"]


def test_delete_unknown_user_raises(store):
    with pytest.raises(ValueError):
        store.delete_user("ghost")


# ---------------------------------------------------------------------------
# Last-sys_admin guard
# ---------------------------------------------------------------------------
# The guard tracks the TOP role only. A company-scoped `admin` is not a fallback: it
# cannot reach the base config, the registries or the user list, so a deployment whose
# last `sys_admin` disappeared would be unmanageable from the UI.

def test_cannot_delete_last_sys_admin(store):
    store.create_user("only-admin", _hash(), role="sys_admin")
    store.create_user("op", _hash(), role="operator")
    with pytest.raises(ValueError):
        store.delete_user("only-admin")
    # a second sys_admin lifts the guard
    store.create_user("admin2", _hash(), role="sys_admin")
    store.delete_user("only-admin")          # now allowed
    assert store.get_user("only-admin") is None


def test_cannot_demote_last_sys_admin(store):
    store.create_user("only-admin", _hash(), role="sys_admin")
    with pytest.raises(ValueError):
        store.update_user("only-admin", role="operator")
    # with a second sys_admin present, demotion is fine
    store.create_user("admin2", _hash(), role="sys_admin")
    store.update_user("only-admin", role="operator")
    assert store.get_user("only-admin")["role"] == "operator"


def test_company_admin_does_not_satisfy_the_sys_admin_guard(store):
    """`admin` is the company-scoped config role after the rename — it must not count as
    cover for the last system administrator."""
    store.create_user("solo", _hash(), role="sys_admin")
    store.create_user("co-admin", _hash(), role="admin")
    with pytest.raises(ValueError):
        store.delete_user("solo")


# ---------------------------------------------------------------------------
# E-mail + one-time passwords
# ---------------------------------------------------------------------------

def test_email_and_must_change_roundtrip(store):
    store.create_user("u", _hash(), email="u@example.com", must_change_password=True)
    u = store.get_user("u")
    assert u["email"] == "u@example.com" and u["must_change_password"] is True
    assert store.list_users()[0]["email"] == "u@example.com"

    store.update_user("u", must_change_password=False)
    assert store.get_user("u")["must_change_password"] is False
    assert store.get_user("u")["email"] == "u@example.com"      # untouched


def test_blank_email_is_stored_as_absent(store):
    """'' and None mean the same thing — no address on file — so resets have one case
    to test instead of two."""
    store.create_user("u", _hash(), email="   ")
    assert store.get_user("u")["email"] is None
    store.update_user("u", email="u@example.com")
    store.update_user("u", email="")
    assert store.get_user("u")["email"] is None


def test_password_generator_is_random_and_unambiguous(store):
    pws = {auth.generate_password() for _ in range(50)}
    assert len(pws) == 50
    assert all(len(p) == 14 for p in pws)
    assert not (set("".join(pws)) & set("0O1lI"))    # no lookalikes to mistype
