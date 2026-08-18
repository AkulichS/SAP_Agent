"""The role/permission table itself.

These pin the *shape* of the model rather than any endpoint: a permission quietly
appearing in a weaker role is exactly the regression that would not show up as a
failing feature test.
"""

import permissions as p


# ---------------------------------------------------------------------------
# The table
# ---------------------------------------------------------------------------

def test_every_role_can_view():
    assert all(p.VIEW in perms for perms in p.PERMISSIONS.values())


def test_manager_is_strictly_read_only():
    assert p.PERMISSIONS[p.MANAGER] == frozenset({p.VIEW})


def test_only_sys_admin_owns_the_base_and_the_registries():
    """The split that lets several company admins coexist: none of them can move the
    product base or mint users underneath everybody else."""
    for perm in (p.CONFIG_BASE, p.MANAGE_REGISTRY, p.MANAGE_USERS, p.MANAGE_LICENSE):
        holders = {role for role, perms in p.PERMISSIONS.items() if perm in perms}
        assert holders == {p.SYS_ADMIN}, f"{perm} leaked to {holders - {p.SYS_ADMIN}}"


def test_company_admin_is_an_operator_plus_company_config():
    assert p.PERMISSIONS[p.ADMIN] == p.PERMISSIONS[p.OPERATOR] | {p.CONFIG_COMPANY}


def test_sys_admin_is_a_superset_of_every_other_role():
    for role, perms in p.PERMISSIONS.items():
        assert perms <= p.PERMISSIONS[p.SYS_ADMIN], role


def test_unknown_role_carries_nothing():
    assert p.permissions_for("wizard") == frozenset()
    assert p.permissions_for(None) == frozenset()
    assert not p.has_permission("wizard", p.VIEW)


# ---------------------------------------------------------------------------
# Scope — independent of the permission set
# ---------------------------------------------------------------------------

def test_explicit_list_scopes_to_itself():
    assert p.in_scope(p.ADMIN, ["RU06"], "RU06")
    assert not p.in_scope(p.ADMIN, ["RU06"], "RU47")


def test_sys_admin_reaches_every_company_without_a_list():
    assert p.has_global_scope(p.SYS_ADMIN, [])
    assert p.in_scope(p.SYS_ADMIN, [], "ANYTHING")
    assert p.resolve_scope(p.SYS_ADMIN, [], ["RU06", "RU47"]) == ["RU06", "RU47"]


def test_wildcard_gives_any_role_global_scope():
    assert p.in_scope(p.MANAGER, ["*"], "RU47")
    assert p.resolve_scope(p.MANAGER, ["*"], ["RU06", "RU47"]) == ["RU06", "RU47"]


def test_resolve_scope_returns_an_explicit_list_unintersected():
    """Only the wildcard consults the registry; an explicit list is a pure function of
    the session, so callers that enumerate the registry intersect it themselves."""
    assert p.resolve_scope(p.OPERATOR, ["RU06", "RU72"], ["RU06"]) == ["RU06", "RU72"]


def test_a_permission_never_implies_scope():
    """The two questions stay independent: holding config_company does not put a company
    in reach, and holding a company does not grant the verb."""
    assert p.has_permission(p.ADMIN, p.CONFIG_COMPANY)
    assert not p.in_scope(p.ADMIN, ["RU06"], "RU47")
    assert p.in_scope(p.OPERATOR, ["RU06"], "RU06")
    assert not p.has_permission(p.OPERATOR, p.CONFIG_COMPANY)


# ---------------------------------------------------------------------------
# Role vocabulary
# ---------------------------------------------------------------------------

def test_role_vocabulary_is_closed():
    assert p.VALID_ROLES == {p.OPERATOR, p.MANAGER, p.ADMIN, p.SYS_ADMIN}


# ---------------------------------------------------------------------------
# Exclusive scope — every company is closed by exactly one operator
# ---------------------------------------------------------------------------

_USERS = [
    {"username": "op1", "role": p.OPERATOR, "company_codes": ["RU06", "RU72"]},
    {"username": "adm", "role": p.ADMIN,    "company_codes": ["RU06", "RU47"]},
]


def test_two_operators_may_not_hold_the_same_company():
    assert p.scope_conflicts(p.OPERATOR, ["RU72"], _USERS) == {"RU72": "op1"}
    assert p.scope_conflicts(p.OPERATOR, ["RU47"], _USERS) == {}


def test_only_the_exclusive_role_contends():
    """An admin overlapping an operator is not a clash in either direction — overlapping
    admins are what makes holiday cover possible."""
    assert p.scope_conflicts(p.ADMIN, ["RU06", "RU72"], _USERS) == {}
    assert p.scope_conflicts(p.OPERATOR, ["RU47"], _USERS) == {}   # RU47 is only the admin's


def test_editing_a_user_does_not_clash_with_itself():
    assert p.scope_conflicts(p.OPERATOR, ["RU06", "RU72"], _USERS,
                             exclude_username="op1") == {}


def test_wildcard_on_an_exclusive_role_claims_everything():
    assert p.scope_conflicts(p.OPERATOR, ["*"], _USERS) == {"RU06": "op1", "RU72": "op1"}
    # …and the mirror: a code requested against an operator who already holds "*"
    holder = [{"username": "all", "role": p.OPERATOR, "company_codes": ["*"]}]
    assert p.scope_conflicts(p.OPERATOR, ["RU06"], holder) == {"RU06": "all"}


def test_free_codes_hides_what_another_operator_owns():
    registry = ["RU06", "RU47", "RU72"]
    assert p.free_codes(p.OPERATOR, _USERS, registry) == ["RU47"]
    assert p.free_codes(p.OPERATOR, _USERS, registry, exclude_username="op1") == registry
    assert p.free_codes(p.ADMIN, _USERS, registry) == registry
