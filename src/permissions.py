"""
permissions.py — the role/permission table and the company-scope rule.

One sentence holds the whole model:

    **role is the verb set, `company_codes` is where.**

A user's `role` says *what* they may do (the permission set below); their
`company_codes` list says *which* companies they may do it to. The two are
independent, so every endpoint answers the same two questions in the same
order: "does this role carry the permission?" then "is this company in scope?"

Roles (ordered by reach, not by a strict ladder — `manager` is deliberately
weaker than `operator` on writes and wider on reads):

    operator   run the close for their companies; answer interrupts
    manager    read-only oversight across their companies (usually all of them)
    admin      operator + edit the per-company config delta for their companies
    sys_admin  everything, plus the product base, the registries and licensing

Two surfaces stay deliberately out of `admin`'s reach and inside `sys_admin`'s:
the **base config** (the product step library — one edit moves every company)
and the **registries** (companies, users) plus licensing. That is the split
that lets several people hold `admin` for disjoint or overlapping company sets
without anyone being able to change the product underneath everybody else.

Scope has one escape hatch: the literal ``"*"`` in `company_codes` means "every
registered company". `sys_admin` gets it implicitly; any other role can be given
it explicitly (a `manager` overseeing the whole close is the intended case).

Scope also has one *exclusivity* rule, and only one: two `operator`s may never hold
the same company (`EXCLUSIVE_SCOPE_ROLES`, checked by `scope_conflicts()`), so every
close has exactly one owner. Every other role overlaps freely — overlapping `admin`s
are what make holiday cover possible.

This module imports nothing from the project — it is a leaf, so both the store
and the web layer can depend on it without a cycle.
"""

from __future__ import annotations

# --- permissions -----------------------------------------------------------

VIEW = "view"                        # see a company's dashboard / run detail
RUN = "run"                          # start, decide, roll back a run
CONFIG_COMPANY = "config_company"    # edit a company's delta (and read the base)
CONFIG_BASE = "config_base"          # edit the product base — every company at once
MANAGE_REGISTRY = "manage_registry"  # create / rename / delete companies
MANAGE_USERS = "manage_users"        # create / edit / delete users
MANAGE_LICENSE = "manage_license"    # licensing surface (reserved)

# --- roles -----------------------------------------------------------------

OPERATOR = "operator"
MANAGER = "manager"
ADMIN = "admin"
SYS_ADMIN = "sys_admin"

PERMISSIONS: dict[str, frozenset[str]] = {
    OPERATOR:  frozenset({VIEW, RUN}),
    MANAGER:   frozenset({VIEW}),
    ADMIN:     frozenset({VIEW, RUN, CONFIG_COMPANY}),
    SYS_ADMIN: frozenset({VIEW, RUN, CONFIG_COMPANY, CONFIG_BASE,
                          MANAGE_REGISTRY, MANAGE_USERS, MANAGE_LICENSE}),
}

VALID_ROLES: frozenset[str] = frozenset(PERMISSIONS)

DEFAULT_ROLE = OPERATOR

#: Roles whose company scope is implicitly every registered company.
GLOBAL_SCOPE_ROLES: frozenset[str] = frozenset({SYS_ADMIN})

#: Wildcard entry in a user's `company_codes` meaning "every registered company".
ALL_COMPANIES = "*"

#: Roles whose company scope must not overlap another holder of the same role. Closing
#: a period is an accountable act: exactly one operator owns each company, so "who ran
#: it" is never ambiguous. Admins deliberately *do* overlap (that is holiday cover);
#: operators deliberately do not.
EXCLUSIVE_SCOPE_ROLES: frozenset[str] = frozenset({OPERATOR})


def permissions_for(role: str | None) -> frozenset[str]:
    """The permission set a role carries; an unknown role carries nothing."""
    return PERMISSIONS.get(role or "", frozenset())


def has_permission(role: str | None, permission: str) -> bool:
    return permission in permissions_for(role)


def has_global_scope(role: str | None, company_codes: list[str] | None) -> bool:
    """True when the user reaches every registered company — either because the
    role always does (`sys_admin`) or because they hold the `"*"` wildcard."""
    return role in GLOBAL_SCOPE_ROLES or ALL_COMPANIES in (company_codes or [])


def in_scope(role: str | None, company_codes: list[str] | None, code: str) -> bool:
    """True when `code` is inside this user's company scope."""
    return has_global_scope(role, company_codes) or code in set(company_codes or [])


def resolve_scope(role: str | None, company_codes: list[str] | None,
                  all_codes) -> list[str]:
    """The concrete company codes this user may reach.

    A global scope expands to `all_codes` (the registry); an explicit list is returned
    as given — deliberately *not* intersected with the registry, so this stays a pure
    function of the session and the registry is only consulted to expand the wildcard.
    Callers that enumerate the registry (the dashboard listing) intersect naturally."""
    if has_global_scope(role, company_codes):
        return list(all_codes)
    return [c for c in (company_codes or []) if c]


def scope_conflicts(role: str | None, company_codes: list[str] | None, users,
                    *, exclude_username: str | None = None) -> dict[str, str]:
    """Company codes this assignment would take from another holder of an exclusive role.

    `users` is the user registry (dicts with `username`, `role`, `company_codes`).
    Returns `{code: username}` for every clash — empty when the assignment is free. A
    non-exclusive role (admin, manager, sys_admin) never clashes, in either direction:
    only two `operator`s contend for the same company.

    `"*"` on an exclusive role claims every company at once, so it clashes with any code
    another operator already holds."""
    if role not in EXCLUSIVE_SCOPE_ROLES:
        return {}
    wanted = {c for c in (company_codes or []) if c}
    conflicts: dict[str, str] = {}
    for other in users:
        if other.get("username") == exclude_username:
            continue
        if other.get("role") not in EXCLUSIVE_SCOPE_ROLES:
            continue
        held = {c for c in (other.get("company_codes") or []) if c}
        overlap = held if ALL_COMPANIES in wanted else (
            wanted if ALL_COMPANIES in held else wanted & held)
        for code in overlap:
            conflicts.setdefault(code, other["username"])
    return conflicts


def free_codes(role: str | None, users, all_codes,
               *, exclude_username: str | None = None) -> list[str]:
    """The registered companies still assignable to a user in `role`.

    For a non-exclusive role that is the whole registry; for an `operator` it is the
    registry minus every company another operator already owns — which is what the
    picker offers, so a clash is normally impossible to build in the first place (the
    server still refuses it, since the UI is not the boundary)."""
    if role not in EXCLUSIVE_SCOPE_ROLES:
        return list(all_codes)
    taken: set[str] = set()
    for other in users:
        if other.get("username") == exclude_username:
            continue
        if other.get("role") in EXCLUSIVE_SCOPE_ROLES:
            taken |= {c for c in (other.get("company_codes") or []) if c}
    return [c for c in all_codes if c not in taken]
