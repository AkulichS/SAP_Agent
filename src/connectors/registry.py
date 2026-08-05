"""
Connector registry — resolve a step's ``connector`` name to a bound driver.

A backend is registered once by name (at import time) as a *factory*
``session -> Connector``; the engine instantiates it per call, bound to the
current run's MCP session. Adding a device is ``register_connector("name",
factory)`` — no engine edits.
"""

from __future__ import annotations

from typing import Any, Callable

from .base import Connector

DEFAULT_CONNECTOR = "sap_rfc"

# name -> factory(session) -> Connector
_FACTORIES: dict[str, Callable[[Any], Connector]] = {}


def register_connector(name: str, factory: Callable[[Any], Connector]) -> None:
    """Register a connector factory under ``name`` (overwrites an existing one)."""
    _FACTORIES[name] = factory


def registered_connectors() -> list[str]:
    """Names of all registered connectors (for the admin UI catalog)."""
    return sorted(_FACTORIES)


def get_connector(session: Any, name: str | None = None) -> Connector:
    """Instantiate the connector registered under ``name``, bound to ``session``.

    Instances are cheap (they just hold the session), so we build one per call
    rather than caching — no lifecycle to track.
    """
    key = name or DEFAULT_CONNECTOR
    factory = _FACTORIES.get(key)
    if factory is None:
        raise KeyError(
            f"Unknown connector: {key!r}. Registered: {registered_connectors()}")
    return factory(session)
