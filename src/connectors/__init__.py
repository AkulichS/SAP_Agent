"""
Connector bus package.

The orchestration engine reaches every backend through this package: it resolves
a step's ``connector`` name via :func:`get_connector` and speaks the
backend-neutral :class:`Connector` contract (see ``base.py``). The default
``sap_rfc`` device is registered here at import time; new devices register with
:func:`register_connector`.
"""

from __future__ import annotations

from .base import (NEUTRAL_DATA_KEYS, POLL_ABORTED, POLL_FINISHED, POLL_RUNNING,
                   TEXT_KEY, BaseConnector, Connector, envelope_data,
                   foreign_data_keys, parse_tool_result, warn_foreign_data_keys)
from .mcp import McpConnector, register_mcp_connector
from .registry import (DEFAULT_CONNECTOR, get_connector, register_connector,
                       registered_connectors)
from .sap_rfc import SapRfcConnector

# Register the built-in SAP-RFC device on import. Other devices (e.g. MCP proxies)
# are registered by the host app at startup once their session is available.
register_connector("sap_rfc", SapRfcConnector)

__all__ = [
    "Connector",
    "BaseConnector",
    "SapRfcConnector",
    "McpConnector",
    "get_connector",
    "register_connector",
    "register_mcp_connector",
    "registered_connectors",
    "parse_tool_result",
    "envelope_data",
    "foreign_data_keys",
    "warn_foreign_data_keys",
    "DEFAULT_CONNECTOR",
    "POLL_RUNNING",
    "POLL_FINISHED",
    "POLL_ABORTED",
    "TEXT_KEY",
    "NEUTRAL_DATA_KEYS",
]
