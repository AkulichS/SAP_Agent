"""
Connector bus — the backend-neutral contract the orchestration engine talks to.

The graph engine (``graph_builder.py``) never talks to a backend directly. It
resolves a step's ``connector`` name to a :class:`Connector` and calls three
methods on it — ``run`` / ``poll`` / ``read_output`` — exchanging only the
unified envelope ``{status, messages, meta, data}``. This is the "USB" seam:
the engine is the host controller, the envelope is the wire protocol, and each
Connector is a device-class driver. A new backend = one Connector class,
registered by name, with **zero** engine edits.

The engine MUST NOT branch on connector name, nor read connector-specific
fields (SAP ``job_name``/``job_id``, a REST operation URL, …) out of an
envelope. Anything the engine needs to resume an async operation travels as an
**opaque handle** (``meta.handle``) that it hands straight back to the *same*
connector's ``poll`` / ``read_output`` — the engine never inspects its fields.

Contract
--------
``run(action_type, object_name, params, *, async_mode, test_run) -> Envelope``
    Execute one action. ``action_type`` is the connector's *own* vocabulary
    (SAP: SUBMIT/FM/BAPI/BDC/TOOLS; another device may define its own). When the
    result is asynchronous, set ``meta.requires_poll = True`` and publish an
    opaque ``meta.handle`` the engine will pass back to ``poll``.

``poll(handle) -> PollResult``
    Single-shot, non-blocking status check for an async operation. Maps the
    backend's native states onto the normalized lifecycle
    ``running | finished | aborted``. Only needed when ``run`` can return
    ``requires_poll``.

``read_output(handle, *, max_lines) -> Envelope``
    Fetch a finished async operation's output as an envelope. Only needed when
    the output is fetched separately from ``poll``.

Envelope ``data`` vocabulary (both ``run`` and ``read_output``)
-----------------------------------------------------------------
Three keys, all optional — a connector populates whichever apply to the
operation it just ran:

``data.text`` — ``{available, line_count, text}``
    One blob of human-readable output (a report spool, an HTTP response body,
    an SQL status message, a generated answer). A connector emits its readable
    output under this canonical key.

``data.rows`` — ``list[dict]``
    Structured records — table rows, an SQL result set, a REST array
    response, RAG hits (each retrieved chunk is a record). Not assumed to
    share a fixed column set.

``data.raw`` — connector-native, unprocessed payload
    The escape hatch for whatever doesn't fit ``text``/``rows`` (a REST
    single JSON object, a driver's native cursor metadata, debug detail).
    **The engine must never read ``data.raw``** — anything the
    execute→poll→validate pipeline depends on has to be expressible as
    ``text`` or ``rows``. ``raw`` exists only for diagnostics/UI/power-user
    visibility, never as a channel the core pipeline consumes.

The engine's ``_text`` / ``_rows`` accessors read only ``data.text`` /
``data.rows``, so consumers stay backend-agnostic regardless of which keys a
given connector happens to populate.

**Build ``data`` with the** :func:`envelope_data` **factory** rather than by hand.
It is the one sanctioned constructor — it accepts only ``text`` / ``rows`` /
``raw`` and drops the ``None`` ones, so a connector author physically cannot
invent a key the engine will silently ignore (``data["response"] = …`` simply
isn't expressible). :func:`foreign_data_keys` is the matching guardrail for
data built by hand: it reports any top-level key outside the neutral set, and
:func:`warn_foreign_data_keys` logs those at a connector's engine-facing return.

``placeholders(handle) -> dict``
    Values this backend publishes for ``{{token}}`` params of downstream
    verification actions (validate ``run`` / checks). SAP publishes
    ``{job_name, job_id}``; another device may publish an operation URL, a
    result-set id, … The engine merges this dict into the resolution context
    without knowing the token names.

Diagnostics (the analysis ReAct agent)
--------------------------------------
When a step fails, the analysis agent investigates using **this backend's** own
read-only tools — so a step on any device is diagnosed with that device's tools,
never a hardcoded SAP set:

``list_diagnostic_tools() -> list[dict]``
    Read-only tool schemas (OpenAI function-calling shape) the agent may call.
    Empty ⇒ the agent reasons from the error context alone.

``call_diagnostic(name, args) -> Envelope``
    Dispatch one read-only diagnostic tool → unified envelope. MUST refuse any
    mutating tool (diagnosis must never change the backend).
"""

from __future__ import annotations

import json
import logging
from typing import Any, Protocol, runtime_checkable

logger = logging.getLogger(__name__)

# Normalized async lifecycle states every connector's ``poll`` maps onto.
POLL_RUNNING = "running"
POLL_FINISHED = "finished"
POLL_ABORTED = "aborted"

# Backend-neutral output kinds inside an envelope's ``data``. See the module
# docstring's "Envelope data vocabulary" section for what each means and when
# a connector should populate it.
TEXT_KEY = "text"       # canonical: {available, line_count, text}
ROWS_KEY = "rows"       # canonical: list[dict] of structured records
RAW_KEY = "raw"         # connector-native passthrough; engine must not read it

# The complete, closed set of top-level keys an envelope's ``data`` may carry.
# Anything else is invisible to the engine's _text/_rows accessors — which is
# exactly the mistake envelope_data() prevents and foreign_data_keys() catches.
NEUTRAL_DATA_KEYS = frozenset({TEXT_KEY, ROWS_KEY, RAW_KEY})


def envelope_data(*, text: dict | None = None, rows: list | None = None,
                  raw: Any = None) -> dict:
    """Build an envelope's ``data`` — the ONE sanctioned way.

    Pass whichever of the three neutral channels apply; ``None`` ones are
    omitted. Because the only parameters are ``text`` / ``rows`` / ``raw``, a
    connector author cannot accidentally emit a key the engine ignores — the
    contract in the module docstring becomes a signature, not a convention.

    ``text`` should already be the ``{available, line_count, text}`` sub-shape;
    ``rows`` a ``list[dict]``; ``raw`` anything (the escape hatch the engine
    must never read). 
    """
    data: dict = {}
    if text is not None:
        data[TEXT_KEY] = text
    if rows is not None:
        data[ROWS_KEY] = rows
    if raw is not None:
        data[RAW_KEY] = raw
    return data


def foreign_data_keys(data: Any) -> set[str]:
    """Top-level keys in ``data`` outside the neutral set — empty when conformant.
    The pure predicate behind the boundary guardrail: a non-empty result means a
    connector populated a key the engine's _text/_rows accessors will never read.
    """
    if not isinstance(data, dict):
        return set()
    return set(data) - NEUTRAL_DATA_KEYS


def warn_foreign_data_keys(env: dict, *, where: str) -> dict:
    """Log a warning if an engine-facing envelope's ``data`` carries foreign keys.

    Call this at a connector's ``run`` / ``read_output`` return — the boundary
    where the envelope crosses into the backend-neutral engine. Non-fatal by
    design: it surfaces a contract slip during connector development without
    breaking a running close. Returns ``env`` for call-site chaining.
    """
    foreign = foreign_data_keys(env.get("data"))
    if foreign:
        logger.warning(
            "%s: envelope data carries non-neutral key(s) %s — the engine reads "
            "only %s, so these are invisible to it. Build data via "
            "envelope_data(text=/rows=/raw=).",
            where, sorted(foreign), sorted(NEUTRAL_DATA_KEYS))
    return env


def parse_tool_result(result: Any) -> dict:
    """Decode an MCP ``call_tool`` result into the envelope dict it carries.

    Mirrors ``graph_builder._parse_tool_result`` but lives here so the connectors
    package never imports the engine (which would be a circular import).
    """
    for block in result.content:
        if block.type == "text":
            try:
                return json.loads(block.text)
            except json.JSONDecodeError:
                return {"raw": block.text}
    return {}


@runtime_checkable   
class Connector(Protocol):
    """A backend device driver. See module docstring for the contract."""

    name: str

    async def run(
        self,
        action_type: str,
        object_name: str,
        params: Any,
        *,
        async_mode: bool = False,
        test_run: bool = False,
    ) -> dict:
        ...

    async def poll(self, handle: dict) -> dict:
        """Return ``{"state": running|finished|aborted, "raw": <native>}``."""
        ...

    async def read_output(self, handle: dict, *, max_lines: int = 500) -> dict:
        """Return an envelope carrying the finished op's output (``data.text``)."""
        ...

    def placeholders(self, handle: dict) -> dict:
        """Return this backend's published ``{{token}}`` values (see module doc)."""
        ...

    async def list_diagnostic_tools(self) -> list[dict]:
        """Read-only tool schemas the analysis agent may call (see module doc)."""
        ...

    async def call_diagnostic(self, name: str, args: dict) -> dict:
        """Dispatch one read-only diagnostic tool → envelope (see module doc)."""
        ...


class BaseConnector:
    """Optional convenience base with no-op defaults.

    A device subclasses this and overrides only what its backend supports — an
    async device implements ``poll``/``read_output``; a purely synchronous one
    leaves them raising. Keeps connector authoring to "the parts that differ",
    which is the whole point of the bus.
    """

    name = "base"

    def __init__(self, session: Any = None):
        self.session = session

    async def run(self, action_type: str, object_name: str, params: Any,
                  *, async_mode: bool = False, test_run: bool = False) -> dict:
        raise NotImplementedError(f"{type(self).__name__} does not implement run()")

    async def poll(self, handle: dict) -> dict:
        raise NotImplementedError(f"{type(self).__name__} does not implement poll()")

    async def read_output(self, handle: dict, *, max_lines: int = 500) -> dict:
        raise NotImplementedError(
            f"{type(self).__name__} does not implement read_output()")

    def placeholders(self, handle: dict) -> dict:
        return {}

    async def list_diagnostic_tools(self) -> list[dict]:
        """No diagnostics by default — the agent reasons from the error context."""
        return []

    async def call_diagnostic(self, name: str, args: dict) -> dict:
        return {"status": "error", "data": {},
                "messages": [{"TYPE": "E", "MESSAGE":
                              f"{self.name}: no diagnostic tool {name!r}"}]}
