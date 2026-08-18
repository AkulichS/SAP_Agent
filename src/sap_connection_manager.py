"""
SAPConnectionManager
--------------------
Thread-safe singleton that owns a **pool** of pyrfc connections.
Lives exclusively inside the MCP server process — the agent never imports this.

Usage:
    mgr = SAPConnectionManager()
    mgr.configure(params)              # once at startup
    with mgr.connection() as conn:     # anywhere in MCP tools
        conn.call(...)

Why a pool: one shared MCP server serves every concurrent company run, and each
RFC call runs in a worker thread (``mcp_server._tool``). A single connection
behind a lock would serialize every run against SAP; a pool lets N calls proceed
in parallel while still *bounding* the load — each busy connection occupies one
SAP dialog work process, so ``SAP_POOL_SIZE`` is the throttle you hand Basis.

The pool is lazy (connections are created on demand up to the cap), LIFO (the
hottest connection is reused first), and self-healing (a connection idle longer
than ``SAP_PING_INTERVAL`` is pinged before hand-out, and one whose call raised
is closed rather than returned to the pool). A checkout blocks up to
``SAP_POOL_TIMEOUT`` seconds waiting for a free slot, then raises.
"""

from __future__ import annotations

import logging
import os
import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Any, Iterator

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# pyrfc is an optional import — keeps unit tests runnable without SAP libs
# ---------------------------------------------------------------------------
try:
    import pyrfc  # type: ignore
    HAS_PYRFC = True
except ImportError:          # pragma: no cover
    pyrfc = None             # type: ignore
    HAS_PYRFC = False
    logger.warning("pyrfc not installed — SAPConnectionManager running in STUB mode")


@dataclass
class SAPConnectionParams:
    ashost: str
    sysnr: str
    client: str
    user: str | None = None
    passwd: str | None = None
    lang: str = "EN"
    snc_mode: str | None = None
    snc_myname: str | None = None
    snc_partnername: str | None = None
    

    def to_pyrfc_dict(self) -> dict[str, str]:
        base = {
            "ashost": self.ashost,                     # SAP server address
            "sysnr": self.sysnr,                       # SAP system number
            "client": self.client,                     # SAP client number
            "user": self.user,                         # SAP user
            "passwd": self.passwd,                     # SAP password
            "lang": self.lang,                         # language
            'snc_mode': self.snc_mode,                 # enabling SNC (1 - is enabled)
            'snc_myname': self.snc_myname,             # user SNC-name
            'snc_partnername': self.snc_partnername,   # SNC-name of SAP server
        }
        
        # strip None values — pyrfc rejects unknown keys
        return {k: v for k, v in base.items() if v is not None}


@dataclass
class _Pooled:
    """One pooled connection plus the moment it was last handed back."""
    conn: Any
    last_used: float


DEFAULT_POOL_SIZE       = 8       # concurrent RFC calls = concurrent SAP dialog work processes
DEFAULT_ACQUIRE_TIMEOUT = 300.0   # seconds a caller waits for a free connection
DEFAULT_PING_INTERVAL   = 60.0    # skip the liveness ping on a connection used this recently


class SAPConnectionManager:
    """
    Singleton pool of pyrfc connections.

    Thread-safe: ``connection()`` may be called from any number of worker threads
    (that is the point — the MCP server offloads every blocking RFC call to one)
    as well as from the event loop thread.
    """

    _instance: SAPConnectionManager | None = None
    _class_lock: threading.Lock = threading.Lock()

    # ------------------------------------------------------------------
    # Singleton construction
    # ------------------------------------------------------------------

    def __new__(cls) -> SAPConnectionManager:
        if cls._instance is None:
            with cls._class_lock:
                if cls._instance is None:
                    instance = super().__new__(cls)
                    instance._init_internal()
                    cls._instance = instance
        return cls._instance

    def _init_internal(self) -> None:
        self._params: SAPConnectionParams | None = None
        # Pool state, all guarded by _cond: _idle holds handed-back connections,
        # _live counts every connection the pool owns (idle + checked out).
        self._cond  = threading.Condition()
        self._idle: list[_Pooled] = []
        self._live  = 0
        self._pool_size       = _env_int("SAP_POOL_SIZE", DEFAULT_POOL_SIZE)
        self._acquire_timeout = _env_float("SAP_POOL_TIMEOUT", DEFAULT_ACQUIRE_TIMEOUT)
        self._ping_interval   = _env_float("SAP_PING_INTERVAL", DEFAULT_PING_INTERVAL)
        self._reconnect_delay_base: float = 2.0      # seconds, doubles on each retry
        self._max_retries: int = 5

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def configure(self, params: SAPConnectionParams, *, pool_size: int | None = None) -> None:
        """Call once at MCP server startup before any tool invocation."""
        self._params = params
        if pool_size is not None:
            self._pool_size = max(1, int(pool_size))
        logger.info(
            f"SAPConnectionManager configured for host={params.ashost} "
            f"client={params.client} pool_size={self._pool_size}",
        )

    @property
    def pool_size(self) -> int:
        """Maximum number of simultaneously open connections (the RFC concurrency cap)."""
        return self._pool_size

    @contextmanager
    def connection(self) -> Iterator[Any]:
        """Check out a live connection for the duration of one RFC call.

        The connection returns to the pool on exit; if the body raised, it is
        closed instead — a pyrfc connection whose call blew up may be in an
        undefined state, and a fresh one costs less than a poisoned one.
        """
        conn   = self._acquire()
        broken = False
        try:
            yield conn
        except Exception:
            broken = True
            raise
        finally:
            self._release(conn, broken=broken)

    def stats(self) -> dict[str, int]:
        """Pool occupancy — for logging and the capacity tests."""
        with self._cond:
            return {"max": self._pool_size, "live": self._live, "idle": len(self._idle)}

    def close(self) -> None:
        """Graceful shutdown — call from MCP server lifespan teardown."""
        with self._cond:
            idle, in_use = self._idle, self._live - len(self._idle)
            self._idle, self._live = [], 0
            self._cond.notify_all()
        for p in idle:
            _close_quietly(p.conn)
        if in_use:
            logger.warning("SAP pool closed with %d connection(s) still checked out", in_use)
        logger.info("SAP connection pool closed (%d idle connections)", len(idle))

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _acquire(self) -> Any:
        """Hand out a live connection: reuse an idle one, open a new one while
        below the cap, else wait for a peer to hand one back."""
        deadline = time.monotonic() + self._acquire_timeout
        while True:
            candidate: _Pooled | None = None
            with self._cond:
                if self._idle:
                    candidate = self._idle.pop()      # LIFO — keep the hot one hot
                elif self._live < self._pool_size:
                    self._live += 1                   # reserve the slot, connect outside the lock
                else:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        raise ConnectionError(
                            f"No SAP connection available after {self._acquire_timeout:.0f}s "
                            f"(pool_size={self._pool_size}, all busy) — raise SAP_POOL_SIZE "
                            f"or reduce concurrent runs"
                        )
                    self._cond.wait(remaining)
                    continue

            # Liveness check runs OUTSIDE the lock — it is a network round trip.
            if candidate is not None:
                if self._is_usable(candidate):
                    return candidate.conn
                self._drop(candidate.conn)            # frees the slot, then retry
                continue

            try:
                return self._connect()
            except Exception:
                with self._cond:                      # give the reserved slot back
                    self._live -= 1
                    self._cond.notify()
                raise

    def _release(self, conn: Any, *, broken: bool) -> None:
        if broken:
            self._drop(conn)
            return
        with self._cond:
            # "Nothing is checked out" per the accounting means the pool was closed
            # under this caller (shutdown) — that connection is orphaned, not idle.
            orphaned = self._live <= len(self._idle)
            if not orphaned:
                self._idle.append(_Pooled(conn, time.monotonic()))
            self._cond.notify()
        if orphaned:
            _close_quietly(conn)

    def _drop(self, conn: Any) -> None:
        """Close a connection and free its pool slot."""
        with self._cond:
            self._live = max(0, self._live - 1)
            self._cond.notify()
        _close_quietly(conn)

    def _is_usable(self, pooled: _Pooled) -> bool:
        """True if the connection can be handed out. Freshly used ones are trusted
        without a ping — pinging every checkout would double the RFC call count."""
        if time.monotonic() - pooled.last_used < self._ping_interval:
            return True
        try:
            pooled.conn.ping()
            return True
        except Exception:
            logger.debug("SAP ping failed — dropping stale pooled connection")
            return False

    def _connect(self) -> Any:
        """Open one new connection, with back-off retries."""
        if self._params is None:
            raise RuntimeError(
                "SAPConnectionManager.configure() must be called before connection()"
            )

        last_exc: Exception | None = None
        delay = self._reconnect_delay_base

        for attempt in range(1, self._max_retries + 1):
            try:
                logger.info(f"SAP connect attempt {attempt}/{self._max_retries} …")

                if HAS_PYRFC:
                    conn = pyrfc.Connection(**self._params.to_pyrfc_dict())
                else:
                    # Stub for local dev / CI without SAP libs
                    conn = _StubConnection(self._params)

                logger.info(f"SAP connection established (attempt {attempt})")
                return conn

            except Exception as exc:
                last_exc = exc
                logger.warning(f"SAP connect attempt {attempt} failed: {exc}")
                if attempt < self._max_retries:
                    time.sleep(delay)
                    delay = min(delay * 2, 30.0)   # exponential back-off, cap 30s

        raise ConnectionError(
            f"Failed to connect to SAP after {self._max_retries} attempts"
        ) from last_exc


def _close_quietly(conn: Any) -> None:
    try:
        conn.close()
    except Exception:
        pass


def _env_int(name: str, default: int) -> int:
    try:
        return max(1, int(os.environ[name]))
    except (KeyError, ValueError):
        return default


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.environ[name])
    except (KeyError, ValueError):
        return default


# ---------------------------------------------------------------------------
# Stub connection — used when pyrfc is not installed
# ---------------------------------------------------------------------------

class _StubConnection:
    """
    Mimics pyrfc.Connection for local dev / CI without SAP libs.

    All calls go through ZFI_AI_PERIOD_CLOSE_RFC and must return the four
    export parameters that _rfc() in mcp_server.py reads:
        EV_STATUS       — "S" | "E" | "W" | "A"
        EV_RESULT_DATA  — business payload JSON string (rows / spool / scalars)
        EV_META         — control JSON string (mode, job ids, state, counts)
        ET_MESSAGES     — list of {TYPE, MESSAGE} dicts
    Mirrors the real RFC's standardized {status, data, meta, messages} split.
    """

    # Realistic per-table stub rows — add more as steps are tested
    _TABLE_DATA: dict[str, list[dict]] = {
        "COKP":   [{"KOKRS": "X500", "GJAHR": "2025", "PERAB": "11", "SPERRE": ""}],
        "BKPF":   [{"BUKRS": "RU06", "GJAHR": "2025", "MONAT": "11",
                    "BELNR": "1000000001", "BSTAT": "", "WAERS": "RUB"}],
        # Master data for the three KO8G orders that fail settlement (see the
        # RKO7KO8G spool below). All three are released (PHAS1=1 → REL) so the
        # KD205 error is genuinely about the settlement rule, and each carries
        # DIFFERENT evidence so the analysis table is informative per order.
        "AUFK":   [
            {"AUFNR": "000005000000", "AUART": "0400", "AUTYP": "01",
             "BUKRS": "RU06", "KOKRS": "X500", "WERKS": "RU01", "LOEKZ": "",
             "PHAS0": "0", "PHAS1": "1", "PHAS2": "0", "PHAS3": "0",
             "KTEXT": "Технический заказ для RU06", "OBJNR": "OR000005000000"},
            {"AUFNR": "FK01", "AUART": "0400", "AUTYP": "01",
             "BUKRS": "RU06", "KOKRS": "X500", "WERKS": "RU01", "LOEKZ": "",
             "PHAS0": "0", "PHAS1": "1", "PHAS2": "0", "PHAS3": "0",
             "KTEXT": "Заказ производства FK01", "OBJNR": "ORFK01"},
            {"AUFNR": "FK02", "AUART": "0400", "AUTYP": "01",
             "BUKRS": "RU06", "KOKRS": "X500", "WERKS": "RU01", "LOEKZ": "",
             "PHAS0": "0", "PHAS1": "1", "PHAS2": "0", "PHAS3": "0",
             "KTEXT": "Заказ производства FK01", "OBJNR": "ORFK02"},
        ],
        # Settlement rules. Orders 5000000 and FK01 have NO rows (rule missing);
        # FK02 has a single rule that distributes only 50% (rule incomplete).
        "COBRB":  [
            {"OBJNR": "ORFK02", "KONTY": "KS", "EMPGE": "0000101000",
             "PROZS": "50.00", "PERBZ": "PER"},
        ],
        "COSS":   [{"LEDNR": "00", "OBJNR": "OR000100000001",
                    "GJAHR": "2025", "PERAB": "11", "WKGBTR": "1000.00"}],
        "CKMLHD": [{"MATNR": "000000000000100001", "BWKEY": "RU06",
                    "POPER": "11", "BDATJ": "2025", "STATUS": "30"}],
        "CSKS":   [{"KOSTL": "0000100000", "KOKRS": "X500",
                    "DATBI": "99991231", "VERAK": "USER01"}],
        "KEKO":   [{"MATNR": "000000000000100001", "BWKEY": "RU06",
                    "PEINH": "1", "STPRS": "120.00", "WAERS": "RUB"}],
    }

    # Spool text keyed by object_name (program/report)
    _SPOOL_TEXT: dict[str, list[str]] = {
        "default": [
            "STUB spool output",
            "Processing complete.",
            "All objects processed successfully.",
            "Settlement completed: 42 orders processed, 0 issues.",
            "Total amount posted: 1 000 000.00 RUB",
        ],
        "RKO7KO8G": [
            "KO8G — CO Internal Order Settlement",
            "Controlling area X500 / Period 11 / FY 2025",
            "Orders selected  : 3",
            "Orders settled   : 0",
            "Orders with error:  3",
            "",
            "Sender: ORD 5000000 Технический заказ для RU06",
            "E KD205  Maintain the settlement rule of the sender",
            "",
            "Sender: ORD FK01 Заказ производства FK01",
            "E KD205  Maintain the settlement rule of the sender",
            "",
            "Sender: ORD FK02 Заказ производства FK01",
            "E KD205  Maintain the settlement rule of the sender",
            "",
            "Settlement completed with errors.",
            # Switch to the success variant below to test the happy path:
            # "Orders selected: 42, Orders settled: 42, Orders with error: 0",
            # "All internal orders settled successfully.",
        ],
        "RKABL000": [
            "KSW5 — Overhead Cost Orders Settlement",
            "42 overhead orders processed, 0 errors.",
        ],
        "RKAV0000": [
            "KSC5 — Cost Centre Distribution",
            "Distribution run completed, 0 errors.",
        ],
        "RKSSVFAK": [
            "KSII — Actual Price Calculation",
            "Actual prices calculated for all cost centres, 0 errors.",
        ],
        "RCOARSR0": [
            "CO88 — WIP Calculation",
            "WIP calculated for 15 orders, 0 errors.",
        ],
    }

    def __init__(self, params: SAPConnectionParams) -> None:
        self._params = params
        self._job_counter = 0
        logger.warning("_StubConnection active — all RFC calls return mock data")

    def ping(self) -> None:
        pass

    def call(self, fm_name: str, **kwargs: Any) -> dict[str, Any]:
        import json as _json

        action = kwargs.get("IV_ACTION_TYPE", "")
        obj    = kwargs.get("IV_OBJECT_NAME", "")
        async_ = kwargs.get("IV_ASYNC", "") == "X"
        test   = kwargs.get("IV_TEST_RUN", "") == "X"

        logger.info("STUB RFC %s  action=%-8s  object=%s  async=%s  test=%s",
                    fm_name, action, obj, async_, test)

        if fm_name != "ZFI_AI_PERIOD_CLOSE_RFC":
            return {"EV_STATUS": "S", "EV_RESULT_DATA": "{}", "EV_META": "{}",
                    "ET_MESSAGES": []}

        # ── SUBMIT ──────────────────────────────────────────────────────────
        # SUBMIT always runs as a background job. async → EV_STATUS 'A' + job ids
        # (caller polls); sync → EV_STATUS 'S' with the spool embedded inline
        # (mode="sync_wait"), mirroring z_execute_submit's inline-wait path.
        if action == "SUBMIT":
            self._job_counter += 1
            job_name = f"STUB_{obj[:10]}"
            job_id   = f"{self._job_counter:08d}"
            suffix   = " (test-run)" if test else ""
            if async_:
                return {
                    "EV_STATUS":      "A",
                    # Control → EV_META; async submit has no payload yet.
                    "EV_META":        _json.dumps({"mode": "async",
                                                   "jobname": job_name, "jobcount": job_id}),
                    "EV_RESULT_DATA": "{}",
                    "ET_MESSAGES":    [{"TYPE": "S",
                                        "MESSAGE": f"Job {job_name}/{job_id} submitted{suffix}"}],
                }
            lines = self._SPOOL_TEXT.get(obj, self._SPOOL_TEXT["default"])
            spool = {"available": True, "line_count": len(lines), "text": "\n".join(lines)}
            return {
                "EV_STATUS":      "S",
                # Job identity + mode → EV_META; the report output is the payload,
                # on the neutral "text" channel → EV_RESULT_DATA.
                "EV_META":        _json.dumps({"mode": "sync_wait",
                                               "jobname": job_name, "jobcount": job_id}),
                "EV_RESULT_DATA": _json.dumps({"text": spool}),
                "ET_MESSAGES":    [{"TYPE": "S",
                                    "MESSAGE": f"Job {job_name}/{job_id} completed inline{suffix}"}],
            }

        # ── FM / BAPI / BDC ─────────────────────────────────────────────────
        if action in ("FM", "BAPI", "BDC"):
            return {
                "EV_STATUS":      "S",
                "EV_RESULT_DATA": "{}",
                "EV_META":        "{}",
                "ET_MESSAGES":    [{"TYPE": "S",
                                    "MESSAGE": f"STUB: {action}/{obj} executed OK"}],
            }

        # ── TOOLS ───────────────────────────────────────────────────────────
        if action == "TOOLS":
            return self._handle_tool(obj, kwargs)

        return {"EV_STATUS": "E", "EV_RESULT_DATA": "{}", "EV_META": "{}",
                "ET_MESSAGES": [{"TYPE": "E",
                                 "MESSAGE": f"STUB: unknown action_type {action!r}"}]}

    # ------------------------------------------------------------------
    def _handle_tool(self, tool_name: str, kwargs: Any) -> dict[str, Any]:
        import json as _json

        raw = kwargs.get("IV_PARAMS_JSON", "{}")
        try:
            params: dict = _json.loads(raw) if isinstance(raw, str) else raw
        except Exception:
            params = {}

        # TOOL_READ_TABLE
        if tool_name == "TOOL_READ_TABLE":
            table  = params.get("table", "").upper()
            fields = [f.strip() for f in params.get("fields", "").split(",") if f.strip()]
            rows   = list(self._TABLE_DATA.get(table, []))
            if fields and rows:
                rows = [{f: r.get(f, "") for f in fields} for r in rows]
            return {
                "EV_STATUS":      "S",
                # Rows are the payload; table name + count are control.
                "EV_RESULT_DATA": _json.dumps({"rows": rows}),
                "EV_META":        _json.dumps({"table": table, "count": len(rows)}),
                "ET_MESSAGES":    [{"TYPE": "S",
                                    "MESSAGE": f"STUB: {len(rows)} rows from {table}"}],
            }

        # TOOL_JOB_STATUS — always FINISHED so poll_node exits immediately
        if tool_name == "TOOL_JOB_STATUS":
            return {
                "EV_STATUS":      "S",
                # State is a poll-control signal → EV_META; no business payload.
                "EV_RESULT_DATA": "{}",
                "EV_META":        _json.dumps({"jobname": params.get("jobname", ""),
                                               "jobcount": params.get("jobcount", ""),
                                               "state": "FINISHED"}),
                "ET_MESSAGES":    [],
            }

        # TOOL_READ_JOB_SPOOL
        if tool_name == "TOOL_READ_JOB_SPOOL":
            job_name = params.get("jobname", "")
            # match on the object_name embedded in the STUB job name (e.g. STUB_RKO7KO8G)
            obj_key  = job_name.replace("STUB_", "") if job_name.startswith("STUB_") else job_name
            lines    = self._SPOOL_TEXT.get(obj_key, self._SPOOL_TEXT["default"])
            return {
                "EV_STATUS":      "S",
                # The spool text {available,line_count,text} IS the payload; no control.
                "EV_RESULT_DATA": _json.dumps({"available": True, "line_count": len(lines),
                                               "text": "\n".join(lines)}),
                "EV_META":        "{}",
                "ET_MESSAGES":    [],
            }

        return {
            "EV_STATUS":      "S",
            "EV_RESULT_DATA": "{}",
            "EV_META":        "{}",
            "ET_MESSAGES":    [{"TYPE": "S", "MESSAGE": f"STUB: {tool_name} OK"}],
        }

    def close(self) -> None:
        pass
