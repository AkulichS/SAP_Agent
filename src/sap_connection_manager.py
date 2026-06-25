"""
SAPConnectionManager
--------------------
Thread-safe singleton that owns the pyrfc connection.
Lives exclusively inside the MCP server process — the agent never imports this.

Usage:
    mgr = SAPConnectionManager()
    mgr.configure(params)          # once at startup
    conn = mgr.get_connection()    # anywhere in MCP tools
"""

from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass, field
from typing import Any

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


class SAPConnectionManager:
    """
    Singleton connection manager.

    Thread-safe: get_connection() can be called from multiple asyncio
    tasks running in the same thread (the MCP server's event loop) as well
    as from background threads used by pyrfc callbacks.
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
        self._connection: Any | None = None          # pyrfc.Connection
        self._conn_lock = threading.Lock()
        self._reconnect_delay_base: float = 2.0      # seconds, doubles on each retry
        self._max_retries: int = 5

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def configure(self, params: SAPConnectionParams) -> None:
        """Call once at MCP server startup before any tool invocation."""
        self._params = params
        logger.info(
            f"SAPConnectionManager configured for host={params.ashost} client={params.client}",
        )

    def get_connection(self) -> Any:
        """
        Return a live pyrfc.Connection.
        Transparently reconnects when the connection is stale.
        """
        with self._conn_lock:
            if self._connection is None or not self._is_alive():
                self._reconnect()
            return self._connection

    def close(self) -> None:
        """Graceful shutdown — call from MCP server lifespan teardown."""
        with self._conn_lock:
            self._close_connection()
        logger.info("SAP connection closed")

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _is_alive(self) -> bool:
        if self._connection is None:
            return False
        try:
            self._connection.ping()
            return True
        except Exception:
            logger.debug("SAP ping failed — connection considered dead")
            return False

    def _close_connection(self) -> None:
        if self._connection is not None:
            try:
                self._connection.close()
            except Exception:
                pass
            self._connection = None

    def _reconnect(self) -> None:
        if self._params is None:
            raise RuntimeError(
                "SAPConnectionManager.configure() must be called before get_connection()"
            )

        self._close_connection()

        last_exc: Exception | None = None
        delay = self._reconnect_delay_base

        for attempt in range(1, self._max_retries + 1):
            try:
                logger.info(f"SAP connect attempt {attempt}/{self._max_retries} …")

                if HAS_PYRFC:
                    self._connection = pyrfc.Connection(**self._params.to_pyrfc_dict())
                else:
                    # Stub for local dev / CI without SAP libs
                    self._connection = _StubConnection(self._params)

                logger.info(f"SAP connection established (attempt {attempt})")
                return

            except Exception as exc:
                last_exc = exc
                logger.warning(f"SAP connect attempt {attempt} failed: {exc}")
                if attempt < self._max_retries:
                    time.sleep(delay)
                    delay = min(delay * 2, 30.0)   # exponential back-off, cap 30s

        raise ConnectionError(
            f"Failed to connect to SAP after {self._max_retries} attempts"
        ) from last_exc


# ---------------------------------------------------------------------------
# Stub connection — used when pyrfc is not installed
# ---------------------------------------------------------------------------

class _StubConnection:
    """
    Mimics pyrfc.Connection for local dev / CI without SAP libs.

    All calls go through ZFI_AI_PERIOD_CLOSE_RFC and must return the three
    export parameters that _rfc() in mcp_server.py reads:
        EV_STATUS       — "S" | "E" | "W"
        EV_RESULT_JSON  — JSON string
        ET_MESSAGES     — list of {TYPE, MESSAGE} dicts
    """

    # Realistic per-table stub rows — add more as steps are tested
    _TABLE_DATA: dict[str, list[dict]] = {
        "COKP":   [{"KOKRS": "X500", "GJAHR": "2025", "PERAB": "11", "SPERRE": ""}],
        "BKPF":   [{"BUKRS": "RU06", "GJAHR": "2025", "MONAT": "11",
                    "BELNR": "1000000001", "BSTAT": "", "WAERS": "RUB"}],
        "AUFK":   [{"AUFNR": "000100000001", "KOKRS": "X500",
                    "KOSTL": "", "OBJNR": "OR000100000001"}],
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
            return {"EV_STATUS": "S", "EV_RESULT_JSON": "{}", "ET_MESSAGES": []}

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
                    "EV_RESULT_JSON": _json.dumps({"status": "submitted", "mode": "async",
                                                   "jobname": job_name, "jobcount": job_id}),
                    "ET_MESSAGES":    [{"TYPE": "S",
                                        "MESSAGE": f"Job {job_name}/{job_id} submitted{suffix}"}],
                }
            lines = self._SPOOL_TEXT.get(obj, self._SPOOL_TEXT["default"])
            spool = {"status": "S", "text": "\n".join(lines)}
            return {
                "EV_STATUS":      "S",
                "EV_RESULT_JSON": _json.dumps({"status": "completed", "mode": "sync_wait",
                                               "jobname": job_name, "jobcount": job_id,
                                               "spool": spool}),
                "ET_MESSAGES":    [{"TYPE": "S",
                                    "MESSAGE": f"Job {job_name}/{job_id} completed inline{suffix}"}],
            }

        # ── FM / BAPI / BDC ─────────────────────────────────────────────────
        if action in ("FM", "BAPI", "BDC"):
            return {
                "EV_STATUS":      "S",
                "EV_RESULT_JSON": "{}",
                "ET_MESSAGES":    [{"TYPE": "S",
                                    "MESSAGE": f"STUB: {action}/{obj} executed OK"}],
            }

        # ── TOOLS ───────────────────────────────────────────────────────────
        if action == "TOOLS":
            return self._handle_tool(obj, kwargs)

        return {"EV_STATUS": "E", "EV_RESULT_JSON": "{}",
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
                "EV_RESULT_JSON": _json.dumps({"data": rows}),
                "ET_MESSAGES":    [{"TYPE": "S",
                                    "MESSAGE": f"STUB: {len(rows)} rows from {table}"}],
            }

        # TOOL_JOB_STATUS — always FINISHED so poll_node exits immediately
        if tool_name == "TOOL_JOB_STATUS":
            return {
                "EV_STATUS":      "S",
                "EV_RESULT_JSON": _json.dumps({"state": "FINISHED"}),
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
                "EV_RESULT_JSON": _json.dumps({"status": "S", "text": "\n".join(lines)}),
                "ET_MESSAGES":    [],
            }

        return {
            "EV_STATUS":      "S",
            "EV_RESULT_JSON": "{}",
            "ET_MESSAGES":    [{"TYPE": "S", "MESSAGE": f"STUB: {tool_name} OK"}],
        }

    def close(self) -> None:
        pass
