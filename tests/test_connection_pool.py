"""SAPConnectionManager pool behaviour.

The pool is what lets ONE shared MCP server serve every concurrent company run:
before it, a run either owned a whole server subprocess (stdio) or every RFC call
in the fleet queued behind a single connection. These tests pin the four
properties the capacity argument rests on — reuse, real parallelism, a hard cap,
and never handing out a connection that just blew up.

pyrfc is absent in CI, so `_connect()` produces `_StubConnection` objects; where a
test needs to count or break connects it substitutes its own factory.
"""

import threading
import time

import pytest

import sap_connection_manager as scm
from sap_connection_manager import SAPConnectionManager, SAPConnectionParams

PARAMS = SAPConnectionParams(ashost="host", sysnr="00", client="100", user="u", passwd="p")


@pytest.fixture
def mgr(monkeypatch):
    """A fresh manager per test — the class is a process-wide singleton, so the
    instance is swapped out and restored rather than mutated in place."""
    saved = SAPConnectionManager._instance
    SAPConnectionManager._instance = None
    m = SAPConnectionManager()
    m.configure(PARAMS)
    try:
        yield m
    finally:
        m.close()
        SAPConnectionManager._instance = saved


class FakeConn:
    """Minimal stand-in: counts pings/closes and can be told to fail either."""

    def __init__(self, idx: int, ping_fails: bool = False):
        self.idx        = idx
        self.ping_fails = ping_fails
        self.closed     = False
        self.pings      = 0

    def ping(self):
        self.pings += 1
        if self.ping_fails:
            raise RuntimeError("connection is dead")

    def close(self):
        self.closed = True


def _counting_factory(monkeypatch, mgr, ping_fails=lambda idx: False):
    """Replace _connect with a counting factory; returns the list of made conns."""
    made: list[FakeConn] = []

    def _connect():
        conn = FakeConn(len(made), ping_fails=ping_fails(len(made)))
        made.append(conn)
        return conn

    monkeypatch.setattr(mgr, "_connect", _connect)
    return made


# ---------------------------------------------------------------------------
# Reuse
# ---------------------------------------------------------------------------

def test_sequential_checkouts_reuse_one_connection(mgr, monkeypatch):
    made = _counting_factory(monkeypatch, mgr)

    for _ in range(5):
        with mgr.connection() as conn:
            assert conn.idx == 0

    assert len(made) == 1                      # opened once, reused four times
    assert mgr.stats() == {"max": mgr.pool_size, "live": 1, "idle": 1}


def test_recently_used_connection_is_not_pinged(mgr, monkeypatch):
    """Pinging on every checkout would double the RFC round-trip count."""
    made = _counting_factory(monkeypatch, mgr)

    with mgr.connection():
        pass
    with mgr.connection():
        pass

    assert made[0].pings == 0


def test_idle_connection_is_pinged_before_handout(mgr, monkeypatch):
    made = _counting_factory(monkeypatch, mgr)
    mgr._ping_interval = 0                      # everything counts as stale

    with mgr.connection():
        pass
    with mgr.connection() as conn:
        assert conn is made[0]

    assert made[0].pings == 1


def test_dead_connection_is_replaced(mgr, monkeypatch):
    # Connection #0 fails its liveness ping; the pool must drop it and open #1.
    made = _counting_factory(monkeypatch, mgr, ping_fails=lambda idx: idx == 0)
    mgr._ping_interval = 0

    with mgr.connection():
        pass
    with mgr.connection() as conn:
        assert conn.idx == 1

    assert made[0].closed is True
    assert mgr.stats()["live"] == 1             # the dead one freed its slot


# ---------------------------------------------------------------------------
# Parallelism and the cap
# ---------------------------------------------------------------------------

def test_concurrent_callers_get_distinct_connections(mgr, monkeypatch):
    """The whole point: two runs calling SAP at once do not queue behind each other."""
    made    = _counting_factory(monkeypatch, mgr)
    entered = threading.Barrier(3, timeout=5)
    seen: list[int] = []

    def worker():
        with mgr.connection() as conn:
            seen.append(conn.idx)
            entered.wait()                      # hold both connections at once

    threads = [threading.Thread(target=worker) for _ in range(2)]
    for t in threads:
        t.start()
    entered.wait()
    for t in threads:
        t.join(timeout=5)

    assert sorted(seen) == [0, 1]
    assert len(made) == 2


def test_pool_size_caps_concurrency(mgr, monkeypatch):
    """A third caller waits for a slot instead of opening a third connection."""
    made = _counting_factory(monkeypatch, mgr)
    mgr._pool_size = 2

    holding = threading.Barrier(3, timeout=5)
    release = threading.Event()
    third_got = threading.Event()

    def holder():
        with mgr.connection():
            holding.wait()
            release.wait(timeout=5)

    holders = [threading.Thread(target=holder) for _ in range(2)]
    for t in holders:
        t.start()
    holding.wait()                              # both slots now in use

    def third():
        with mgr.connection():
            third_got.set()

    t3 = threading.Thread(target=third)
    t3.start()
    assert third_got.wait(timeout=0.3) is False  # blocked — no slot free
    assert len(made) == 2

    release.set()
    assert third_got.wait(timeout=5) is True     # a holder handed one back
    for t in (*holders, t3):
        t.join(timeout=5)
    assert len(made) == 2                        # reused, never exceeded the cap


def test_acquire_times_out_with_actionable_error(mgr, monkeypatch):
    _counting_factory(monkeypatch, mgr)
    mgr._pool_size       = 1
    mgr._acquire_timeout = 0.1

    with mgr.connection():
        with pytest.raises(ConnectionError, match="SAP_POOL_SIZE"):
            with mgr.connection():
                pass


def test_failed_connect_frees_its_slot(mgr, monkeypatch):
    """A refused connect must not permanently consume a pool slot."""
    def _boom():
        raise ConnectionError("SAP is down")

    monkeypatch.setattr(mgr, "_connect", _boom)
    for _ in range(3):
        with pytest.raises(ConnectionError):
            with mgr.connection():
                pass

    assert mgr.stats()["live"] == 0


# ---------------------------------------------------------------------------
# Error handling and shutdown
# ---------------------------------------------------------------------------

def test_connection_whose_call_raised_is_discarded(mgr, monkeypatch):
    """A pyrfc connection whose call blew up may be in an undefined state — it is
    closed rather than handed to the next run."""
    made = _counting_factory(monkeypatch, mgr)

    with pytest.raises(RuntimeError):
        with mgr.connection():
            raise RuntimeError("RFC_ERROR")

    assert made[0].closed is True
    assert mgr.stats() == {"max": mgr.pool_size, "live": 0, "idle": 0}

    with mgr.connection() as conn:
        assert conn.idx == 1                    # a fresh one, not the poisoned one


def test_close_closes_idle_connections(mgr, monkeypatch):
    made = _counting_factory(monkeypatch, mgr)
    mgr._pool_size = 2

    barrier = threading.Barrier(3, timeout=5)

    def worker():
        with mgr.connection():
            barrier.wait()

    threads = [threading.Thread(target=worker) for _ in range(2)]
    for t in threads:
        t.start()
    barrier.wait()
    for t in threads:
        t.join(timeout=5)

    mgr.close()
    assert all(c.closed for c in made)
    assert mgr.stats() == {"max": 2, "live": 0, "idle": 0}


def test_connection_released_after_close_is_not_pooled(mgr, monkeypatch):
    """Shutdown while a call is in flight: the late arrival is closed, not stashed
    where it would be handed out with the pool's accounting already reset."""
    made = _counting_factory(monkeypatch, mgr)

    with mgr.connection() as conn:
        mgr.close()                             # closes idle ones; this one is in flight

    assert conn.closed is True
    assert made[0] is conn
    assert mgr.stats() == {"max": mgr.pool_size, "live": 0, "idle": 0}


def test_pool_size_comes_from_env(monkeypatch):
    monkeypatch.setenv("SAP_POOL_SIZE", "24")
    saved = SAPConnectionManager._instance
    SAPConnectionManager._instance = None
    try:
        assert SAPConnectionManager().pool_size == 24
    finally:
        SAPConnectionManager._instance = saved


def test_configure_pool_size_overrides_default(mgr):
    mgr.configure(PARAMS, pool_size=3)
    assert mgr.pool_size == 3


def test_stub_connection_is_served_without_pyrfc(mgr):
    # Guard: with pyrfc absent the pool must still produce working stub connections,
    # which is what the whole offline test suite runs on.
    assert scm.HAS_PYRFC is False
    with mgr.connection() as conn:
        assert conn.ping() is None
        assert "EV_STATUS" in conn.call("ZFI_AI_PERIOD_CLOSE_RFC", IV_ACTION_TYPE="FM",
                                        IV_OBJECT_NAME="X", IV_PARAMS_JSON="{}")


def test_pool_survives_thrash(mgr, monkeypatch):
    """Many short checkouts across threads: no leaked slots, no lost connections."""
    _counting_factory(monkeypatch, mgr)
    mgr._pool_size = 4
    errors: list[Exception] = []

    def worker():
        try:
            for _ in range(25):
                with mgr.connection():
                    time.sleep(0.001)
        except Exception as exc:                # pragma: no cover - failure path
            errors.append(exc)

    threads = [threading.Thread(target=worker) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=15)

    assert not errors
    stats = mgr.stats()
    assert stats["live"] == stats["idle"] <= 4  # everything handed back
