import asyncio
import json
import signal
from datetime import UTC, datetime, timedelta

import pytest
from websockets.asyncio.client import connect

from crypto_research.db.models import BackfillObjectRow
from crypto_research.db.repositories import SymbolState
from crypto_research.market import __main__ as market_main
from crypto_research.market.binance.archive_paths import DatasetKind, plan_archives
from crypto_research.market.binance.streams import group_streams, streams_for_symbols
from crypto_research.market.worker import (
    ConnectionMode,
    ManagedConnection,
    MarketWorker,
    install_sigterm_handler,
)

NOW = datetime(2026, 7, 21, 12, tzinfo=UTC)


class Repository:
    def __init__(self) -> None:
        self.active = (
            SymbolState("BTCUSDT", True, None, None, False),
            SymbolState("PEPEUSDT", True, None, None, True),
        )
        self.streams = []
        self.gaps = {}
        self.heartbeats = []
        self.transitions = []
        self.checkpoints = 0

    async def list_active_symbols(self):
        return self.active

    async def update_stream(self, state) -> None:
        self.streams.append(state)

    async def record_gap(self, gap):
        self.gaps.setdefault(gap.id, gap)
        return self.gaps[gap.id]

    async def update_worker_heartbeat(self, heartbeat) -> None:
        self.heartbeats.append(heartbeat)

    async def record_source_transition(self, transition) -> None:
        self.transitions.append(transition)

    async def checkpoint(self) -> None:
        self.checkpoints += 1


class Supervisor:
    def __init__(self) -> None:
        self.refreshes = []
        self.shutdown_count = 0
        self.connections = ()

    async def refresh(self, groups, now) -> None:
        self.refreshes.append((groups, now))

    async def shutdown(self) -> None:
        self.shutdown_count += 1

    async def discard(self, connection, reason: str) -> None:
        await connection.close(reason)
        self.connections = tuple(
            current for current in self.connections if current is not connection
        )


class Storage:
    def __init__(self) -> None:
        self.events = []

    def persist(self, event) -> None:
        self.events.append(event)


class Backfill:
    def __init__(self) -> None:
        self.calls = []

    async def run_once(self, worker_id: str, lease_duration: timedelta) -> None:
        self.calls.append((worker_id, lease_duration))


class Socket:
    async def ping(self):
        future = asyncio.get_running_loop().create_future()
        future.set_result(None)
        return future

    async def close(self, code: int = 1000, reason: str = "") -> None:
        return None


class ReceiveSocket(Socket):
    def __init__(self, message: str) -> None:
        self.message = message
        self.delivered = False
        self.closed = asyncio.Event()

    async def recv(self) -> str:
        if not self.delivered:
            self.delivered = True
            return self.message
        await self.closed.wait()
        raise ConnectionError("closed")

    async def close(self, code: int = 1000, reason: str = "") -> None:
        self.closed.set()


def worker(repository, supervisor, storage, backfill, clock=lambda: NOW, **kwargs):
    return MarketWorker(
        repository,
        supervisor,
        storage,
        backfill_runner=backfill,
        worker_id="worker-a",
        clock=clock,
        stale_after=timedelta(seconds=120),
        refresh_interval=0.01,
        lease_duration=timedelta(minutes=5),
        **kwargs,
    )


def test_cycle_refreshes_independent_active_symbols_and_schedules_backfill() -> None:
    async def scenario() -> None:
        repository = Repository()
        supervisor = Supervisor()
        backfill = Backfill()
        market_worker = worker(repository, supervisor, Storage(), backfill)

        await market_worker.run_cycle()

        groups = supervisor.refreshes[0][0]
        assert len(groups) == 2
        assert {stream.symbol for group in groups for stream in group.streams} == {
            "BTCUSDT",
            "PEPEUSDT",
        }
        assert backfill.calls == [("worker-a", timedelta(minutes=5))]
        assert repository.heartbeats[-1].status == "running"
        assert repository.heartbeats[-1].details["active_symbols"] == 2
        assert repository.checkpoints == 1

    asyncio.run(scenario())


def test_event_is_stored_before_stream_freshness_update_and_then_marked_stale() -> None:
    async def scenario() -> None:
        repository = Repository()
        storage = Storage()
        moments = [NOW, NOW + timedelta(seconds=121)]
        market_worker = worker(
            repository,
            Supervisor(),
            storage,
            Backfill(),
            clock=lambda: moments[0],
        )
        message = json.dumps(
            {
                "stream": "btcusdt@aggtrade",
                "data": {
                    "e": "aggTrade",
                    "E": 1_753_099_200_010,
                    "s": "BTCUSDT",
                    "a": 42,
                    "p": "1.000000000000000000",
                    "q": "2.000000000000000000",
                    "f": 100,
                    "l": 102,
                    "T": 1_753_099_200_009,
                    "m": True,
                },
            }
        )

        await market_worker.handle_message(message)
        assert len(storage.events) == 1
        assert repository.streams[-1].status == "connected"
        assert repository.streams[-1].last_event_at == NOW

        await market_worker.update_stale_states(moments[1])
        assert repository.streams[-1].status == "degraded"
        assert repository.streams[-1].details == {"reason": "stale_live_data"}

    asyncio.run(scenario())


def test_connected_stream_without_first_event_becomes_stale() -> None:
    class SilentSocket(Socket):
        async def recv(self) -> str:
            await asyncio.Event().wait()
            raise AssertionError("unreachable")

    async def scenario() -> None:
        repository = Repository()
        supervisor = Supervisor()
        group = group_streams(streams_for_symbols(("BTCUSDT",)))[0]
        supervisor.connections = (
            ManagedConnection(
                group,
                SilentSocket(),
                ConnectionMode.DIRECT,
                NOW,
                timedelta(hours=23, minutes=55),
            ),
        )
        market_worker = worker(
            repository, supervisor, Storage(), Backfill()
        )

        await market_worker.run_cycle()
        await market_worker.update_stale_states(NOW + timedelta(seconds=121))

        stale = [state for state in repository.streams if state.status == "degraded"]
        assert {state.stream_name for state in stale} == {
            "btcusdt@aggtrade",
            "btcusdt@bookticker",
        }

    asyncio.run(scenario())


def test_disconnect_window_becomes_idempotent_per_stream_gap_on_reconnect() -> None:
    async def scenario() -> None:
        repository = Repository()
        market_worker = worker(
            repository, Supervisor(), Storage(), Backfill()
        )
        group = group_streams(streams_for_symbols(("BTCUSDT",)))[0]
        connection = ManagedConnection(
            group,
            Socket(),
            ConnectionMode.DIRECT,
            NOW,
            timedelta(hours=23, minutes=55),
        )

        await market_worker.note_disconnect(connection, NOW + timedelta(minutes=1))
        await market_worker.note_reconnect(group, NOW + timedelta(minutes=2))
        await market_worker.note_reconnect(group, NOW + timedelta(minutes=2))

        assert len(repository.gaps) == 2
        assert {gap.dataset for gap in repository.gaps.values()} == {
            "agg_trades",
            "book_ticker",
        }
        assert all(gap.reason == "source_unknown_disconnect" for gap in repository.gaps.values())
        disconnected = [
            state for state in repository.streams if state.status == "disconnected"
        ]
        assert all(
            state.details["disconnected_at"]
            == (NOW + timedelta(minutes=1)).isoformat()
            for state in disconnected
        )
        assert repository.streams[-1].status == "connected"

    asyncio.run(scenario())


def test_run_shuts_down_connections_and_persists_stopped_heartbeat() -> None:
    async def scenario() -> None:
        repository = Repository()
        supervisor = Supervisor()
        market_worker = worker(
            repository, supervisor, Storage(), Backfill()
        )

        task = asyncio.create_task(market_worker.run())
        while not supervisor.refreshes:
            await asyncio.sleep(0)
        market_worker.request_shutdown()
        await task

        assert supervisor.shutdown_count == 1
        assert repository.heartbeats[-1].status == "stopped"
        assert repository.checkpoints >= 2

    asyncio.run(scenario())


def test_run_consumes_managed_websocket_messages_before_shutdown() -> None:
    async def scenario() -> None:
        repository = Repository()
        supervisor = Supervisor()
        storage = Storage()
        message = json.dumps(
            {
                "stream": "btcusdt@aggtrade",
                "data": {
                    "e": "aggTrade",
                    "E": 1_753_099_200_010,
                    "s": "BTCUSDT",
                    "a": 42,
                    "p": "1.000000000000000000",
                    "q": "2.000000000000000000",
                    "f": 100,
                    "l": 102,
                    "T": 1_753_099_200_009,
                    "m": True,
                },
            }
        )
        group = group_streams(streams_for_symbols(("BTCUSDT",)))[0]
        supervisor.connections = (
            ManagedConnection(
                group,
                ReceiveSocket(message),
                ConnectionMode.DIRECT,
                NOW,
                timedelta(hours=23, minutes=55),
            ),
        )
        market_worker = worker(repository, supervisor, storage, Backfill())

        task = asyncio.create_task(market_worker.run())
        for _ in range(100):
            if storage.events:
                break
            await asyncio.sleep(0)
        market_worker.request_shutdown()
        await task

        assert len(storage.events) == 1
        assert storage.events[0].source_id == "42"

    asyncio.run(scenario())


def test_connection_loss_uses_backoff_before_discarding_for_reconnect() -> None:
    class FailedSocket(Socket):
        async def recv(self) -> str:
            raise ConnectionError("network lost")

    async def scenario() -> None:
        repository = Repository()
        supervisor = Supervisor()
        group = group_streams(streams_for_symbols(("BTCUSDT",)))[0]
        supervisor.connections = (
            ManagedConnection(
                group,
                FailedSocket(),
                ConnectionMode.DIRECT,
                NOW,
                timedelta(hours=23, minutes=55),
            ),
        )
        delays = []

        async def sleep(delay: float) -> None:
            delays.append(delay)

        market_worker = worker(
            repository,
            supervisor,
            Storage(),
            Backfill(),
            sleeper=sleep,
            random_source=lambda: 0.5,
        )

        task = asyncio.create_task(market_worker.run())
        for _ in range(100):
            if delays:
                break
            await asyncio.sleep(0)
        market_worker.request_shutdown()
        await task

        assert delays == [1.0]
        assert any(state.status == "disconnected" for state in repository.streams)

    asyncio.run(scenario())


def test_initial_connection_failure_is_degraded_and_retried_with_backoff() -> None:
    class FlakySupervisor(Supervisor):
        def __init__(self) -> None:
            super().__init__()
            self.attempts = 0

        async def refresh(self, groups, now) -> None:
            self.attempts += 1
            if self.attempts == 1:
                raise TimeoutError("handshake timed out")
            await super().refresh(groups, now)

    async def scenario() -> None:
        repository = Repository()
        supervisor = FlakySupervisor()
        delays = []

        async def sleep(delay: float) -> None:
            delays.append(delay)

        market_worker = worker(
            repository,
            supervisor,
            Storage(),
            Backfill(),
            sleeper=sleep,
            random_source=lambda: 0.5,
        )

        task = asyncio.create_task(market_worker.run())
        for _ in range(100):
            if supervisor.attempts >= 2:
                break
            await asyncio.sleep(0)
        market_worker.request_shutdown()
        await task

        assert supervisor.attempts >= 2
        assert delays == [1.0]
        assert any(
            heartbeat.status == "degraded"
            and heartbeat.details["reason"] == "timeout"
            for heartbeat in repository.heartbeats
        )

    asyncio.run(scenario())


def test_shutdown_interrupts_connection_backoff() -> None:
    class FailedSupervisor(Supervisor):
        async def refresh(self, groups, now) -> None:
            raise TimeoutError("handshake timed out")

    async def scenario() -> None:
        entered_sleep = asyncio.Event()

        async def sleep(_delay: float) -> None:
            entered_sleep.set()
            await asyncio.Event().wait()

        market_worker = worker(
            Repository(),
            FailedSupervisor(),
            Storage(),
            Backfill(),
            sleeper=sleep,
        )

        task = asyncio.create_task(market_worker.run())
        await entered_sleep.wait()
        market_worker.request_shutdown()

        await asyncio.wait_for(task, timeout=0.1)

    asyncio.run(scenario())


def test_sigterm_handler_only_requests_graceful_stop() -> None:
    class Loop:
        def __init__(self) -> None:
            self.signal_number = None
            self.callback = None

        def add_signal_handler(self, signal_number, callback) -> None:
            self.signal_number = signal_number
            self.callback = callback

    loop = Loop()
    stop = asyncio.Event()

    install_sigterm_handler(loop, stop)  # type: ignore[arg-type]

    assert loop.signal_number is signal.SIGTERM
    assert loop.callback is not None
    loop.callback()
    assert stop.is_set()


def test_runtime_uses_websockets_16_asyncio_connector() -> None:
    assert market_main.websocket_connect is connect


def test_runtime_reconstructs_only_official_persisted_archive_plan() -> None:
    start = datetime(2026, 7, 20, tzinfo=UTC)
    archive = plan_archives(
        DatasetKind.KLINES,
        "BTCUSDT",
        start,
        start + timedelta(days=1),
        as_of=NOW,
    )[0]
    row = BackfillObjectRow(
        id="00000000-0000-0000-0000-000000000001",
        job_id="00000000-0000-0000-0000-000000000002",
        source_url=archive.url,
        source_checksum="",
        start_at=archive.start,
        end_at=archive.end,
        state="planned",
        attempt_count=0,
    )

    assert market_main.resolve_planned_archive(row, NOW) == archive
    row.source_url = "https://attacker.invalid/day.zip"
    with pytest.raises(ValueError, match="approved archive plan"):
        market_main.resolve_planned_archive(row, NOW)
