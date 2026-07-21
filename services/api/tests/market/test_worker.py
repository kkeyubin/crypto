import asyncio
import json
import signal
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy.exc import DBAPIError, DisconnectionError, OperationalError
from websockets.asyncio.client import connect

from crypto_research.db.models import BackfillObjectRow
from crypto_research.db.repositories import StreamState, SymbolState
from crypto_research.market import __main__ as market_main
from crypto_research.market.binance.archive_paths import DatasetKind, plan_archives
from crypto_research.market.binance.streams import StreamKind, group_streams, streams_for_symbols
from crypto_research.market.live_storage import LiveAcceptResult, LiveWriteResult
from crypto_research.market.worker import (
    ConnectionMode,
    ConnectionPolicy,
    ManagedConnection,
    MarketWorker,
    RoutedConnectionFactory,
    StreamConnectionSupervisor,
    install_sigterm_handler,
    is_transient_database_error,
)

NOW = datetime(2026, 7, 21, 12, tzinfo=UTC)


class Repository:
    def __init__(self) -> None:
        self.active = (
            SymbolState("BTCUSDT", True, None, None, False),
            SymbolState("1000PEPEUSDT", True, None, None, True),
        )
        self.streams = []
        self.gaps = {}
        self.heartbeats = []
        self.transitions = []
        self.checkpoints = 0
        self.stream_batches = []
        self.live_batches = []
        self.persisted_stream_states = ()

    async def list_active_symbols(self):
        return self.active

    async def update_stream(self, state) -> None:
        self.streams.append(state)

    async def update_streams(self, states) -> None:
        batch = tuple(states)
        self.stream_batches.append(batch)
        self.streams.extend(batch)

    async def list_stream_states(self):
        return self.persisted_stream_states

    async def commit_live_batch(self, result, states) -> None:
        self.live_batches.append(result)
        await self.update_streams(states)

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
        self.batches = []
        self.lease_closed = False
        self.pending = []
        self.published = None
        self.publish_calls = 0
        self.acknowledged = []

    def acquire_writer(self, owner):
        storage = self

        class Lease:
            active = True

            def close(self):
                self.active = False
                storage.lease_closed = True

        return Lease()

    def accept(self, lease, event):
        assert lease.active
        self.events.append(event)
        self.pending.append(event)
        return LiveAcceptResult(True, False)

    def publish_next_batch(self, lease, *, max_events):
        assert lease.active
        self.publish_calls += 1
        if self.published is not None:
            return self.published
        if not self.pending:
            return None
        batch = tuple(self.pending[:max_events])
        del self.pending[:max_events]
        self.batches.append(batch)
        self.published = LiveWriteResult(
            (), (), batch_id=f"batch-{len(self.batches)}", events=batch
        )
        return self.published

    def acknowledge_cataloged(self, lease, batch_id):
        assert lease.active
        assert self.published is not None
        assert self.published.batch_id == batch_id
        self.acknowledged.append(batch_id)
        self.published = None


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
        await asyncio.sleep(0)

        groups = supervisor.refreshes[0][0]
        assert len(groups) == 2
        assert {stream.symbol for group in groups for stream in group.streams} == {
            "BTCUSDT",
            "1000PEPEUSDT",
        }
        assert backfill.calls == [("worker-a", timedelta(minutes=5))]
        assert repository.heartbeats[-1].status == "running"
        assert repository.heartbeats[-1].details["active_symbols"] == 2
        assert repository.checkpoints == 1
        await market_worker._stop_backfill()

    asyncio.run(scenario())


def test_events_are_durably_spooled_then_cataloged_and_committed_as_one_batch() -> None:
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
                "stream": "btcusdt@aggTrade",
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
        await market_worker.handle_message(message)
        assert len(storage.events) == 2
        assert storage.batches == []
        assert repository.stream_batches == []

        await market_worker.flush_events(force=True)

        assert len(storage.batches) == 1
        assert len(storage.events) == 2
        assert storage.acknowledged == ["batch-1"]
        assert len(repository.stream_batches) == 1
        assert repository.streams[-1].status == "connected"
        assert "disconnected_at" not in repository.streams[-1].details
        assert repository.streams[-1].last_event_at == NOW

        await market_worker.update_stale_states(moments[1])
        assert repository.streams[-1].status == "degraded"
        assert repository.streams[-1].details == {
            "dataset": "agg_trades",
            "reason": "stale_live_data",
        }

    asyncio.run(scenario())


def test_connected_stream_without_first_event_becomes_stale() -> None:
    class SilentSocket(Socket):
        async def recv(self) -> str:
            await asyncio.Event().wait()
            raise AssertionError("unreachable")

    async def scenario() -> None:
        repository = Repository()
        supervisor = Supervisor()
        groups = group_streams(streams_for_symbols(("BTCUSDT",)))
        supervisor.connections = tuple(
            ManagedConnection(
                group, SilentSocket(), ConnectionMode.DIRECT, NOW, timedelta(hours=23, minutes=55)
            )
            for group in groups
        )
        market_worker = worker(
            repository, supervisor, Storage(), Backfill()
        )

        await market_worker.run_cycle()
        await market_worker.update_stale_states(NOW + timedelta(seconds=121))

        stale = [state for state in repository.streams if state.status == "degraded"]
        assert {state.stream_name for state in stale} == {
            "btcusdt@aggTrade",
            "btcusdt@bookTicker",
            "btcusdt@kline_1m",
            "btcusdt@markPrice@1s",
        }

    asyncio.run(scenario())


def test_disconnect_window_becomes_idempotent_per_stream_gap_on_reconnect() -> None:
    async def scenario() -> None:
        repository = Repository()
        market_worker = worker(
            repository, Supervisor(), Storage(), Backfill()
        )
        groups = group_streams(streams_for_symbols(("BTCUSDT",)))
        for group in groups:
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

        assert len(repository.gaps) == 4
        assert {gap.dataset for gap in repository.gaps.values()} == {
            "agg_trade",
            "best_bid_ask",
            "kline_1m",
            "mark_price",
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
        assert "disconnected_at" not in repository.streams[-1].details

    asyncio.run(scenario())


def test_gap_anchor_is_only_cleared_after_gap_commit_succeeds() -> None:
    class FlakyGapRepository(Repository):
        def __init__(self) -> None:
            super().__init__()
            self.fail_once = True

        async def record_gap(self, gap):
            if self.fail_once:
                self.fail_once = False
                raise OperationalError("INSERT data_gaps", {}, Exception("offline"))
            return await super().record_gap(gap)

    async def scenario() -> None:
        repository = FlakyGapRepository()
        market_worker = worker(repository, Supervisor(), Storage(), Backfill())
        group = group_streams(streams_for_symbols(("BTCUSDT",)))[0]
        connection = ManagedConnection(
            group,
            Socket(),
            ConnectionMode.DIRECT,
            NOW,
            timedelta(hours=23, minutes=55),
        )
        await market_worker.note_disconnect(connection, NOW + timedelta(minutes=1))

        with pytest.raises(OperationalError):
            await market_worker.note_reconnect(group, NOW + timedelta(minutes=2))
        assert len(market_worker._disconnects) == len(group.streams)

        await market_worker.note_reconnect(group, NOW + timedelta(minutes=2))
        assert len(repository.gaps) == len(group.streams)
        assert market_worker._disconnects == {}

    asyncio.run(scenario())


def test_planned_rotation_records_gap_with_authoritative_data_types() -> None:
    async def scenario() -> None:
        repository = Repository()
        market_worker = worker(repository, Supervisor(), Storage(), Backfill())
        groups = group_streams(streams_for_symbols(("BTCUSDT",)))
        for group in groups:
            connection = ManagedConnection(
                group,
                Socket(),
                ConnectionMode.DIRECT,
                NOW,
                timedelta(hours=23, minutes=55),
            )
            await market_worker.note_disconnect(
                connection,
                NOW + timedelta(minutes=1),
                reason="planned_pre_24_hour_rotation",
            )
            await market_worker.note_reconnect(
                group, NOW + timedelta(minutes=1, seconds=2)
            )

        assert {gap.dataset for gap in repository.gaps.values()} == {
            "agg_trade",
            "best_bid_ask",
            "kline_1m",
            "mark_price",
        }
        assert {
            gap.reason for gap in repository.gaps.values()
        } == {"planned_pre_24_hour_rotation"}

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
                "stream": "btcusdt@aggTrade",
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
        group = next(
            group
            for group in group_streams(streams_for_symbols(("BTCUSDT",)))
            if any(stream.kind is StreamKind.AGG_TRADE for stream in group.streams)
        )
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
        assert storage.lease_closed is True

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


def test_main_loop_retries_transient_sqlalchemy_failure_with_bound() -> None:
    class FlakyDatabaseRepository(Repository):
        def __init__(self) -> None:
            super().__init__()
            self.failures = 1

        async def list_active_symbols(self):
            if self.failures:
                self.failures -= 1
                raise OperationalError("SELECT symbols", {}, Exception("offline"))
            return await super().list_active_symbols()

    async def scenario() -> None:
        repository = FlakyDatabaseRepository()
        supervisor = Supervisor()
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
            if supervisor.refreshes:
                break
            await asyncio.sleep(0)
        market_worker.request_shutdown()
        await task

        assert delays == [1.0]
        assert len(supervisor.refreshes) == 1

    asyncio.run(scenario())


def test_invalidated_sqlalchemy_dbapi_connection_is_transient() -> None:
    error = DBAPIError(
        "SELECT symbols",
        {},
        Exception("connection was invalidated"),
        connection_invalidated=True,
    )

    assert is_transient_database_error(error) is True


def test_non_invalidated_generic_sqlalchemy_dbapi_error_is_not_transient() -> None:
    error = DBAPIError(
        "SELECT symbols",
        {},
        Exception("statement failed"),
        connection_invalidated=False,
    )

    assert is_transient_database_error(error) is False


def test_sqlalchemy_pool_disconnection_is_transient() -> None:
    assert is_transient_database_error(DisconnectionError("stale pool socket")) is True


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


def test_handle_message_waits_for_durable_accept_not_batch_publish() -> None:
    async def scenario() -> None:
        repository = Repository()
        storage = Storage()
        market_worker = worker(
            repository,
            Supervisor(),
            storage,
            Backfill(),
            max_buffer_events=1,
            batch_size=1,
        )
        message = json.dumps(
            {
                "stream": "btcusdt@aggTrade",
                "data": {
                    "e": "aggTrade",
                    "E": 1_753_099_200_010,
                    "s": "BTCUSDT",
                    "a": 42,
                    "p": "1",
                    "q": "2",
                    "f": 100,
                    "l": 102,
                    "T": 1_753_099_200_009,
                    "m": True,
                },
            }
        )

        await market_worker.handle_message(message)
        second = asyncio.create_task(market_worker.handle_message(message))
        await second
        assert len(storage.events) == 2
        assert storage.batches == []

        await market_worker.flush_events(force=True)
        await market_worker.flush_events(force=True)
        assert [len(batch) for batch in storage.batches] == [1, 1]

    asyncio.run(scenario())


def test_unrepresentable_decimal_is_rejected_before_durable_accept() -> None:
    async def scenario() -> None:
        storage = Storage()
        market_worker = worker(
            Repository(),
            Supervisor(),
            storage,
            Backfill(),
        )
        message = json.dumps(
            {
                "stream": "btcusdt@aggTrade",
                "data": {
                    "e": "aggTrade",
                    "E": 1_753_099_200_010,
                    "s": "BTCUSDT",
                    "a": 42,
                    "p": "1E-19",
                    "q": "2",
                    "f": 100,
                    "l": 102,
                    "T": 1_753_099_200_009,
                    "m": True,
                },
            }
        )

        with pytest.raises(ValueError, match=r"decimal128\(38, 18\)"):
            await market_worker.handle_message(message)

        assert storage.events == []

    asyncio.run(scenario())


def test_scientific_zero_is_arrow_safe_before_durable_accept() -> None:
    async def scenario() -> None:
        storage = Storage()
        market_worker = worker(
            Repository(),
            Supervisor(),
            storage,
            Backfill(),
        )
        message = json.dumps(
            {
                "stream": "btcusdt@bookTicker",
                "data": {
                    "e": "bookTicker",
                    "E": 1_753_099_200_011,
                    "T": 1_753_099_200_010,
                    "s": "BTCUSDT",
                    "u": 99,
                    "b": "1.24",
                    "B": "-0E+1000",
                    "a": "1.25",
                    "A": "2",
                },
            }
        )

        await market_worker.handle_message(message)

        assert len(storage.events) == 1
        assert storage.events[0].values["bid_quantity"] == "0"

    asyncio.run(scenario())


def test_low_volume_events_roll_together_at_the_fixed_flush_deadline() -> None:
    async def scenario() -> None:
        repository = Repository()
        supervisor = Supervisor()
        storage = Storage()
        market_worker = worker(
            repository,
            supervisor,
            storage,
            Backfill(),
            flush_interval=0.05,
        )
        task = asyncio.create_task(market_worker.run())
        while not supervisor.refreshes:
            await asyncio.sleep(0)
        base = {
            "e": "aggTrade",
            "s": "BTCUSDT",
            "p": "1",
            "q": "2",
            "m": True,
        }
        for identity in (41, 42):
            await market_worker.handle_message(
                json.dumps(
                    {
                        "stream": "btcusdt@aggTrade",
                        "data": {
                            **base,
                            "E": 1_753_099_200_000 + identity,
                            "a": identity,
                            "f": identity,
                            "l": identity,
                            "T": 1_753_099_200_000 + identity,
                        },
                    }
                )
            )

        await asyncio.sleep(0.005)
        assert storage.batches == []
        await asyncio.sleep(0.07)
        market_worker.request_shutdown()
        await task

        assert [len(batch) for batch in storage.batches] == [2]

    asyncio.run(scenario())


def test_stream_message_details_preserve_source_mode_after_batch_flush() -> None:
    class BlockingSocket(Socket):
        async def recv(self):
            await asyncio.Event().wait()

    async def scenario() -> None:
        repository = Repository()
        supervisor = Supervisor()
        group = next(
            group
            for group in group_streams(streams_for_symbols(("BTCUSDT",)))
            if any(stream.kind is StreamKind.AGG_TRADE for stream in group.streams)
        )
        supervisor.connections = (
            ManagedConnection(
                group,
                BlockingSocket(),
                ConnectionMode.PROXY,
                NOW,
                timedelta(hours=23, minutes=55),
            ),
        )
        market_worker = worker(repository, supervisor, Storage(), Backfill())
        await market_worker.run_cycle()
        message = json.dumps(
            {
                "stream": "btcusdt@aggTrade",
                "data": {
                    "e": "aggTrade",
                    "E": 1_753_099_200_010,
                    "s": "BTCUSDT",
                    "a": 42,
                    "p": "1",
                    "q": "2",
                    "f": 100,
                    "l": 102,
                    "T": 1_753_099_200_009,
                    "m": True,
                },
            }
        )

        await market_worker.handle_message(message)
        await market_worker.flush_events(force=True)

        assert repository.streams[-1].details == {
            "source_mode": "proxy",
            "dataset": "agg_trades",
        }
        await market_worker._stop_readers()
        await market_worker._stop_backfill()

    asyncio.run(scenario())


def test_connection_mode_change_is_persisted_while_stream_stays_connected() -> None:
    class BlockingSocket(Socket):
        async def recv(self):
            await asyncio.Event().wait()

    async def scenario() -> None:
        repository = Repository()
        supervisor = Supervisor()
        group = group_streams(streams_for_symbols(("BTCUSDT",)))[0]
        supervisor.connections = (
            ManagedConnection(
                group,
                BlockingSocket(),
                ConnectionMode.DIRECT,
                NOW,
                timedelta(hours=23, minutes=55),
            ),
        )
        market_worker = worker(repository, supervisor, Storage(), Backfill())
        await market_worker.run_cycle()
        supervisor.connections = (
            ManagedConnection(
                group,
                BlockingSocket(),
                ConnectionMode.PROXY,
                NOW + timedelta(seconds=1),
                timedelta(hours=23, minutes=55),
            ),
        )

        await market_worker.run_cycle()

        connected = [
            state
            for state in repository.streams
            if state.status == "connected" and state.stream_name in {
                stream.name for stream in group.streams
            }
        ]
        assert connected[-1].details["source_mode"] == "proxy"
        assert all(
            market_worker._stream_details[(stream.symbol, stream.name)]["source_mode"]
            == "proxy"
            for stream in group.streams
        )
        await market_worker._stop_readers()
        await market_worker._stop_backfill()

    asyncio.run(scenario())


def test_restart_restores_stream_anchor_and_records_worker_restart_gap() -> None:
    async def scenario() -> None:
        repository = Repository()
        group = group_streams(streams_for_symbols(("BTCUSDT",)))[0]
        repository.persisted_stream_states = tuple(
            StreamState(
                stream.symbol,
                stream.name,
                None,
                "connected",
                {"source_mode": "direct"},
                NOW,
            )
            for stream in group.streams
        )
        market_worker = worker(repository, Supervisor(), Storage(), Backfill())

        await market_worker._restore_stream_states()
        await market_worker.note_reconnect(group, NOW + timedelta(minutes=1))

        assert len(repository.gaps) == len(group.streams)
        assert {gap.reason for gap in repository.gaps.values()} == {"worker_restart"}
        assert all(gap.start_at == NOW for gap in repository.gaps.values())

    asyncio.run(scenario())


def test_blocking_backfill_does_not_block_live_cycle_or_shutdown() -> None:
    class BlockingBackfill:
        def __init__(self) -> None:
            self.started = asyncio.Event()
            self.cancelled = asyncio.Event()

        async def run_once(self, worker_id, lease_duration):
            self.started.set()
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                self.cancelled.set()
                raise

    async def scenario() -> None:
        backfill = BlockingBackfill()
        market_worker = worker(Repository(), Supervisor(), Storage(), backfill)

        await asyncio.wait_for(market_worker.run_cycle(), timeout=0.05)
        await backfill.started.wait()
        await market_worker._stop_backfill()

        assert backfill.cancelled.is_set()

    asyncio.run(scenario())


def test_transient_batch_checkpoint_failure_retries_without_losing_buffer() -> None:
    class FlakyRepository(Repository):
        def __init__(self) -> None:
            super().__init__()
            self.failures = 2

        async def commit_live_batch(self, result, states) -> None:
            if self.failures:
                self.failures -= 1
                raise OperationalError(
                    "INSERT live_data_partitions", {}, Exception("database unavailable")
                )
            await super().commit_live_batch(result, states)

    async def scenario() -> None:
        repository = FlakyRepository()
        supervisor = Supervisor()
        storage = Storage()
        delays = []

        async def sleep(delay: float) -> None:
            delays.append(delay)

        market_worker = worker(
            repository,
            supervisor,
            storage,
            Backfill(),
            sleeper=sleep,
            random_source=lambda: 0.5,
            flush_interval=0.001,
        )
        task = asyncio.create_task(market_worker.run())
        while not supervisor.refreshes:
            await asyncio.sleep(0)
        await market_worker.handle_message(
            json.dumps(
                {
                    "stream": "btcusdt@aggTrade",
                    "data": {
                        "e": "aggTrade",
                        "E": 1_753_099_200_010,
                        "s": "BTCUSDT",
                        "a": 42,
                        "p": "1",
                        "q": "2",
                        "f": 100,
                        "l": 102,
                        "T": 1_753_099_200_009,
                        "m": True,
                    },
                }
            )
        )
        for _ in range(100):
            if repository.stream_batches:
                break
            await asyncio.sleep(0)
        market_worker.request_shutdown()
        await task

        assert len(repository.stream_batches) == 1
        assert delays[:2] == [1.0, 2.0]
        assert len(storage.batches) == 1
        assert storage.publish_calls >= 3
        assert storage.acknowledged == ["batch-1"]

    asyncio.run(scenario())


def test_done_reader_is_removed_and_rebuilt_for_an_active_connection() -> None:
    class FailingSocket(Socket):
        def __init__(self) -> None:
            self.receives = 0

        async def recv(self):
            self.receives += 1
            raise ConnectionError("lost")

    class StickySupervisor(Supervisor):
        async def discard(self, connection, reason: str) -> None:
            return None

    async def scenario() -> None:
        repository = Repository()
        supervisor = StickySupervisor()
        socket_ = FailingSocket()
        group = group_streams(streams_for_symbols(("BTCUSDT",)))[0]
        supervisor.connections = (
            ManagedConnection(
                group,
                socket_,
                ConnectionMode.DIRECT,
                NOW,
                timedelta(hours=23, minutes=55),
            ),
        )

        async def sleep(_delay: float) -> None:
            return None

        market_worker = worker(
            repository, supervisor, Storage(), Backfill(), sleeper=sleep
        )
        await market_worker.run_cycle()
        for _ in range(20):
            if socket_.receives:
                break
            await asyncio.sleep(0)
        for _ in range(20):
            await market_worker.run_cycle()
            if socket_.receives >= 2:
                break
            await asyncio.sleep(0)

        assert socket_.receives >= 2
        await market_worker._stop_readers()
        await market_worker._stop_backfill()

    asyncio.run(scenario())


def test_cleanup_failures_do_not_mask_primary_worker_failure() -> None:
    class BrokenRepository(Repository):
        async def update_worker_heartbeat(self, heartbeat) -> None:
            if heartbeat.status == "stopped":
                raise RuntimeError("heartbeat cleanup failed")
            await super().update_worker_heartbeat(heartbeat)

    class BrokenSupervisor(Supervisor):
        async def refresh(self, groups, now) -> None:
            raise ValueError("primary cycle failure")

        async def shutdown(self) -> None:
            raise RuntimeError("socket cleanup failed")

    async def scenario() -> None:
        market_worker = worker(
            BrokenRepository(), BrokenSupervisor(), Storage(), Backfill()
        )

        with pytest.raises(ValueError, match="primary cycle failure"):
            await market_worker.run()

    asyncio.run(scenario())


def test_real_supervisor_rotation_lifecycle_records_all_stream_gaps() -> None:
    class BlockingSocket(Socket):
        async def recv(self):
            await asyncio.Event().wait()

    class Connector:
        async def __call__(self, uri, **kwargs):
            return BlockingSocket()

    async def scenario() -> None:
        repository = Repository()
        repository.active = (SymbolState("BTCUSDT", True, None, None, False),)
        supervisor = StreamConnectionSupervisor(
            RoutedConnectionFactory(
                Connector(),
                ConnectionPolicy(
                    proxy_mode="direct",
                    rotate_after=timedelta(hours=23, minutes=55),
                ),
            )
        )
        rotated_at = NOW + timedelta(hours=23, minutes=55)
        moments = iter(
            (
                NOW,
                rotated_at,
                rotated_at + timedelta(seconds=1),
                rotated_at + timedelta(seconds=1),
            )
        )
        market_worker = worker(
            repository,
            supervisor,
            Storage(),
            Backfill(),
            clock=lambda: next(moments),
        )

        await market_worker.run_cycle()
        await market_worker.run_cycle()

        assert {gap.dataset for gap in repository.gaps.values()} == {
            "kline_1m",
            "mark_price",
            "agg_trade",
            "best_bid_ask",
        }
        assert all(
            gap.reason == "planned_pre_24_hour_rotation"
            for gap in repository.gaps.values()
        )
        await market_worker._stop_readers()
        await market_worker._stop_backfill()
        await supervisor.shutdown()

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
