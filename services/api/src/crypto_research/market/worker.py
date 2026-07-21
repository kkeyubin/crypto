from __future__ import annotations

import asyncio
import inspect
import math
import random
import signal
import socket
import ssl
from collections.abc import Awaitable, Callable, Sequence
from contextlib import suppress
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import StrEnum
from ipaddress import ip_address
from typing import Any, Protocol
from urllib.parse import urlparse
from uuid import NAMESPACE_URL, uuid5

from crypto_research.contracts.manifest import DataType
from crypto_research.db.repositories import (
    GapRecord,
    StreamState,
    SymbolState,
    WorkerHeartbeat,
)
from crypto_research.market.binance.streams import (
    ParsedStreamEvent,
    StreamGroup,
    group_streams,
    parse_stream_message,
    streams_for_symbols,
)


class ConnectionMode(StrEnum):
    DIRECT = "direct"
    PROXY = "proxy"


class ConnectionFailureKind(StrEnum):
    TIMEOUT = "timeout"
    DNS = "dns"
    TLS = "tls"
    CONNECT = "connect"
    GEOGRAPHIC_RESTRICTION = "geographic_restriction"


@dataclass(frozen=True)
class DirectConnectionFailure:
    at: datetime
    kind: ConnectionFailureKind
    route: str
    error_type: str
    detail: str


@dataclass(frozen=True)
class ConnectionModeTransition:
    at: datetime
    route: str
    from_mode: ConnectionMode
    to_mode: ConnectionMode
    reason: str


@dataclass(frozen=True)
class BackoffPolicy:
    base_seconds: float = 1.0
    maximum_seconds: float = 60.0
    jitter_ratio: float = 0.2

    def __post_init__(self) -> None:
        if self.base_seconds <= 0 or self.maximum_seconds <= 0:
            raise ValueError("backoff durations must be positive")
        if not 0 <= self.jitter_ratio <= 1:
            raise ValueError("backoff jitter ratio must be between zero and one")

    def delay(self, attempt: int, *, random_fraction: float) -> float:
        if attempt < 0:
            raise ValueError("backoff attempt cannot be negative")
        if not 0 <= random_fraction <= 1:
            raise ValueError("random fraction must be between zero and one")
        if self.base_seconds >= self.maximum_seconds:
            effective_attempt = 0
        else:
            saturation_attempt = math.ceil(
                math.log2(self.maximum_seconds / self.base_seconds)
            )
            effective_attempt = min(attempt, saturation_attempt)
        bounded = self.base_seconds * (2**effective_attempt)
        jitter = (random_fraction * 2 - 1) * self.jitter_ratio
        return min(self.maximum_seconds, bounded * (1 + jitter))


@dataclass(frozen=True)
class ConnectionPolicy:
    proxy_mode: str
    proxy_url: str | None = None
    direct_probe_interval: timedelta = timedelta(minutes=5)
    rotate_after: timedelta = timedelta(hours=23, minutes=55)
    ping_interval_seconds: float = 20
    ping_timeout_seconds: float = 20

    def __post_init__(self) -> None:
        if self.proxy_mode not in {"auto", "direct", "proxy"}:
            raise ValueError("proxy mode must be auto, direct, or proxy")
        if self.proxy_mode == "proxy" and self.proxy_url is None:
            raise ValueError("proxy mode requires a proxy URL")
        if self.proxy_url is not None and not _is_loopback_proxy(self.proxy_url):
            raise ValueError("market WebSocket proxy must use a loopback address")
        if self.direct_probe_interval <= timedelta(0):
            raise ValueError("direct recovery probe interval must be positive")
        if not timedelta(0) < self.rotate_after < timedelta(hours=24):
            raise ValueError("planned rotation must occur before 24 hours")
        if self.ping_interval_seconds <= 0 or self.ping_timeout_seconds <= 0:
            raise ValueError("protocol ping durations must be positive")


class WebSocketConnection(Protocol):
    async def recv(self, decode: bool | None = None) -> str | bytes: ...

    async def ping(self) -> Awaitable[object]: ...

    async def close(self, code: int = 1000, reason: str = "") -> None: ...


class WebSocketConnector(Protocol):
    def __call__(self, uri: str, **kwargs: object) -> Awaitable[WebSocketConnection]: ...


FailureRecorder = Callable[
    [DirectConnectionFailure], Awaitable[None] | None
]
TransitionRecorder = Callable[
    [ConnectionModeTransition], Awaitable[None] | None
]


@dataclass(frozen=True)
class ManagedConnection:
    group: StreamGroup
    websocket: WebSocketConnection
    mode: ConnectionMode
    connected_at: datetime
    rotate_after: timedelta

    def rotation_due(self, now: datetime) -> bool:
        return now >= self.connected_at + self.rotate_after

    async def protocol_ping(self) -> None:
        pong_waiter = await self.websocket.ping()
        await pong_waiter

    async def close(self, reason: str) -> None:
        await self.websocket.close(code=1000, reason=reason)


@dataclass(frozen=True)
class ConnectionLifecycle:
    at: datetime
    group: StreamGroup
    state: str
    reason: str
    mode: ConnectionMode


class RoutedConnectionFactory:
    """Open only structured Binance route groups with explicit proxy behavior."""

    def __init__(
        self,
        connector: WebSocketConnector,
        policy: ConnectionPolicy,
        *,
        record_direct_failure: FailureRecorder | None = None,
        record_mode_transition: TransitionRecorder | None = None,
    ) -> None:
        if (
            policy.proxy_mode == "auto"
            and policy.proxy_url is not None
            and record_direct_failure is None
        ):
            raise ValueError("auto proxy fallback requires a direct failure recorder")
        self._connector = connector
        self._policy = policy
        self._record_direct_failure = record_direct_failure
        self._record_mode_transition = record_mode_transition
        self._using_proxy = policy.proxy_mode == "proxy"
        self._next_direct_probe_at: datetime | None = None

    @property
    def preferred_mode(self) -> ConnectionMode:
        if self._policy.proxy_mode == "proxy" or self._using_proxy:
            return ConnectionMode.PROXY
        return ConnectionMode.DIRECT

    def direct_probe_due(self, now: datetime) -> bool:
        return (
            self._policy.proxy_mode == "auto"
            and self._using_proxy
            and self._next_direct_probe_at is not None
            and now >= self._next_direct_probe_at
        )

    async def open(self, group: StreamGroup, now: datetime) -> ManagedConnection:
        if self._policy.proxy_mode == "direct":
            return await self._open(group, now, ConnectionMode.DIRECT)
        if self._policy.proxy_mode == "proxy":
            return await self._open(group, now, ConnectionMode.PROXY)
        if (
            self._using_proxy
            and self._next_direct_probe_at is not None
            and now < self._next_direct_probe_at
        ):
            return await self._open(group, now, ConnectionMode.PROXY)
        was_using_proxy = self._using_proxy
        try:
            opened = await self._open(group, now, ConnectionMode.DIRECT)
        except Exception as error:
            kind = classify_connection_failure(error)
            if kind is None or self._policy.proxy_url is None:
                raise
            failure = DirectConnectionFailure(
                at=now,
                kind=kind,
                route=group.route.value,
                error_type=type(error).__name__,
                detail=str(error),
            )
            await self._record(failure)
            opened = await self._open(group, now, ConnectionMode.PROXY)
            if not was_using_proxy:
                try:
                    await self._record_transition(
                        ConnectionModeTransition(
                            now,
                            group.route.value,
                            ConnectionMode.DIRECT,
                            ConnectionMode.PROXY,
                            kind.value,
                        )
                    )
                except Exception:
                    with suppress(Exception):
                        await opened.close("mode transition audit failed")
                    raise
            self._using_proxy = True
            self._next_direct_probe_at = now + self._policy.direct_probe_interval
            return opened
        if was_using_proxy:
            try:
                await self._record_transition(
                    ConnectionModeTransition(
                        now,
                        group.route.value,
                        ConnectionMode.PROXY,
                        ConnectionMode.DIRECT,
                        "direct_recovery_probe_succeeded",
                    )
                )
            except Exception:
                with suppress(Exception):
                    await opened.close("mode transition audit failed")
                self._next_direct_probe_at = (
                    now + self._policy.direct_probe_interval
                )
                raise
        self._using_proxy = False
        self._next_direct_probe_at = None
        return opened

    async def _open(
        self, group: StreamGroup, now: datetime, mode: ConnectionMode
    ) -> ManagedConnection:
        proxy = None if mode is ConnectionMode.DIRECT else self._policy.proxy_url
        websocket = await self._connector(
            group.uri,
            proxy=proxy,
            ping_interval=self._policy.ping_interval_seconds,
            ping_timeout=self._policy.ping_timeout_seconds,
        )
        return ManagedConnection(
            group=group,
            websocket=websocket,
            mode=mode,
            connected_at=now,
            rotate_after=self._policy.rotate_after,
        )

    async def _record(self, failure: DirectConnectionFailure) -> None:
        if self._record_direct_failure is None:
            raise RuntimeError("direct connection failure recorder is unavailable")
        result = self._record_direct_failure(failure)
        if inspect.isawaitable(result):
            await result

    async def _record_transition(self, transition: ConnectionModeTransition) -> None:
        if self._record_mode_transition is None:
            return
        result = self._record_mode_transition(transition)
        if inspect.isawaitable(result):
            await result


class StreamConnectionSupervisor:
    """Reconcile desired routed groups without accepting caller-provided URLs."""

    def __init__(self, factory: RoutedConnectionFactory) -> None:
        self._factory = factory
        self._connections: dict[tuple[str, ...], ManagedConnection] = {}
        self._shut_down = False
        self._lifecycle_events: list[ConnectionLifecycle] = []

    @property
    def connections(self) -> tuple[ManagedConnection, ...]:
        return tuple(self._connections.values())

    def drain_lifecycle_events(self) -> tuple[ConnectionLifecycle, ...]:
        events = tuple(self._lifecycle_events)
        self._lifecycle_events.clear()
        return events

    async def refresh(self, groups: Sequence[StreamGroup], now: datetime) -> None:
        if self._shut_down:
            raise RuntimeError("stream connection supervisor is shut down")
        desired = {_group_key(group): group for group in groups}
        reconnect_reasons: dict[tuple[str, ...], str] = {}
        disconnected_streams: dict[str, str] = {}
        for key, connection in tuple(self._connections.items()):
            if key not in desired:
                reason = "subscription_refresh"
                self._lifecycle_events.append(
                    ConnectionLifecycle(
                        now, connection.group, "disconnected", reason, connection.mode
                    )
                )
                disconnected_streams.update(
                    (stream.name, reason) for stream in connection.group.streams
                )
                with suppress(Exception):
                    await connection.close("subscription refresh")
                self._connections.pop(key, None)
            elif connection.rotation_due(now):
                reason = "planned_pre_24_hour_rotation"
                self._lifecycle_events.append(
                    ConnectionLifecycle(
                        now, connection.group, "disconnected", reason, connection.mode
                    )
                )
                reconnect_reasons[key] = reason
                with suppress(Exception):
                    await connection.close("planned pre-24-hour rotation")
                self._connections.pop(key, None)
        for key, group in desired.items():
            if key not in self._connections:
                opened = await self._factory.open(group, now)
                self._connections[key] = opened
                reason = reconnect_reasons.get(key)
                if reason is None:
                    reason = next(
                        (
                            disconnected_streams[stream.name]
                            for stream in group.streams
                            if stream.name in disconnected_streams
                        ),
                        None,
                    )
                if reason is not None:
                    self._lifecycle_events.append(
                        ConnectionLifecycle(
                            now, group, "connected", reason, opened.mode
                        )
                    )
        if self._factory.direct_probe_due(now):
            await self._probe_direct_recovery(now)
        if self._factory.preferred_mode is ConnectionMode.DIRECT:
            await self._migrate_proxy_connections(now)

    async def _probe_direct_recovery(self, now: datetime) -> None:
        candidate = next(
            (
                (key, connection)
                for key, connection in self._connections.items()
                if connection.mode is ConnectionMode.PROXY
            ),
            None,
        )
        if candidate is None:
            return
        key, current = candidate
        replacement = await self._factory.open(current.group, now)
        self._connections[key] = replacement
        with suppress(Exception):
            await current.close("direct recovery probe")

    async def _migrate_proxy_connections(self, now: datetime) -> None:
        for key, current in tuple(self._connections.items()):
            if current.mode is not ConnectionMode.PROXY:
                continue
            replacement = await self._factory.open(current.group, now)
            self._connections[key] = replacement
            with suppress(Exception):
                await current.close("direct recovery migration")

    async def shutdown(self) -> None:
        if self._shut_down:
            return
        self._shut_down = True
        errors: list[Exception] = []
        for connection in tuple(self._connections.values()):
            try:
                await connection.close("worker shutdown")
            except Exception as error:
                errors.append(error)
        self._connections.clear()
        if errors:
            raise ExceptionGroup("stream connection shutdown failed", errors)

    async def discard(self, connection: ManagedConnection, reason: str) -> None:
        key = _group_key(connection.group)
        if self._connections.get(key) is connection:
            try:
                await connection.close(reason)
            finally:
                self._connections.pop(key, None)


class WorkerRepository(Protocol):
    async def list_active_symbols(self) -> tuple[SymbolState, ...]: ...

    async def update_stream(self, state: StreamState) -> None: ...

    async def update_streams(self, states: Sequence[StreamState]) -> None: ...

    async def record_gap(self, gap: GapRecord) -> object: ...

    async def update_worker_heartbeat(self, heartbeat: WorkerHeartbeat) -> None: ...

    async def checkpoint(self) -> None: ...


class WorkerSupervisor(Protocol):
    @property
    def connections(self) -> tuple[ManagedConnection, ...]: ...

    async def refresh(self, groups: Sequence[StreamGroup], now: datetime) -> None: ...

    async def shutdown(self) -> None: ...

    async def discard(self, connection: ManagedConnection, reason: str) -> None: ...


class LiveEventStorage(Protocol):
    def acquire_writer(self, owner: str) -> object: ...

    def persist_batch(
        self, lease: object, events: Sequence[ParsedStreamEvent]
    ) -> object: ...


class BackfillLeaseRunner(Protocol):
    async def run_once(self, worker_id: str, lease_duration: timedelta) -> object: ...


class MarketWorker:
    """Coordinate active symbols, durable live capture, gaps, and backfill leases."""

    def __init__(
        self,
        repository: WorkerRepository,
        supervisor: WorkerSupervisor,
        storage: LiveEventStorage,
        *,
        backfill_runner: BackfillLeaseRunner,
        worker_id: str,
        clock: Callable[[], datetime],
        stale_after: timedelta,
        refresh_interval: float,
        lease_duration: timedelta,
        backoff_policy: BackoffPolicy | None = None,
        sleeper: Callable[[float], Awaitable[None]] = asyncio.sleep,
        random_source: Callable[[], float] = random.random,
        max_buffer_events: int = 4096,
        max_event_bytes: int = 1_048_576,
        batch_size: int = 256,
        flush_interval: float = 1.0,
        database_retry_limit: int = 5,
    ) -> None:
        if not worker_id:
            raise ValueError("worker id must not be empty")
        if stale_after <= timedelta(0):
            raise ValueError("stale threshold must be positive")
        if refresh_interval <= 0 or lease_duration <= timedelta(0):
            raise ValueError("worker intervals must be positive")
        if max_buffer_events <= 0 or max_event_bytes <= 0 or batch_size <= 0:
            raise ValueError("live buffer bounds must be positive")
        if batch_size > max_buffer_events:
            raise ValueError("live batch size cannot exceed buffer capacity")
        if flush_interval <= 0 or database_retry_limit < 0:
            raise ValueError("flush and retry settings are invalid")
        self._repository = repository
        self._supervisor = supervisor
        self._storage = storage
        self._backfill_runner = backfill_runner
        self._worker_id = worker_id
        self._clock = clock
        self._stale_after = stale_after
        self._refresh_interval = refresh_interval
        self._lease_duration = lease_duration
        self._backoff_policy = backoff_policy or BackoffPolicy()
        self._sleeper = sleeper
        self._random_source = random_source
        self._max_event_bytes = max_event_bytes
        self._batch_size = batch_size
        self._flush_interval = flush_interval
        self._database_retry_limit = database_retry_limit
        self._stop = asyncio.Event()
        self._buffer: asyncio.Queue[ParsedStreamEvent] = asyncio.Queue(
            maxsize=max_buffer_events
        )
        self._buffer_changed = asyncio.Event()
        self._flush_lock = asyncio.Lock()
        self._pending_flush: tuple[ParsedStreamEvent, ...] | None = None
        self._writer_lease: object | None = None
        self._flush_task: asyncio.Task[None] | None = None
        self._flush_failure: BaseException | None = None
        self._backfill_task: asyncio.Task[object] | None = None
        self._last_events: dict[tuple[str, str], datetime] = {}
        self._connected_at: dict[tuple[str, str], datetime] = {}
        self._stream_status: dict[tuple[str, str], str] = {}
        self._stream_details: dict[tuple[str, str], dict[str, Any]] = {}
        self._disconnects: dict[tuple[str, str], datetime] = {}
        self._disconnect_reasons: dict[tuple[str, str], str] = {}
        self._active_symbols: tuple[str, ...] = ()
        self._readers: dict[int, asyncio.Task[None]] = {}
        self._reconnect_attempts: dict[tuple[str, ...], int] = {}

    def request_shutdown(self) -> None:
        self._stop.set()

    async def run(self) -> None:
        connection_attempt = 0
        primary_error: BaseException | None = None
        try:
            await self._ensure_writer_lease()
            self._flush_task = asyncio.create_task(self._flush_loop())
            while not self._stop.is_set():
                if self._flush_failure is not None:
                    raise self._flush_failure
                try:
                    await self.run_cycle()
                except Exception as error:
                    failure_kind = classify_connection_failure(error)
                    if failure_kind is None:
                        raise
                    now = self._clock()
                    await self._repository.update_worker_heartbeat(
                        WorkerHeartbeat(
                            self._worker_id,
                            "degraded",
                            now,
                            {
                                "active_symbols": len(self._active_symbols),
                                "reason": failure_kind.value,
                                "error_type": type(error).__name__,
                            },
                        )
                    )
                    await self._repository.checkpoint()
                    delay = self._backoff_policy.delay(
                        connection_attempt,
                        random_fraction=self._random_source(),
                    )
                    connection_attempt += 1
                    await self._sleep_until_retry_or_stop(delay)
                    continue
                connection_attempt = 0
                with suppress(TimeoutError):
                    await asyncio.wait_for(
                        self._stop.wait(), timeout=self._refresh_interval
                    )
        except BaseException as error:
            primary_error = error
            raise
        finally:
            self._stop.set()
            cleanup_errors: list[BaseException] = []
            for cleanup in (
                self._stop_readers,
                self._stop_backfill,
                self._stop_flusher,
                self._supervisor.shutdown,
                self._record_stopped,
            ):
                try:
                    await cleanup()
                except BaseException as error:
                    cleanup_errors.append(error)
            if self._writer_lease is not None:
                try:
                    await asyncio.to_thread(self._writer_lease.close)  # type: ignore[attr-defined]
                except BaseException as error:
                    cleanup_errors.append(error)
                self._writer_lease = None
            if primary_error is None and cleanup_errors:
                raise BaseExceptionGroup("market worker cleanup failed", cleanup_errors)

    async def run_cycle(self) -> None:
        now = self._clock()
        active = tuple(
            symbol for symbol in await self._repository.list_active_symbols() if symbol.enabled
        )
        self._active_symbols = tuple(symbol.symbol for symbol in active)
        desired_streams = streams_for_symbols(self._active_symbols)
        desired_keys = {(stream.symbol, stream.name) for stream in desired_streams}
        for key in tuple(self._stream_status):
            if key not in desired_keys:
                await self._set_stream(
                    key[0], key[1], "disconnected", self._last_events.get(key), {}
                )
                del self._stream_status[key]
                self._last_events.pop(key, None)
                self._connected_at.pop(key, None)
                self._disconnects.pop(key, None)
                self._disconnect_reasons.pop(key, None)
                self._stream_details.pop(key, None)
        for stream in desired_streams:
            key = (stream.symbol, stream.name)
            if key not in self._stream_status:
                await self._set_stream(stream.symbol, stream.name, "connecting", None, {})
        groups = group_streams(desired_streams)
        await self._supervisor.refresh(groups, now)
        drain_lifecycle = getattr(self._supervisor, "drain_lifecycle_events", None)
        if drain_lifecycle is not None:
            for lifecycle in drain_lifecycle():
                if lifecycle.state == "disconnected":
                    await self.note_disconnect(
                        lifecycle,
                        lifecycle.at,
                        reason=lifecycle.reason,
                        only_keys=desired_keys,
                    )
                elif lifecycle.state == "connected":
                    await self.note_reconnect(lifecycle.group, self._clock())
        for connection in self._supervisor.connections:
            if any(
                (stream.symbol, stream.name) in self._disconnects
                for stream in connection.group.streams
            ):
                await self.note_reconnect(connection.group, now)
            for stream in connection.group.streams:
                key = (stream.symbol, stream.name)
                self._connected_at.setdefault(key, connection.connected_at)
                if self._stream_status.get(key) != "connected":
                    await self._set_stream(
                        stream.symbol,
                        stream.name,
                        "connected",
                        self._last_events.get(key),
                        {"source_mode": connection.mode.value},
                    )
        await self._sync_readers()
        self._schedule_backfill()
        await self.update_stale_states(now)
        await self._repository.update_worker_heartbeat(
            WorkerHeartbeat(
                self._worker_id,
                "running",
                now,
                {
                    "active_symbols": len(self._active_symbols),
                    "stream_groups": len(groups),
                },
            )
        )
        await self._repository.checkpoint()

    async def _sync_readers(self) -> None:
        active = {id(connection): connection for connection in self._supervisor.connections}
        stale = [
            key
            for key, task in self._readers.items()
            if key not in active or task.done()
        ]
        for key in stale:
            task = self._readers.pop(key)
            if not task.done():
                task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        for key, connection in active.items():
            if key not in self._readers:
                self._readers[key] = asyncio.create_task(self._consume(connection))

    async def _consume(self, connection: ManagedConnection) -> None:
        try:
            while not self._stop.is_set():
                message = await connection.websocket.recv()
                await self.handle_message(message)
        except asyncio.CancelledError:
            raise
        except Exception:
            disconnected_at = self._clock()
            await self._retry_database(
                lambda: self.note_disconnect(connection, disconnected_at)
            )
            group_key = _group_key(connection.group)
            attempt = self._reconnect_attempts.get(group_key, 0)
            delay = self._backoff_policy.delay(
                attempt, random_fraction=self._random_source()
            )
            self._reconnect_attempts[group_key] = attempt + 1
            await self._sleep_until_retry_or_stop(delay)
            if not self._stop.is_set():
                with suppress(Exception):
                    await self._supervisor.discard(connection, "connection lost")

    async def _stop_readers(self) -> None:
        tasks = tuple(self._readers.values())
        self._readers.clear()
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _sleep_until_retry_or_stop(self, delay: float) -> None:
        sleep_task = asyncio.create_task(self._sleeper(delay))
        stop_task = asyncio.create_task(self._stop.wait())
        try:
            await asyncio.wait(
                {sleep_task, stop_task}, return_when=asyncio.FIRST_COMPLETED
            )
        finally:
            for task in (sleep_task, stop_task):
                if not task.done():
                    task.cancel()
            await asyncio.gather(sleep_task, stop_task, return_exceptions=True)

    async def handle_message(self, message: str | bytes) -> ParsedStreamEvent:
        if len(message) > self._max_event_bytes:
            raise ValueError("live stream message exceeds the configured byte bound")
        receive_time = self._clock()
        event = parse_stream_message(message, receive_time)
        await self._buffer.put(event)
        self._buffer_changed.set()
        return event

    async def flush_events(self, *, force: bool = False) -> bool:
        """Publish one queued batch, then commit its stream states together."""
        async with self._flush_lock:
            await self._ensure_writer_lease()
            if self._pending_flush is None:
                if self._buffer.empty():
                    return False
                events: list[ParsedStreamEvent] = []
                limit = self._batch_size
                while len(events) < limit:
                    try:
                        events.append(self._buffer.get_nowait())
                    except asyncio.QueueEmpty:
                        break
                self._pending_flush = tuple(events)
            batch = self._pending_flush
            assert batch
            await asyncio.to_thread(
                self._storage.persist_batch, self._writer_lease, batch
            )
            latest: dict[tuple[str, str], ParsedStreamEvent] = {}
            for event in batch:
                latest[(event.symbol, event.stream.name)] = event
            states = []
            for key, event in latest.items():
                details = self._details_for_status(
                    key, "connected", {"dataset": event.dataset.value}
                )
                states.append(
                    StreamState(
                        event.symbol,
                        event.stream.name,
                        event.receive_time,
                        "connected",
                        details,
                    )
                )
            await self._repository.update_streams(tuple(states))
            for state in states:
                key = (state.symbol, state.stream_name)
                assert state.last_event_at is not None
                self._connected_at.setdefault(key, state.last_event_at)
                self._last_events[key] = state.last_event_at
                self._stream_status[key] = state.status
                self._stream_details[key] = dict(state.details or {})
            for _ in batch:
                self._buffer.task_done()
            self._pending_flush = None
            if self._buffer.empty():
                self._buffer_changed.clear()
            return True

    async def _ensure_writer_lease(self) -> None:
        if self._writer_lease is None:
            self._writer_lease = await asyncio.to_thread(
                self._storage.acquire_writer, self._worker_id
            )

    async def _flush_loop(self) -> None:
        attempt = 0
        while not self._stop.is_set() or not self._buffer.empty() or self._pending_flush:
            if self._pending_flush is None and self._buffer.empty():
                self._buffer_changed.clear()
                if self._stop.is_set():
                    break
                await self._buffer_changed.wait()
                continue
            if (
                self._pending_flush is None
                and self._buffer.qsize() < self._batch_size
                and not self._stop.is_set()
            ):
                loop = asyncio.get_running_loop()
                deadline = loop.time() + self._flush_interval
                while (
                    self._buffer.qsize() < self._batch_size
                    and not self._stop.is_set()
                ):
                    remaining = deadline - loop.time()
                    if remaining <= 0:
                        break
                    self._buffer_changed.clear()
                    with suppress(TimeoutError):
                        await asyncio.wait_for(
                            self._buffer_changed.wait(), timeout=remaining
                        )
            try:
                flushed = await self.flush_events(force=self._stop.is_set())
            except asyncio.CancelledError:
                raise
            except Exception as error:
                if attempt >= self._database_retry_limit:
                    self._flush_failure = error
                    return
                delay = self._backoff_policy.delay(
                    attempt, random_fraction=self._random_source()
                )
                attempt += 1
                await self._sleep_until_retry_or_stop(delay)
                continue
            attempt = 0
            if not flushed:
                self._buffer_changed.clear()

    async def _stop_flusher(self) -> None:
        self._buffer_changed.set()
        if self._flush_task is None:
            while await self.flush_events(force=True):
                pass
            return
        await self._flush_task
        self._flush_task = None
        if self._flush_failure is not None:
            raise self._flush_failure

    def _schedule_backfill(self) -> None:
        if self._backfill_task is not None and not self._backfill_task.done():
            return
        if self._backfill_task is not None:
            with suppress(asyncio.CancelledError, Exception):
                self._backfill_task.result()
        self._backfill_task = asyncio.create_task(
            self._backfill_runner.run_once(self._worker_id, self._lease_duration)
        )

    async def _stop_backfill(self) -> None:
        if self._backfill_task is None:
            return
        self._backfill_task.cancel()
        await asyncio.gather(self._backfill_task, return_exceptions=True)
        self._backfill_task = None

    async def _record_stopped(self) -> None:
        now = self._clock()
        await self._repository.update_worker_heartbeat(
            WorkerHeartbeat(
                self._worker_id,
                "stopped",
                now,
                {"active_symbols": len(self._active_symbols)},
            )
        )
        await self._repository.checkpoint()

    async def _retry_database(
        self, operation: Callable[[], Awaitable[object]]
    ) -> object:
        attempt = 0
        while True:
            try:
                return await operation()
            except asyncio.CancelledError:
                raise
            except Exception:
                if attempt >= self._database_retry_limit:
                    raise
                delay = self._backoff_policy.delay(
                    attempt, random_fraction=self._random_source()
                )
                attempt += 1
                await self._sleep_until_retry_or_stop(delay)

    async def update_stale_states(self, now: datetime) -> None:
        for (symbol, stream_name), connected_at in tuple(self._connected_at.items()):
            last_event = self._last_events.get((symbol, stream_name))
            freshness_origin = last_event or connected_at
            if (
                now - freshness_origin > self._stale_after
                and self._stream_status.get((symbol, stream_name)) != "degraded"
            ):
                await self._set_stream(
                    symbol,
                    stream_name,
                    "degraded",
                    last_event,
                    {"reason": "stale_live_data"},
                )

    async def note_disconnect(
        self,
        connection: ManagedConnection | ConnectionLifecycle,
        disconnected_at: datetime,
        *,
        reason: str = "source_unknown_disconnect",
        only_keys: set[tuple[str, str]] | None = None,
    ) -> None:
        for stream in connection.group.streams:
            key = (stream.symbol, stream.name)
            if only_keys is not None and key not in only_keys:
                continue
            self._disconnects.setdefault(key, disconnected_at)
            self._disconnect_reasons.setdefault(key, reason)
            await self._set_stream(
                stream.symbol,
                stream.name,
                "disconnected",
                self._last_events.get(key),
                {
                    "source_mode": connection.mode.value,
                    "disconnected_at": disconnected_at.isoformat(),
                },
            )
        await self._repository.checkpoint()

    async def note_reconnect(self, group: StreamGroup, reconnected_at: datetime) -> None:
        self._reconnect_attempts.pop(_group_key(group), None)
        for stream in group.streams:
            key = (stream.symbol, stream.name)
            self._connected_at[key] = reconnected_at
            disconnected_at = self._disconnects.pop(key, None)
            reason = self._disconnect_reasons.pop(
                key, "source_unknown_disconnect"
            )
            if disconnected_at is not None and reconnected_at > disconnected_at:
                identity = "|".join(
                    (
                        stream.symbol,
                        stream.name,
                        disconnected_at.isoformat(),
                        reconnected_at.isoformat(),
                        reason,
                    )
                )
                await self._repository.record_gap(
                    GapRecord(
                        id=str(uuid5(NAMESPACE_URL, identity)),
                        symbol=stream.symbol,
                        dataset=_stream_dataset(stream.kind.value).value,
                        start_at=disconnected_at,
                        end_at=reconnected_at,
                        reason=reason,
                        details={"stream_name": stream.name},
                    )
                )
            await self._set_stream(
                stream.symbol,
                stream.name,
                "connected",
                self._last_events.get(key),
                {},
            )
        await self._repository.checkpoint()

    async def _set_stream(
        self,
        symbol: str,
        stream_name: str,
        status: str,
        last_event_at: datetime | None,
        details: dict[str, Any],
    ) -> None:
        await self._repository.update_stream(
            StreamState(
                symbol,
                stream_name,
                last_event_at,
                status,
                self._details_for_status(
                    (symbol, stream_name), status, details
                ),
            )
        )
        self._stream_status[(symbol, stream_name)] = status
        self._stream_details[(symbol, stream_name)] = self._details_for_status(
            (symbol, stream_name), status, details
        )

    def _details_for_status(
        self,
        key: tuple[str, str],
        status: str,
        details: dict[str, Any],
    ) -> dict[str, Any]:
        merged = {**self._stream_details.get(key, {}), **details}
        if status in {"connecting", "connected"}:
            merged.pop("disconnected_at", None)
            merged.pop("reason", None)
        return merged


def classify_connection_failure(error: Exception) -> ConnectionFailureKind | None:
    if isinstance(error, TimeoutError):
        return ConnectionFailureKind.TIMEOUT
    if isinstance(error, socket.gaierror):
        return ConnectionFailureKind.DNS
    if isinstance(error, ssl.SSLError):
        return ConnectionFailureKind.TLS
    status_code = getattr(error, "status_code", None)
    response = getattr(error, "response", None)
    if status_code is None and response is not None:
        status_code = getattr(response, "status_code", None)
    if status_code == 451:
        return ConnectionFailureKind.GEOGRAPHIC_RESTRICTION
    if isinstance(error, (ConnectionError, OSError)):
        return ConnectionFailureKind.CONNECT
    return None


def install_sigterm_handler(
    loop: asyncio.AbstractEventLoop, stop_event: asyncio.Event
) -> None:
    loop.add_signal_handler(signal.SIGTERM, stop_event.set)


def _group_key(group: StreamGroup) -> tuple[str, ...]:
    return (group.route.value, *(stream.name for stream in group.streams))


def _stream_dataset(kind: str) -> DataType:
    return {
        "agg_trade": DataType.AGG_TRADE,
        "book_ticker": DataType.BEST_BID_ASK,
        "kline": DataType.KLINE_1M,
        "mark_price": DataType.MARK_PRICE,
    }[kind]


def _is_loopback_proxy(value: str) -> bool:
    parsed = urlparse(value)
    if parsed.scheme not in {"http", "https", "socks5", "socks5h"}:
        return False
    hostname = parsed.hostname
    if hostname is None:
        return False
    if hostname.lower() == "localhost":
        return True
    try:
        return ip_address(hostname).is_loopback
    except ValueError:
        return False
