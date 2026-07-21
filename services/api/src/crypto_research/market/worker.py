from __future__ import annotations

import asyncio
import inspect
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

from crypto_research.db.repositories import (
    GapRecord,
    StreamState,
    SymbolState,
    WorkerHeartbeat,
)
from crypto_research.market.binance.streams import (
    LiveDataset,
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
        bounded = min(self.maximum_seconds, self.base_seconds * (2**attempt))
        jitter = (random_fraction * 2 - 1) * self.jitter_ratio
        return bounded * (1 + jitter)


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
            self._using_proxy = True
            self._next_direct_probe_at = now + self._policy.direct_probe_interval
            if not was_using_proxy:
                await self._record_transition(
                    ConnectionModeTransition(
                        now,
                        group.route.value,
                        ConnectionMode.DIRECT,
                        ConnectionMode.PROXY,
                        kind.value,
                    )
                )
            return opened
        self._using_proxy = False
        self._next_direct_probe_at = None
        if was_using_proxy:
            await self._record_transition(
                ConnectionModeTransition(
                    now,
                    group.route.value,
                    ConnectionMode.PROXY,
                    ConnectionMode.DIRECT,
                    "direct_recovery_probe_succeeded",
                )
            )
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

    @property
    def connections(self) -> tuple[ManagedConnection, ...]:
        return tuple(self._connections.values())

    async def refresh(self, groups: Sequence[StreamGroup], now: datetime) -> None:
        if self._shut_down:
            raise RuntimeError("stream connection supervisor is shut down")
        desired = {_group_key(group): group for group in groups}
        for key, connection in tuple(self._connections.items()):
            if key not in desired:
                await connection.close("subscription refresh")
                del self._connections[key]
            elif connection.rotation_due(now):
                await connection.close("planned pre-24-hour rotation")
                del self._connections[key]
        for key, group in desired.items():
            if key not in self._connections:
                self._connections[key] = await self._factory.open(group, now)
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
        await current.close("direct recovery probe")
        self._connections[key] = replacement

    async def _migrate_proxy_connections(self, now: datetime) -> None:
        for key, current in tuple(self._connections.items()):
            if current.mode is not ConnectionMode.PROXY:
                continue
            replacement = await self._factory.open(current.group, now)
            await current.close("direct recovery migration")
            self._connections[key] = replacement

    async def shutdown(self) -> None:
        if self._shut_down:
            return
        self._shut_down = True
        for connection in tuple(self._connections.values()):
            await connection.close("worker shutdown")
        self._connections.clear()

    async def discard(self, connection: ManagedConnection, reason: str) -> None:
        key = _group_key(connection.group)
        if self._connections.get(key) is connection:
            await connection.close(reason)
            del self._connections[key]


class WorkerRepository(Protocol):
    async def list_active_symbols(self) -> tuple[SymbolState, ...]: ...

    async def update_stream(self, state: StreamState) -> None: ...

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
    def persist(self, event: ParsedStreamEvent) -> object: ...


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
    ) -> None:
        if not worker_id:
            raise ValueError("worker id must not be empty")
        if stale_after <= timedelta(0):
            raise ValueError("stale threshold must be positive")
        if refresh_interval <= 0 or lease_duration <= timedelta(0):
            raise ValueError("worker intervals must be positive")
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
        self._stop = asyncio.Event()
        self._last_events: dict[tuple[str, str], datetime] = {}
        self._connected_at: dict[tuple[str, str], datetime] = {}
        self._stream_status: dict[tuple[str, str], str] = {}
        self._disconnects: dict[tuple[str, str], datetime] = {}
        self._active_symbols: tuple[str, ...] = ()
        self._readers: dict[int, asyncio.Task[None]] = {}
        self._reconnect_attempts: dict[tuple[str, ...], int] = {}

    def request_shutdown(self) -> None:
        self._stop.set()

    async def run(self) -> None:
        connection_attempt = 0
        try:
            while not self._stop.is_set():
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
        finally:
            await self._stop_readers()
            await self._supervisor.shutdown()
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
        for stream in desired_streams:
            key = (stream.symbol, stream.name)
            if key not in self._stream_status:
                await self._set_stream(stream.symbol, stream.name, "connecting", None, {})
        groups = group_streams(desired_streams)
        await self._supervisor.refresh(groups, now)
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
        await self._backfill_runner.run_once(self._worker_id, self._lease_duration)
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
        stale = [key for key in self._readers if key not in active]
        for key in stale:
            task = self._readers.pop(key)
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
            await self.note_disconnect(connection, disconnected_at)
            group_key = _group_key(connection.group)
            attempt = self._reconnect_attempts.get(group_key, 0)
            delay = self._backoff_policy.delay(
                attempt, random_fraction=self._random_source()
            )
            self._reconnect_attempts[group_key] = attempt + 1
            await self._sleep_until_retry_or_stop(delay)
            if not self._stop.is_set():
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
        receive_time = self._clock()
        event = parse_stream_message(message, receive_time)
        await asyncio.to_thread(self._storage.persist, event)
        key = (event.symbol, event.stream.name)
        self._connected_at.setdefault(key, receive_time)
        self._last_events[key] = receive_time
        await self._set_stream(
            event.symbol,
            event.stream.name,
            "connected",
            receive_time,
            {"dataset": event.dataset.value},
        )
        await self._repository.checkpoint()
        return event

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
        self, connection: ManagedConnection, disconnected_at: datetime
    ) -> None:
        for stream in connection.group.streams:
            key = (stream.symbol, stream.name)
            self._disconnects.setdefault(key, disconnected_at)
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
            if disconnected_at is not None and reconnected_at > disconnected_at:
                identity = "|".join(
                    (
                        stream.symbol,
                        stream.name,
                        disconnected_at.isoformat(),
                        reconnected_at.isoformat(),
                    )
                )
                await self._repository.record_gap(
                    GapRecord(
                        id=str(uuid5(NAMESPACE_URL, identity)),
                        symbol=stream.symbol,
                        dataset=_stream_dataset(stream.kind.value).value,
                        start_at=disconnected_at,
                        end_at=reconnected_at,
                        reason="source_unknown_disconnect",
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
            StreamState(symbol, stream_name, last_event_at, status, details)
        )
        self._stream_status[(symbol, stream_name)] = status


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


def _stream_dataset(kind: str) -> LiveDataset:
    return {
        "agg_trade": LiveDataset.AGG_TRADES,
        "book_ticker": LiveDataset.BOOK_TICKER,
        "kline": LiveDataset.KLINES,
        "mark_price": LiveDataset.MARK_PRICE,
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
