import asyncio
import socket
from datetime import UTC, datetime, timedelta

import pytest

from crypto_research.market.binance.streams import group_streams, streams_for_symbols
from crypto_research.market.worker import (
    BackoffPolicy,
    ConnectionFailureKind,
    ConnectionMode,
    ConnectionPolicy,
    RoutedConnectionFactory,
    StreamConnectionSupervisor,
    classify_connection_failure,
)

NOW = datetime(2026, 7, 21, 12, tzinfo=UTC)
PROXY = "http://127.0.0.1:17891"


class FakeSocket:
    def __init__(self) -> None:
        self.ping_count = 0
        self.closes: list[tuple[int, str]] = []

    async def ping(self):
        self.ping_count += 1
        future = asyncio.get_running_loop().create_future()
        future.set_result(None)
        return future

    async def close(self, code: int = 1000, reason: str = "") -> None:
        self.closes.append((code, reason))


class FakeConnector:
    def __init__(self, outcomes: list[object] | None = None) -> None:
        self.outcomes = list(outcomes or [])
        self.calls: list[tuple[str, dict[str, object]]] = []
        self.before_call = None

    async def __call__(self, uri: str, **kwargs: object):
        if self.before_call is not None:
            self.before_call(uri, kwargs)
        self.calls.append((uri, kwargs))
        if self.outcomes:
            outcome = self.outcomes.pop(0)
            if isinstance(outcome, BaseException):
                raise outcome
            return outcome
        return FakeSocket()


def public_group(symbol: str = "BTCUSDT"):
    return group_streams(streams_for_symbols((symbol,)))[0]


def test_auto_records_classified_direct_failure_before_proxy_fallback() -> None:
    async def scenario() -> None:
        proxy_socket = FakeSocket()
        connector = FakeConnector([TimeoutError("handshake timed out"), proxy_socket])
        failures = []
        transitions = []

        async def record(failure) -> None:
            failures.append(failure)
            assert len(connector.calls) == 1

        factory = RoutedConnectionFactory(
            connector,
            ConnectionPolicy(proxy_mode="auto", proxy_url=PROXY),
            record_direct_failure=record,
            record_mode_transition=transitions.append,
        )

        opened = await factory.open(public_group(), NOW)

        assert opened.mode is ConnectionMode.PROXY
        assert [call[1]["proxy"] for call in connector.calls] == [None, PROXY]
        assert failures[0].kind is ConnectionFailureKind.TIMEOUT
        assert failures[0].at == NOW
        assert transitions[0].from_mode is ConnectionMode.DIRECT
        assert transitions[0].to_mode is ConnectionMode.PROXY
        assert all(call[1]["ping_interval"] == 20 for call in connector.calls)
        assert all(call[1]["ping_timeout"] == 20 for call in connector.calls)

    asyncio.run(scenario())


def test_direct_success_never_touches_proxy() -> None:
    async def scenario() -> None:
        socket_ = FakeSocket()
        connector = FakeConnector([socket_])
        failures = []
        factory = RoutedConnectionFactory(
            connector,
            ConnectionPolicy(proxy_mode="auto", proxy_url=PROXY),
            record_direct_failure=failures.append,
        )

        opened = await factory.open(public_group(), NOW)

        assert opened.mode is ConnectionMode.DIRECT
        assert len(connector.calls) == 1
        assert connector.calls[0][1]["proxy"] is None
        assert failures == []

    asyncio.run(scenario())


def test_proxy_mode_periodically_probes_and_recovers_direct() -> None:
    async def scenario() -> None:
        sockets = [FakeSocket(), FakeSocket(), FakeSocket(), FakeSocket()]
        connector = FakeConnector(
            [TimeoutError("direct blocked"), sockets[0], sockets[1], sockets[2]]
        )
        failures = []
        transitions = []
        factory = RoutedConnectionFactory(
            connector,
            ConnectionPolicy(
                proxy_mode="auto",
                proxy_url=PROXY,
                direct_probe_interval=timedelta(minutes=5),
            ),
            record_direct_failure=failures.append,
            record_mode_transition=transitions.append,
        )

        first = await factory.open(public_group(), NOW)
        before_probe = await factory.open(public_group("ETHUSDT"), NOW + timedelta(minutes=4))
        recovered = await factory.open(
            public_group("PEPEUSDT"), NOW + timedelta(minutes=5)
        )

        assert [first.mode, before_probe.mode, recovered.mode] == [
            ConnectionMode.PROXY,
            ConnectionMode.PROXY,
            ConnectionMode.DIRECT,
        ]
        assert [call[1]["proxy"] for call in connector.calls] == [
            None,
            PROXY,
            PROXY,
            None,
        ]
        assert len(failures) == 1
        assert [transition.to_mode for transition in transitions] == [
            ConnectionMode.PROXY,
            ConnectionMode.DIRECT,
        ]

    asyncio.run(scenario())


@pytest.mark.parametrize(
    ("error", "expected"),
    [
        (TimeoutError(), ConnectionFailureKind.TIMEOUT),
        (socket.gaierror(), ConnectionFailureKind.DNS),
        (ConnectionRefusedError(), ConnectionFailureKind.CONNECT),
    ],
)
def test_direct_failure_classification(error: Exception, expected) -> None:
    assert classify_connection_failure(error) is expected


def test_unclassified_direct_failure_does_not_unlock_proxy() -> None:
    async def scenario() -> None:
        connector = FakeConnector([RuntimeError("application bug"), FakeSocket()])
        factory = RoutedConnectionFactory(
            connector,
            ConnectionPolicy(proxy_mode="auto", proxy_url=PROXY),
            record_direct_failure=lambda _failure: None,
        )

        with pytest.raises(RuntimeError, match="application bug"):
            await factory.open(public_group(), NOW)
        assert len(connector.calls) == 1

    asyncio.run(scenario())


def test_backoff_is_exponential_bounded_and_jittered() -> None:
    policy = BackoffPolicy(base_seconds=1, maximum_seconds=30, jitter_ratio=0.2)

    assert policy.delay(0, random_fraction=0.0) == pytest.approx(0.8)
    assert policy.delay(3, random_fraction=0.5) == pytest.approx(8.0)
    assert policy.delay(10, random_fraction=1.0) == pytest.approx(36.0)


def test_protocol_ping_rotation_subscription_refresh_and_shutdown() -> None:
    async def scenario() -> None:
        sockets = [FakeSocket(), FakeSocket(), FakeSocket()]
        connector = FakeConnector(sockets.copy())
        factory = RoutedConnectionFactory(
            connector,
            ConnectionPolicy(proxy_mode="direct"),
        )
        supervisor = StreamConnectionSupervisor(factory)
        btc = public_group("BTCUSDT")
        eth = public_group("ETHUSDT")

        await supervisor.refresh((btc,), NOW)
        opened = supervisor.connections[0]
        await opened.protocol_ping()
        assert sockets[0].ping_count == 1
        assert not opened.rotation_due(NOW + timedelta(hours=23, minutes=54))

        await supervisor.refresh((btc,), NOW + timedelta(hours=23, minutes=55))
        assert sockets[0].closes == [(1000, "planned pre-24-hour rotation")]

        await supervisor.refresh((eth,), NOW + timedelta(hours=23, minutes=56))
        assert sockets[1].closes == [(1000, "subscription refresh")]
        assert supervisor.connections[0].group == eth

        await supervisor.shutdown()
        assert sockets[2].closes == [(1000, "worker shutdown")]
        assert supervisor.connections == ()
        with pytest.raises(RuntimeError, match="shut down"):
            await supervisor.refresh((btc,), NOW + timedelta(days=1))

    asyncio.run(scenario())


def test_supervisor_periodically_probes_existing_proxy_and_migrates_groups() -> None:
    async def scenario() -> None:
        sockets = [FakeSocket(), FakeSocket(), FakeSocket(), FakeSocket()]
        connector = FakeConnector(
            [TimeoutError("direct blocked"), *sockets]
        )
        transitions = []
        factory = RoutedConnectionFactory(
            connector,
            ConnectionPolicy(
                proxy_mode="auto",
                proxy_url=PROXY,
                direct_probe_interval=timedelta(minutes=5),
            ),
            record_direct_failure=lambda _failure: None,
            record_mode_transition=transitions.append,
        )
        supervisor = StreamConnectionSupervisor(factory)
        groups = (public_group("BTCUSDT"), public_group("ETHUSDT"))

        await supervisor.refresh(groups, NOW)
        await supervisor.refresh(groups, NOW + timedelta(minutes=4))
        assert len(connector.calls) == 3

        await supervisor.refresh(groups, NOW + timedelta(minutes=5))

        assert [connection.mode for connection in supervisor.connections] == [
            ConnectionMode.DIRECT,
            ConnectionMode.DIRECT,
        ]
        assert [call[1]["proxy"] for call in connector.calls] == [
            None,
            PROXY,
            PROXY,
            None,
            None,
        ]
        assert sockets[0].closes == [(1000, "direct recovery probe")]
        assert sockets[1].closes == [(1000, "direct recovery migration")]
        assert transitions[-1].to_mode is ConnectionMode.DIRECT

    asyncio.run(scenario())


def test_proxy_configuration_must_be_loopback_and_auto_requires_recorder() -> None:
    with pytest.raises(ValueError, match="loopback"):
        ConnectionPolicy(proxy_mode="auto", proxy_url="http://proxy.example:17891")
    with pytest.raises(ValueError, match="recorder"):
        RoutedConnectionFactory(
            FakeConnector(), ConnectionPolicy(proxy_mode="auto", proxy_url=PROXY)
        )
