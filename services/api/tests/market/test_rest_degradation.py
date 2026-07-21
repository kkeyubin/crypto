import asyncio
import socket
import ssl
from dataclasses import dataclass
from datetime import UTC, datetime

import pytest

from crypto_research.contracts.manifest import (
    BinanceRestEndpoint,
    BinanceRestSource,
)
from crypto_research.market.rest import (
    PublicBinanceRestAdapter,
    RestCapability,
    RestFailureKind,
    RestRequestError,
    SourceCapabilityHealth,
)

NOW = datetime(2026, 7, 21, 12, tzinfo=UTC)


@dataclass
class FakeResponse:
    status_code: int
    payload: object

    def json(self) -> object:
        return self.payload


class FakeClient:
    def __init__(self, result: FakeResponse | Exception) -> None:
        self.result = result
        self.urls: list[str] = []

    async def get(self, url: str) -> FakeResponse:
        self.urls.append(url)
        if isinstance(self.result, Exception):
            raise self.result
        return self.result


def source() -> BinanceRestSource:
    return BinanceRestSource(
        kind="binance_rest",
        endpoint=BinanceRestEndpoint.KLINES,
        symbol="BTCUSDT",
        interval="1m",
        start_time=1,
        end_time=2,
        limit=2,
    )


@pytest.mark.parametrize(
    ("failure", "expected"),
    [
        (TimeoutError("timed out"), RestFailureKind.TIMEOUT),
        (socket.gaierror("dns"), RestFailureKind.DNS),
        (ssl.SSLError("tls"), RestFailureKind.TLS),
        (ConnectionError("connect"), RestFailureKind.CONNECT),
    ],
)
def test_transport_failures_are_classified_without_degrading_archive_or_live(
    failure: Exception, expected: RestFailureKind
) -> None:
    async def scenario() -> None:
        health = SourceCapabilityHealth()
        adapter = PublicBinanceRestAdapter(FakeClient(failure), health, clock=lambda: NOW)
        with pytest.raises(RestRequestError) as captured:
            await adapter.fetch_validated(source(), RestCapability.REPAIR, lambda payload: payload)

        assert captured.value.kind is expected
        assert health.repair.failure is expected
        assert health.archive_healthy is True
        assert health.live_healthy is True

    asyncio.run(scenario())


@pytest.mark.parametrize(
    ("status", "expected"),
    [
        (451, RestFailureKind.GEOGRAPHIC_RESTRICTION),
        (429, RestFailureKind.RATE_LIMITED),
        (500, RestFailureKind.SERVER_ERROR),
        (503, RestFailureKind.SERVER_ERROR),
    ],
)
def test_http_failures_are_classified(status: int, expected: RestFailureKind) -> None:
    async def scenario() -> None:
        health = SourceCapabilityHealth()
        adapter = PublicBinanceRestAdapter(
            FakeClient(FakeResponse(status, {})), health, clock=lambda: NOW
        )
        with pytest.raises(RestRequestError) as captured:
            await adapter.fetch_validated(
                source(), RestCapability.REPAIR, lambda payload: payload
            )
        assert captured.value.kind is expected
        assert health.repair.healthy is False

    asyncio.run(scenario())


def test_metadata_becomes_verified_only_after_exchange_info_validation() -> None:
    async def scenario() -> None:
        health = SourceCapabilityHealth()
        client = FakeClient(FakeResponse(200, {"symbols": [{"symbol": "BTCUSDT"}]}))
        adapter = PublicBinanceRestAdapter(client, health, clock=lambda: NOW)

        result = await adapter.fetch_exchange_info(
            "BTCUSDT",
            lambda payload: payload
            if isinstance(payload, dict) and payload.get("symbols")
            else (_ for _ in ()).throw(ValueError("invalid exchangeInfo")),
        )

        assert result["symbols"][0]["symbol"] == "BTCUSDT"
        assert health.metadata.healthy is True
        assert client.urls == [
            "https://fapi.binance.com/fapi/v1/exchangeInfo?symbol=BTCUSDT"
        ]
        assert "@" not in client.urls[0]

    asyncio.run(scenario())


def test_invalid_public_response_degrades_only_requested_capability() -> None:
    async def scenario() -> None:
        health = SourceCapabilityHealth()
        adapter = PublicBinanceRestAdapter(
            FakeClient(FakeResponse(200, {"unexpected": True})), health, clock=lambda: NOW
        )
        with pytest.raises(RestRequestError) as captured:
            await adapter.fetch_exchange_info(
                "BTCUSDT", lambda _payload: (_ for _ in ()).throw(ValueError("bad schema"))
            )

        assert captured.value.kind is RestFailureKind.INVALID_RESPONSE
        assert health.metadata.healthy is False
        assert health.repair.failure is None
        assert health.archive_healthy is True
        assert health.live_healthy is True

    asyncio.run(scenario())


def test_endpoint_internally_determines_capability_and_rejects_mislabeling() -> None:
    async def scenario() -> None:
        health = SourceCapabilityHealth()
        adapter = PublicBinanceRestAdapter(
            FakeClient(FakeResponse(200, [])), health, clock=lambda: NOW
        )
        with pytest.raises(ValueError, match="capability"):
            await adapter.fetch_validated(
                source(), RestCapability.METADATA, lambda payload: payload
            )
        exchange_info = BinanceRestSource(
            kind="binance_rest",
            endpoint=BinanceRestEndpoint.EXCHANGE_INFO,
            symbol="BTCUSDT",
            limit=1,
        )
        with pytest.raises(ValueError, match="capability"):
            await adapter.fetch_validated(
                exchange_info, RestCapability.REPAIR, lambda payload: payload
            )

        assert health.metadata.checked_at is None
        assert health.repair.checked_at is None

    asyncio.run(scenario())
