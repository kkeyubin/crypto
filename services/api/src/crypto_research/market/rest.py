from __future__ import annotations

import socket
import ssl
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
from typing import Protocol, TypeVar

import httpx2

from crypto_research.contracts.manifest import BinanceRestEndpoint, BinanceRestSource

T = TypeVar("T")


class RestCapability(StrEnum):
    METADATA = "metadata"
    REPAIR = "repair"


class RestFailureKind(StrEnum):
    GEOGRAPHIC_RESTRICTION = "geographic_restriction"
    TIMEOUT = "timeout"
    DNS = "dns"
    TLS = "tls"
    CONNECT = "connect"
    RATE_LIMITED = "rate_limited"
    SERVER_ERROR = "server_error"
    HTTP_ERROR = "http_error"
    INVALID_RESPONSE = "invalid_response"


class RestRequestError(RuntimeError):
    def __init__(self, kind: RestFailureKind, message: str, status_code: int | None = None) -> None:
        self.kind = kind
        self.status_code = status_code
        super().__init__(message)


@dataclass
class CapabilityStatus:
    healthy: bool = False
    failure: RestFailureKind | None = None
    checked_at: datetime | None = None


@dataclass
class SourceCapabilityHealth:
    archive_healthy: bool = True
    live_healthy: bool = True
    metadata: CapabilityStatus = field(default_factory=CapabilityStatus)
    repair: CapabilityStatus = field(default_factory=CapabilityStatus)

    def status(self, capability: RestCapability) -> CapabilityStatus:
        return self.metadata if capability is RestCapability.METADATA else self.repair

    def succeeded(self, capability: RestCapability, checked_at: datetime) -> None:
        status = self.status(capability)
        status.healthy = True
        status.failure = None
        status.checked_at = checked_at

    def failed(
        self,
        capability: RestCapability,
        failure: RestFailureKind,
        checked_at: datetime,
    ) -> None:
        status = self.status(capability)
        status.healthy = False
        status.failure = failure
        status.checked_at = checked_at


class RestResponse(Protocol):
    status_code: int

    def json(self) -> object: ...


class RestClient(Protocol):
    async def get(self, url: str) -> RestResponse: ...


class PublicBinanceRestAdapter:
    """Public-only structured REST access with per-capability degradation."""

    def __init__(
        self,
        client: RestClient,
        health: SourceCapabilityHealth,
        *,
        clock: Callable[[], datetime],
    ) -> None:
        self._client = client
        self._health = health
        self._clock = clock

    async def fetch_validated(
        self,
        source: BinanceRestSource,
        capability: RestCapability,
        validator: Callable[[object], T],
    ) -> T:
        expected_capability = (
            RestCapability.METADATA
            if source.endpoint is BinanceRestEndpoint.EXCHANGE_INFO
            else RestCapability.REPAIR
        )
        if capability is not expected_capability:
            raise ValueError(
                f"REST endpoint capability is {expected_capability.value}, not {capability.value}"
            )
        checked_at = _utc(self._clock())
        try:
            response = await self._client.get(source.resolved_url)
        except Exception as error:
            kind = _classify_transport(error)
            self._health.failed(capability, kind, checked_at)
            raise RestRequestError(kind, f"Binance REST transport failed: {kind.value}") from error
        if not 200 <= response.status_code < 300:
            kind = _classify_status(response.status_code)
            self._health.failed(capability, kind, checked_at)
            raise RestRequestError(
                kind,
                f"Binance REST returned HTTP {response.status_code}",
                response.status_code,
            )
        try:
            validated = validator(response.json())
        except Exception as error:
            kind = RestFailureKind.INVALID_RESPONSE
            self._health.failed(capability, kind, checked_at)
            raise RestRequestError(kind, "Binance REST response validation failed") from error
        self._health.succeeded(capability, checked_at)
        return validated

    async def fetch_exchange_info(
        self, symbol: str, validator: Callable[[object], T]
    ) -> T:
        source = BinanceRestSource(
            kind="binance_rest",
            endpoint=BinanceRestEndpoint.EXCHANGE_INFO,
            symbol=symbol.strip().upper(),
            limit=1,
        )
        return await self.fetch_validated(source, RestCapability.METADATA, validator)


def _classify_status(status_code: int) -> RestFailureKind:
    if status_code == 451:
        return RestFailureKind.GEOGRAPHIC_RESTRICTION
    if status_code == 429:
        return RestFailureKind.RATE_LIMITED
    if 500 <= status_code < 600:
        return RestFailureKind.SERVER_ERROR
    return RestFailureKind.HTTP_ERROR


def _classify_transport(error: Exception) -> RestFailureKind:
    chain: list[BaseException] = []
    current: BaseException | None = error
    while current is not None and current not in chain:
        chain.append(current)
        current = current.__cause__ or current.__context__
    if any(
        isinstance(item, (TimeoutError, httpx2.TimeoutException)) for item in chain
    ):
        return RestFailureKind.TIMEOUT
    if any(isinstance(item, socket.gaierror) for item in chain):
        return RestFailureKind.DNS
    if any(isinstance(item, ssl.SSLError) for item in chain):
        return RestFailureKind.TLS
    if any(
        isinstance(item, (ConnectionError, httpx2.ConnectError, OSError))
        for item in chain
    ):
        return RestFailureKind.CONNECT
    return RestFailureKind.CONNECT


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() != UTC.utcoffset(value):
        raise ValueError("REST capability clock must return UTC")
    return value.astimezone(UTC)
