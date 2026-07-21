from __future__ import annotations

from datetime import UTC, datetime, timedelta
from uuid import UUID

import pytest
from fastapi.testclient import TestClient

from crypto_research.api import create_app
from crypto_research.config import Settings
from crypto_research.contracts.data import (
    AddSymbolRequest,
    BackfillRequest,
    DataGapStatus,
    DataGapView,
    DataPartitionStatus,
    DataPartitionView,
    EligibilityReasonCode,
    EligibilityView,
    IngestionJobStatus,
    IngestionJobView,
    MarketDataHealthView,
    MetadataStatus,
    SourceMode,
    StreamStateView,
    StreamStatus,
    SymbolDataStatus,
    SymbolProfileView,
    SymbolView,
)
from crypto_research.contracts.manifest import DataType
from crypto_research.market.control import MarketDataConflict, MarketDataNotFound

NOW = datetime(2026, 7, 21, 10, tzinfo=UTC)
START = datetime(2026, 7, 1, tzinfo=UTC)
END = START + timedelta(days=2)
BTC_JOB = UUID("00000000-0000-0000-0000-000000000101")
PEPE_JOB = UUID("00000000-0000-0000-0000-000000000102")


class ReadyProbe:
    async def ready(self) -> bool:
        return True

    async def close(self) -> None:
        return None


def symbol_view(symbol: str, *, enabled: bool = True) -> SymbolView:
    return SymbolView(
        symbol=symbol,
        enabled=enabled,
        history_start=START,
        history_end=END,
        include_agg_trades=False,
        data_status=(SymbolDataStatus.REQUESTED if enabled else SymbolDataStatus.DISABLED),
        metadata_status=MetadataStatus.METADATA_UNVERIFIED,
        created_at=START,
        updated_at=START,
    )


def job_view(symbol: str, job_id: UUID, data_type: DataType) -> IngestionJobView:
    return IngestionJobView(
        job_id=job_id,
        symbol=symbol,
        data_type=data_type,
        status=IngestionJobStatus.QUEUED,
        requested_start=START,
        requested_end=END,
        created_at=START,
        updated_at=START,
    )


class FakeMarketDataControl:
    def __init__(self) -> None:
        self.symbols = {
            "BTCUSDT": symbol_view("BTCUSDT"),
            "PEPEUSDT": symbol_view("PEPEUSDT"),
        }
        self.jobs = {
            BTC_JOB: job_view("BTCUSDT", BTC_JOB, DataType.KLINE_1M),
            PEPE_JOB: job_view("PEPEUSDT", PEPE_JOB, DataType.FUNDING),
        }
        self.audit_actions: list[str] = []

    async def list_symbols(self, *, limit: int, offset: int) -> tuple[SymbolView, ...]:
        ordered = tuple(self.symbols[symbol] for symbol in sorted(self.symbols))
        return ordered[offset : offset + limit]

    async def add_symbol(self, request: AddSymbolRequest) -> SymbolView:
        existing = self.symbols.get(request.symbol)
        if existing is not None:
            if (
                existing.history_start != request.history_start
                or existing.history_end != request.history_end
                or existing.include_agg_trades != request.include_agg_trades
            ):
                raise MarketDataConflict("symbol configuration conflicts")
            return existing
        created = SymbolView(
            symbol=request.symbol,
            enabled=True,
            history_start=request.history_start,
            history_end=request.history_end,
            include_agg_trades=request.include_agg_trades,
            data_status=SymbolDataStatus.REQUESTED,
            metadata_status=MetadataStatus.METADATA_UNVERIFIED,
            created_at=START,
            updated_at=START,
        )
        self.symbols[request.symbol] = created
        self.audit_actions.append("symbol_added")
        return created

    async def get_symbol(self, symbol: str) -> SymbolView:
        try:
            return self.symbols[symbol]
        except KeyError as error:
            raise MarketDataNotFound("symbol is not configured") from error

    async def disable_symbol(self, symbol: str) -> SymbolView:
        current = await self.get_symbol(symbol)
        if current.enabled:
            current = current.model_copy(
                update={"enabled": False, "data_status": SymbolDataStatus.DISABLED}
            )
            self.symbols[symbol] = current
            self.audit_actions.append("symbol_disabled")
        return current

    async def create_backfills(
        self, symbol: str, request: BackfillRequest
    ) -> tuple[IngestionJobView, ...]:
        if symbol != request.symbol:
            raise MarketDataConflict("path symbol conflicts with request symbol")
        if symbol not in self.symbols:
            raise MarketDataNotFound("symbol is not configured")
        return tuple(
            job_view(symbol, BTC_JOB, data_type)
            for data_type in sorted(request.data_types, key=lambda item: item.value)
        )

    async def get_backfill(self, job_id: UUID) -> IngestionJobView:
        try:
            return self.jobs[job_id]
        except KeyError as error:
            raise MarketDataNotFound("backfill does not exist") from error

    async def list_partitions(
        self, symbol: str, *, limit: int, offset: int
    ) -> tuple[DataPartitionView, ...]:
        await self.get_symbol(symbol)
        values = {
            "BTCUSDT": (
                DataPartitionView(
                    partition_id=UUID("00000000-0000-0000-0000-000000000201"),
                    symbol="BTCUSDT",
                    data_type=DataType.KLINE_1M,
                    start=START,
                    end=START + timedelta(days=1),
                    parquet_path="normalized/binance/usdm/BTCUSDT/klines/date=2026-07-01/a.parquet",
                    checksum="a" * 64,
                    row_count=1440,
                    version=1,
                    status=DataPartitionStatus.APPROVED,
                    created_at=START,
                    approved_at=START,
                ),
            ),
            "PEPEUSDT": (),
        }
        return values[symbol][offset : offset + limit]

    async def list_gaps(
        self, symbol: str, *, limit: int, offset: int
    ) -> tuple[DataGapView, ...]:
        await self.get_symbol(symbol)
        values = {
            "BTCUSDT": (),
            "PEPEUSDT": (
                DataGapView(
                    gap_id=UUID("00000000-0000-0000-0000-000000000301"),
                    symbol="PEPEUSDT",
                    data_type=DataType.KLINE_1M,
                    start=START,
                    end=START + timedelta(minutes=1),
                    reason="source_unknown",
                    status=DataGapStatus.OPEN,
                    opened_at=START,
                ),
            ),
        }
        return values[symbol][offset : offset + limit]

    async def get_profile(self, symbol: str) -> SymbolProfileView:
        await self.get_symbol(symbol)
        return SymbolProfileView(
            symbol=symbol,
            calculated_at=NOW,
            coverage_start=START,
            coverage_end=END,
            sample_count=10 if symbol == "BTCUSDT" else 3,
            coverage_fraction=1 if symbol == "BTCUSDT" else 0.5,
            realized_volatility=0.1 if symbol == "BTCUSDT" else 0.9,
            jump_frequency=0.01 if symbol == "BTCUSDT" else 0.2,
            median_spread_bps=1 if symbol == "BTCUSDT" else 8,
            median_hourly_volume=1000 if symbol == "BTCUSDT" else 20,
            funding_rate_mean=0.0001,
        )

    async def get_eligibility(self, symbol: str) -> EligibilityView:
        await self.get_symbol(symbol)
        reasons = (
            (EligibilityReasonCode.METADATA_UNVERIFIED,)
            if symbol == "BTCUSDT"
            else (
                EligibilityReasonCode.UNREPAIRED_GAP,
                EligibilityReasonCode.METADATA_UNVERIFIED,
            )
        )
        return EligibilityView(
            symbol=symbol,
            eligible=False,
            reason_codes=reasons,
            evaluated_at=NOW,
        )

    async def list_streams(
        self, symbol: str, *, limit: int, offset: int
    ) -> tuple[StreamStateView, ...]:
        await self.get_symbol(symbol)
        streams = (
            StreamStateView(
                symbol=symbol,
                stream_name=f"{symbol.lower()}@kline_1m",
                status=StreamStatus.CONNECTED,
                last_event_at=NOW - timedelta(seconds=1),
                updated_at=NOW,
            ),
        )
        return streams[offset : offset + limit]

    async def market_data_health(self) -> MarketDataHealthView:
        return MarketDataHealthView(
            source_mode=SourceMode.PROXY,
            archive_healthy=True,
            rest_healthy=False,
            worker_heartbeat_at=NOW,
            streams=(await self.list_streams("BTCUSDT", limit=100, offset=0)),
            checked_at=NOW,
        )


@pytest.fixture
def control() -> FakeMarketDataControl:
    return FakeMarketDataControl()


@pytest.fixture
def client(control: FakeMarketDataControl):
    settings = Settings(_env_file=None, allowed_hosts=["testserver"])
    with TestClient(
        create_app(settings, ReadyProbe(), market_data_service=control)
    ) as test_client:
        yield test_client
