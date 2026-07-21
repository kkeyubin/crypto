from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Protocol

from sqlalchemy.ext.asyncio import AsyncSession

from crypto_research.db.models import (
    AuditEventRow,
    DataGapRow,
    DataPartitionRow,
    IngestionJobRow,
    StreamStateRow,
    SymbolRow,
    is_legal_job_transition,
    new_id,
    normalize_symbol,
    utc_now,
)


@dataclass(frozen=True)
class AddSymbolCommand:
    symbol: str
    history_start: str | datetime
    history_end: str | datetime
    include_agg_trades: bool = False


@dataclass(frozen=True)
class BackfillCommand:
    id: str
    symbol: str
    dataset: str
    requested_start: datetime | None = None
    requested_end: datetime | None = None


@dataclass(frozen=True)
class PartitionCandidate:
    id: str
    symbol: str
    dataset: str
    partition_date: str
    version: int
    checksum_sha256: str
    parquet_path: str
    source_object_id: str | None = None
    validation_details: dict[str, Any] | None = None


@dataclass(frozen=True)
class GapRecord:
    id: str
    symbol: str
    dataset: str
    start_at: datetime
    end_at: datetime
    reason: str


@dataclass(frozen=True)
class StreamState:
    symbol: str
    stream_name: str
    last_event_at: datetime | None
    status: str
    details: dict[str, Any] | None = None


@dataclass(frozen=True)
class SymbolState:
    symbol: str
    enabled: bool
    history_start: datetime | None
    history_end: datetime | None
    include_agg_trades: bool


@dataclass(frozen=True)
class IngestionJob:
    id: str
    symbol: str
    dataset: str
    status: str


@dataclass(frozen=True)
class DataPartition:
    id: str
    symbol: str
    dataset: str
    partition_date: str
    version: int
    approval_status: str
    checksum_sha256: str
    parquet_path: str


@dataclass(frozen=True)
class DataGap:
    id: str
    symbol: str
    dataset: str
    start_at: datetime
    end_at: datetime
    reason: str
    status: str
    repaired_at: datetime | None
    repair_details: dict[str, Any] | None


class DataStateRepository(Protocol):
    async def add_symbol(self, command: AddSymbolCommand) -> SymbolState: ...

    async def create_backfill(self, command: BackfillCommand) -> IngestionJob: ...

    async def approve_partition(self, candidate: PartitionCandidate) -> DataPartition: ...

    async def record_gap(self, gap: GapRecord) -> DataGap: ...

    async def update_stream(self, state: StreamState) -> None: ...


class SqlAlchemyDataStateRepository:
    """Transaction-scoped state operations that return immutable domain values."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def add_symbol(self, command: AddSymbolCommand) -> SymbolState:
        symbol = normalize_symbol(command.symbol)
        row = await self._session.get(SymbolRow, symbol)
        if row is None:
            row = SymbolRow(
                symbol=symbol,
                enabled=True,
                history_start=_as_utc_datetime(command.history_start),
                history_end=_as_utc_datetime(command.history_end),
                include_agg_trades=command.include_agg_trades,
            )
            self._session.add(row)
            self._add_audit("symbol_added", "symbol", symbol)
            await self._session.flush()
        return _symbol_state(row)

    async def disable_symbol(self, symbol: str) -> SymbolState:
        normalized = normalize_symbol(symbol)
        row = await self._session.get(SymbolRow, normalized)
        if row is None:
            raise ValueError(f"symbol is not configured: {normalized}")
        if row.enabled:
            row.enabled = False
            self._add_audit("symbol_disabled", "symbol", normalized)
            await self._session.flush()
        return _symbol_state(row)

    async def create_backfill(self, command: BackfillCommand) -> IngestionJob:
        existing = await self._session.get(IngestionJobRow, command.id)
        if existing is not None:
            return _ingestion_job(existing)
        symbol = normalize_symbol(command.symbol)
        if await self._session.get(SymbolRow, symbol) is None:
            raise ValueError(f"symbol is not configured: {symbol}")
        row = IngestionJobRow(
            id=command.id,
            symbol=symbol,
            dataset=command.dataset,
            status="queued",
            requested_start=command.requested_start,
            requested_end=command.requested_end,
        )
        self._session.add(row)
        self._add_audit("backfill_created", "ingestion_job", row.id)
        await self._session.flush()
        return _ingestion_job(row)

    async def transition_job(self, job_id: str, target_status: str) -> IngestionJob:
        row = await self._session.get(IngestionJobRow, job_id)
        if row is None:
            raise ValueError(f"ingestion job does not exist: {job_id}")
        if not is_legal_job_transition(row.status, target_status):
            raise ValueError(f"illegal ingestion job transition: {row.status} -> {target_status}")
        row.status = target_status
        self._add_audit("job_transitioned", "ingestion_job", row.id)
        await self._session.flush()
        return _ingestion_job(row)

    async def approve_partition(self, candidate: PartitionCandidate) -> DataPartition:
        existing = await self._session.get(DataPartitionRow, candidate.id)
        if existing is not None:
            if _partition_matches(existing, candidate):
                return _data_partition(existing)
            raise ValueError("approved partition is immutable")
        row = DataPartitionRow(
            id=candidate.id,
            symbol=normalize_symbol(candidate.symbol),
            dataset=candidate.dataset,
            partition_date=candidate.partition_date,
            version=candidate.version,
            source_object_id=candidate.source_object_id,
            checksum_sha256=candidate.checksum_sha256,
            parquet_path=candidate.parquet_path,
            approval_status="approved",
            validation_details=candidate.validation_details or {},
            approved_at=utc_now(),
        )
        self._session.add(row)
        self._add_audit("partition_approved", "data_partition", row.id)
        await self._session.flush()
        return _data_partition(row)

    async def record_gap(self, gap: GapRecord) -> DataGap:
        existing = await self._session.get(DataGapRow, gap.id)
        if existing is not None:
            return _data_gap(existing)
        row = DataGapRow(
            id=gap.id,
            symbol=normalize_symbol(gap.symbol),
            dataset=gap.dataset,
            start_at=_require_utc(gap.start_at),
            end_at=_require_utc(gap.end_at),
            reason=gap.reason,
            status="open",
        )
        self._session.add(row)
        self._add_audit("gap_recorded", "data_gap", row.id)
        await self._session.flush()
        return _data_gap(row)

    async def repair_gap(self, gap_id: str, repair_details: dict[str, Any]) -> DataGap:
        row = await self._session.get(DataGapRow, gap_id)
        if row is None:
            raise ValueError(f"gap does not exist: {gap_id}")
        if row.status == "open":
            row.status = "repaired"
            row.repaired_at = utc_now()
            row.repair_details = repair_details
            self._add_audit("gap_repaired", "data_gap", row.id)
            await self._session.flush()
        return _data_gap(row)

    async def update_stream(self, state: StreamState) -> None:
        symbol = normalize_symbol(state.symbol)
        row = await self._session.get(StreamStateRow, (symbol, state.stream_name))
        if row is None:
            row = StreamStateRow(
                symbol=symbol,
                stream_name=state.stream_name,
                status=state.status,
                last_event_at=state.last_event_at,
                details=state.details or {},
            )
            self._session.add(row)
        else:
            row.status = state.status
            row.last_event_at = state.last_event_at
            row.details = state.details or {}
        await self._session.flush()

    def _add_audit(self, action: str, subject_type: str, subject_id: str) -> None:
        self._session.add(
            AuditEventRow(
                id=new_id(),
                action=action,
                subject_type=subject_type,
                subject_id=subject_id,
            )
        )


def _as_utc_datetime(value: str | datetime) -> datetime:
    if isinstance(value, str):
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    else:
        parsed = value
    return _require_utc(parsed)


def _require_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _symbol_state(row: SymbolRow) -> SymbolState:
    return SymbolState(
        symbol=row.symbol,
        enabled=row.enabled,
        history_start=row.history_start,
        history_end=row.history_end,
        include_agg_trades=row.include_agg_trades,
    )


def _ingestion_job(row: IngestionJobRow) -> IngestionJob:
    return IngestionJob(id=row.id, symbol=row.symbol, dataset=row.dataset, status=row.status)


def _data_partition(row: DataPartitionRow) -> DataPartition:
    return DataPartition(
        id=row.id,
        symbol=row.symbol,
        dataset=row.dataset,
        partition_date=row.partition_date,
        version=row.version,
        approval_status=row.approval_status,
        checksum_sha256=row.checksum_sha256,
        parquet_path=row.parquet_path,
    )


def _data_gap(row: DataGapRow) -> DataGap:
    return DataGap(
        id=row.id,
        symbol=row.symbol,
        dataset=row.dataset,
        start_at=row.start_at,
        end_at=row.end_at,
        reason=row.reason,
        status=row.status,
        repaired_at=row.repaired_at,
        repair_details=row.repair_details,
    )


def _partition_matches(row: DataPartitionRow, candidate: PartitionCandidate) -> bool:
    return (
        row.symbol == normalize_symbol(candidate.symbol)
        and row.dataset == candidate.dataset
        and row.partition_date == candidate.partition_date
        and row.version == candidate.version
        and row.checksum_sha256 == candidate.checksum_sha256
        and row.parquet_path == candidate.parquet_path
        and row.approval_status == "approved"
    )
