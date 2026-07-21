import json
import re
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Protocol
from uuid import NAMESPACE_URL, uuid5

from sqlalchemy import case, func, literal, or_, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from crypto_research.contracts.manifest import DataManifest, DataType, ValidationState
from crypto_research.db.models import (
    AuditEventRow,
    BackfillObjectRow,
    DataGapRow,
    DataManifestRow,
    DataPartitionRow,
    IngestionJobRow,
    LiveDataPartitionRow,
    StreamStateRow,
    SymbolMetadataSnapshotRow,
    SymbolRow,
    WorkerHeartbeatRow,
    is_legal_job_transition,
    new_id,
    normalize_symbol,
    utc_now,
)
from crypto_research.market.backfill import (
    BackfillObject,
    BackfillState,
    is_legal_backfill_transition,
)
from crypto_research.market.gaps import (
    ApprovedCoverage,
    DetectedGap,
    GapReason,
    GapStatus,
    TimeRange,
    approved_evidence_repairs,
)

_REQUIRED_SUMMARY_DATASETS = (
    DataType.KLINE_1M.value,
    DataType.MARK_PRICE.value,
    DataType.FUNDING.value,
)
_TRUSTED_METADATA_SOURCE = "binance_usdm_exchange_info"
_METADATA_MAX_AGE = timedelta(hours=24)


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


class MutationIdentityConflict(ValueError):
    """A concurrent idempotent mutation reused an immutable identity differently."""


class RepositoryNotFound(LookupError):
    """Required durable market-data evidence does not exist."""


class ApprovedPartitionEvidenceNotFound(RepositoryNotFound):
    """One or more requested partitions are not approved catalog evidence."""


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
    details: dict[str, Any] | None = None


@dataclass(frozen=True)
class StreamState:
    symbol: str
    stream_name: str
    last_event_at: datetime | None
    status: str
    details: dict[str, Any] | None = None
    updated_at: datetime | None = None


@dataclass(frozen=True)
class WorkerHeartbeat:
    worker_id: str
    status: str
    heartbeat_at: datetime
    details: dict[str, Any] | None = None


@dataclass(frozen=True)
class SourceTransition:
    worker_id: str
    at: datetime
    from_mode: str
    to_mode: str
    reason: str
    details: dict[str, Any] | None = None


@dataclass(frozen=True)
class SymbolState:
    symbol: str
    enabled: bool
    history_start: datetime | None
    history_end: datetime | None
    include_agg_trades: bool
    created_at: datetime | None = None
    updated_at: datetime | None = None


@dataclass(frozen=True)
class IngestionJob:
    id: str
    symbol: str
    dataset: str
    status: str
    requested_start: datetime | None = None
    requested_end: datetime | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None


@dataclass(frozen=True)
class ArchiveRecheckDecision:
    original: BackfillObject
    observed_checksum: str
    replacement_job: IngestionJob | None
    replacement_object: BackfillObject | None
    created: bool


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
    start_at: datetime | None = None
    end_at: datetime | None = None
    row_count: int | None = None
    created_at: datetime | None = None
    approved_at: datetime | None = None


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
    opened_at: datetime | None = None


@dataclass(frozen=True)
class SymbolOperationalSummary:
    approved_data_types: tuple[str, ...] = ()
    archive_intervals: tuple["ArchiveCoverageInterval", ...] = ()
    metadata_verified: bool = False
    open_gap_count: int = 0
    job_statuses: tuple[str, ...] = ()


@dataclass(frozen=True)
class ArchiveCoverageInterval:
    dataset: str
    start: datetime
    end: datetime


@dataclass(frozen=True)
class ActiveStreamHealth:
    expected_count: int
    observed_count: int
    healthy_count: int


class DataStateRepository(Protocol):
    async def list_symbols(
        self, *, limit: int, offset: int
    ) -> tuple[SymbolState, ...]: ...

    async def get_symbol(self, symbol: str) -> SymbolState | None: ...

    async def list_active_symbols(self) -> tuple[SymbolState, ...]: ...

    async def add_symbol(self, command: AddSymbolCommand) -> SymbolState: ...

    async def set_symbol_enabled(self, symbol: str, enabled: bool) -> SymbolState: ...

    async def create_backfill(self, command: BackfillCommand) -> IngestionJob: ...

    async def get_backfill(self, job_id: str) -> IngestionJob | None: ...

    async def retry_backfill_job(
        self, job_id: str, now: datetime
    ) -> IngestionJob: ...

    async def get_approved_backfill_object(
        self, job_id: str, partition_id: str
    ) -> BackfillObject | None: ...

    async def register_archive_recheck(
        self,
        *,
        job_id: str,
        object_id: str,
        partition_id: str,
        observed_checksum: str,
        checked_at: datetime,
    ) -> ArchiveRecheckDecision: ...

    async def approve_partition(self, candidate: PartitionCandidate) -> DataPartition: ...

    async def record_gap(self, gap: GapRecord) -> DataGap: ...

    async def reconcile_gap(
        self,
        gap_id: str,
        partition_ids: tuple[str, ...],
        attempted_at: datetime,
        source: str,
    ) -> DataGap: ...

    async def update_stream(self, state: StreamState) -> None: ...

    async def list_stream_states(
        self,
        symbol: str | None = None,
        *,
        limit: int | None = None,
        offset: int = 0,
    ) -> tuple[StreamState, ...]: ...

    async def list_active_stream_states(self) -> tuple[StreamState, ...]: ...

    async def get_active_stream_health(
        self, *, checked_at: datetime, stale_after: timedelta
    ) -> ActiveStreamHealth: ...

    async def update_worker_heartbeat(self, heartbeat: WorkerHeartbeat) -> None: ...

    async def record_source_transition(self, transition: SourceTransition) -> None: ...

    async def plan_backfill_object(self, work: BackfillObject) -> BackfillObject: ...

    async def claim_backfill_object(
        self, worker_id: str, now: datetime, duration: timedelta
    ) -> BackfillObject | None: ...


class ApprovedCoverageResolver(Protocol):
    async def resolve(
        self, partition_ids: tuple[str, ...]
    ) -> tuple[ApprovedCoverage, ...]: ...


class SqlAlchemyApprovedCoverageResolver:
    """Derive repair evidence from approved catalog rows in the current transaction."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def resolve(
        self, partition_ids: tuple[str, ...]
    ) -> tuple[ApprovedCoverage, ...]:
        if not partition_ids:
            return ()
        statement = (
            select(DataPartitionRow, DataManifestRow)
            .join(DataManifestRow, DataManifestRow.partition_id == DataPartitionRow.id)
            .where(
                DataPartitionRow.id.in_(set(partition_ids)),
                DataPartitionRow.approval_status == "approved",
            )
        )

        rows = (await self._session.execute(statement)).all()
        requested_ids = set(partition_ids)
        resolved_ids = {partition.id for partition, _stored in rows}
        if resolved_ids != requested_ids:
            raise ApprovedPartitionEvidenceNotFound(
                "one or more approved partition evidence records do not exist"
            )
        coverage: list[ApprovedCoverage] = []
        for partition, stored in rows:
            validation = partition.validation_details or {}
            if any(
                validation.get(name) is not True
                for name in (
                    "checksum",
                    "schema",
                    "ordering",
                    "uniqueness",
                    "range",
                    "row_count",
                )
            ):
                continue
            manifest = DataManifest.model_validate_json(json.dumps(stored.manifest))
            coverage_ranges = _manifest_coverage_intervals(manifest)
            recovered_ranges = validation.get("recovered_id_ranges")
            if isinstance(recovered_ranges, list):
                for start, end in coverage_ranges:
                    for recovered in recovered_ranges:
                        if (
                            isinstance(recovered, list)
                            and len(recovered) == 2
                            and all(
                                isinstance(value, int) and not isinstance(value, bool)
                                for value in recovered
                            )
                        ):
                            coverage.append(
                                ApprovedCoverage(
                                    symbol=partition.symbol,
                                    data_type=DataType(partition.dataset),
                                    time_range=TimeRange(start, end),
                                    partition_id=partition.id,
                                    recovered_id_start=recovered[0],
                                    recovered_id_end=recovered[1],
                                )
                            )
                continue
            coverage.extend(
                ApprovedCoverage(
                    symbol=partition.symbol,
                    data_type=DataType(partition.dataset),
                    time_range=TimeRange(start, end),
                    partition_id=partition.id,
                )
                for start, end in coverage_ranges
            )
        return tuple(coverage)


class SqlAlchemyDataStateRepository:
    """Transaction-scoped state operations that return immutable domain values."""

    def __init__(
        self,
        session: AsyncSession,
        coverage_resolver: ApprovedCoverageResolver | None = None,
    ) -> None:
        self._session = session
        self._coverage_resolver = coverage_resolver or SqlAlchemyApprovedCoverageResolver(
            session
        )

    async def add_symbol(self, command: AddSymbolCommand) -> SymbolState:
        symbol = normalize_symbol(command.symbol)
        history_start = _as_utc_datetime(command.history_start)
        history_end = _as_utc_datetime(command.history_end)
        await self._lock_identity(f"crypto-research:symbol:{symbol}")
        row = await self._session.get(SymbolRow, symbol, populate_existing=True)
        if row is None:
            row = SymbolRow(
                symbol=symbol,
                enabled=True,
                history_start=history_start,
                history_end=history_end,
                include_agg_trades=command.include_agg_trades,
            )
            self._session.add(row)
            self._add_audit("symbol_added", "symbol", symbol)
            await self._session.flush()
        elif (
            row.history_start != history_start
            or row.history_end != history_end
            or row.include_agg_trades != command.include_agg_trades
        ):
            raise MutationIdentityConflict("symbol configuration identity is immutable")
        return _symbol_state(row)

    async def list_symbols(
        self, *, limit: int, offset: int
    ) -> tuple[SymbolState, ...]:
        statement = select(SymbolRow).order_by(SymbolRow.symbol).limit(limit).offset(offset)
        rows = (await self._session.execute(statement)).scalars().all()
        return tuple(_symbol_state(row) for row in rows)

    async def get_symbol(self, symbol: str) -> SymbolState | None:
        row = await self._session.get(SymbolRow, normalize_symbol(symbol))
        return None if row is None else _symbol_state(row)

    async def list_active_symbols(self) -> tuple[SymbolState, ...]:
        statement = (
            select(SymbolRow)
            .where(SymbolRow.enabled.is_(True))
            .order_by(SymbolRow.symbol)
        )
        rows = (await self._session.execute(statement)).scalars().all()
        return tuple(_symbol_state(row) for row in rows if row.enabled)

    async def disable_symbol(self, symbol: str) -> SymbolState:
        return await self.set_symbol_enabled(symbol, False)

    async def set_symbol_enabled(self, symbol: str, enabled: bool) -> SymbolState:
        normalized = normalize_symbol(symbol)
        await self._lock_identity(f"crypto-research:symbol:{normalized}")
        row = await self._session.get(SymbolRow, normalized, populate_existing=True)
        if row is None:
            raise ValueError(f"symbol is not configured: {normalized}")
        if row.enabled != enabled:
            row.enabled = enabled
            self._add_audit(
                "symbol_enabled" if enabled else "symbol_disabled",
                "symbol",
                normalized,
            )
            await self._session.flush()
        return _symbol_state(row)

    async def create_backfill(self, command: BackfillCommand) -> IngestionJob:
        requested_start = _optional_utc_datetime(command.requested_start)
        requested_end = _optional_utc_datetime(command.requested_end)
        symbol = normalize_symbol(command.symbol)
        await self._lock_identity(f"crypto-research:symbol:{symbol}")
        await self._lock_identity(f"crypto-research:backfill-job:{command.id}")
        existing = await self._session.get(
            IngestionJobRow, command.id, populate_existing=True
        )
        if existing is not None:
            if (
                existing.symbol != symbol
                or existing.dataset != command.dataset
                or existing.requested_start != requested_start
                or existing.requested_end != requested_end
            ):
                raise MutationIdentityConflict("backfill job identity is immutable")
            return _ingestion_job(existing)
        configured = await self._session.get(
            SymbolRow, symbol, populate_existing=True
        )
        if configured is None:
            raise ValueError(f"symbol is not configured: {symbol}")
        if not configured.enabled:
            raise MutationIdentityConflict("disabled symbols cannot accept new backfills")
        row = IngestionJobRow(
            id=command.id,
            symbol=symbol,
            dataset=command.dataset,
            status="queued",
            requested_start=requested_start,
            requested_end=requested_end,
        )
        self._session.add(row)
        self._add_audit("backfill_created", "ingestion_job", row.id)
        await self._session.flush()
        return _ingestion_job(row)

    async def get_backfill(self, job_id: str) -> IngestionJob | None:
        row = await self._session.get(IngestionJobRow, job_id)
        if row is None:
            return None
        statement = (
            select(BackfillObjectRow.state)
            .where(BackfillObjectRow.job_id == job_id)
            .order_by(BackfillObjectRow.id)
        )
        states = (await self._session.execute(statement)).scalars().all()
        return _ingestion_job(row, status=_effective_job_status(row.status, states))

    async def retry_backfill_job(self, job_id: str, now: datetime) -> IngestionJob:
        now = _require_utc(now)
        await self._lock_identity(f"crypto-research:backfill-retry:{job_id}")
        job = await self._session.get(IngestionJobRow, job_id, populate_existing=True)
        if job is None:
            raise RepositoryNotFound(f"backfill does not exist: {job_id}")
        if job.status == "cancelled":
            raise MutationIdentityConflict("cancelled backfills cannot be retried")
        rows = (
            await self._session.execute(
                select(BackfillObjectRow)
                .where(BackfillObjectRow.job_id == job_id)
                .order_by(BackfillObjectRow.id)
                .with_for_update()
            )
        ).scalars().all()
        retryable = [
            row
            for row in rows
            if row.state
            in {BackfillState.FAILED.value, BackfillState.SOURCE_PENDING.value}
        ]
        if not retryable and rows and all(
            row.state == BackfillState.CATALOG_APPROVED.value for row in rows
        ):
            raise MutationIdentityConflict("approved backfills cannot be retried")
        for row in retryable:
            row.state = BackfillState.PLANNED.value
            row.updated_at = now
            row.lease_owner = None
            row.lease_expires_at = None
        self._add_audit(
            "backfill_retry_requested",
            "ingestion_job",
            job_id,
            {"retried_object_ids": [row.id for row in retryable]},
        )
        await self._session.flush()
        return _ingestion_job(
            job,
            status=_effective_job_status(job.status, [row.state for row in rows]),
        )

    async def get_approved_backfill_object(
        self, job_id: str, partition_id: str
    ) -> BackfillObject | None:
        statement = (
            select(BackfillObjectRow)
            .join(DataPartitionRow, DataPartitionRow.id == BackfillObjectRow.partition_id)
            .join(
                DataManifestRow,
                DataManifestRow.manifest_id == BackfillObjectRow.manifest_id,
            )
            .where(
                BackfillObjectRow.job_id == job_id,
                BackfillObjectRow.partition_id == partition_id,
                BackfillObjectRow.state == BackfillState.CATALOG_APPROVED.value,
                DataPartitionRow.approval_status == "approved",
                DataManifestRow.partition_id == partition_id,
            )
        )
        row = (await self._session.execute(statement)).scalars().first()
        return None if row is None else _backfill_object(row)

    async def register_archive_recheck(
        self,
        *,
        job_id: str,
        object_id: str,
        partition_id: str,
        observed_checksum: str,
        checked_at: datetime,
    ) -> ArchiveRecheckDecision:
        checked_at = _require_utc(checked_at)
        if re.fullmatch(r"[0-9a-f]{64}", observed_checksum) is None:
            raise ValueError("observed source checksum is invalid")
        await self._lock_identity(f"crypto-research:archive-recheck:{object_id}")
        statement = (
            select(BackfillObjectRow, IngestionJobRow)
            .join(IngestionJobRow, IngestionJobRow.id == BackfillObjectRow.job_id)
            .join(DataPartitionRow, DataPartitionRow.id == BackfillObjectRow.partition_id)
            .join(
                DataManifestRow,
                DataManifestRow.manifest_id == BackfillObjectRow.manifest_id,
            )
            .where(
                BackfillObjectRow.id == object_id,
                BackfillObjectRow.job_id == job_id,
                BackfillObjectRow.partition_id == partition_id,
                BackfillObjectRow.state == BackfillState.CATALOG_APPROVED.value,
                DataPartitionRow.approval_status == "approved",
                DataManifestRow.partition_id == partition_id,
            )
            .with_for_update()
        )
        result = (await self._session.execute(statement)).first()
        if result is None:
            raise RepositoryNotFound("approved backfill evidence does not exist")
        original_row, original_job = result
        original = _backfill_object(original_row)
        if original.source_checksum == observed_checksum:
            self._add_audit(
                "archive_recheck_unchanged",
                "backfill_object",
                object_id,
                {"checksum": observed_checksum, "checked_at": checked_at.isoformat()},
            )
            await self._session.flush()
            return ArchiveRecheckDecision(original, observed_checksum, None, None, False)

        replacement_job_id = str(
            uuid5(
                NAMESPACE_URL,
                f"crypto-research:archive-replacement-job:{object_id}:{observed_checksum}",
            )
        )
        replacement_object_id = str(
            uuid5(
                NAMESPACE_URL,
                f"crypto-research:archive-replacement-object:{object_id}:{observed_checksum}",
            )
        )
        replacement_job = await self._session.get(
            IngestionJobRow, replacement_job_id, populate_existing=True
        )
        replacement_row = await self._session.get(
            BackfillObjectRow, replacement_object_id, populate_existing=True
        )
        if (replacement_job is None) != (replacement_row is None):
            raise MutationIdentityConflict("archive replacement identity is incomplete")
        created = replacement_job is None and replacement_row is None
        replacement_details = {
            "kind": "source_replacement",
            "replacement_for_job_id": job_id,
            "replacement_for_object_id": object_id,
            "observed_checksum": observed_checksum,
        }
        if created:
            replacement_job = IngestionJobRow(
                id=replacement_job_id,
                symbol=original_job.symbol,
                dataset=original_job.dataset,
                status="queued",
                requested_start=original_row.start_at,
                requested_end=original_row.end_at,
                details=replacement_details,
            )
            self._session.add(replacement_job)
            await self._session.flush()
            replacement_row = BackfillObjectRow(
                id=replacement_object_id,
                job_id=replacement_job_id,
                source_url=original_row.source_url,
                source_checksum=observed_checksum,
                start_at=original_row.start_at,
                end_at=original_row.end_at,
                state=BackfillState.PLANNED.value,
                attempt_count=0,
            )
            self._session.add(replacement_row)
            await self._session.flush()
        else:
            if replacement_job is None or replacement_row is None:
                raise AssertionError("replacement identity pair was not loaded")
            if (
                replacement_job.symbol != original_job.symbol
                or replacement_job.dataset != original_job.dataset
                or replacement_job.requested_start != original_row.start_at
                or replacement_job.requested_end != original_row.end_at
                or replacement_job.details != replacement_details
                or _backfill_identity(replacement_row)
                != (
                    replacement_job_id,
                    original_row.source_url,
                    original_row.start_at,
                    original_row.end_at,
                )
                or replacement_row.source_checksum != observed_checksum
            ):
                raise MutationIdentityConflict("archive replacement identity is immutable")
        self._add_audit(
            "archive_replacement_planned" if created else "archive_replacement_reused",
            "ingestion_job",
            replacement_job_id,
            {
                "replacement_for_job_id": job_id,
                "replacement_for_object_id": object_id,
                "observed_checksum": observed_checksum,
                "checked_at": checked_at.isoformat(),
            },
        )
        await self._session.flush()
        return ArchiveRecheckDecision(
            original,
            observed_checksum,
            _ingestion_job(replacement_job),
            _backfill_object(replacement_row),
            created,
        )

    async def list_partitions(
        self, symbol: str, *, limit: int, offset: int
    ) -> tuple[DataPartition, ...]:
        normalized = normalize_symbol(symbol)
        window = limit + offset
        archive_statement = (
            select(DataPartitionRow, DataManifestRow)
            .join(DataManifestRow, DataManifestRow.partition_id == DataPartitionRow.id)
            .where(
                DataPartitionRow.symbol == normalized,
                DataPartitionRow.approval_status == "approved",
            )
            .order_by(
                DataPartitionRow.partition_date,
                DataPartitionRow.dataset,
                DataPartitionRow.version,
                DataPartitionRow.id,
            )
            .limit(window)
        )
        archive_rows = (await self._session.execute(archive_statement)).all()
        partitions = [
            _manifest_partition(partition, stored)
            for partition, stored in archive_rows
        ]
        live_statement = (
            select(LiveDataPartitionRow)
            .where(
                LiveDataPartitionRow.symbol == normalized,
                LiveDataPartitionRow.layer == "normalized",
                LiveDataPartitionRow.approval_status == "approved",
            )
            .order_by(
                LiveDataPartitionRow.min_canonical_time,
                LiveDataPartitionRow.dataset,
                LiveDataPartitionRow.id,
            )
            .limit(window)
        )
        live_rows = (await self._session.execute(live_statement)).scalars().all()
        partitions.extend(_live_partition(row) for row in live_rows)
        ordered = sorted(
            partitions,
            key=lambda item: (
                item.start_at or datetime.min.replace(tzinfo=utc_now().tzinfo),
                item.dataset,
                item.version,
                item.id,
            ),
        )
        return tuple(ordered[offset : offset + limit])

    async def list_gaps(
        self, symbol: str, *, limit: int, offset: int
    ) -> tuple[DataGap, ...]:
        statement = (
            select(DataGapRow)
            .where(DataGapRow.symbol == normalize_symbol(symbol))
            .order_by(DataGapRow.start_at, DataGapRow.dataset, DataGapRow.id)
            .limit(limit)
            .offset(offset)
        )
        rows = (await self._session.execute(statement)).scalars().all()
        return tuple(_data_gap(row) for row in rows)

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
        start_at = _require_utc(gap.start_at)
        end_at = _require_utc(gap.end_at)
        existing = await self._session.get(DataGapRow, gap.id)
        if existing is not None:
            return _data_gap(existing)
        row = DataGapRow(
            id=gap.id,
            symbol=normalize_symbol(gap.symbol),
            dataset=gap.dataset,
            start_at=start_at,
            end_at=end_at,
            reason=gap.reason,
            status="open",
            repair_details={"gap": gap.details or {}, "history": []},
        )
        self._session.add(row)
        self._add_audit("gap_recorded", "data_gap", row.id)
        await self._session.flush()
        return _data_gap(row)

    async def reconcile_gap(
        self,
        gap_id: str,
        partition_ids: tuple[str, ...],
        attempted_at: datetime,
        source: str,
    ) -> DataGap:
        row = await self._session.get(DataGapRow, gap_id, with_for_update=True)
        if row is None:
            raise RepositoryNotFound(f"gap does not exist: {gap_id}")
        attempted_at = _require_utc(attempted_at)
        previous = row.repair_details or {}
        approved_ranges = await self._coverage_resolver.resolve(partition_ids)
        covered = approved_evidence_repairs(
            DetectedGap(
                gap_id=row.id,
                symbol=row.symbol,
                data_type=DataType(row.dataset),
                start=row.start_at,
                end=row.end_at,
                reason=_gap_reason(row.reason),
                details=previous.get("gap", {}),
                status=GapStatus(row.status),
            ),
            approved_ranges,
        )
        history = list(previous.get("history", []))
        history.append(
            {
                "attempted_at": attempted_at.isoformat(),
                "source": source,
                "result": "repaired" if covered else "partial",
            }
        )
        row.repair_details = {"gap": previous.get("gap", {}), "history": history}
        if row.status == "open" and covered:
            row.status = "repaired"
            row.repaired_at = attempted_at
            self._add_audit("gap_repaired", "data_gap", row.id)
        else:
            self._add_audit("gap_repair_attempted", "data_gap", row.id)
        await self._session.flush()
        return _data_gap(row)

    async def update_stream(self, state: StreamState) -> None:
        symbol = normalize_symbol(state.symbol)
        last_event_at = _optional_utc_datetime(state.last_event_at)
        row = await self._session.get(StreamStateRow, (symbol, state.stream_name))
        if row is None:
            row = StreamStateRow(
                symbol=symbol,
                stream_name=state.stream_name,
                status=state.status,
                last_event_at=last_event_at,
                details=state.details or {},
            )
            self._session.add(row)
        else:
            row.status = state.status
            row.last_event_at = last_event_at
            row.details = state.details or {}
        await self._session.flush()

    async def list_stream_states(
        self,
        symbol: str | None = None,
        *,
        limit: int | None = None,
        offset: int = 0,
    ) -> tuple[StreamState, ...]:
        statement = select(StreamStateRow)
        if symbol is not None:
            statement = statement.where(StreamStateRow.symbol == normalize_symbol(symbol))
        statement = statement.order_by(
            StreamStateRow.symbol, StreamStateRow.stream_name
        ).offset(offset)
        if limit is not None:
            statement = statement.limit(limit)
        rows = (await self._session.execute(statement)).scalars().all()
        return tuple(
            StreamState(
                row.symbol,
                row.stream_name,
                row.last_event_at,
                row.status,
                dict(row.details),
                row.updated_at,
            )
            for row in rows
        )

    async def list_active_stream_states(self) -> tuple[StreamState, ...]:
        statement = (
            select(StreamStateRow)
            .join(SymbolRow, SymbolRow.symbol == StreamStateRow.symbol)
            .where(SymbolRow.enabled.is_(True))
            .order_by(StreamStateRow.symbol, StreamStateRow.stream_name)
        )
        rows = (await self._session.execute(statement)).scalars().all()
        return tuple(
            StreamState(
                row.symbol,
                row.stream_name,
                row.last_event_at,
                row.status,
                dict(row.details),
                row.updated_at,
            )
            for row in rows
        )

    async def get_active_stream_health(
        self, *, checked_at: datetime, stale_after: timedelta
    ) -> ActiveStreamHealth:
        checked_at = _require_utc(checked_at)
        _require_positive_duration(stale_after)
        expected_name = or_(
            *(
                StreamStateRow.stream_name
                == func.lower(SymbolRow.symbol) + literal(suffix)
                for suffix in (
                    "@aggTrade",
                    "@bookTicker",
                    "@kline_1m",
                    "@markPrice@1s",
                )
            )
        )
        active_count = (
            select(func.count())
            .select_from(SymbolRow)
            .where(SymbolRow.enabled.is_(True))
            .scalar_subquery()
        )
        observed_count = (
            select(func.count())
            .select_from(StreamStateRow)
            .join(SymbolRow, SymbolRow.symbol == StreamStateRow.symbol)
            .where(SymbolRow.enabled.is_(True), expected_name)
            .scalar_subquery()
        )
        healthy_count = (
            select(
                func.coalesce(
                    func.sum(
                        case(
                            (
                                (StreamStateRow.status == "connected")
                                & (StreamStateRow.last_event_at.is_not(None))
                                & (
                                    StreamStateRow.last_event_at
                                    >= checked_at - stale_after
                                )
                                & (StreamStateRow.last_event_at <= checked_at),
                                1,
                            ),
                            else_=0,
                        )
                    ),
                    0,
                )
            )
            .select_from(StreamStateRow)
            .join(SymbolRow, SymbolRow.symbol == StreamStateRow.symbol)
            .where(SymbolRow.enabled.is_(True), expected_name)
            .scalar_subquery()
        )
        statement = select(
            (active_count * 4).label("expected_count"),
            observed_count.label("observed_count"),
            healthy_count.label("healthy_count"),
        )
        row = (await self._session.execute(statement)).first()
        if row is None:
            return ActiveStreamHealth(0, 0, 0)
        return ActiveStreamHealth(*(int(value or 0) for value in row))

    async def get_symbol_summary(self, symbol: str) -> SymbolOperationalSummary:
        normalized = normalize_symbol(symbol)
        state = await self.get_symbol(normalized)
        if state is None:
            return SymbolOperationalSummary()
        return (await self.list_symbol_summaries((state,)))[normalized]

    async def list_symbol_summaries(
        self, symbols: tuple[SymbolState, ...]
    ) -> dict[str, SymbolOperationalSummary]:
        if not symbols:
            return {}
        symbol_names = tuple(state.symbol for state in symbols)
        current_versions = (
            select(
                DataPartitionRow.symbol.label("symbol"),
                DataPartitionRow.dataset.label("dataset"),
                DataPartitionRow.partition_date.label("partition_date"),
                func.max(DataPartitionRow.version).label("version"),
            )
            .where(
                DataPartitionRow.symbol.in_(symbol_names),
                DataPartitionRow.dataset.in_(_REQUIRED_SUMMARY_DATASETS),
                DataPartitionRow.approval_status == "approved",
            )
            .group_by(
                DataPartitionRow.symbol,
                DataPartitionRow.dataset,
                DataPartitionRow.partition_date,
            )
            .subquery()
        )
        partition_statement = (
            select(DataPartitionRow, DataManifestRow)
            .join(
                current_versions,
                (current_versions.c.symbol == DataPartitionRow.symbol)
                & (current_versions.c.dataset == DataPartitionRow.dataset)
                & (
                    current_versions.c.partition_date
                    == DataPartitionRow.partition_date
                )
                & (current_versions.c.version == DataPartitionRow.version),
            )
            .join(SymbolRow, SymbolRow.symbol == DataPartitionRow.symbol)
            .join(DataManifestRow, DataManifestRow.partition_id == DataPartitionRow.id)
            .where(
                DataPartitionRow.symbol.in_(symbol_names),
                DataPartitionRow.dataset.in_(_REQUIRED_SUMMARY_DATASETS),
                DataPartitionRow.approval_status == "approved",
                DataPartitionRow.partition_date
                >= func.to_char(
                    func.date_trunc("month", SymbolRow.history_start), "YYYY-MM-DD"
                ),
                DataPartitionRow.partition_date
                <= func.to_char(SymbolRow.history_end, "YYYY-MM-DD"),
            )
            .order_by(
                DataPartitionRow.symbol,
                DataPartitionRow.dataset,
                DataPartitionRow.partition_date,
                DataPartitionRow.id,
            )
        )
        checked_at = utc_now()
        ranked_metadata = (
            select(
                SymbolMetadataSnapshotRow.id.label("id"),
                func.row_number()
                .over(
                    partition_by=SymbolMetadataSnapshotRow.symbol,
                    order_by=(
                        SymbolMetadataSnapshotRow.captured_at.desc(),
                        SymbolMetadataSnapshotRow.id.desc(),
                    ),
                )
                .label("row_number"),
            )
            .where(
                SymbolMetadataSnapshotRow.symbol.in_(symbol_names),
                SymbolMetadataSnapshotRow.source == _TRUSTED_METADATA_SOURCE,
                SymbolMetadataSnapshotRow.captured_at
                >= checked_at - _METADATA_MAX_AGE,
                SymbolMetadataSnapshotRow.captured_at <= checked_at,
            )
            .subquery()
        )
        metadata_statement = (
            select(SymbolMetadataSnapshotRow)
            .join(ranked_metadata, ranked_metadata.c.id == SymbolMetadataSnapshotRow.id)
            .where(ranked_metadata.c.row_number == 1)
            .order_by(SymbolMetadataSnapshotRow.symbol)
        )
        gap_statement = (
            select(DataGapRow.symbol, func.count())
            .select_from(DataGapRow)
            .where(
                DataGapRow.symbol.in_(symbol_names),
                DataGapRow.status == "open",
            )
            .group_by(DataGapRow.symbol)
        )
        object_stats = _object_status_stats()
        effective_status = _effective_job_status_sql(object_stats)
        job_status_statement = (
            select(
                IngestionJobRow.symbol,
                effective_status.label("effective_status"),
                func.count(),
            )
            .outerjoin(object_stats, object_stats.c.job_id == IngestionJobRow.id)
            .where(IngestionJobRow.symbol.in_(symbol_names))
            .group_by(IngestionJobRow.symbol, effective_status)
        )
        intervals_by_symbol: dict[str, list[ArchiveCoverageInterval]] = {
            symbol: [] for symbol in symbol_names
        }
        for partition, stored in (
            await self._session.execute(partition_statement)
        ).all():
            manifest = DataManifest.model_validate_json(json.dumps(stored.manifest))
            if (
                manifest.validation_state is ValidationState.VALIDATED
                and manifest.instrument.symbol == partition.symbol
                and manifest.data_type.value == partition.dataset
            ):
                intervals_by_symbol[partition.symbol].extend(
                    ArchiveCoverageInterval(partition.dataset, start, end)
                    for start, end in _manifest_coverage_intervals(manifest)
                )
        metadata_by_symbol = {
            snapshot.symbol: snapshot
            for snapshot in (
                await self._session.execute(metadata_statement)
            ).scalars().all()
        }
        gaps_by_symbol = {
            symbol: int(count)
            for symbol, count in (
                await self._session.execute(gap_statement)
            ).all()
        }
        statuses_by_symbol: dict[str, set[str]] = {
            symbol: set() for symbol in symbol_names
        }
        for symbol, status, _count in (
            await self._session.execute(job_status_statement)
        ).all():
            statuses_by_symbol[symbol].add(status)
        return {
            symbol: SymbolOperationalSummary(
                approved_data_types=tuple(
                    sorted(
                        {
                            interval.dataset
                            for interval in intervals_by_symbol[symbol]
                        }
                    )
                ),
                archive_intervals=tuple(intervals_by_symbol[symbol]),
                metadata_verified=_trusted_metadata_snapshot(
                    metadata_by_symbol.get(symbol), symbol, checked_at
                ),
                open_gap_count=gaps_by_symbol.get(symbol, 0),
                job_statuses=tuple(sorted(statuses_by_symbol[symbol])),
            )
            for symbol in symbol_names
        }

    async def get_worker_heartbeat(self, worker_id: str) -> WorkerHeartbeat | None:
        row = await self._session.get(WorkerHeartbeatRow, worker_id)
        if row is None:
            return None
        return WorkerHeartbeat(
            row.worker_id,
            row.status,
            row.heartbeat_at,
            dict(row.details),
        )

    async def count_failed_jobs(self) -> int:
        object_stats = _object_status_stats()
        effective_status = _effective_job_status_sql(object_stats)
        statement = (
            select(func.count())
            .select_from(IngestionJobRow)
            .outerjoin(object_stats, object_stats.c.job_id == IngestionJobRow.id)
            .where(effective_status == "failed")
        )
        return int((await self._session.scalar(statement)) or 0)

    async def update_worker_heartbeat(self, heartbeat: WorkerHeartbeat) -> None:
        heartbeat_at = _require_utc(heartbeat.heartbeat_at)
        if not heartbeat.worker_id:
            raise ValueError("worker id must not be empty")
        row = await self._session.get(WorkerHeartbeatRow, heartbeat.worker_id)
        if row is None:
            row = WorkerHeartbeatRow(
                worker_id=heartbeat.worker_id,
                status=heartbeat.status,
                heartbeat_at=heartbeat_at,
                details=heartbeat.details or {},
            )
            self._session.add(row)
        else:
            row.status = heartbeat.status
            row.heartbeat_at = heartbeat_at
            row.details = heartbeat.details or {}
        await self._session.flush()

    async def record_source_transition(self, transition: SourceTransition) -> None:
        at = _require_utc(transition.at)
        if not transition.worker_id:
            raise ValueError("worker id must not be empty")
        self._session.add(
            AuditEventRow(
                id=new_id(),
                action="market_source_transition",
                subject_type="market_worker",
                subject_id=transition.worker_id,
                details={
                    "at": at.isoformat(),
                    "from_mode": transition.from_mode,
                    "to_mode": transition.to_mode,
                    "reason": transition.reason,
                    **(transition.details or {}),
                },
            )
        )
        await self._session.flush()

    async def plan_backfill_object(self, work: BackfillObject) -> BackfillObject:
        await self._lock_identity(
            f"crypto-research:backfill-object:{work.object_id}"
        )
        existing = await self._session.get(
            BackfillObjectRow, work.object_id, populate_existing=True
        )
        if existing is not None:
            if _backfill_identity(existing) != (
                work.job_id,
                work.source_url,
                work.start,
                work.end,
            ):
                raise MutationIdentityConflict("backfill object identity is immutable")
            return _backfill_object(existing)
        row = BackfillObjectRow(
            id=work.object_id,
            job_id=work.job_id,
            source_url=work.source_url,
            source_checksum=work.source_checksum,
            start_at=_require_utc(work.start),
            end_at=_require_utc(work.end),
            state=work.state.value,
            raw_path=work.raw_path,
            normalized_path=work.normalized_path,
            normalized_checksum=work.normalized_checksum,
            row_count=work.row_count,
            partition_id=work.partition_id,
            manifest_id=work.manifest_id,
            attempt_count=work.attempt_count,
            last_error=work.last_error,
        )
        self._session.add(row)
        await self._session.flush()
        return _backfill_object(row)

    async def plan(self, work: BackfillObject) -> BackfillObject:
        return await self.plan_backfill_object(work)

    async def claim_backfill_object(
        self, worker_id: str, now: datetime, duration: timedelta
    ) -> BackfillObject | None:
        now = _require_utc(now)
        _require_positive_duration(duration)
        statement = (
            select(BackfillObjectRow)
            .join(IngestionJobRow, IngestionJobRow.id == BackfillObjectRow.job_id)
            .join(SymbolRow, SymbolRow.symbol == IngestionJobRow.symbol)
            .where(
                SymbolRow.enabled.is_(True),
                BackfillObjectRow.state.in_(
                    (
                        BackfillState.PLANNED.value,
                        BackfillState.DOWNLOADING.value,
                        BackfillState.CHECKSUM_VERIFIED.value,
                        BackfillState.NORMALIZED.value,
                        BackfillState.VALIDATED.value,
                    )
                ),
                or_(
                    BackfillObjectRow.lease_expires_at.is_(None),
                    BackfillObjectRow.lease_expires_at <= func.now(),
                ),
            )
            .order_by(BackfillObjectRow.created_at, BackfillObjectRow.id)
            .limit(1)
            .with_for_update(skip_locked=True)
        )
        result = await self._session.execute(statement)
        row = result.scalars().first()
        if row is None:
            return None
        claim_statement = (
            update(BackfillObjectRow)
            .where(BackfillObjectRow.id == row.id)
            .values(
                lease_owner=worker_id,
                lease_expires_at=func.now() + duration,
                attempt_count=BackfillObjectRow.attempt_count + 1,
                updated_at=func.now(),
            )
            .returning(BackfillObjectRow)
            .execution_options(populate_existing=True)
        )
        claimed = (await self._session.execute(claim_statement)).scalars().first()
        if claimed is None:
            raise ValueError("backfill object could not be leased")
        return _backfill_object(claimed)

    async def claim(
        self, worker_id: str, now: datetime, duration: timedelta
    ) -> BackfillObject | None:
        return await self.claim_backfill_object(worker_id, now, duration)

    async def claim_backfill_object_row(
        self,
        row: BackfillObjectRow,
        worker_id: str,
        now: datetime,
        duration: timedelta,
    ) -> BackfillObject:
        now = _require_utc(now)
        _require_positive_duration(duration)
        if row.lease_expires_at is not None and row.lease_expires_at > now:
            raise ValueError("backfill object already has an unexpired lease")
        row.lease_owner = worker_id
        row.lease_expires_at = now + duration
        row.attempt_count += 1
        await self._session.flush()
        return _backfill_object(row)

    async def renew_backfill_object(
        self,
        object_id: str,
        worker_id: str,
        lease_attempt: int,
        now: datetime,
        duration: timedelta,
    ) -> BackfillObject:
        now = _require_utc(now)
        _require_positive_duration(duration)
        row = await self._backfill_row(object_id)
        _require_row_lease(row, worker_id, lease_attempt, now)
        row.lease_expires_at = now + duration
        await self._session.flush()
        return _backfill_object(row)

    async def renew(
        self,
        object_id: str,
        worker_id: str,
        lease_attempt: int,
        now: datetime,
        duration: timedelta,
    ) -> BackfillObject:
        now = _require_utc(now)
        _require_positive_duration(duration)
        statement = (
            update(BackfillObjectRow)
            .where(
                BackfillObjectRow.id == object_id,
                BackfillObjectRow.lease_owner == worker_id,
                BackfillObjectRow.attempt_count == lease_attempt,
                BackfillObjectRow.lease_expires_at > func.now(),
            )
            .values(
                lease_expires_at=func.now() + duration,
                updated_at=func.now(),
            )
            .returning(BackfillObjectRow)
            .execution_options(populate_existing=True)
        )
        row = (await self._session.execute(statement)).scalars().first()
        if row is None:
            raise ValueError("backfill lease is not owned, current, and unexpired")
        return _backfill_object(row)

    async def advance_backfill_object(
        self,
        object_id: str,
        worker_id: str,
        lease_attempt: int,
        now: datetime,
        target: BackfillState,
        *,
        evidence: dict[str, Any] | None = None,
    ) -> BackfillObject:
        now = _require_utc(now)
        row = await self._backfill_row(object_id)
        return await self._advance_row(
            row, worker_id, lease_attempt, now, target, evidence=evidence
        )

    async def _advance_row(
        self,
        row: BackfillObjectRow,
        worker_id: str,
        lease_attempt: int,
        now: datetime,
        target: BackfillState,
        *,
        evidence: dict[str, Any] | None = None,
    ) -> BackfillObject:
        current = BackfillState(row.state)
        if not is_legal_backfill_transition(current, target):
            raise ValueError(f"illegal backfill transition: {current} -> {target}")
        _require_row_lease(row, worker_id, lease_attempt, now)
        evidence = evidence or {}
        for name in (
            "source_checksum",
            "raw_path",
            "normalized_path",
            "normalized_checksum",
            "row_count",
            "partition_id",
            "manifest_id",
            "last_error",
        ):
            if name in evidence:
                setattr(row, name, evidence[name])
        row.state = target.value
        row.updated_at = now
        timestamp_field = {
            BackfillState.DOWNLOADING: "downloading_at",
            BackfillState.CHECKSUM_VERIFIED: "checksum_verified_at",
            BackfillState.NORMALIZED: "normalized_at",
            BackfillState.VALIDATED: "validated_at",
            BackfillState.CATALOG_APPROVED: "catalog_approved_at",
            BackfillState.SOURCE_PENDING: "terminal_at",
            BackfillState.FAILED: "terminal_at",
        }[target]
        setattr(row, timestamp_field, now)
        if target in {
            BackfillState.CATALOG_APPROVED,
            BackfillState.SOURCE_PENDING,
            BackfillState.FAILED,
        }:
            row.lease_owner = None
            row.lease_expires_at = None
        await self._session.flush()
        return _backfill_object(row)

    async def advance(
        self,
        object_id: str,
        worker_id: str,
        lease_attempt: int,
        now: datetime,
        target: BackfillState,
        *,
        evidence: dict[str, Any] | None = None,
    ) -> BackfillObject:
        _require_utc(now)
        evidence = evidence or {}
        allowed_current = _allowed_current_states(target)
        values: dict[str, Any] = {
            "state": target.value,
            "updated_at": func.now(),
            _state_timestamp_field(target): func.now(),
        }
        values.update(
            {
                name: evidence[name]
                for name in (
                    "source_checksum",
                    "raw_path",
                    "normalized_path",
                    "normalized_checksum",
                    "row_count",
                    "partition_id",
                    "manifest_id",
                    "last_error",
                )
                if name in evidence
            }
        )
        if target in {
            BackfillState.CATALOG_APPROVED,
            BackfillState.SOURCE_PENDING,
            BackfillState.FAILED,
        }:
            values.update(lease_owner=None, lease_expires_at=None)
        statement = (
            update(BackfillObjectRow)
            .where(
                BackfillObjectRow.id == object_id,
                BackfillObjectRow.lease_owner == worker_id,
                BackfillObjectRow.attempt_count == lease_attempt,
                BackfillObjectRow.lease_expires_at > func.now(),
                BackfillObjectRow.state.in_(allowed_current),
            )
            .values(**values)
            .returning(BackfillObjectRow)
            .execution_options(populate_existing=True)
        )
        row = (await self._session.execute(statement)).scalars().first()
        if row is None:
            raise ValueError("backfill transition rejected by state or current lease token")
        return _backfill_object(row)

    async def retry_backfill_object(
        self, object_id: str, now: datetime
    ) -> BackfillObject:
        now = _require_utc(now)
        row = await self._backfill_row(object_id)
        if row.state not in {
            BackfillState.FAILED.value,
            BackfillState.SOURCE_PENDING.value,
        }:
            raise ValueError(f"backfill object is not eligible for retry: {row.state}")
        row.state = BackfillState.PLANNED.value
        row.updated_at = now
        row.lease_owner = None
        row.lease_expires_at = None
        await self._session.flush()
        return _backfill_object(row)

    async def retry(self, object_id: str, now: datetime) -> BackfillObject:
        return await self.retry_backfill_object(object_id, now)

    async def checkpoint(self) -> None:
        await self._session.commit()

    async def _backfill_row(self, object_id: str) -> BackfillObjectRow:
        row = await self._session.get(BackfillObjectRow, object_id)
        if row is None:
            raise ValueError(f"backfill object does not exist: {object_id}")
        return row

    def _add_audit(
        self,
        action: str,
        subject_type: str,
        subject_id: str,
        details: dict[str, Any] | None = None,
    ) -> None:
        self._session.add(
            AuditEventRow(
                id=new_id(),
                action=action,
                subject_type=subject_type,
                subject_id=subject_id,
                details=details or {},
            )
        )

    async def _lock_identity(self, lock_key: str) -> None:
        await self._session.execute(
            select(func.pg_advisory_xact_lock(func.hashtextextended(lock_key, 0)))
        )


def _as_utc_datetime(value: str | datetime) -> datetime:
    if isinstance(value, str):
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    else:
        parsed = value
    return _require_utc(parsed)


def _optional_utc_datetime(value: datetime | None) -> datetime | None:
    return None if value is None else _require_utc(value)


def _require_utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() != timedelta(0):
        raise ValueError("datetime must be UTC-aware with a zero offset")
    return value


def _symbol_state(row: SymbolRow) -> SymbolState:
    return SymbolState(
        symbol=row.symbol,
        enabled=row.enabled,
        history_start=row.history_start,
        history_end=row.history_end,
        include_agg_trades=row.include_agg_trades,
        created_at=row.created_at,
        updated_at=row.updated_at,
    )


def _ingestion_job(
    row: IngestionJobRow, *, status: str | None = None
) -> IngestionJob:
    return IngestionJob(
        id=row.id,
        symbol=row.symbol,
        dataset=row.dataset,
        status=status or row.status,
        requested_start=row.requested_start,
        requested_end=row.requested_end,
        created_at=row.created_at,
        updated_at=row.updated_at,
    )


def _effective_job_status(persisted: str, object_states: Any) -> str:
    states = tuple(object_states)
    if not states:
        return persisted
    if any(state == BackfillState.FAILED.value for state in states):
        return "failed"
    if any(state == BackfillState.SOURCE_PENDING.value for state in states):
        return "source_pending"
    if all(state == BackfillState.CATALOG_APPROVED.value for state in states):
        return "succeeded"
    active = {
        BackfillState.DOWNLOADING.value,
        BackfillState.CHECKSUM_VERIFIED.value,
        BackfillState.NORMALIZED.value,
        BackfillState.VALIDATED.value,
    }
    if any(state in active for state in states):
        return "running"
    return "queued"


def _object_status_stats():
    active_states = (
        BackfillState.DOWNLOADING.value,
        BackfillState.CHECKSUM_VERIFIED.value,
        BackfillState.NORMALIZED.value,
        BackfillState.VALIDATED.value,
    )
    return (
        select(
            BackfillObjectRow.job_id.label("job_id"),
            func.count().label("object_count"),
            func.sum(
                case((BackfillObjectRow.state == BackfillState.FAILED.value, 1), else_=0)
            ).label("failed_count"),
            func.sum(
                case(
                    (
                        BackfillObjectRow.state
                        == BackfillState.SOURCE_PENDING.value,
                        1,
                    ),
                    else_=0,
                )
            ).label("source_pending_count"),
            func.sum(
                case(
                    (
                        BackfillObjectRow.state
                        == BackfillState.CATALOG_APPROVED.value,
                        1,
                    ),
                    else_=0,
                )
            ).label("approved_count"),
            func.sum(
                case((BackfillObjectRow.state.in_(active_states), 1), else_=0)
            ).label("active_count"),
        )
        .group_by(BackfillObjectRow.job_id)
        .subquery()
    )


def _effective_job_status_sql(object_stats):
    return case(
        (object_stats.c.object_count.is_(None), IngestionJobRow.status),
        (object_stats.c.failed_count > 0, "failed"),
        (object_stats.c.source_pending_count > 0, "source_pending"),
        (
            object_stats.c.approved_count == object_stats.c.object_count,
            "succeeded",
        ),
        (object_stats.c.active_count > 0, "running"),
        else_="queued",
    )


def _trusted_metadata_snapshot(
    snapshot: SymbolMetadataSnapshotRow | None,
    symbol: str,
    checked_at: datetime,
) -> bool:
    if (
        snapshot is None
        or snapshot.symbol != symbol
        or snapshot.source != _TRUSTED_METADATA_SOURCE
        or snapshot.captured_at > checked_at
        or snapshot.captured_at < checked_at - _METADATA_MAX_AGE
        or not isinstance(snapshot.payload, dict)
    ):
        return False
    symbols = snapshot.payload.get("symbols")
    if not isinstance(symbols, list):
        return False
    return any(
        isinstance(item, dict)
        and item.get("symbol") == symbol
        and item.get("contractType") == "PERPETUAL"
        and item.get("status") == "TRADING"
        for item in symbols
    )


def _manifest_coverage_intervals(
    manifest: DataManifest,
) -> tuple[tuple[datetime, datetime], ...]:
    """Return validated manifest coverage with declared holes removed."""
    coverage: list[tuple[datetime, datetime]] = []
    cursor = manifest.start
    for missing in sorted(
        manifest.missing_intervals, key=lambda interval: (interval.start, interval.end)
    ):
        if missing.end <= cursor:
            continue
        if missing.start > cursor:
            coverage.append((cursor, missing.start))
        cursor = max(cursor, missing.end)
    if cursor < manifest.end:
        coverage.append((cursor, manifest.end))
    return tuple(coverage)


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
        created_at=row.created_at,
        approved_at=row.approved_at,
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
        opened_at=row.opened_at,
    )


def _manifest_partition(
    row: DataPartitionRow, stored: DataManifestRow
) -> DataPartition:
    manifest = DataManifest.model_validate_json(json.dumps(stored.manifest))
    return DataPartition(
        id=row.id,
        symbol=row.symbol,
        dataset=row.dataset,
        partition_date=row.partition_date,
        version=row.version,
        approval_status=row.approval_status,
        checksum_sha256=row.checksum_sha256,
        parquet_path=row.parquet_path,
        start_at=manifest.start,
        end_at=manifest.end,
        row_count=manifest.row_count,
        created_at=row.created_at,
        approved_at=row.approved_at,
    )


def _live_partition(row: LiveDataPartitionRow) -> DataPartition:
    epoch = datetime(1970, 1, 1, tzinfo=utc_now().tzinfo)
    start = epoch + timedelta(milliseconds=row.min_canonical_time)
    end = epoch + timedelta(milliseconds=row.max_canonical_time + 1)
    return DataPartition(
        id=row.id,
        symbol=row.symbol,
        dataset=_live_data_type(row.dataset).value,
        partition_date=row.partition_date,
        version=1,
        approval_status=row.approval_status,
        checksum_sha256=row.checksum_sha256,
        parquet_path=row.relative_path,
        start_at=start,
        end_at=end,
        row_count=row.row_count,
        created_at=row.created_at,
        approved_at=row.approved_at,
    )


def _live_data_type(dataset: str) -> DataType:
    try:
        return {
            "klines": DataType.KLINE_1M,
            "agg_trades": DataType.AGG_TRADE,
            "mark_price": DataType.MARK_PRICE,
            "book_ticker": DataType.BEST_BID_ASK,
        }[dataset]
    except KeyError as error:
        raise ValueError(f"unsupported persisted live dataset: {dataset}") from error


def _gap_reason(value: str) -> GapReason:
    try:
        return GapReason(value)
    except ValueError:
        return GapReason.SOURCE_UNKNOWN


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


def _backfill_object(row: BackfillObjectRow) -> BackfillObject:
    timestamp_fields = {
        BackfillState.DOWNLOADING: row.downloading_at,
        BackfillState.CHECKSUM_VERIFIED: row.checksum_verified_at,
        BackfillState.NORMALIZED: row.normalized_at,
        BackfillState.VALIDATED: row.validated_at,
        BackfillState.CATALOG_APPROVED: row.catalog_approved_at,
    }
    if row.state in {BackfillState.SOURCE_PENDING.value, BackfillState.FAILED.value}:
        timestamp_fields[BackfillState(row.state)] = row.terminal_at
    return BackfillObject(
        object_id=row.id,
        job_id=row.job_id,
        source_url=row.source_url,
        start=row.start_at,
        end=row.end_at,
        state=BackfillState(row.state),
        source_checksum=row.source_checksum,
        raw_path=row.raw_path,
        normalized_path=row.normalized_path,
        normalized_checksum=row.normalized_checksum,
        row_count=row.row_count,
        partition_id=row.partition_id,
        manifest_id=row.manifest_id,
        lease_owner=row.lease_owner,
        lease_expires_at=row.lease_expires_at,
        attempt_count=row.attempt_count,
        last_error=row.last_error,
        state_timestamps={state: value for state, value in timestamp_fields.items() if value},
    )


def _backfill_identity(row: BackfillObjectRow) -> tuple[object, ...]:
    return (row.job_id, row.source_url, row.start_at, row.end_at)


def _require_positive_duration(duration: timedelta) -> None:
    if duration <= timedelta(0):
        raise ValueError("lease duration must be positive")


def _require_row_lease(
    row: BackfillObjectRow, worker_id: str, lease_attempt: int, now: datetime
) -> None:
    if (
        row.lease_owner != worker_id
        or row.attempt_count != lease_attempt
        or row.lease_expires_at is None
        or row.lease_expires_at <= now
    ):
        raise ValueError("backfill lease is not owned and unexpired")


def _allowed_current_states(target: BackfillState) -> tuple[str, ...]:
    predecessor = {
        BackfillState.DOWNLOADING: BackfillState.PLANNED,
        BackfillState.CHECKSUM_VERIFIED: BackfillState.DOWNLOADING,
        BackfillState.NORMALIZED: BackfillState.CHECKSUM_VERIFIED,
        BackfillState.VALIDATED: BackfillState.NORMALIZED,
        BackfillState.CATALOG_APPROVED: BackfillState.VALIDATED,
    }.get(target)
    if predecessor is not None:
        return (predecessor.value,)
    if target in {BackfillState.SOURCE_PENDING, BackfillState.FAILED}:
        return (
            BackfillState.PLANNED.value,
            BackfillState.DOWNLOADING.value,
            BackfillState.CHECKSUM_VERIFIED.value,
            BackfillState.NORMALIZED.value,
            BackfillState.VALIDATED.value,
        )
    return ()


def _state_timestamp_field(target: BackfillState) -> str:
    return {
        BackfillState.DOWNLOADING: "downloading_at",
        BackfillState.CHECKSUM_VERIFIED: "checksum_verified_at",
        BackfillState.NORMALIZED: "normalized_at",
        BackfillState.VALIDATED: "validated_at",
        BackfillState.CATALOG_APPROVED: "catalog_approved_at",
        BackfillState.SOURCE_PENDING: "terminal_at",
        BackfillState.FAILED: "terminal_at",
    }[target]
