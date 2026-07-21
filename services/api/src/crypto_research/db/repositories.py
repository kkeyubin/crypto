import json
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Protocol

from sqlalchemy import func, or_, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from crypto_research.contracts.manifest import DataManifest, DataType
from crypto_research.db.models import (
    AuditEventRow,
    BackfillObjectRow,
    DataGapRow,
    DataManifestRow,
    DataPartitionRow,
    IngestionJobRow,
    StreamStateRow,
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
    async def list_active_symbols(self) -> tuple[SymbolState, ...]: ...

    async def add_symbol(self, command: AddSymbolCommand) -> SymbolState: ...

    async def create_backfill(self, command: BackfillCommand) -> IngestionJob: ...

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

    async def list_stream_states(self) -> tuple[StreamState, ...]: ...

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
            .with_for_update()
        )
        rows = (await self._session.execute(statement)).all()
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
            base = {
                "symbol": partition.symbol,
                "data_type": DataType(partition.dataset),
                "time_range": TimeRange(manifest.start, manifest.end),
                "partition_id": partition.id,
            }
            recovered_ranges = validation.get("recovered_id_ranges")
            if isinstance(recovered_ranges, list):
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
                                **base,
                                recovered_id_start=recovered[0],
                                recovered_id_end=recovered[1],
                            )
                        )
                continue
            coverage.append(ApprovedCoverage(**base))
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
        row = await self._session.get(SymbolRow, symbol)
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
        return _symbol_state(row)

    async def list_active_symbols(self) -> tuple[SymbolState, ...]:
        statement = (
            select(SymbolRow)
            .where(SymbolRow.enabled.is_(True))
            .order_by(SymbolRow.symbol)
        )
        rows = (await self._session.execute(statement)).scalars().all()
        return tuple(_symbol_state(row) for row in rows if row.enabled)

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
        requested_start = _optional_utc_datetime(command.requested_start)
        requested_end = _optional_utc_datetime(command.requested_end)
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
            requested_start=requested_start,
            requested_end=requested_end,
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
            raise ValueError(f"gap does not exist: {gap_id}")
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

    async def list_stream_states(self) -> tuple[StreamState, ...]:
        statement = select(StreamStateRow).order_by(
            StreamStateRow.symbol, StreamStateRow.stream_name
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
        existing = await self._session.get(BackfillObjectRow, work.object_id)
        if existing is not None:
            if _backfill_identity(existing) != (
                work.job_id,
                work.source_url,
                work.start,
                work.end,
            ):
                raise ValueError("backfill object identity is immutable")
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
            .where(
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
