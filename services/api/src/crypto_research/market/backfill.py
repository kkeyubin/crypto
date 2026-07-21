from __future__ import annotations

import asyncio
import hashlib
import json
import os
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from pathlib import Path
from typing import Protocol, TypeVar
from uuid import NAMESPACE_URL, uuid5

import pyarrow as pa
import pyarrow.parquet as pq

from crypto_research.contracts.manifest import (
    DataManifest,
    DataType,
    DeduplicationMethod,
    MissingInterval,
    ValidationState,
)
from crypto_research.contracts.strategy import InstrumentRef
from crypto_research.market.binance.archive import (
    ArchiveChecksumError,
    ArchiveNotFoundError,
    HttpClient,
    fetch_archive,
    read_verified_archive_csv,
)
from crypto_research.market.binance.archive_paths import ArchiveObject, DatasetKind
from crypto_research.market.binance.normalization import (
    AGG_TRADE_SCHEMA,
    FUNDING_SCHEMA,
    KLINE_SCHEMA,
    normalize_csv,
)
from crypto_research.market.catalog import CatalogCandidate, CatalogPartition
from crypto_research.market.gaps import (
    DetectedGap,
    TimeRange,
    detect_aggregate_trade_id_gaps,
    detect_minute_gaps,
)
from crypto_research.market.storage import (
    open_secure_relative_file,
    retain_raw_archive,
    write_versioned_normalized_parquet,
)

T = TypeVar("T")


class BackfillState(StrEnum):
    PLANNED = "planned"
    DOWNLOADING = "downloading"
    CHECKSUM_VERIFIED = "checksum_verified"
    NORMALIZED = "normalized"
    VALIDATED = "validated"
    CATALOG_APPROVED = "catalog_approved"
    SOURCE_PENDING = "source_pending"
    FAILED = "failed"


class MissingArchiveDisposition(StrEnum):
    PENDING = "source_pending"
    FAILED_WITH_GAP = "failed_with_gap"


_NEXT_STATE = {
    BackfillState.PLANNED: BackfillState.DOWNLOADING,
    BackfillState.DOWNLOADING: BackfillState.CHECKSUM_VERIFIED,
    BackfillState.CHECKSUM_VERIFIED: BackfillState.NORMALIZED,
    BackfillState.NORMALIZED: BackfillState.VALIDATED,
    BackfillState.VALIDATED: BackfillState.CATALOG_APPROVED,
}
_ACTIVE_STATES = frozenset(_NEXT_STATE)
_RETRYABLE_STATES = frozenset({BackfillState.FAILED, BackfillState.SOURCE_PENDING})


@dataclass(frozen=True)
class BackfillObject:
    object_id: str
    job_id: str
    source_url: str
    start: datetime
    end: datetime
    state: BackfillState = BackfillState.PLANNED
    source_checksum: str = ""
    raw_path: str | None = None
    normalized_path: str | None = None
    normalized_checksum: str | None = None
    row_count: int | None = None
    partition_id: str | None = None
    manifest_id: str | None = None
    lease_owner: str | None = None
    lease_expires_at: datetime | None = None
    attempt_count: int = 0
    last_error: str | None = None
    state_timestamps: Mapping[BackfillState, datetime] = field(default_factory=dict)

    def __post_init__(self) -> None:
        _require_utc_range(self.start, self.end)


@dataclass(frozen=True)
class DownloadEvidence:
    source_checksum: str
    raw_path: str


@dataclass(frozen=True)
class NormalizeEvidence:
    normalized_path: str
    normalized_checksum: str
    row_count: int


@dataclass(frozen=True)
class PublishEvidence:
    partition_id: str
    manifest_id: str


class BackfillRepository(Protocol):
    async def plan(self, work: BackfillObject) -> BackfillObject: ...

    async def claim(
        self, worker_id: str, now: datetime, duration: timedelta
    ) -> BackfillObject | None: ...

    async def renew(
        self,
        object_id: str,
        worker_id: str,
        lease_attempt: int,
        now: datetime,
        duration: timedelta,
    ) -> BackfillObject: ...

    async def advance(
        self,
        object_id: str,
        worker_id: str,
        lease_attempt: int,
        now: datetime,
        target: BackfillState,
        *,
        evidence: Mapping[str, object] | None = None,
    ) -> BackfillObject: ...

    async def retry(self, object_id: str, now: datetime) -> BackfillObject: ...

    async def checkpoint(self) -> None: ...


class LeaseHeartbeatRepository(Protocol):
    async def renew(
        self,
        object_id: str,
        worker_id: str,
        lease_attempt: int,
        now: datetime,
        duration: timedelta,
    ) -> BackfillObject: ...

    async def checkpoint(self) -> None: ...


class BackfillStages(Protocol):
    async def download(self, work: BackfillObject) -> DownloadEvidence: ...

    async def normalize(self, work: BackfillObject) -> NormalizeEvidence: ...

    async def validate(self, work: BackfillObject) -> Mapping[str, object]: ...

    async def publish(self, work: BackfillObject) -> PublishEvidence: ...


class MissingArchiveGapSink(Protocol):
    async def record_missing_archive(self, work: BackfillObject) -> None: ...


class CatalogPublisher(Protocol):
    async def approve(self, candidate: CatalogCandidate) -> CatalogPartition: ...


class DetectedGapSink(Protocol):
    async def record_gap(self, gap: DetectedGap) -> None: ...


class BackfillRunner:
    """Lease one object and resume its persisted stage through catalog approval."""

    def __init__(
        self,
        repository: BackfillRepository,
        stages: BackfillStages,
        *,
        clock: Callable[[], datetime],
        gap_sink: MissingArchiveGapSink | None = None,
        heartbeat_repository: LeaseHeartbeatRepository | None = None,
    ) -> None:
        if heartbeat_repository is None and not getattr(
            repository, "allows_shared_heartbeat", False
        ):
            raise ValueError(
                "database-backed runners require an independent heartbeat repository"
            )
        self._repository = repository
        self._heartbeat_repository = heartbeat_repository or repository
        self._stages = stages
        self._clock = clock
        self._gap_sink = gap_sink

    async def run_once(
        self, worker_id: str, lease_duration: timedelta
    ) -> BackfillObject | None:
        work = await self._repository.claim(worker_id, self._clock(), lease_duration)
        if work is None:
            return None
        await self._repository.checkpoint()
        while work.state in _ACTIVE_STATES:
            if work.state is BackfillState.PLANNED:
                work = await self._advance(
                    work, worker_id, BackfillState.DOWNLOADING
                )
                continue
            if work.state is BackfillState.DOWNLOADING:
                try:
                    downloaded = await self._with_lease_heartbeat(
                        work,
                        worker_id,
                        lease_duration,
                        lambda current=work: self._stages.download(current),
                    )
                except ArchiveNotFoundError as error:
                    now = self._clock()
                    disposition = classify_missing_archive(work.start, work.end, now)
                    target = (
                        BackfillState.SOURCE_PENDING
                        if disposition is MissingArchiveDisposition.PENDING
                        else BackfillState.FAILED
                    )
                    if target is BackfillState.FAILED and self._gap_sink is not None:
                        await self._gap_sink.record_missing_archive(work)
                    return await self._advance(
                        work,
                        worker_id,
                        target,
                        evidence={"last_error": str(error)},
                    )
                except Exception as error:
                    return await self._fail(work, worker_id, error)
                work = await self._advance(
                    work,
                    worker_id,
                    BackfillState.CHECKSUM_VERIFIED,
                    evidence={
                        "source_checksum": downloaded.source_checksum,
                        "raw_path": downloaded.raw_path,
                    },
                )
                continue
            if work.state is BackfillState.CHECKSUM_VERIFIED:
                try:
                    normalized = await self._with_lease_heartbeat(
                        work,
                        worker_id,
                        lease_duration,
                        lambda current=work: self._stages.normalize(current),
                    )
                except Exception as error:
                    return await self._fail(work, worker_id, error)
                work = await self._advance(
                    work,
                    worker_id,
                    BackfillState.NORMALIZED,
                    evidence={
                        "normalized_path": normalized.normalized_path,
                        "normalized_checksum": normalized.normalized_checksum,
                        "row_count": normalized.row_count,
                    },
                )
                continue
            if work.state is BackfillState.NORMALIZED:
                try:
                    validation = await self._with_lease_heartbeat(
                        work,
                        worker_id,
                        lease_duration,
                        lambda current=work: self._stages.validate(current),
                    )
                except Exception as error:
                    return await self._fail(work, worker_id, error)
                failed = [
                    name
                    for name in (
                        "checksum",
                        "schema",
                        "ordering",
                        "uniqueness",
                        "range",
                        "row_count",
                    )
                    if validation.get(name) is not True
                ]
                if failed:
                    return await self._fail(
                        work,
                        worker_id,
                        ValueError(f"validation failed: {', '.join(failed)}"),
                    )
                work = await self._advance(
                    work, worker_id, BackfillState.VALIDATED
                )
                continue
            if work.state is BackfillState.VALIDATED:
                try:
                    published = await self._with_lease_heartbeat(
                        work,
                        worker_id,
                        lease_duration,
                        lambda current=work: self._stages.publish(current),
                    )
                except Exception as error:
                    return await self._fail(work, worker_id, error)
                return await self._advance(
                    work,
                    worker_id,
                    BackfillState.CATALOG_APPROVED,
                    evidence={
                        "partition_id": published.partition_id,
                        "manifest_id": published.manifest_id,
                    },
                )
        return work

    async def _fail(
        self,
        work: BackfillObject,
        worker_id: str,
        error: Exception,
    ) -> BackfillObject:
        return await self._advance(
            work,
            worker_id,
            BackfillState.FAILED,
            evidence={"last_error": f"{type(error).__name__}: {error}"},
        )

    async def _advance(
        self,
        work: BackfillObject,
        worker_id: str,
        target: BackfillState,
        *,
        evidence: Mapping[str, object] | None = None,
    ) -> BackfillObject:
        advanced = await self._repository.advance(
            work.object_id,
            worker_id,
            work.attempt_count,
            self._clock(),
            target,
            evidence=evidence,
        )
        await self._repository.checkpoint()
        return advanced

    async def _with_lease_heartbeat(
        self,
        work: BackfillObject,
        worker_id: str,
        lease_duration: timedelta,
        operation: Callable[[], Awaitable[T]],
    ) -> T:
        await self._heartbeat_repository.renew(
            work.object_id,
            worker_id,
            work.attempt_count,
            self._clock(),
            lease_duration,
        )
        await self._heartbeat_repository.checkpoint()
        task = asyncio.create_task(operation())
        interval = max(0.01, min(30.0, lease_duration.total_seconds() / 3))
        try:
            while True:
                done, _ = await asyncio.wait({task}, timeout=interval)
                if done:
                    return task.result()
                await self._heartbeat_repository.renew(
                    work.object_id,
                    worker_id,
                    work.attempt_count,
                    self._clock(),
                    lease_duration,
                )
                await self._heartbeat_repository.checkpoint()
        finally:
            if not task.done():
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)


class ArchiveBackfillStages:
    """Concrete, restart-safe Task 3 archive-to-catalog pipeline."""

    def __init__(
        self,
        archives: Mapping[str, ArchiveObject],
        data_root: Path,
        staging_root: Path,
        client: HttpClient,
        catalog: CatalogPublisher,
        gap_sink: DetectedGapSink,
        *,
        clock: Callable[[], datetime],
    ) -> None:
        self._archives = dict(archives)
        self._data_root = data_root
        self._staging_root = staging_root
        self._client = client
        self._catalog = catalog
        self._gap_sink = gap_sink
        self._clock = clock

    async def download(self, work: BackfillObject) -> DownloadEvidence:
        obj = self._object(work)
        staging_name = f"{hashlib.sha256(obj.url.encode()).hexdigest()}.zip"
        verified = await fetch_archive(
            obj, self._staging_root / staging_name, self._client
        )
        _require_expected_source_checksum(work, verified.sha256)
        retained = await asyncio.to_thread(
            retain_raw_archive,
            verified.path,
            self._data_root,
            obj.symbol,
            obj.dataset,
            obj.start,
            verified.sha256,
        )
        return DownloadEvidence(
            source_checksum=verified.sha256,
            raw_path=retained.relative_to(self._data_root).as_posix(),
        )

    async def normalize(self, work: BackfillObject) -> NormalizeEvidence:
        obj = self._object(work)
        if not work.raw_path or not work.source_checksum:
            raise ValueError("checksum-verified work requires retained raw evidence")
        stored = await asyncio.to_thread(
            _normalize_retained_archive,
            obj,
            self._data_root,
            work.raw_path,
            work.source_checksum,
        )
        return NormalizeEvidence(
            normalized_path=stored.path.relative_to(self._data_root).as_posix(),
            normalized_checksum=stored.sha256,
            row_count=stored.row_count,
        )

    async def validate(self, work: BackfillObject) -> Mapping[str, object]:
        obj = self._object(work)
        if (
            not work.normalized_path
            or not work.normalized_checksum
            or work.row_count is None
        ):
            raise ValueError("normalized work requires durable normalized evidence")
        table, actual_checksum = await asyncio.to_thread(
            _read_normalized_table,
            self._data_root,
            work.normalized_path,
        )
        expected_schema, ordering_key, range_key = _dataset_validation(obj.dataset)
        ordering_values = table.column(ordering_key).to_pylist()
        range_values = table.column(range_key).to_pylist()
        start_ms = _epoch_milliseconds(obj.start)
        end_ms = _epoch_milliseconds(obj.end)
        result: dict[str, object] = {
            "checksum": actual_checksum == work.normalized_checksum,
            "schema": table.schema.equals(expected_schema, check_metadata=True),
            "ordering": ordering_values == sorted(ordering_values),
            "uniqueness": len(ordering_values) == len(set(ordering_values)),
            "range": all(start_ms <= value < end_ms for value in range_values),
            "row_count": table.num_rows == work.row_count and table.num_rows > 0,
        }
        if all(result.get(name) is True for name in _VALIDATION_NAMES):
            gaps = _content_gaps(obj, table)
            result["missing_intervals"] = _serialize_content_gaps(gaps)
            for gap in gaps:
                await self._gap_sink.record_gap(gap)
        return result

    async def publish(self, work: BackfillObject) -> PublishEvidence:
        obj = self._object(work)
        if not all(
            (
                work.raw_path,
                work.normalized_path,
                work.source_checksum,
                work.normalized_checksum,
                work.row_count,
            )
        ):
            raise ValueError("validated work requires complete durable evidence")
        table, actual_checksum = await asyncio.to_thread(
            _read_normalized_table,
            self._data_root,
            work.normalized_path,
        )
        if actual_checksum != work.normalized_checksum:
            raise ValueError("normalized bytes changed after validation")
        validation_evidence = _catalog_validation_evidence(obj, table)
        missing_intervals = _missing_intervals_from_evidence(
            validation_evidence,
            TimeRange(obj.start, obj.end),
        )
        now = _require_utc(self._clock(), "clock")
        manifest = DataManifest(
            manifest_id=uuid5(NAMESPACE_URL, f"manifest|{work.object_id}|{work.source_checksum}"),
            instrument=InstrumentRef(
                venue="BINANCE", market="USD_M_PERPETUAL", symbol=obj.symbol
            ),
            data_type=_data_type(obj.dataset),
            start=obj.start,
            end=obj.end,
            retrieved_at=now,
            schema_version="2.0.0",
            normalization_version="1.0.0",
            source=obj.source,
            raw_path=work.raw_path,
            normalized_path=work.normalized_path,
            source_checksum=work.source_checksum,
            normalized_checksum=work.normalized_checksum,
            row_count=work.row_count,
            validation_state=ValidationState.VALIDATED,
            primary_key_fields=(_dataset_validation(obj.dataset)[1],),
            deduplication_method=DeduplicationMethod.REJECT_DUPLICATES,
            duplicates_removed=0,
            missing_intervals=missing_intervals,
        )
        approved = await self._catalog.approve(
            CatalogCandidate(
                manifest,
                validation_evidence,
            )
        )
        return PublishEvidence(approved.partition_id, approved.manifest_id)

    def _object(self, work: BackfillObject) -> ArchiveObject:
        try:
            obj = self._archives[work.source_url]
        except KeyError as error:
            raise ValueError("backfill source is not in the approved archive plan") from error
        if (obj.start, obj.end) != (work.start, work.end):
            raise ValueError("backfill range does not match the approved archive plan")
        return obj


class InMemoryBackfillRepository:
    """Deterministic repository used by orchestration tests and offline workers."""

    allows_shared_heartbeat = True

    def __init__(self) -> None:
        self._objects: dict[str, BackfillObject] = {}

    async def plan(self, work: BackfillObject) -> BackfillObject:
        existing = self._objects.get(work.object_id)
        if existing is not None:
            if _planned_identity(existing) != _planned_identity(work):
                raise ValueError("backfill object identity is immutable")
            return existing
        self._objects[work.object_id] = work
        return work

    async def claim(
        self, worker_id: str, now: datetime, duration: timedelta
    ) -> BackfillObject | None:
        _require_utc(now, "now")
        _require_duration(duration)
        candidates = sorted(
            (
                work
                for work in self._objects.values()
                if work.state in _ACTIVE_STATES
                and (work.lease_expires_at is None or work.lease_expires_at <= now)
            ),
            key=lambda work: (work.start, work.source_url, work.object_id),
        )
        if not candidates:
            return None
        work = candidates[0]
        claimed = replace(
            work,
            lease_owner=worker_id,
            lease_expires_at=now + duration,
            attempt_count=work.attempt_count + 1,
        )
        self._objects[work.object_id] = claimed
        return claimed

    async def renew(
        self,
        object_id: str,
        worker_id: str,
        lease_attempt: int,
        now: datetime,
        duration: timedelta,
    ) -> BackfillObject:
        _require_utc(now, "now")
        _require_duration(duration)
        work = self._get(object_id)
        _require_current_lease(work, worker_id, lease_attempt, now)
        renewed = replace(work, lease_expires_at=now + duration)
        self._objects[object_id] = renewed
        return renewed

    async def advance(
        self,
        object_id: str,
        worker_id: str,
        lease_attempt: int,
        now: datetime,
        target: BackfillState,
        *,
        evidence: Mapping[str, object] | None = None,
    ) -> BackfillObject:
        _require_utc(now, "now")
        work = self._get(object_id)
        if not is_legal_backfill_transition(work.state, target):
            raise ValueError(f"illegal backfill transition: {work.state} -> {target}")
        _require_current_lease(work, worker_id, lease_attempt, now)
        evidence = evidence or {}
        timestamps = dict(work.state_timestamps)
        timestamps[target] = now
        terminal = target not in _ACTIVE_STATES
        advanced = replace(
            work,
            state=target,
            source_checksum=str(evidence.get("source_checksum", work.source_checksum)),
            raw_path=_optional_string(evidence, "raw_path", work.raw_path),
            normalized_path=_optional_string(
                evidence, "normalized_path", work.normalized_path
            ),
            normalized_checksum=_optional_string(
                evidence, "normalized_checksum", work.normalized_checksum
            ),
            row_count=_optional_int(evidence, "row_count", work.row_count),
            partition_id=_optional_string(evidence, "partition_id", work.partition_id),
            manifest_id=_optional_string(evidence, "manifest_id", work.manifest_id),
            last_error=_optional_string(evidence, "last_error", work.last_error),
            state_timestamps=timestamps,
            lease_owner=None if terminal else work.lease_owner,
            lease_expires_at=None if terminal else work.lease_expires_at,
        )
        self._objects[object_id] = advanced
        return advanced

    async def retry(self, object_id: str, now: datetime) -> BackfillObject:
        _require_utc(now, "now")
        work = self._get(object_id)
        if work.state not in _RETRYABLE_STATES:
            raise ValueError(f"backfill object is not eligible for retry: {work.state}")
        timestamps = dict(work.state_timestamps)
        timestamps[BackfillState.PLANNED] = now
        retried = replace(
            work,
            state=BackfillState.PLANNED,
            lease_owner=None,
            lease_expires_at=None,
            state_timestamps=timestamps,
        )
        self._objects[object_id] = retried
        return retried

    async def checkpoint(self) -> None:
        return None

    def _get(self, object_id: str) -> BackfillObject:
        try:
            return self._objects[object_id]
        except KeyError as error:
            raise ValueError(f"backfill object does not exist: {object_id}") from error


def classify_missing_archive(
    start: datetime, end: datetime, now: datetime
) -> MissingArchiveDisposition:
    start, end = _require_utc_range(start, end)
    now = _require_utc(now, "now")
    current_midnight = now.replace(hour=0, minute=0, second=0, microsecond=0)
    if (
        end - start == timedelta(days=1)
        and current_midnight - timedelta(days=2) <= start < current_midnight
        and end <= current_midnight
    ):
        return MissingArchiveDisposition.PENDING
    return MissingArchiveDisposition.FAILED_WITH_GAP


def is_legal_backfill_transition(current: BackfillState, target: BackfillState) -> bool:
    return target == _NEXT_STATE.get(current) or (
        current in _ACTIVE_STATES
        and target in {BackfillState.FAILED, BackfillState.SOURCE_PENDING}
    )


def _require_current_lease(
    work: BackfillObject, worker_id: str, lease_attempt: int, now: datetime
) -> None:
    if (
        work.lease_owner != worker_id
        or work.attempt_count != lease_attempt
        or work.lease_expires_at is None
        or work.lease_expires_at <= now
    ):
        raise ValueError("backfill lease is not owned and unexpired")


def _require_duration(duration: timedelta) -> None:
    if duration <= timedelta(0):
        raise ValueError("lease duration must be positive")


def _require_utc(value: datetime, field_name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() != UTC.utcoffset(value):
        raise ValueError(f"{field_name} must be UTC")
    return value.astimezone(UTC)


def _require_utc_range(start: datetime, end: datetime) -> tuple[datetime, datetime]:
    start = _require_utc(start, "start")
    end = _require_utc(end, "end")
    if end <= start:
        raise ValueError("backfill range must be half-open with end after start")
    return start, end


def _planned_identity(work: BackfillObject) -> tuple[object, ...]:
    return (work.job_id, work.source_url, work.start, work.end)


def _require_expected_source_checksum(work: BackfillObject, actual: str) -> None:
    if work.source_checksum and work.source_checksum != actual:
        raise ArchiveChecksumError(
            "official archive checksum changed after replacement planning"
        )


def _optional_string(
    evidence: Mapping[str, object], key: str, current: str | None
) -> str | None:
    value = evidence.get(key, current)
    return None if value is None else str(value)


def _optional_int(
    evidence: Mapping[str, object], key: str, current: int | None
) -> int | None:
    value = evidence.get(key, current)
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{key} must be an integer")
    return value


def _hash_descriptor(descriptor: int) -> str:
    os.lseek(descriptor, 0, os.SEEK_SET)
    digest = hashlib.sha256()
    while chunk := os.read(descriptor, 1024 * 1024):
        digest.update(chunk)
    os.lseek(descriptor, 0, os.SEEK_SET)
    return digest.hexdigest()


def _read_normalized_table(data_root: Path, relative_path: str) -> tuple[pa.Table, str]:
    with open_secure_relative_file(data_root, relative_path) as descriptor:
        actual_checksum = _hash_descriptor(descriptor)
        with os.fdopen(os.dup(descriptor), "rb") as source:
            return pq.read_table(source), actual_checksum


def _normalize_retained_archive(
    obj: ArchiveObject,
    data_root: Path,
    raw_path: str,
    source_checksum: str,
):
    with open_secure_relative_file(data_root, raw_path) as descriptor:
        payload = read_verified_archive_csv(descriptor, source_checksum)
    normalized = normalize_csv(obj.dataset, payload, obj.start, obj.end)
    return write_versioned_normalized_parquet(
        normalized,
        data_root,
        obj.symbol,
        obj.dataset,
        obj.start,
        source_checksum,
    )


def _content_gaps(obj: ArchiveObject, table: pa.Table) -> tuple[DetectedGap, ...]:
    if obj.dataset in {DatasetKind.KLINES, DatasetKind.MARK_PRICE_KLINES}:
        data_type = (
            DataType.KLINE_1M
            if obj.dataset is DatasetKind.KLINES
            else DataType.MARK_PRICE
        )
        return detect_minute_gaps(
            obj.symbol,
            (_from_epoch_milliseconds(value) for value in table["open_time"].to_pylist()),
            TimeRange(obj.start, obj.end),
            data_type=data_type,
        )
    if obj.dataset is DatasetKind.AGG_TRADES:
        return detect_aggregate_trade_id_gaps(
            obj.symbol,
            zip(
                table["aggregate_trade_id"].to_pylist(),
                (
                    _from_epoch_milliseconds(value)
                    for value in table["transact_time"].to_pylist()
                ),
                strict=True,
            ),
        )
    return ()


def _catalog_validation_evidence(
    obj: ArchiveObject, table: pa.Table
) -> dict[str, object]:
    evidence: dict[str, object] = {
        "checksum": True,
        "schema": True,
        "ordering": True,
        "uniqueness": True,
        "range": True,
        "row_count": True,
        "missing_intervals": _serialize_content_gaps(_content_gaps(obj, table)),
    }
    if obj.dataset is DatasetKind.AGG_TRADES:
        aggregate_ids = table["aggregate_trade_id"].to_pylist()
        recovered_ranges: list[list[int]] = []
        range_start = previous = aggregate_ids[0]
        for aggregate_id in aggregate_ids[1:]:
            if aggregate_id != previous + 1:
                recovered_ranges.append([range_start, previous])
                range_start = aggregate_id
            previous = aggregate_id
        recovered_ranges.append([range_start, previous])
        evidence["recovered_id_ranges"] = recovered_ranges
    return evidence


_VALIDATION_NAMES = (
    "checksum",
    "schema",
    "ordering",
    "uniqueness",
    "range",
    "row_count",
)
_CONTENT_GAP_REASONS = frozenset(
    {"missing_minute_open_time", "aggregate_trade_id_discontinuity"}
)


def _serialize_content_gaps(gaps: tuple[DetectedGap, ...]) -> list[dict[str, str]]:
    return [
        {
            "start": gap.start.isoformat(),
            "end": gap.end.isoformat(),
            "reason": gap.reason.value,
        }
        for gap in gaps
    ]


def _missing_intervals_from_evidence(
    evidence: Mapping[str, object],
    coverage: TimeRange,
) -> tuple[MissingInterval, ...]:
    serialized = evidence.get("missing_intervals")
    if not isinstance(serialized, list):
        raise ValueError("missing interval evidence must be a JSON array")
    intervals: list[MissingInterval] = []
    for item in serialized:
        if not isinstance(item, dict) or set(item) != {"start", "end", "reason"}:
            raise ValueError("missing interval evidence is malformed")
        reason = item["reason"]
        if not isinstance(reason, str) or reason not in _CONTENT_GAP_REASONS:
            raise ValueError("missing interval evidence has an unsupported reason")
        interval = MissingInterval.model_validate_json(
            json.dumps({"start": item["start"], "end": item["end"]})
        )
        if interval.start < coverage.start or interval.end > coverage.end:
            raise ValueError("missing interval evidence is outside archive coverage")
        intervals.append(interval)
    intervals.sort(key=lambda interval: (interval.start, interval.end))
    if any(
        current.start < previous.end
        for previous, current in zip(intervals, intervals[1:], strict=False)
    ):
        raise ValueError("missing interval evidence cannot overlap")
    return tuple(intervals)


def _dataset_validation(dataset: DatasetKind) -> tuple[pa.Schema, str, str]:
    if dataset in {DatasetKind.KLINES, DatasetKind.MARK_PRICE_KLINES}:
        return KLINE_SCHEMA, "open_time", "open_time"
    if dataset is DatasetKind.FUNDING_RATE:
        return FUNDING_SCHEMA, "funding_time", "funding_time"
    if dataset is DatasetKind.AGG_TRADES:
        return AGG_TRADE_SCHEMA, "aggregate_trade_id", "transact_time"
    raise ValueError(f"unsupported archive dataset: {dataset}")


def _data_type(dataset: DatasetKind) -> DataType:
    return {
        DatasetKind.KLINES: DataType.KLINE_1M,
        DatasetKind.MARK_PRICE_KLINES: DataType.MARK_PRICE,
        DatasetKind.FUNDING_RATE: DataType.FUNDING,
        DatasetKind.AGG_TRADES: DataType.AGG_TRADE,
    }[dataset]


def _epoch_milliseconds(value: datetime) -> int:
    difference = value - datetime(1970, 1, 1, tzinfo=UTC)
    return (
        difference.days * 86_400_000
        + difference.seconds * 1_000
        + difference.microseconds // 1_000
    )


def _from_epoch_milliseconds(value: int) -> datetime:
    return datetime(1970, 1, 1, tzinfo=UTC) + timedelta(milliseconds=value)
