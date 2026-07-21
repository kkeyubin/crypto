from __future__ import annotations

import hashlib
import json
import os
import stat
from collections.abc import Iterator, Mapping
from contextlib import ExitStack, contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path, PurePosixPath
from typing import Protocol
from uuid import NAMESPACE_URL, uuid5

import duckdb
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from crypto_research.contracts.manifest import DataManifest, DataType, ValidationState
from crypto_research.db.models import DataManifestRow, DataPartitionRow, SourceObjectRow
from crypto_research.market.storage import _open_secure_root

REQUIRED_VALIDATIONS = (
    "checksum",
    "schema",
    "ordering",
    "uniqueness",
    "range",
    "row_count",
)


class CatalogValidationError(ValueError):
    """A candidate cannot cross the queryable catalog boundary."""


@dataclass(frozen=True)
class CatalogCandidate:
    manifest: DataManifest
    validation_results: Mapping[str, object]


@dataclass(frozen=True)
class CatalogPartition:
    partition_id: str
    manifest_id: str
    symbol: str
    data_type: DataType
    start: datetime
    end: datetime
    version: int
    parquet_path: str
    normalized_checksum: str
    source_url: str
    source_checksum: str
    row_count: int
    manifest: DataManifest


class ApprovedCatalogRepository(Protocol):
    async def approved(
        self,
        symbol: str,
        data_type: DataType,
        start: datetime,
        end: datetime,
    ) -> tuple[CatalogPartition, ...]: ...


class InMemoryCatalogRepository:
    def __init__(self) -> None:
        self._approved: list[CatalogPartition] = []

    async def approve(self, candidate: CatalogCandidate) -> CatalogPartition:
        _validate_candidate(candidate)
        manifest = candidate.manifest
        source_key = _source_version_key(manifest)
        for approved in self._approved:
            if _source_version_key(approved.manifest) == source_key:
                return approved

        identity = _partition_identity(manifest)
        if any(
            _partition_identity(item.manifest) != identity
            and item.start < manifest.end
            and item.end > manifest.start
            for item in self._approved
        ):
            raise CatalogValidationError(
                "approved partition coverage cannot overlap a different active range"
            )
        versions = [
            item
            for item in self._approved
            if _partition_identity(item.manifest) == identity
        ]
        if any(item.parquet_path == manifest.normalized_path for item in versions):
            raise CatalogValidationError(
                "source replacement must use immutable partition bytes and a new path"
            )
        version = max((item.version for item in versions), default=0) + 1
        partition_id = str(
            uuid5(
                NAMESPACE_URL,
                "|".join(
                    (
                        manifest.instrument.symbol,
                        manifest.data_type.value,
                        manifest.start.isoformat(),
                        manifest.end.isoformat(),
                        str(version),
                        manifest.source_checksum,
                    )
                ),
            )
        )
        approved = CatalogPartition(
            partition_id=partition_id,
            manifest_id=str(manifest.manifest_id),
            symbol=manifest.instrument.symbol,
            data_type=manifest.data_type,
            start=manifest.start,
            end=manifest.end,
            version=version,
            parquet_path=manifest.normalized_path,
            normalized_checksum=manifest.normalized_checksum,
            source_url=manifest.source.resolved_url,
            source_checksum=manifest.source_checksum,
            row_count=manifest.row_count,
            manifest=manifest,
        )
        self._approved.append(approved)
        return approved

    async def approved(
        self,
        symbol: str,
        data_type: DataType,
        start: datetime,
        end: datetime,
    ) -> tuple[CatalogPartition, ...]:
        symbol = symbol.strip().upper()
        start, end = _utc_range(start, end)
        matches = (
            item
            for item in self._approved
            if item.symbol == symbol
            and item.data_type is data_type
            and item.start < end
            and item.end > start
        )
        return tuple(
            sorted(
                matches,
                key=lambda item: (item.start, item.end, item.version, item.partition_id),
            )
        )


class SqlAlchemyCatalogRepository:
    """Persist immutable approved partition and full manifest versions."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def approve(self, candidate: CatalogCandidate) -> CatalogPartition:
        _validate_candidate(candidate)
        manifest = candidate.manifest
        partition_date = manifest.start.date().isoformat()
        lock_key = f"{manifest.instrument.symbol}|{manifest.data_type.value}"
        await self._session.execute(
            select(func.pg_advisory_xact_lock(func.hashtextextended(lock_key, 0)))
        )
        existing_statement = (
            select(DataPartitionRow, DataManifestRow)
            .join(DataManifestRow, DataManifestRow.partition_id == DataPartitionRow.id)
            .where(
                DataPartitionRow.symbol == manifest.instrument.symbol,
                DataPartitionRow.dataset == manifest.data_type.value,
                DataPartitionRow.partition_date == partition_date,
                DataPartitionRow.approval_status == "approved",
                DataManifestRow.source_url == manifest.source.resolved_url,
                DataManifestRow.source_checksum == manifest.source_checksum,
            )
        )
        existing = (await self._session.execute(existing_statement)).first()
        if existing is not None:
            return _catalog_partition(existing[0], existing[1])

        active_statement = (
            select(DataPartitionRow, DataManifestRow)
            .join(DataManifestRow, DataManifestRow.partition_id == DataPartitionRow.id)
            .where(
                DataPartitionRow.symbol == manifest.instrument.symbol,
                DataPartitionRow.dataset == manifest.data_type.value,
                DataPartitionRow.approval_status == "approved",
            )
            .with_for_update()
        )
        active_rows = (await self._session.execute(active_statement)).all()
        exact_versions: list[DataPartitionRow] = []
        for partition, stored in active_rows:
            existing_manifest = _stored_data_manifest(stored.manifest)
            if _partition_identity(existing_manifest) == _partition_identity(manifest):
                exact_versions.append(partition)
            elif existing_manifest.start < manifest.end and existing_manifest.end > manifest.start:
                raise CatalogValidationError(
                    "approved partition coverage cannot overlap a different active range"
                )
        if any(
            row.parquet_path == manifest.normalized_path for row, _stored in active_rows
        ):
            raise CatalogValidationError(
                "source replacement must use immutable partition bytes and a new path"
            )
        version = max((row.version for row in exact_versions), default=0) + 1

        source_statement = select(SourceObjectRow).where(
            SourceObjectRow.source_url == manifest.source.resolved_url,
            SourceObjectRow.checksum_sha256 == manifest.source_checksum,
        )
        source = (await self._session.execute(source_statement)).scalars().first()
        if source is None:
            source_identity = f"{manifest.source.resolved_url}|{manifest.source_checksum}"
            source = SourceObjectRow(
                id=str(uuid5(NAMESPACE_URL, source_identity)),
                source_url=manifest.source.resolved_url,
                checksum_sha256=manifest.source_checksum,
            )
            self._session.add(source)
            await self._session.flush()

        partition_id = str(
            uuid5(
                NAMESPACE_URL,
                "|".join(
                    (
                        manifest.instrument.symbol,
                        manifest.data_type.value,
                        manifest.start.isoformat(),
                        manifest.end.isoformat(),
                        str(version),
                        manifest.source_checksum,
                    )
                ),
            )
        )
        partition = DataPartitionRow(
            id=partition_id,
            symbol=manifest.instrument.symbol,
            dataset=manifest.data_type.value,
            partition_date=partition_date,
            version=version,
            source_object_id=source.id,
            checksum_sha256=manifest.normalized_checksum,
            parquet_path=manifest.normalized_path,
            approval_status="approved",
            validation_details=dict(candidate.validation_results),
            approved_at=manifest.retrieved_at,
        )
        stored_manifest = DataManifestRow(
            manifest_id=str(manifest.manifest_id),
            partition_id=partition_id,
            source_object_id=source.id,
            source_url=manifest.source.resolved_url,
            source_checksum=manifest.source_checksum,
            manifest=manifest.model_dump(mode="json", exclude_computed_fields=True),
        )
        self._session.add(partition)
        await self._session.flush()
        self._session.add(stored_manifest)
        await self._session.flush()
        return _catalog_partition(partition, stored_manifest)

    async def approved(
        self,
        symbol: str,
        data_type: DataType,
        start: datetime,
        end: datetime,
    ) -> tuple[CatalogPartition, ...]:
        start, end = _utc_range(start, end)
        earliest_partition_date = start.replace(day=1).date().isoformat()
        latest_partition_date = (end - timedelta(microseconds=1)).date().isoformat()
        statement = (
            select(DataPartitionRow, DataManifestRow)
            .join(DataManifestRow, DataManifestRow.partition_id == DataPartitionRow.id)
            .where(
                DataPartitionRow.symbol == symbol.strip().upper(),
                DataPartitionRow.dataset == data_type.value,
                DataPartitionRow.approval_status == "approved",
                DataPartitionRow.partition_date >= earliest_partition_date,
                DataPartitionRow.partition_date <= latest_partition_date,
            )
            .order_by(
                DataPartitionRow.partition_date,
                DataPartitionRow.version,
                DataPartitionRow.id,
            )
        )
        rows = (await self._session.execute(statement)).all()
        approved = tuple(_catalog_partition(partition, manifest) for partition, manifest in rows)
        return tuple(item for item in approved if item.start < end and item.end > start)


class SecureDuckDBCatalog:
    """Read-only deterministic queries over catalog-approved Parquet paths."""

    def __init__(self, data_root: Path, repository: ApprovedCatalogRepository) -> None:
        descriptor = _open_secure_root(data_root)
        os.close(descriptor)
        self._data_root = data_root
        self._repository = repository

    async def read(
        self,
        symbol: str,
        data_type: DataType,
        start: datetime,
        end: datetime,
    ) -> tuple[dict[str, object], ...]:
        start, end = _utc_range(start, end)
        approved = await self._repository.approved(symbol, data_type, start, end)
        records = _current_approved(approved)
        if not records:
            return ()
        with ExitStack() as descriptors:
            paths = [
                descriptors.enter_context(
                    _open_catalog_descriptor(
                        self._data_root,
                        record.parquet_path,
                        record.normalized_checksum,
                    )
                )
                for record in records
            ]
            key = _ordering_key(data_type)
            start_ms = _epoch_milliseconds(start)
            end_ms = _epoch_milliseconds(end)
            sql = (
                f'SELECT * FROM read_parquet(?) WHERE "{key}" >= ? AND "{key}" < ? '
                f'ORDER BY "{key}" ASC'
            )
            connection = duckdb.connect(":memory:")
            try:
                cursor = connection.execute(sql, [paths, start_ms, end_ms])
                names = [column[0] for column in cursor.description]
                return tuple(
                    dict(zip(names, row, strict=True)) for row in cursor.fetchall()
                )
            finally:
                connection.close()


@contextmanager
def _open_catalog_descriptor(
    data_root: Path, relative_path: str, expected_checksum: str
) -> Iterator[str]:
    relative = PurePosixPath(relative_path)
    if relative.is_absolute() or ".." in relative.parts or len(relative.parts) < 2:
        raise ValueError("catalog path must be relative to the secure data root")
    descriptors = [_open_secure_root(data_root)]
    try:
        directory_flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC
        for component in relative.parts[:-1]:
            try:
                descriptor = os.open(component, directory_flags, dir_fd=descriptors[-1])
            except OSError as error:
                raise ValueError(
                    "catalog path resolved outside the secure data root"
                ) from error
            descriptors.append(descriptor)
        try:
            file_descriptor = os.open(
                relative.parts[-1],
                os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC,
                dir_fd=descriptors[-1],
            )
        except OSError as error:
            raise ValueError("catalog path resolved outside the secure data root") from error
        descriptors.append(file_descriptor)
        try:
            if not stat.S_ISREG(os.fstat(file_descriptor).st_mode):
                raise ValueError(
                    "catalog path must resolve to a regular file under data root"
                )
            digest = hashlib.sha256()
            while chunk := os.read(file_descriptor, 1024 * 1024):
                digest.update(chunk)
            os.lseek(file_descriptor, 0, os.SEEK_SET)
            if digest.hexdigest() != expected_checksum:
                raise ValueError("catalog file checksum no longer matches approved manifest")
            descriptor_root = "/proc/self/fd" if Path("/proc/self/fd").is_dir() else "/dev/fd"
            yield f"{descriptor_root}/{file_descriptor}"
        finally:
            os.lseek(file_descriptor, 0, os.SEEK_SET)
    finally:
        for descriptor in reversed(descriptors):
            os.close(descriptor)


def _validate_candidate(candidate: CatalogCandidate) -> None:
    manifest = candidate.manifest
    if manifest.validation_state is not ValidationState.VALIDATED:
        raise CatalogValidationError("manifest must be validated before catalog approval")
    failed = [
        name
        for name in REQUIRED_VALIDATIONS
        if candidate.validation_results.get(name) is not True
    ]
    if failed:
        raise CatalogValidationError(f"catalog validation failed: {', '.join(failed)}")


def _source_version_key(manifest: DataManifest) -> tuple[object, ...]:
    return (*_partition_identity(manifest), manifest.source.resolved_url, manifest.source_checksum)


def _current_approved(
    records: tuple[CatalogPartition, ...],
) -> tuple[CatalogPartition, ...]:
    current: dict[tuple[object, ...], CatalogPartition] = {}
    for record in records:
        identity = (record.symbol, record.data_type, record.start, record.end)
        previous = current.get(identity)
        if previous is None or record.version > previous.version:
            current[identity] = record
    return tuple(
        sorted(
            current.values(),
            key=lambda item: (item.start, item.end, item.version, item.partition_id),
        )
    )


def _partition_identity(manifest: DataManifest) -> tuple[object, ...]:
    return (
        manifest.instrument.symbol,
        manifest.data_type,
        manifest.start,
        manifest.end,
    )


def _ordering_key(data_type: DataType) -> str:
    try:
        return {
            DataType.KLINE_1M: "open_time",
            DataType.MARK_PRICE: "open_time",
            DataType.FUNDING: "funding_time",
            DataType.AGG_TRADE: "aggregate_trade_id",
            DataType.BEST_BID_ASK: "event_time",
        }[data_type]
    except KeyError as error:
        raise ValueError(f"unsupported catalog data type: {data_type}") from error


def _utc_range(start: datetime, end: datetime) -> tuple[datetime, datetime]:
    if (
        start.tzinfo is None
        or end.tzinfo is None
        or start.utcoffset() != UTC.utcoffset(start)
        or end.utcoffset() != UTC.utcoffset(end)
        or end <= start
    ):
        raise ValueError("catalog query requires an exact half-open UTC range")
    if start.microsecond % 1000 or end.microsecond % 1000:
        raise ValueError("catalog query boundaries must be millisecond-aligned")
    return start, end


def _epoch_milliseconds(value: datetime) -> int:
    difference = value - datetime(1970, 1, 1, tzinfo=UTC)
    return (
        difference.days * 86_400_000
        + difference.seconds * 1_000
        + difference.microseconds // 1_000
    )


def _catalog_partition(
    partition: DataPartitionRow, stored_manifest: DataManifestRow
) -> CatalogPartition:
    manifest = _stored_data_manifest(stored_manifest.manifest)
    return CatalogPartition(
        partition_id=partition.id,
        manifest_id=stored_manifest.manifest_id,
        symbol=partition.symbol,
        data_type=DataType(partition.dataset),
        start=manifest.start,
        end=manifest.end,
        version=partition.version,
        parquet_path=partition.parquet_path,
        normalized_checksum=partition.checksum_sha256,
        source_url=stored_manifest.source_url,
        source_checksum=stored_manifest.source_checksum,
        row_count=manifest.row_count,
        manifest=manifest,
    )


def _stored_data_manifest(payload: Mapping[str, object]) -> DataManifest:
    return DataManifest.model_validate_json(json.dumps(payload))
