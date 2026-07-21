from __future__ import annotations

import hashlib
import os
import re
from collections.abc import Iterator
from contextlib import ExitStack, contextmanager
from datetime import date
from pathlib import Path
from typing import Protocol
from uuid import NAMESPACE_URL, uuid5

import duckdb
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from crypto_research.db.models import LiveDataPartitionRow, normalize_symbol, utc_now
from crypto_research.market.live_storage import (
    LiveWriteResult,
    StoredLivePartition,
)
from crypto_research.market.storage import _open_secure_root, open_secure_relative_file

_SHA256 = re.compile(r"[0-9a-f]{64}")
_PARTITION_DATE = re.compile(r"[0-9]{4}-[0-9]{2}-[0-9]{2}")
_KEY_NAME = re.compile(r"[a-z][a-z0-9_]*")
_RAW_SCHEMA = "ndjson/binance-stream-event-v1"
_RAW_SORT_KEYS = ("source_event_time", "source_id", "event_key")
_RAW_UNIQUE_KEYS = ("event_key",)
_NORMALIZED_CONTRACTS = {
    "klines": (
        "parquet/binance-kline-1m-v1",
        ("open_time",),
        ("open_time",),
    ),
    "agg_trades": (
        "parquet/binance-aggregate-trade-v1",
        ("aggregate_trade_id",),
        ("aggregate_trade_id",),
    ),
    "mark_price": (
        "parquet/binance-mark-price-v1",
        ("event_time",),
        ("event_time",),
    ),
    "book_ticker": (
        "parquet/binance-book-ticker-v1",
        ("update_id",),
        ("update_id",),
    ),
}
_LIVE_QUERY_CONTRACTS = {
    "klines": ("open_time", ("open_time",)),
    "agg_trades": ("transact_time", ("aggregate_trade_id",)),
    "mark_price": ("event_time", ("event_time",)),
    "book_ticker": ("transact_time", ("update_id",)),
}


class LiveCatalogError(ValueError):
    """A live shard cannot cross the approved query boundary."""


class ApprovedLiveCatalogRepository(Protocol):
    async def list_approved_normalized(
        self,
        *,
        symbol: str,
        dataset: str,
        start_event_time: int,
        end_event_time: int,
    ) -> tuple[LiveDataPartitionRow, ...]: ...


class SqlAlchemyLiveCatalogRepository:
    """Register immutable live batch manifests in the current transaction."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def register_batch(
        self, result: LiveWriteResult
    ) -> tuple[LiveDataPartitionRow, ...]:
        if not result.batch_id:
            raise LiveCatalogError("live write result has no batch identity")
        parts = (*result.raw, *result.normalized)
        if not parts:
            raise LiveCatalogError("live write result has no artifacts")
        rows: list[LiveDataPartitionRow] = []
        for part in parts:
            _validate_partition(part)
            identity = str(
                uuid5(
                    NAMESPACE_URL,
                    f"crypto-research:live:{result.batch_id}:{part.relative_path}",
                )
            )
            existing = await self._session.get(LiveDataPartitionRow, identity)
            if existing is not None:
                if _row_identity(existing) != _part_identity(result.batch_id, part):
                    raise LiveCatalogError("approved live partition is immutable")
                rows.append(existing)
                continue
            now = utc_now()
            row = LiveDataPartitionRow(
                id=identity,
                batch_id=result.batch_id,
                layer=part.layer,
                symbol=normalize_symbol(part.symbol),
                dataset=part.dataset,
                partition_date=part.partition_date,
                relative_path=part.relative_path,
                checksum_sha256=part.sha256,
                schema_name=part.schema_name,
                sort_keys=list(part.sort_keys),
                unique_keys=list(part.unique_keys),
                min_source_event_time=part.min_source_event_time,
                max_source_event_time=part.max_source_event_time,
                min_canonical_time=part.min_canonical_time,
                max_canonical_time=part.max_canonical_time,
                row_count=part.row_count,
                approval_status="approved",
                approved_at=now,
            )
            self._session.add(row)
            rows.append(row)
        await self._session.flush()
        return tuple(rows)

    async def list_approved_normalized(
        self,
        *,
        symbol: str,
        dataset: str,
        start_event_time: int,
        end_event_time: int,
    ) -> tuple[LiveDataPartitionRow, ...]:
        if start_event_time > end_event_time:
            raise LiveCatalogError("live query time range is inverted")
        statement = (
            select(LiveDataPartitionRow)
            .where(
                LiveDataPartitionRow.symbol == normalize_symbol(symbol),
                LiveDataPartitionRow.dataset == dataset,
                LiveDataPartitionRow.layer == "normalized",
                LiveDataPartitionRow.approval_status == "approved",
                LiveDataPartitionRow.max_canonical_time >= start_event_time,
                LiveDataPartitionRow.min_canonical_time < end_event_time,
            )
            .order_by(
                LiveDataPartitionRow.min_canonical_time,
                LiveDataPartitionRow.relative_path,
            )
        )
        rows = (await self._session.execute(statement)).scalars().all()
        return tuple(rows)

class SecureLiveDuckDBCatalog:
    """Execute one fixed read-only query over approved live Parquet descriptors."""

    def __init__(
        self, data_root: Path, repository: ApprovedLiveCatalogRepository
    ) -> None:
        descriptor = _open_secure_root(data_root)
        os.close(descriptor)
        self._data_root = data_root
        self._repository = repository

    async def read(
        self,
        symbol: str,
        dataset: str,
        start_event_time: int,
        end_event_time: int,
    ) -> tuple[dict[str, object], ...]:
        if (
            isinstance(start_event_time, bool)
            or isinstance(end_event_time, bool)
            or not isinstance(start_event_time, int)
            or not isinstance(end_event_time, int)
            or start_event_time < 0
            or start_event_time >= end_event_time
        ):
            raise LiveCatalogError("live query event-time range is invalid")
        contract = _LIVE_QUERY_CONTRACTS.get(dataset)
        if contract is None:
            raise LiveCatalogError("live query dataset is unsupported")
        requested_symbol = _validated_symbol(symbol)
        approved = await self._repository.list_approved_normalized(
            symbol=requested_symbol,
            dataset=dataset,
            start_event_time=start_event_time,
            end_event_time=end_event_time,
        )
        if not approved:
            return ()
        time_key, order_keys = contract
        with ExitStack() as descriptors:
            paths = []
            for row in approved:
                if (
                    row.layer != "normalized"
                    or row.approval_status != "approved"
                    or row.symbol != requested_symbol
                    or row.dataset != dataset
                    or row.max_canonical_time < start_event_time
                    or row.min_canonical_time >= end_event_time
                ):
                    raise LiveCatalogError(
                        "live query repository returned an unapproved partition"
                    )
                part = _part_from_row(row, self._data_root)
                _validate_partition(part)
                paths.append(
                    descriptors.enter_context(
                        _open_live_catalog_descriptor(
                            self._data_root,
                            row.relative_path,
                            row.checksum_sha256,
                        )
                    )
                )
            order_sql = ", ".join(f'"{key}" ASC' for key in order_keys)
            sql = (
                f'SELECT * FROM read_parquet(?) WHERE "{time_key}" >= ? '
                f'AND "{time_key}" < ? ORDER BY {order_sql}'
            )
            connection = duckdb.connect(":memory:")
            try:
                cursor = connection.execute(
                    sql, [paths, start_event_time, end_event_time]
                )
                names = [column[0] for column in cursor.description]
                return tuple(
                    dict(zip(names, row, strict=True))
                    for row in cursor.fetchall()
                )
            finally:
                connection.close()


@contextmanager
def _open_live_catalog_descriptor(
    data_root: Path, relative_path: str, expected_checksum: str
) -> Iterator[str]:
    try:
        with open_secure_relative_file(data_root, relative_path) as descriptor:
            digest = hashlib.sha256()
            while chunk := os.read(descriptor, 1024 * 1024):
                digest.update(chunk)
            os.lseek(descriptor, 0, os.SEEK_SET)
            if digest.hexdigest() != expected_checksum:
                raise LiveCatalogError(
                    "live catalog file checksum no longer matches approval"
                )
            descriptor_root = (
                "/proc/self/fd"
                if Path("/proc/self/fd").is_dir()
                else "/dev/fd"
            )
            yield f"{descriptor_root}/{descriptor}"
    except ValueError as error:
        if isinstance(error, LiveCatalogError):
            raise
        raise LiveCatalogError(
            "live catalog path resolved outside the secure data root"
        ) from error


def _validate_partition(part: StoredLivePartition) -> None:
    if part.layer not in {"raw", "normalized"}:
        raise LiveCatalogError("live partition layer is invalid")
    if not _SHA256.fullmatch(part.sha256):
        raise LiveCatalogError("live partition checksum is invalid")
    if part.row_count <= 0:
        raise LiveCatalogError("live partition row count must be positive")
    if part.symbol != _validated_symbol(part.symbol):
        raise LiveCatalogError("live partition symbol is invalid")
    if part.dataset not in _NORMALIZED_CONTRACTS:
        raise LiveCatalogError("live partition dataset is invalid")
    try:
        parsed_date = date.fromisoformat(part.partition_date)
    except (TypeError, ValueError) as error:
        raise LiveCatalogError("live partition partition date is invalid") from error
    if (
        not _PARTITION_DATE.fullmatch(part.partition_date)
        or parsed_date.isoformat() != part.partition_date
    ):
        raise LiveCatalogError("live partition partition date is invalid")
    if (
        part.min_source_event_time is None
        or part.max_source_event_time is None
        or part.min_source_event_time > part.max_source_event_time
    ):
        raise LiveCatalogError("live partition event range is invalid")
    if (
        part.min_canonical_time is None
        or part.max_canonical_time is None
        or part.min_canonical_time < 0
        or part.min_canonical_time > part.max_canonical_time
    ):
        raise LiveCatalogError("live partition canonical range is invalid")
    expected = (
        _NORMALIZED_CONTRACTS[part.dataset]
        if part.layer == "normalized"
        else (_RAW_SCHEMA, _RAW_SORT_KEYS, _RAW_UNIQUE_KEYS)
    )
    expected_schema, expected_sort, expected_unique = expected
    if part.schema_name != expected_schema:
        raise LiveCatalogError("live partition schema contract is invalid")
    if (
        tuple(part.sort_keys) != expected_sort
        or tuple(part.unique_keys) != expected_unique
        or not all(_KEY_NAME.fullmatch(key) for key in (*part.sort_keys, *part.unique_keys))
    ):
        raise LiveCatalogError("live partition query contract is invalid")
    suffix = ".parquet" if part.layer == "normalized" else ".ndjson.gz"
    expected_name = f"part-{part.sha256[:24]}{suffix}"
    path = Path(part.relative_path)
    if path.name != expected_name:
        raise LiveCatalogError("live partition filename does not match checksum")
    expected_path = Path(
        part.layer,
        "binance",
        "usdm",
        part.symbol,
        part.dataset,
        f"date={part.partition_date}",
        expected_name,
    ).as_posix()
    if (
        part.relative_path.startswith("/")
        or ".." in path.parts
        or part.relative_path != expected_path
    ):
        raise LiveCatalogError("live partition relative path is invalid")


def _validated_symbol(symbol: str) -> str:
    try:
        normalized = normalize_symbol(symbol)
    except (AttributeError, TypeError, ValueError) as error:
        raise LiveCatalogError("live partition symbol is invalid") from error
    if not normalized.isalnum() or not 3 <= len(normalized) <= 32:
        raise LiveCatalogError("live partition symbol is invalid")
    return normalized


def _part_identity(batch_id: str, part: StoredLivePartition) -> tuple[object, ...]:
    return (
        batch_id,
        part.layer,
        part.symbol,
        part.dataset,
        part.partition_date,
        part.relative_path,
        part.sha256,
        part.schema_name,
        list(part.sort_keys),
        list(part.unique_keys),
        part.min_source_event_time,
        part.max_source_event_time,
        part.min_canonical_time,
        part.max_canonical_time,
        part.row_count,
        "approved",
    )


def _row_identity(row: LiveDataPartitionRow) -> tuple[object, ...]:
    return (
        row.batch_id,
        row.layer,
        row.symbol,
        row.dataset,
        row.partition_date,
        row.relative_path,
        row.checksum_sha256,
        row.schema_name,
        row.sort_keys,
        row.unique_keys,
        row.min_source_event_time,
        row.max_source_event_time,
        row.min_canonical_time,
        row.max_canonical_time,
        row.row_count,
        row.approval_status,
    )


def _part_from_row(row: LiveDataPartitionRow, data_root: Path) -> StoredLivePartition:
    return StoredLivePartition(
        path=data_root / row.relative_path,
        sha256=row.checksum_sha256,
        row_count=row.row_count,
        layer=row.layer,
        symbol=row.symbol,
        dataset=row.dataset,
        partition_date=row.partition_date,
        schema_name=row.schema_name,
        sort_keys=tuple(row.sort_keys),
        unique_keys=tuple(row.unique_keys),
        min_source_event_time=row.min_source_event_time,
        max_source_event_time=row.max_source_event_time,
        min_canonical_time=row.min_canonical_time,
        max_canonical_time=row.max_canonical_time,
        relative_path=row.relative_path,
    )
