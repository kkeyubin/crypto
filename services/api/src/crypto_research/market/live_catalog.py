from __future__ import annotations

import re
from collections.abc import Iterable
from pathlib import Path
from uuid import NAMESPACE_URL, uuid5

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from crypto_research.db.models import LiveDataPartitionRow, normalize_symbol, utc_now
from crypto_research.market.live_storage import (
    LiveWriteResult,
    StoredLivePartition,
)

_SHA256 = re.compile(r"[0-9a-f]{64}")
_PART_NAME = re.compile(r"part-[0-9a-f]{24}\.(?:parquet|ndjson\.gz)")


class LiveCatalogError(ValueError):
    """A live shard cannot cross the approved query boundary."""


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
                LiveDataPartitionRow.max_source_event_time >= start_event_time,
                LiveDataPartitionRow.min_source_event_time <= end_event_time,
            )
            .order_by(
                LiveDataPartitionRow.min_source_event_time,
                LiveDataPartitionRow.relative_path,
            )
        )
        rows = (await self._session.execute(statement)).scalars().all()
        return tuple(rows)

    async def approved_parquet_paths(
        self,
        *,
        data_root: Path,
        symbol: str,
        dataset: str,
        start_event_time: int,
        end_event_time: int,
    ) -> tuple[Path, ...]:
        """The sole database-backed DuckDB boundary for live artifacts."""
        rows = await self.list_approved_normalized(
            symbol=symbol,
            dataset=dataset,
            start_event_time=start_event_time,
            end_event_time=end_event_time,
        )
        return approved_live_parquet_paths(rows, data_root)


def approved_live_parquet_paths(
    rows: Iterable[LiveDataPartitionRow], data_root: Path
) -> tuple[Path, ...]:
    """Convert only approved catalog rows into fixed-root DuckDB input paths."""
    root = data_root.resolve(strict=False)
    paths: list[tuple[int, str, Path]] = []
    for row in rows:
        if row.layer != "normalized":
            raise LiveCatalogError("DuckDB live query accepts normalized rows only")
        if row.approval_status != "approved":
            raise LiveCatalogError("DuckDB live query accepts approved rows only")
        part = _part_from_row(row, root)
        _validate_partition(part)
        candidate = (root / row.relative_path).resolve(strict=False)
        try:
            candidate.relative_to(root)
        except ValueError as error:
            raise LiveCatalogError("live catalog path escapes the data root") from error
        paths.append((row.min_source_event_time, row.relative_path, candidate))
    return tuple(path for _minimum, _relative, path in sorted(paths))


def _validate_partition(part: StoredLivePartition) -> None:
    if part.layer not in {"raw", "normalized"}:
        raise LiveCatalogError("live partition layer is invalid")
    if not _SHA256.fullmatch(part.sha256):
        raise LiveCatalogError("live partition checksum is invalid")
    if part.row_count <= 0:
        raise LiveCatalogError("live partition row count must be positive")
    if (
        part.min_source_event_time is None
        or part.max_source_event_time is None
        or part.min_source_event_time > part.max_source_event_time
    ):
        raise LiveCatalogError("live partition event range is invalid")
    if not part.schema_name or not part.sort_keys or not part.unique_keys:
        raise LiveCatalogError("live partition schema contract is incomplete")
    suffix = ".parquet" if part.layer == "normalized" else ".ndjson.gz"
    prefix = (
        f"{part.layer}/binance/usdm/{part.symbol}/{part.dataset}/"
        f"date={part.partition_date}/"
    )
    path = Path(part.relative_path)
    if (
        part.relative_path.startswith("/")
        or ".." in path.parts
        or not part.relative_path.startswith(prefix)
        or not part.relative_path.endswith(suffix)
        or not _PART_NAME.fullmatch(path.name)
    ):
        raise LiveCatalogError("live partition relative path is invalid")


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
        relative_path=row.relative_path,
    )
