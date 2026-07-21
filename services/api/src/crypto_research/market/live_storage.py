from __future__ import annotations

import json
import os
import threading
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from crypto_research.market.binance.normalization import (
    AGG_TRADE_SCHEMA,
    DECIMAL,
    KLINE_SCHEMA,
)
from crypto_research.market.binance.streams import LiveDataset, ParsedStreamEvent
from crypto_research.market.storage import (
    _hash_fd,
    _open_existing_regular,
    _open_or_create_directory,
    _open_secure_root,
    _open_unique_partial,
    _reject_destination_symlink,
    _unlink_at,
)

RAW_EVENT_SCHEMA = pa.schema(
    [
        pa.field("event_key", pa.string(), nullable=False),
        pa.field("stream_name", pa.string(), nullable=False),
        pa.field("source_event_time", pa.int64(), nullable=False),
        pa.field("receive_time", pa.int64(), nullable=False),
        pa.field("source_id", pa.string()),
        pa.field("is_final", pa.bool_()),
        pa.field("payload_json", pa.string(), nullable=False),
    ]
)
MARK_PRICE_SCHEMA = pa.schema(
    [
        pa.field("event_time", pa.int64(), nullable=False),
        pa.field("mark_price", DECIMAL, nullable=False),
        pa.field("index_price", DECIMAL, nullable=False),
        pa.field("estimated_settle_price", DECIMAL, nullable=False),
        pa.field("funding_rate", DECIMAL, nullable=False),
        pa.field("next_funding_time", pa.int64(), nullable=False),
    ]
)
BOOK_TICKER_SCHEMA = pa.schema(
    [
        pa.field("update_id", pa.int64(), nullable=False),
        pa.field("transact_time", pa.int64(), nullable=False),
        pa.field("bid_price", DECIMAL, nullable=False),
        pa.field("bid_quantity", DECIMAL, nullable=False),
        pa.field("ask_price", DECIMAL, nullable=False),
        pa.field("ask_quantity", DECIMAL, nullable=False),
    ]
)


class LiveStorageError(ValueError):
    """A live event conflicts with already published immutable source identity."""


@dataclass(frozen=True)
class StoredLivePartition:
    path: Path
    sha256: str
    row_count: int


@dataclass(frozen=True)
class LiveWriteResult:
    raw: StoredLivePartition
    normalized: tuple[StoredLivePartition, ...]


@dataclass(frozen=True)
class _PartitionDescriptor:
    layer: str
    symbol: str
    dataset: LiveDataset
    partition_date: str
    filename: str

    @property
    def components(self) -> tuple[str, ...]:
        return (
            self.layer,
            "binance",
            "usdm",
            self.symbol,
            self.dataset.value,
            f"date={self.partition_date}",
        )


class LiveStorage:
    """Atomically retain raw events, then publish validated live normalization."""

    def __init__(self, data_root: Path) -> None:
        self.data_root = data_root
        self._locks: dict[_PartitionDescriptor, threading.Lock] = {}
        self._locks_guard = threading.Lock()

    def persist(self, event: ParsedStreamEvent) -> LiveWriteResult:
        partition_date = _event_date(event.source_event_time)
        raw_descriptor = _PartitionDescriptor(
            "raw",
            event.symbol,
            event.dataset,
            partition_date,
            "live-events.parquet",
        )
        raw = self._merge_publish(
            raw_descriptor,
            _raw_table(event),
            ("event_key",),
        )
        normalized_table = _normalized_table(event)
        if normalized_table is None:
            return LiveWriteResult(raw=raw, normalized=())
        normalized_descriptor = _PartitionDescriptor(
            "normalized",
            event.symbol,
            event.dataset,
            partition_date,
            "live.parquet",
        )
        normalized = self._merge_publish(
            normalized_descriptor,
            normalized_table[0],
            normalized_table[1],
        )
        return LiveWriteResult(raw=raw, normalized=(normalized,))

    def _merge_publish(
        self,
        descriptor: _PartitionDescriptor,
        incoming: pa.Table,
        primary_key: tuple[str, ...],
    ) -> StoredLivePartition:
        with self._locks_guard:
            partition_lock = self._locks.setdefault(descriptor, threading.Lock())
        with partition_lock:
            return self._merge_publish_locked(descriptor, incoming, primary_key)

    def _merge_publish_locked(
        self,
        descriptor: _PartitionDescriptor,
        incoming: pa.Table,
        primary_key: tuple[str, ...],
    ) -> StoredLivePartition:
        destination = self.data_root.joinpath(*descriptor.components, descriptor.filename)
        with _open_partition(self.data_root, descriptor) as partition_fd:
            existing = _read_existing(partition_fd, descriptor.filename, incoming.schema)
            merged = _merge_tables(existing, incoming, primary_key)
            if existing is not None and merged.equals(existing, check_metadata=True):
                existing_fd = _open_existing_regular(
                    partition_fd, descriptor.filename, "live partition"
                )
                if existing_fd is None:
                    raise LiveStorageError("published live partition disappeared")
                try:
                    return StoredLivePartition(
                        destination, _hash_fd(existing_fd), existing.num_rows
                    )
                finally:
                    os.close(existing_fd)
            checksum = _publish_table(partition_fd, descriptor.filename, merged)
        return StoredLivePartition(destination, checksum, merged.num_rows)


def _raw_table(event: ParsedStreamEvent) -> pa.Table:
    receive_time = _epoch_milliseconds(event.receive_time)
    event_key = "|".join(
        (
            event.symbol,
            event.dataset.value,
            event.source_id or "",
            str(event.source_event_time),
        )
    )
    row = {
        "event_key": event_key,
        "stream_name": event.stream.name,
        "source_event_time": event.source_event_time,
        "receive_time": receive_time,
        "source_id": event.source_id,
        "is_final": event.is_final,
        "payload_json": json.dumps(
            event.raw, sort_keys=True, separators=(",", ":"), ensure_ascii=True
        ),
    }
    return pa.Table.from_pylist([row], schema=RAW_EVENT_SCHEMA)


def _normalized_table(
    event: ParsedStreamEvent,
) -> tuple[pa.Table, tuple[str, ...]] | None:
    if event.dataset is LiveDataset.KLINES:
        if event.is_final is not True:
            return None
        return pa.Table.from_pylist(
            [_decimal_row(event.values, KLINE_SCHEMA)], schema=KLINE_SCHEMA
        ), ("open_time",)
    if event.dataset is LiveDataset.AGG_TRADES:
        return pa.Table.from_pylist(
            [_decimal_row(event.values, AGG_TRADE_SCHEMA)], schema=AGG_TRADE_SCHEMA
        ), ("aggregate_trade_id",)
    if event.dataset is LiveDataset.MARK_PRICE:
        values = {"event_time": event.source_event_time, **event.values}
        return pa.Table.from_pylist(
            [_decimal_row(values, MARK_PRICE_SCHEMA)], schema=MARK_PRICE_SCHEMA
        ), ("event_time",)
    if event.dataset is LiveDataset.BOOK_TICKER:
        return pa.Table.from_pylist(
            [_decimal_row(event.values, BOOK_TICKER_SCHEMA)], schema=BOOK_TICKER_SCHEMA
        ), ("update_id",)
    return None


def _decimal_row(values: object, schema: pa.Schema) -> dict[str, object]:
    if not isinstance(values, dict):
        values = dict(values)  # type: ignore[arg-type]
    row = dict(values)
    for field in schema:
        if pa.types.is_decimal(field.type):
            row[field.name] = Decimal(str(row[field.name]))
    return row


def _event_date(milliseconds: int) -> str:
    try:
        return datetime.fromtimestamp(milliseconds / 1_000, UTC).date().isoformat()
    except (OSError, OverflowError, ValueError) as error:
        raise LiveStorageError("event time cannot be partitioned") from error


def _epoch_milliseconds(value: datetime) -> int:
    epoch = datetime(1970, 1, 1, tzinfo=UTC)
    difference = value.astimezone(UTC) - epoch
    return (
        difference.days * 86_400_000
        + difference.seconds * 1_000
        + difference.microseconds // 1_000
    )


@contextmanager
def _open_partition(
    data_root: Path, descriptor: _PartitionDescriptor
) -> Iterator[int]:
    descriptors = [_open_secure_root(data_root)]
    try:
        for component in descriptor.components:
            descriptors.append(_open_or_create_directory(descriptors[-1], component))
        yield descriptors[-1]
    finally:
        for opened in reversed(descriptors):
            os.close(opened)


def _read_existing(
    partition_fd: int, filename: str, schema: pa.Schema
) -> pa.Table | None:
    descriptor = _open_existing_regular(partition_fd, filename, "live partition")
    if descriptor is None:
        return None
    try:
        with os.fdopen(os.dup(descriptor), "rb") as source:
            table = pq.read_table(source)
    except Exception as error:
        raise LiveStorageError("existing live partition is not readable Parquet") from error
    finally:
        os.close(descriptor)
    if not table.schema.equals(schema, check_metadata=True):
        raise LiveStorageError("existing live partition schema does not match")
    return table


def _merge_tables(
    existing: pa.Table | None,
    incoming: pa.Table,
    primary_key: tuple[str, ...],
) -> pa.Table:
    rows: dict[tuple[object, ...], dict[str, object]] = {}
    if existing is not None:
        for row in existing.to_pylist():
            rows[tuple(row[field] for field in primary_key)] = row
    for row in incoming.to_pylist():
        key = tuple(row[field] for field in primary_key)
        previous = rows.get(key)
        if previous is not None and previous != row:
            raise LiveStorageError("live replay conflicts with published source identity")
        rows[key] = row
    ordered = [rows[key] for key in sorted(rows)]
    return pa.Table.from_pylist(ordered, schema=incoming.schema)


def _publish_table(partition_fd: int, filename: str, table: pa.Table) -> str:
    temp_name, temp_fd = _open_unique_partial(partition_fd, filename)
    try:
        with os.fdopen(os.dup(temp_fd), "wb") as output:
            pq.write_table(
                table,
                output,
                compression="zstd",
                row_group_size=max(1, table.num_rows),
            )
            output.flush()
        os.fsync(temp_fd)
        with os.fdopen(os.dup(temp_fd), "rb") as source:
            published = pq.read_table(source)
        if not published.equals(table, check_metadata=True):
            raise LiveStorageError("live Parquet validation failed")
        checksum = _hash_fd(temp_fd)
        _reject_destination_symlink(partition_fd, filename)
        os.replace(
            temp_name,
            filename,
            src_dir_fd=partition_fd,
            dst_dir_fd=partition_fd,
        )
        os.fsync(partition_fd)
        return checksum
    finally:
        os.close(temp_fd)
        _unlink_at(partition_fd, temp_name)
