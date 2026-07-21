from __future__ import annotations

import fcntl
import gzip
import hashlib
import io
import json
import os
import stat
import threading
from collections import defaultdict
from collections.abc import Iterable, Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from uuid import uuid4

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
    open_secure_relative_file,
)

MARK_PRICE_SCHEMA = pa.schema(
    [
        pa.field("event_time", pa.int64(), nullable=False),
        pa.field("mark_price", DECIMAL, nullable=False),
        pa.field("index_price", DECIMAL, nullable=False),
        pa.field("estimated_settle_price", DECIMAL, nullable=False),
        # These are a provisional next-funding observation, not realized funding.
        pa.field("provisional_funding_rate", DECIMAL, nullable=False),
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

_LOCK_NAME = ".live-writer.lock"
_INDEX_NAME = "events-index.json"


class LiveStorageError(ValueError):
    """Live bytes or replay identity violate the append-only storage contract."""


class LiveWriterUnavailable(LiveStorageError):
    """Another process owns the authoritative live-writer lock."""


@dataclass(frozen=True)
class StoredLivePartition:
    path: Path
    sha256: str
    row_count: int


@dataclass(frozen=True)
class LiveWriteResult:
    raw: tuple[StoredLivePartition, ...]
    normalized: tuple[StoredLivePartition, ...]
    replayed_count: int = 0


@dataclass(frozen=True)
class _PartitionDescriptor:
    layer: str
    symbol: str
    dataset: LiveDataset
    partition_date: str

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


@dataclass(frozen=True)
class _IndexedEvent:
    identity: str
    payload_hash: str
    event: ParsedStreamEvent


class LiveWriterLease:
    """Descriptor-backed ownership token; closing it releases the OS lock."""

    def __init__(self, storage: LiveStorage, descriptor: int, owner: str) -> None:
        self._storage = storage
        self._descriptor = descriptor
        self.owner = owner
        self.token = uuid4().hex
        self._closed = False

    @property
    def active(self) -> bool:
        return not self._closed

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        try:
            fcntl.flock(self._descriptor, fcntl.LOCK_UN)
        finally:
            os.close(self._descriptor)

    def __enter__(self) -> LiveWriterLease:
        return self

    def __exit__(self, *_args: object) -> None:
        self.close()


class LiveStorage:
    """Publish bounded batches as immutable raw and normalized shards."""

    def __init__(self, data_root: Path) -> None:
        self.data_root = data_root
        self._write_lock = threading.Lock()

    def acquire_writer(self, owner: str) -> LiveWriterLease:
        if not owner:
            raise ValueError("live writer owner must not be empty")
        root_fd = _open_secure_root(self.data_root)
        try:
            flags = os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW | os.O_CLOEXEC
            lock_fd = os.open(_LOCK_NAME, flags, 0o600, dir_fd=root_fd)
        except Exception:
            os.close(root_fd)
            raise
        os.close(root_fd)
        try:
            lock_stat = os.fstat(lock_fd)
            if not stat.S_ISREG(lock_stat.st_mode) or stat.S_IMODE(lock_stat.st_mode) & 0o077:
                raise LiveStorageError("live writer lock must be a private regular file")
            try:
                fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as error:
                raise LiveWriterUnavailable(
                    "another authoritative live writer is already active"
                ) from error
            os.ftruncate(lock_fd, 0)
            _write_all(lock_fd, owner.encode("utf-8"))
            os.fsync(lock_fd)
            return LiveWriterLease(self, lock_fd, owner)
        except Exception:
            os.close(lock_fd)
            raise

    def persist_batch(
        self, lease: LiveWriterLease, events: Iterable[ParsedStreamEvent]
    ) -> LiveWriteResult:
        if lease._storage is not self or not lease.active:
            raise LiveWriterUnavailable("authoritative live writer lease is not active")
        batch = tuple(events)
        if not batch:
            return LiveWriteResult((), ())
        by_identity: dict[str, _IndexedEvent] = {}
        repeated = 0
        for event in batch:
            item = _index_event(event)
            previous = by_identity.get(item.identity)
            if previous is None:
                by_identity[item.identity] = item
            elif previous.payload_hash != item.payload_hash:
                raise LiveStorageError(
                    "live replay conflicts with published source identity"
                )
            else:
                repeated += 1
        with self._write_lock:
            result = self._persist_batch_locked(tuple(by_identity.values()))
        return LiveWriteResult(
            result.raw,
            result.normalized,
            result.replayed_count + repeated,
        )

    def _persist_batch_locked(
        self, indexed: tuple[_IndexedEvent, ...]
    ) -> LiveWriteResult:
        groups: dict[_PartitionDescriptor, list[_IndexedEvent]] = defaultdict(list)
        for item in indexed:
            event = item.event
            groups[
                _PartitionDescriptor(
                    "raw", event.symbol, event.dataset, _event_date(event.source_event_time)
                )
            ].append(item)

        plans: list[
            tuple[_PartitionDescriptor, dict[str, object], list[_IndexedEvent], list[_IndexedEvent]]
        ] = []
        replayed = 0
        replay_parts: dict[str, StoredLivePartition] = {}
        # Preflight every partition before publishing any bytes.
        for descriptor, items in groups.items():
            ledger = _read_index(self.data_root, descriptor)
            new_items: list[_IndexedEvent] = []
            existing_items: list[_IndexedEvent] = []
            entries = ledger["events"]
            if not isinstance(entries, dict):
                raise LiveStorageError("live event index has an invalid shape")
            for item in items:
                previous = entries.get(item.identity)
                if previous is None:
                    new_items.append(item)
                    continue
                if (
                    not isinstance(previous, dict)
                    or previous.get("payload_hash") != item.payload_hash
                ):
                    raise LiveStorageError(
                        "live replay conflicts with published source identity"
                    )
                existing_items.append(item)
                replayed += 1
                for part in _entry_partitions(self.data_root, previous):
                    replay_parts[str(part.path)] = part
            plans.append((descriptor, ledger, new_items, existing_items))

        raw_parts: dict[str, StoredLivePartition] = dict(replay_parts)
        normalized_parts: dict[str, StoredLivePartition] = {}
        for descriptor, ledger, new_items, _existing_items in plans:
            if not new_items:
                continue
            ordered = sorted(new_items, key=_event_order)
            raw_bytes = _raw_ndjson(ordered)
            raw_name = f"part-{hashlib.sha256(raw_bytes).hexdigest()[:24]}.ndjson.gz"
            raw_part = _publish_immutable_bytes(
                self.data_root, descriptor, raw_name, raw_bytes, len(ordered)
            )
            raw_parts[str(raw_part.path)] = raw_part

            normalized_by_descriptor: dict[
                _PartitionDescriptor, list[ParsedStreamEvent]
            ] = defaultdict(list)
            for item in ordered:
                if _normalizes(item.event):
                    normalized_by_descriptor[
                        _PartitionDescriptor(
                            "normalized",
                            item.event.symbol,
                            item.event.dataset,
                            _event_date(item.event.source_event_time),
                        )
                    ].append(item.event)
            published_normalized: list[StoredLivePartition] = []
            for normalized_descriptor, normalized_events in normalized_by_descriptor.items():
                table = _normalized_table(normalized_events)
                parquet_bytes = _parquet_bytes(table)
                normalized_name = (
                    f"part-{hashlib.sha256(parquet_bytes).hexdigest()[:24]}.parquet"
                )
                part = _publish_immutable_bytes(
                    self.data_root,
                    normalized_descriptor,
                    normalized_name,
                    parquet_bytes,
                    table.num_rows,
                )
                normalized_parts[str(part.path)] = part
                published_normalized.append(part)

            entries = ledger["events"]
            assert isinstance(entries, dict)
            raw_record = _partition_record(self.data_root, raw_part)
            normalized_records = [
                _partition_record(self.data_root, part) for part in published_normalized
            ]
            for item in ordered:
                entries[item.identity] = {
                    "payload_hash": item.payload_hash,
                    "raw": raw_record,
                    "normalized": normalized_records if _normalizes(item.event) else [],
                }
            _write_index(self.data_root, descriptor, ledger)

        for part in replay_parts.values():
            if part.path.relative_to(self.data_root).parts[0] == "normalized":
                normalized_parts[str(part.path)] = part
                raw_parts.pop(str(part.path), None)
        return LiveWriteResult(
            tuple(sorted(raw_parts.values(), key=lambda part: str(part.path))),
            tuple(sorted(normalized_parts.values(), key=lambda part: str(part.path))),
            replayed,
        )


def _index_event(event: ParsedStreamEvent) -> _IndexedEvent:
    source_identity = (
        f"source-id:{event.source_id}"
        if event.source_id is not None
        else f"event-time:{event.source_event_time}"
    )
    identity = "|".join(
        (event.symbol, event.dataset.value, event.stream.name, source_identity)
    )
    canonical = {
        "symbol": event.symbol,
        "dataset": event.dataset.value,
        "stream": event.stream.name,
        "source_event_time": event.source_event_time,
        "source_id": event.source_id,
        "is_final": event.is_final,
        "values": dict(event.values),
        "payload": dict(event.raw),
    }
    payload_hash = hashlib.sha256(_json_bytes(canonical)).hexdigest()
    return _IndexedEvent(identity, payload_hash, event)


def _event_order(item: _IndexedEvent) -> tuple[int, int | str, str]:
    source_id: int | str
    if item.event.source_id is not None and item.event.source_id.isdecimal():
        source_id = int(item.event.source_id)
    else:
        source_id = item.event.source_id or ""
    return item.event.source_event_time, source_id, item.identity


def _raw_ndjson(items: list[_IndexedEvent]) -> bytes:
    rows = []
    for item in items:
        event = item.event
        rows.append(
            _json_bytes(
                {
                    "event_key": item.identity,
                    "stream_name": event.stream.name,
                    "source_event_time": event.source_event_time,
                    "receive_time": _epoch_milliseconds(event.receive_time),
                    "source_id": event.source_id,
                    "is_final": event.is_final,
                    "payload": dict(event.raw),
                }
            )
        )
    return gzip.compress(b"\n".join(rows) + b"\n", compresslevel=6, mtime=0)


def _normalizes(event: ParsedStreamEvent) -> bool:
    return event.dataset is not LiveDataset.KLINES or event.is_final is True


def _normalized_table(events: list[ParsedStreamEvent]) -> pa.Table:
    event = events[0]
    if any(current.dataset is not event.dataset for current in events):
        raise LiveStorageError("normalized batch cannot mix datasets")
    if event.dataset is LiveDataset.KLINES:
        schema = KLINE_SCHEMA
        rows = [_decimal_row(current.values, schema) for current in events]
    elif event.dataset is LiveDataset.AGG_TRADES:
        schema = AGG_TRADE_SCHEMA
        rows = [_decimal_row(current.values, schema) for current in events]
    elif event.dataset is LiveDataset.MARK_PRICE:
        schema = MARK_PRICE_SCHEMA
        rows = [
            _decimal_row(
                {
                    "event_time": current.source_event_time,
                    "mark_price": current.values["mark_price"],
                    "index_price": current.values["index_price"],
                    "estimated_settle_price": current.values[
                        "estimated_settle_price"
                    ],
                    "provisional_funding_rate": current.values[
                        "provisional_funding_rate"
                    ],
                    "next_funding_time": current.values["next_funding_time"],
                },
                schema,
            )
            for current in events
        ]
    elif event.dataset is LiveDataset.BOOK_TICKER:
        schema = BOOK_TICKER_SCHEMA
        rows = [_decimal_row(current.values, schema) for current in events]
    else:
        raise LiveStorageError(f"unsupported live dataset: {event.dataset}")
    return pa.Table.from_pylist(rows, schema=schema)


def _decimal_row(values: Mapping[str, object], schema: pa.Schema) -> dict[str, object]:
    row = dict(values)
    for field in schema:
        if pa.types.is_decimal(field.type):
            row[field.name] = Decimal(str(row[field.name]))
    return row


def _parquet_bytes(table: pa.Table) -> bytes:
    output = io.BytesIO()
    pq.write_table(
        table,
        output,
        compression="zstd",
        row_group_size=max(1, table.num_rows),
    )
    payload = output.getvalue()
    published = pq.read_table(io.BytesIO(payload))
    if not published.equals(table, check_metadata=True):
        raise LiveStorageError("live Parquet validation failed")
    return payload


def _read_index(data_root: Path, descriptor: _PartitionDescriptor) -> dict[str, object]:
    with _open_partition(data_root, descriptor) as partition_fd:
        existing = _open_existing_regular(partition_fd, _INDEX_NAME, "live event index")
        if existing is None:
            return {"version": 1, "events": {}}
        try:
            with os.fdopen(os.dup(existing), "rb") as source:
                payload = json.load(source)
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
            raise LiveStorageError("live event index is unreadable") from error
        finally:
            os.close(existing)
    if not isinstance(payload, dict) or payload.get("version") != 1:
        raise LiveStorageError("live event index has an invalid version")
    return payload


def _write_index(
    data_root: Path, descriptor: _PartitionDescriptor, ledger: dict[str, object]
) -> None:
    payload = _json_bytes(ledger) + b"\n"
    with _open_partition(data_root, descriptor) as partition_fd:
        temp_name, temp_fd = _open_unique_partial(partition_fd, _INDEX_NAME)
        try:
            _write_all(temp_fd, payload)
            os.fsync(temp_fd)
            _reject_destination_symlink(partition_fd, _INDEX_NAME)
            os.replace(
                temp_name,
                _INDEX_NAME,
                src_dir_fd=partition_fd,
                dst_dir_fd=partition_fd,
            )
            os.fsync(partition_fd)
        finally:
            os.close(temp_fd)
            _unlink_at(partition_fd, temp_name)


def _publish_immutable_bytes(
    data_root: Path,
    descriptor: _PartitionDescriptor,
    filename: str,
    payload: bytes,
    row_count: int,
) -> StoredLivePartition:
    destination = data_root.joinpath(*descriptor.components, filename)
    checksum = hashlib.sha256(payload).hexdigest()
    with _open_partition(data_root, descriptor) as partition_fd:
        existing = _open_existing_regular(partition_fd, filename, "live shard")
        if existing is not None:
            try:
                if _hash_fd(existing) != checksum:
                    raise LiveStorageError("immutable live shard checksum conflicts")
                return StoredLivePartition(destination, checksum, row_count)
            finally:
                os.close(existing)
        temp_name, temp_fd = _open_unique_partial(partition_fd, filename)
        try:
            _write_all(temp_fd, payload)
            os.fsync(temp_fd)
            _reject_destination_symlink(partition_fd, filename)
            os.link(
                temp_name,
                filename,
                src_dir_fd=partition_fd,
                dst_dir_fd=partition_fd,
                follow_symlinks=False,
            )
            os.fsync(partition_fd)
        except FileExistsError:
            existing = _open_existing_regular(partition_fd, filename, "live shard")
            if existing is None:
                raise LiveStorageError(
                    "immutable live shard disappeared"
                ) from None
            try:
                if _hash_fd(existing) != checksum:
                    raise LiveStorageError("immutable live shard checksum conflicts")
            finally:
                os.close(existing)
        finally:
            os.close(temp_fd)
            _unlink_at(partition_fd, temp_name)
    return StoredLivePartition(destination, checksum, row_count)


def _partition_record(data_root: Path, part: StoredLivePartition) -> dict[str, object]:
    return {
        "path": part.path.relative_to(data_root).as_posix(),
        "sha256": part.sha256,
        "row_count": part.row_count,
    }


def _entry_partitions(
    data_root: Path, entry: dict[str, object]
) -> tuple[StoredLivePartition, ...]:
    normalized = entry.get("normalized", [])
    if not isinstance(normalized, list):
        raise LiveStorageError("live event index partition is invalid")
    records = [entry.get("raw"), *normalized]
    result: list[StoredLivePartition] = []
    for record in records:
        if not isinstance(record, dict):
            raise LiveStorageError("live event index partition is invalid")
        relative = record.get("path")
        checksum = record.get("sha256")
        row_count = record.get("row_count")
        if (
            not isinstance(relative, str)
            or relative.startswith("/")
            or ".." in Path(relative).parts
            or not isinstance(checksum, str)
            or not isinstance(row_count, int)
        ):
            raise LiveStorageError("live event index partition is invalid")
        path = data_root / relative
        try:
            with open_secure_relative_file(data_root, relative) as descriptor:
                if _hash_fd(descriptor) != checksum:
                    raise LiveStorageError("indexed live shard checksum conflicts")
        except ValueError as error:
            raise LiveStorageError("indexed live shard is missing or unsafe") from error
        result.append(StoredLivePartition(path, checksum, row_count))
    return tuple(result)


def _json_bytes(value: object) -> bytes:
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=True
    ).encode("ascii")


def _write_all(descriptor: int, payload: bytes) -> None:
    view = memoryview(payload)
    while view:
        written = os.write(descriptor, view)
        if written <= 0:
            raise LiveStorageError("live shard write did not make progress")
        view = view[written:]


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
