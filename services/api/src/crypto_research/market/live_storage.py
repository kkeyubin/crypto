from __future__ import annotations

import fcntl
import gzip
import hashlib
import io
import json
import os
import stat
import threading
from collections.abc import Iterator, Mapping
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
from crypto_research.market.binance.streams import (
    LiveDataset,
    ParsedStreamEvent,
    parse_stream_message,
)
from crypto_research.market.live_journal import (
    JournalBatch,
    JournalEvent,
    LiveJournalError,
    LivePartitionJournal,
)
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
_LIVE_JOURNAL_BUCKETS = 64
_DIRECTORY_READ_FLAGS = (
    os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC
)
_LEGACY_RECOVERY = (
    "stop the worker; cp -a DATA_ROOT/spool/binance/usdm RECOVERY_ROOT/usdm; "
    "follow docs/runbooks/market-data-recovery.md; do not delete or merge journals"
)


class LiveStorageError(ValueError):
    """Live bytes or replay identity violate the append-only storage contract."""


class LiveWriterUnavailable(LiveStorageError):
    """Another process owns the authoritative live-writer lock."""


@dataclass(frozen=True)
class StoredLivePartition:
    path: Path
    sha256: str
    row_count: int
    layer: str = ""
    symbol: str = ""
    dataset: str = ""
    partition_date: str = ""
    schema_name: str = ""
    sort_keys: tuple[str, ...] = ()
    unique_keys: tuple[str, ...] = ()
    min_source_event_time: int | None = None
    max_source_event_time: int | None = None
    min_canonical_time: int | None = None
    max_canonical_time: int | None = None
    relative_path: str = ""


@dataclass(frozen=True)
class LiveWriteResult:
    raw: tuple[StoredLivePartition, ...]
    normalized: tuple[StoredLivePartition, ...]
    replayed_count: int = 0
    batch_id: str | None = None
    events: tuple[ParsedStreamEvent, ...] = ()


@dataclass(frozen=True)
class LiveAcceptResult:
    accepted: bool
    replayed: bool


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
        with self._storage._write_lock:
            return not self._closed

    def close(self) -> None:
        self._storage._close_lease(self)

    def __enter__(self) -> LiveWriterLease:
        return self

    def __exit__(self, *_args: object) -> None:
        self.close()


class LiveStorage:
    """Durably spool events, then publish recoverable immutable live shards."""

    def __init__(self, data_root: Path) -> None:
        self.data_root = data_root
        self._write_lock = threading.RLock()
        self._journals_by_bucket: dict[int, LivePartitionJournal] = {}
        self._journals_discovered = False

    def acquire_writer(self, owner: str) -> LiveWriterLease:
        if not owner:
            raise ValueError("live writer owner must not be empty")
        with self._write_lock:
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
                if not stat.S_ISREG(lock_stat.st_mode) or (
                    stat.S_IMODE(lock_stat.st_mode) & 0o077
                ):
                    raise LiveStorageError(
                        "live writer lock must be a private regular file"
                    )
                try:
                    fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                except BlockingIOError as error:
                    raise LiveWriterUnavailable(
                        "another authoritative live writer is already active"
                    ) from error
                os.ftruncate(lock_fd, 0)
                _write_all(lock_fd, owner.encode("utf-8"))
                os.fsync(lock_fd)
                self._discover_journals()
                return LiveWriterLease(self, lock_fd, owner)
            except Exception:
                os.close(lock_fd)
                raise

    def _close_lease(self, lease: LiveWriterLease) -> None:
        with self._write_lock:
            if lease._storage is not self or lease._closed:
                return
            lease._closed = True
            try:
                fcntl.flock(lease._descriptor, fcntl.LOCK_UN)
            finally:
                os.close(lease._descriptor)

    def accept(
        self, lease: LiveWriterLease, event: ParsedStreamEvent
    ) -> LiveAcceptResult:
        """Commit one event to its WAL spool before acknowledging the caller."""
        with self._write_lock:
            self._require_lease_locked(lease)
            indexed = _index_event(event)
            normalized_key = _normalized_key(event)
            _bucket, journal = self._journal_for_event(event)
            try:
                result = journal.accept(
                    identity=indexed.identity,
                    payload_hash=indexed.payload_hash,
                    event_json=_serialize_event(event),
                    source_event_time=event.source_event_time,
                    partition_key=_source_partition_key(event),
                    normalized_key=normalized_key,
                    normalized_hash=(
                        _normalized_payload_hash(event)
                        if normalized_key is not None
                        else None
                    ),
                )
            except LiveJournalError as error:
                raise LiveStorageError(str(error)) from error
            return LiveAcceptResult(result.accepted, not result.accepted)

    def publish_next_batch(
        self, lease: LiveWriterLease, *, max_events: int
    ) -> LiveWriteResult | None:
        """Publish or recover one journaled batch; catalog acknowledgement is separate."""
        if max_events <= 0:
            raise ValueError("max_events must be positive")
        with self._write_lock:
            self._require_lease_locked(lease)
            journals = self._journals()
            for _bucket, journal in journals:
                batch = journal.next_open_batch()
                if batch is not None:
                    return self._publish_journal_batch(journal, batch)
            for bucket, journal in journals:
                queued = journal.queued(max_events)
                if not queued:
                    continue
                batch = self._prepare_journal_batch(bucket, journal, queued)
                return self._publish_journal_batch(journal, batch)
            return None

    def acknowledge_cataloged(
        self, lease: LiveWriterLease, batch_id: str
    ) -> None:
        if not batch_id:
            raise ValueError("batch_id must not be empty")
        with self._write_lock:
            self._require_lease_locked(lease)
            bucket = _batch_bucket(batch_id)
            journal = self._journal_for_bucket(bucket, create=False)
            if journal is None:
                raise LiveStorageError("live batch is not present in the durable spool")
            try:
                if journal.acknowledge_cataloged(batch_id):
                    return
            except LiveJournalError as error:
                raise LiveStorageError(str(error)) from error
            raise LiveStorageError("live batch is not present in the durable spool")

    def _require_lease_locked(self, lease: LiveWriterLease) -> None:
        if lease._storage is not self or lease._closed:
            raise LiveWriterUnavailable("authoritative live writer lease is not active")

    def _journal_for_event(
        self, event: ParsedStreamEvent
    ) -> tuple[int, LivePartitionJournal]:
        bucket = _journal_bucket(event)
        journal = self._journal_for_bucket(bucket, create=True)
        assert journal is not None
        return bucket, journal

    def _journal_for_bucket(
        self, bucket: int, *, create: bool
    ) -> LivePartitionJournal | None:
        journal = self._journals_by_bucket.get(bucket)
        if journal is not None:
            return journal
        path = _journal_path(self.data_root, bucket)
        if not create:
            try:
                file_stat = os.lstat(path)
            except FileNotFoundError:
                return None
            if not stat.S_ISREG(file_stat.st_mode) or stat.S_ISLNK(file_stat.st_mode):
                raise LiveStorageError("live journal must be a private regular file")
        with _open_journal_bucket(self.data_root, bucket):
            pass
        journal = LivePartitionJournal(path)
        self._journals_by_bucket[bucket] = journal
        return journal

    def _discover_journals(self) -> None:
        if self._journals_discovered:
            return
        _reject_legacy_spool(self.data_root)
        for bucket in range(_LIVE_JOURNAL_BUCKETS):
            self._journal_for_bucket(bucket, create=False)
        self._journals_discovered = True

    def _journals(self) -> tuple[tuple[int, LivePartitionJournal], ...]:
        self._discover_journals()
        return tuple(sorted(self._journals_by_bucket.items()))

    def _prepare_journal_batch(
        self,
        bucket: int,
        journal: LivePartitionJournal,
        events: tuple[JournalEvent, ...],
    ) -> JournalBatch:
        digest = hashlib.sha256(
            _json_bytes(
                {
                    "journal": journal.path.relative_to(self.data_root).as_posix(),
                    "events": [
                        [event.identity, event.payload_hash] for event in events
                    ],
                }
            )
        ).hexdigest()[:32]
        batch_id = f"b{bucket:02x}-{digest}"
        artifacts = _planned_artifacts(self.data_root, events)
        manifest = {
            "version": 1,
            "batch_id": batch_id,
            "artifacts": [artifact[2] for artifact in artifacts],
        }
        try:
            return journal.prepare(batch_id, events, _json_bytes(manifest).decode("ascii"))
        except LiveJournalError as error:
            raise LiveStorageError(str(error)) from error

    def _publish_journal_batch(
        self, journal: LivePartitionJournal, batch: JournalBatch
    ) -> LiveWriteResult:
        try:
            events = journal.batch_events(batch.batch_id)
            artifacts = _planned_artifacts(self.data_root, events)
            manifest = _manifest_object(batch.manifest_json)
            planned_records = [artifact[2] for artifact in artifacts]
            if (
                manifest.get("batch_id") != batch.batch_id
                or manifest.get("artifacts") != planned_records
            ):
                raise LiveStorageError("prepared live batch manifest is inconsistent")
            for descriptor, payload, record in artifacts:
                _publish_immutable_bytes(
                    self.data_root,
                    descriptor,
                    Path(str(record["path"])).name,
                    payload,
                    int(record["row_count"]),
                )
            journal.mark_published(batch.batch_id)
        except LiveJournalError as error:
            raise LiveStorageError(str(error)) from error
        result = _result_from_manifest(self.data_root, batch.manifest_json)
        return LiveWriteResult(
            result.raw,
            result.normalized,
            result.replayed_count,
            result.batch_id,
            tuple(_deserialize_event(event.event_json) for event in events),
        )


def _serialize_event(event: ParsedStreamEvent) -> str:
    return _json_bytes(
        {
            "stream": event.stream.name,
            "receive_time": event.receive_time.isoformat(),
            "payload": dict(event.raw),
        }
    ).decode("ascii")


def _deserialize_event(payload: str) -> ParsedStreamEvent:
    try:
        stored = json.loads(payload)
        stream = stored["stream"]
        receive_time = datetime.fromisoformat(stored["receive_time"])
        raw = stored["payload"]
    except (KeyError, TypeError, ValueError, json.JSONDecodeError) as error:
        raise LiveStorageError("journaled live event is invalid") from error
    if not isinstance(stream, str) or not isinstance(raw, dict):
        raise LiveStorageError("journaled live event is invalid")
    try:
        return parse_stream_message(
            {"stream": stream, "data": raw}, receive_time
        )
    except ValueError as error:
        raise LiveStorageError("journaled live event cannot be reparsed") from error


def _normalized_key(event: ParsedStreamEvent) -> str | None:
    if not _normalizes(event):
        return None
    return _source_natural_key(event)


def _source_natural_key(event: ParsedStreamEvent) -> str:
    if event.dataset is LiveDataset.KLINES:
        value = event.values["open_time"]
    elif event.dataset is LiveDataset.AGG_TRADES:
        value = event.values["aggregate_trade_id"]
    elif event.dataset is LiveDataset.MARK_PRICE:
        value = event.source_event_time
    elif event.dataset is LiveDataset.BOOK_TICKER:
        value = event.values["update_id"]
    else:
        raise LiveStorageError(f"unsupported live dataset: {event.dataset}")
    return f"{event.symbol}|{event.dataset.value}|{value}"


def _journal_bucket(event: ParsedStreamEvent) -> int:
    # A stream is the durable uniqueness domain for all of its source/natural
    # keys.  Keeping the domain stable across dates preserves cross-day checks
    # while allowing one source partition to publish as one recoverable batch.
    domain = f"{event.symbol}|{event.dataset.value}"
    digest = hashlib.sha256(domain.encode("utf-8")).digest()
    return int.from_bytes(digest[:8], "big") % _LIVE_JOURNAL_BUCKETS


def _source_partition_key(event: ParsedStreamEvent) -> str:
    return "|".join(
        (event.symbol, event.dataset.value, _event_date(event.source_event_time))
    )


def _journal_path(data_root: Path, bucket: int) -> Path:
    if not 0 <= bucket < _LIVE_JOURNAL_BUCKETS:
        raise LiveStorageError("live journal bucket is invalid")
    return (
        data_root
        / "spool"
        / "binance"
        / "usdm"
        / f"bucket={bucket:02x}"
        / "journal.sqlite3"
    )


def _batch_bucket(batch_id: str) -> int:
    prefix, separator, digest = batch_id.partition("-")
    if (
        separator != "-"
        or len(prefix) != 3
        or prefix[0] != "b"
        or len(digest) != 32
        or any(character not in "0123456789abcdef" for character in prefix[1:] + digest)
    ):
        raise LiveStorageError("live batch identity is invalid")
    bucket = int(prefix[1:], 16)
    if bucket >= _LIVE_JOURNAL_BUCKETS:
        raise LiveStorageError("live batch identity is invalid")
    return bucket


def _reject_legacy_spool(data_root: Path) -> None:
    descriptors = [_open_secure_root(data_root)]
    try:
        for component in ("spool", "binance", "usdm"):
            descriptor = _open_existing_spool_directory(
                descriptors[-1], component
            )
            if descriptor is None:
                return
            descriptors.append(descriptor)
        usdm_fd = descriptors[-1]
        for entry in sorted(os.listdir(usdm_fd)):
            if _is_bucket_component(entry):
                bucket_fd = _open_existing_spool_directory(usdm_fd, entry)
                if bucket_fd is None:
                    continue
                os.close(bucket_fd)
                continue
            legacy_path = _find_legacy_journal(usdm_fd, entry)
            if legacy_path is not None:
                raise LiveStorageError(
                    f"legacy per-day live spool detected at {legacy_path}; "
                    f"recovery: {_LEGACY_RECOVERY}"
                )
            raise LiveStorageError(
                f"unsafe legacy live spool entry detected at "
                f"spool/binance/usdm/{entry}; recovery: {_LEGACY_RECOVERY}"
            )
    finally:
        for descriptor in reversed(descriptors):
            os.close(descriptor)


def _is_bucket_component(value: str) -> bool:
    if len(value) != len("bucket=00") or not value.startswith("bucket="):
        return False
    try:
        bucket = int(value.removeprefix("bucket="), 16)
    except ValueError:
        return False
    return 0 <= bucket < _LIVE_JOURNAL_BUCKETS


def _open_existing_spool_directory(parent_fd: int, name: str) -> int | None:
    try:
        before = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
    except FileNotFoundError:
        return None
    if stat.S_ISLNK(before.st_mode) or not stat.S_ISDIR(before.st_mode):
        raise LiveStorageError(
            f"unsafe legacy live spool entry detected; recovery: {_LEGACY_RECOVERY}"
        )
    try:
        descriptor = os.open(name, _DIRECTORY_READ_FLAGS, dir_fd=parent_fd)
    except OSError as error:
        raise LiveStorageError(
            f"unsafe legacy live spool entry detected; recovery: {_LEGACY_RECOVERY}"
        ) from error
    opened = os.fstat(descriptor)
    if (
        not stat.S_ISDIR(opened.st_mode)
        or opened.st_dev != before.st_dev
        or opened.st_ino != before.st_ino
    ):
        os.close(descriptor)
        raise LiveStorageError(
            f"unsafe legacy live spool entry detected; recovery: {_LEGACY_RECOVERY}"
        )
    return descriptor


def _find_legacy_journal(usdm_fd: int, symbol: str) -> str | None:
    symbol_fd = _open_existing_spool_directory(usdm_fd, symbol)
    if symbol_fd is None:
        return None
    try:
        for dataset in sorted(os.listdir(symbol_fd)):
            dataset_fd = _open_existing_spool_directory(symbol_fd, dataset)
            if dataset_fd is None:
                continue
            try:
                for partition in sorted(os.listdir(dataset_fd)):
                    if not partition.startswith("date="):
                        continue
                    partition_fd = _open_existing_spool_directory(
                        dataset_fd, partition
                    )
                    if partition_fd is None:
                        continue
                    try:
                        try:
                            journal = os.stat(
                                "journal.sqlite3",
                                dir_fd=partition_fd,
                                follow_symlinks=False,
                            )
                        except FileNotFoundError:
                            continue
                        if not stat.S_ISREG(journal.st_mode):
                            raise LiveStorageError(
                                "unsafe legacy live spool entry detected; "
                                f"recovery: {_LEGACY_RECOVERY}"
                            )
                        return (
                            f"spool/binance/usdm/{symbol}/{dataset}/"
                            f"{partition}/journal.sqlite3"
                        )
                    finally:
                        os.close(partition_fd)
            finally:
                os.close(dataset_fd)
    finally:
        os.close(symbol_fd)
    return None


def _normalized_payload_hash(event: ParsedStreamEvent) -> str:
    return hashlib.sha256(
        _json_bytes(
            {
                "symbol": event.symbol,
                "dataset": event.dataset.value,
                "values": dict(event.values),
            }
        )
    ).hexdigest()


def _planned_artifacts(
    data_root: Path, journal_events: tuple[JournalEvent, ...]
) -> tuple[tuple[_PartitionDescriptor, bytes, dict[str, object]], ...]:
    if not journal_events:
        raise LiveStorageError("prepared live batch cannot be empty")
    indexed = [
        _IndexedEvent(
            journal_event.identity,
            journal_event.payload_hash,
            _deserialize_event(journal_event.event_json),
        )
        for journal_event in journal_events
    ]
    indexed.sort(key=_event_order)
    first = indexed[0].event
    partition_date = _event_date(first.source_event_time)
    partition_key = _source_partition_key(first)
    if any(
        item.event.symbol != first.symbol
        or item.event.dataset is not first.dataset
        or _event_date(item.event.source_event_time) != partition_date
        for item in indexed
    ) or any(
        journal_event.partition_key != partition_key
        for journal_event in journal_events
    ):
        raise LiveStorageError("journal batch crosses its source partition")

    raw_descriptor = _PartitionDescriptor(
        "raw", first.symbol, first.dataset, partition_date
    )
    raw_payload = _raw_ndjson(indexed)
    artifacts = [
        (
            raw_descriptor,
            raw_payload,
            _artifact_record(
                data_root,
                raw_descriptor,
                raw_payload,
                len(indexed),
                "ndjson/binance-stream-event-v1",
                ("source_event_time", "source_id", "event_key"),
                ("event_key",),
                min(item.event.source_event_time for item in indexed),
                max(item.event.source_event_time for item in indexed),
                min(_canonical_time(item.event) for item in indexed),
                max(_canonical_time(item.event) for item in indexed),
                ".ndjson.gz",
            ),
        )
    ]
    normalize_identities = {
        item.identity for item in journal_events if item.normalize
    }
    normalized_events = [
        item.event for item in indexed if item.identity in normalize_identities
    ]
    if normalized_events:
        normalized_events.sort(key=_normalized_order)
        table = _normalized_table(normalized_events)
        normalized_payload = _parquet_bytes(table)
        normalized_descriptor = _PartitionDescriptor(
            "normalized", first.symbol, first.dataset, partition_date
        )
        schema_name, sort_keys, unique_keys = _normalized_contract(first.dataset)
        artifacts.append(
            (
                normalized_descriptor,
                normalized_payload,
                _artifact_record(
                    data_root,
                    normalized_descriptor,
                    normalized_payload,
                    table.num_rows,
                    schema_name,
                    sort_keys,
                    unique_keys,
                    min(event.source_event_time for event in normalized_events),
                    max(event.source_event_time for event in normalized_events),
                    min(_canonical_time(event) for event in normalized_events),
                    max(_canonical_time(event) for event in normalized_events),
                    ".parquet",
                ),
            )
        )
    return tuple(artifacts)


def _artifact_record(
    data_root: Path,
    descriptor: _PartitionDescriptor,
    payload: bytes,
    row_count: int,
    schema_name: str,
    sort_keys: tuple[str, ...],
    unique_keys: tuple[str, ...],
    minimum_source_event_time: int,
    maximum_source_event_time: int,
    minimum_canonical_time: int,
    maximum_canonical_time: int,
    suffix: str,
) -> dict[str, object]:
    checksum = hashlib.sha256(payload).hexdigest()
    filename = f"part-{checksum[:24]}{suffix}"
    relative = Path(*descriptor.components, filename).as_posix()
    destination = data_root / relative
    if destination.relative_to(data_root).as_posix() != relative:
        raise LiveStorageError("live artifact path escapes the data root")
    return {
        "layer": descriptor.layer,
        "symbol": descriptor.symbol,
        "dataset": descriptor.dataset.value,
        "partition_date": descriptor.partition_date,
        "path": relative,
        "sha256": checksum,
        "row_count": row_count,
        "schema_name": schema_name,
        "sort_keys": list(sort_keys),
        "unique_keys": list(unique_keys),
        "min_source_event_time": minimum_source_event_time,
        "max_source_event_time": maximum_source_event_time,
        "min_canonical_time": minimum_canonical_time,
        "max_canonical_time": maximum_canonical_time,
    }


def _canonical_time(event: ParsedStreamEvent) -> int:
    if event.dataset is LiveDataset.KLINES:
        value = event.values["open_time"]
    elif event.dataset in {LiveDataset.AGG_TRADES, LiveDataset.BOOK_TICKER}:
        value = event.values["transact_time"]
    elif event.dataset is LiveDataset.MARK_PRICE:
        value = event.source_event_time
    else:
        raise LiveStorageError(f"unsupported live dataset: {event.dataset}")
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise LiveStorageError("live event canonical time is invalid")
    return value


def _normalized_order(event: ParsedStreamEvent) -> tuple[int, int]:
    if event.dataset is LiveDataset.KLINES:
        primary = int(event.values["open_time"])
    elif event.dataset is LiveDataset.AGG_TRADES:
        primary = int(event.values["aggregate_trade_id"])
    elif event.dataset is LiveDataset.MARK_PRICE:
        primary = event.source_event_time
    elif event.dataset is LiveDataset.BOOK_TICKER:
        primary = int(event.values["update_id"])
    else:
        raise LiveStorageError(f"unsupported live dataset: {event.dataset}")
    return primary, event.source_event_time


def _normalized_contract(
    dataset: LiveDataset,
) -> tuple[str, tuple[str, ...], tuple[str, ...]]:
    if dataset is LiveDataset.KLINES:
        return "parquet/binance-kline-1m-v1", ("open_time",), ("open_time",)
    if dataset is LiveDataset.AGG_TRADES:
        return (
            "parquet/binance-aggregate-trade-v1",
            ("aggregate_trade_id",),
            ("aggregate_trade_id",),
        )
    if dataset is LiveDataset.MARK_PRICE:
        return "parquet/binance-mark-price-v1", ("event_time",), ("event_time",)
    if dataset is LiveDataset.BOOK_TICKER:
        return "parquet/binance-book-ticker-v1", ("update_id",), ("update_id",)
    raise LiveStorageError(f"unsupported live dataset: {dataset}")


def _manifest_object(manifest_json: str) -> dict[str, object]:
    try:
        manifest = json.loads(manifest_json)
    except (TypeError, json.JSONDecodeError) as error:
        raise LiveStorageError("live batch manifest is unreadable") from error
    if not isinstance(manifest, dict) or manifest.get("version") != 1:
        raise LiveStorageError("live batch manifest has an invalid version")
    if not isinstance(manifest.get("artifacts"), list):
        raise LiveStorageError("live batch manifest has an invalid shape")
    return manifest


def _result_from_manifest(data_root: Path, manifest_json: str) -> LiveWriteResult:
    manifest = _manifest_object(manifest_json)
    batch_id = manifest.get("batch_id")
    if not isinstance(batch_id, str) or not batch_id:
        raise LiveStorageError("live batch manifest has no batch identity")
    raw: list[StoredLivePartition] = []
    normalized: list[StoredLivePartition] = []
    artifacts = manifest["artifacts"]
    assert isinstance(artifacts, list)
    for record in artifacts:
        part = _stored_partition(data_root, record)
        if part.layer == "raw":
            raw.append(part)
        elif part.layer == "normalized":
            normalized.append(part)
        else:
            raise LiveStorageError("live batch manifest layer is invalid")
    return LiveWriteResult(tuple(raw), tuple(normalized), batch_id=batch_id)


def _stored_partition(data_root: Path, record: object) -> StoredLivePartition:
    if not isinstance(record, dict):
        raise LiveStorageError("live batch artifact record is invalid")
    relative = record.get("path")
    checksum = record.get("sha256")
    row_count = record.get("row_count")
    layer = record.get("layer")
    symbol = record.get("symbol")
    dataset = record.get("dataset")
    partition_date = record.get("partition_date")
    schema_name = record.get("schema_name")
    sort_keys = record.get("sort_keys")
    unique_keys = record.get("unique_keys")
    minimum = record.get("min_source_event_time")
    maximum = record.get("max_source_event_time")
    minimum_canonical = record.get("min_canonical_time")
    maximum_canonical = record.get("max_canonical_time")
    if (
        not isinstance(relative, str)
        or relative.startswith("/")
        or ".." in Path(relative).parts
        or not isinstance(checksum, str)
        or len(checksum) != 64
        or not isinstance(row_count, int)
        or row_count <= 0
        or layer not in {"raw", "normalized"}
        or not isinstance(symbol, str)
        or not isinstance(dataset, str)
        or not isinstance(partition_date, str)
        or not isinstance(schema_name, str)
        or not isinstance(sort_keys, list)
        or not all(isinstance(item, str) for item in sort_keys)
        or not isinstance(unique_keys, list)
        or not all(isinstance(item, str) for item in unique_keys)
        or not isinstance(minimum, int)
        or not isinstance(maximum, int)
        or minimum > maximum
        or not isinstance(minimum_canonical, int)
        or not isinstance(maximum_canonical, int)
        or minimum_canonical < 0
        or minimum_canonical > maximum_canonical
    ):
        raise LiveStorageError("live batch artifact record is invalid")
    try:
        with open_secure_relative_file(data_root, relative) as descriptor:
            if _hash_fd(descriptor) != checksum:
                raise LiveStorageError("live batch artifact checksum conflicts")
    except ValueError as error:
        raise LiveStorageError("live batch artifact is missing or unsafe") from error
    return StoredLivePartition(
        path=data_root / relative,
        sha256=checksum,
        row_count=row_count,
        layer=layer,
        symbol=symbol,
        dataset=dataset,
        partition_date=partition_date,
        schema_name=schema_name,
        sort_keys=tuple(sort_keys),
        unique_keys=tuple(unique_keys),
        min_source_event_time=minimum,
        max_source_event_time=maximum,
        min_canonical_time=minimum_canonical,
        max_canonical_time=maximum_canonical,
        relative_path=relative,
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


@contextmanager
def _open_journal_bucket(data_root: Path, bucket: int) -> Iterator[int]:
    _journal_path(data_root, bucket)
    descriptors = [_open_secure_root(data_root)]
    try:
        for component in (
            "spool",
            "binance",
            "usdm",
            f"bucket={bucket:02x}",
        ):
            descriptors.append(_open_or_create_directory(descriptors[-1], component))
        yield descriptors[-1]
    finally:
        for opened in reversed(descriptors):
            os.close(opened)
