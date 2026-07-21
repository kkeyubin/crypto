import hashlib
import os
import secrets
import stat
from collections.abc import Iterator
from contextlib import contextmanager, suppress
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path, PurePosixPath

import pyarrow.parquet as pq

from crypto_research.market.binance.archive_paths import DatasetKind
from crypto_research.market.binance.normalization import NormalizedDataset
from crypto_research.market.validation import normalize_symbol, require_utc_midnight

_DIRECTORY_FLAGS = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC
_READ_FLAGS = os.O_RDONLY | os.O_NONBLOCK | os.O_NOFOLLOW | os.O_CLOEXEC
_TEMP_FLAGS = os.O_RDWR | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC


@dataclass(frozen=True)
class StoredParquet:
    path: Path
    sha256: str
    row_count: int


@contextmanager
def open_secure_relative_file(data_root: Path, relative_path: str) -> Iterator[int]:
    """Open a regular file beneath ``data_root`` without following symlinks."""
    relative = PurePosixPath(relative_path)
    if relative.is_absolute() or ".." in relative.parts or not relative.parts:
        raise ValueError("storage path must be relative to the secure data root")
    descriptors = [_open_secure_root(data_root)]
    try:
        for component in relative.parts[:-1]:
            _require_component(component)
            try:
                descriptors.append(
                    os.open(component, _DIRECTORY_FLAGS, dir_fd=descriptors[-1])
                )
            except OSError as error:
                raise ValueError("storage path could not be opened securely") from error
        _require_component(relative.parts[-1])
        try:
            descriptor = os.open(
                relative.parts[-1], _READ_FLAGS, dir_fd=descriptors[-1]
            )
        except OSError as error:
            raise ValueError("storage file could not be opened securely") from error
        descriptors.append(descriptor)
        if not stat.S_ISREG(os.fstat(descriptor).st_mode):
            raise ValueError("storage file must be a regular file")
        yield descriptor
    finally:
        for descriptor in reversed(descriptors):
            os.close(descriptor)


def raw_archive_path(
    data_root: Path,
    symbol: str,
    dataset: DatasetKind,
    period_start: datetime,
    source_checksum: str,
) -> Path:
    directory = _partition_path(data_root, "raw", symbol, dataset, period_start)
    return directory / f"{_checksum(source_checksum)}.zip"


def normalized_archive_path(
    data_root: Path, symbol: str, dataset: DatasetKind, period_start: datetime
) -> Path:
    return _partition_path(data_root, "normalized", symbol, dataset, period_start) / "data.parquet"


def normalized_archive_version_path(
    data_root: Path,
    symbol: str,
    dataset: DatasetKind,
    period_start: datetime,
    source_checksum: str,
) -> Path:
    checksum = _checksum(source_checksum)
    directory = _partition_path(data_root, "normalized", symbol, dataset, period_start)
    return directory / f"data-{checksum}.parquet"


def retain_raw_archive(
    staging: Path,
    data_root: Path,
    symbol: str,
    dataset: DatasetKind,
    period_start: datetime,
    source_checksum: str,
) -> Path:
    """Copy a fixed staging descriptor into an immutable descriptor-anchored raw path."""
    source_checksum = _checksum(source_checksum)
    final_name = f"{source_checksum}.zip"
    destination = raw_archive_path(data_root, symbol, dataset, period_start, source_checksum)
    staging_fd = _open_regular_staging(staging)
    try:
        with _open_partition(data_root, "raw", symbol, dataset, period_start) as partition_fd:
            existing = _open_existing_regular(partition_fd, final_name, "raw destination")
            if existing is not None:
                try:
                    if _hash_fd(existing) != source_checksum:
                        raise ValueError("existing raw object does not match source checksum")
                    return destination
                finally:
                    os.close(existing)
            temp_name, temp_fd = _open_unique_partial(partition_fd, final_name)
            try:
                digest = _copy_and_hash(staging_fd, temp_fd)
                if digest != source_checksum:
                    raise ValueError("staging raw object does not match source checksum")
                os.fsync(temp_fd)
                _reject_destination_symlink(partition_fd, final_name)
                os.replace(
                    temp_name,
                    final_name,
                    src_dir_fd=partition_fd,
                    dst_dir_fd=partition_fd,
                )
                os.fsync(partition_fd)
                return destination
            finally:
                os.close(temp_fd)
                _unlink_at(partition_fd, temp_name)
    finally:
        os.close(staging_fd)


def write_normalized_parquet(
    dataset: NormalizedDataset,
    data_root: Path,
    symbol: str,
    dataset_kind: DatasetKind,
    period_start: datetime,
) -> StoredParquet:
    """Publish validated Parquet with only fixed names relative to opened directories."""
    return _write_normalized_parquet(
        dataset,
        data_root,
        symbol,
        dataset_kind,
        period_start,
        "data.parquet",
    )


def write_versioned_normalized_parquet(
    dataset: NormalizedDataset,
    data_root: Path,
    symbol: str,
    dataset_kind: DatasetKind,
    period_start: datetime,
    source_checksum: str,
) -> StoredParquet:
    """Publish immutable normalized bytes under their official source version."""
    final_name = f"data-{_checksum(source_checksum)}.parquet"
    return _write_normalized_parquet(
        dataset,
        data_root,
        symbol,
        dataset_kind,
        period_start,
        final_name,
    )


def _write_normalized_parquet(
    dataset: NormalizedDataset,
    data_root: Path,
    symbol: str,
    dataset_kind: DatasetKind,
    period_start: datetime,
    final_name: str,
) -> StoredParquet:
    if dataset.row_count <= 0:
        raise ValueError("normalized dataset must contain at least one row")
    destination = (
        _partition_path(data_root, "normalized", symbol, dataset_kind, period_start)
        / final_name
    )
    with _open_partition(
        data_root, "normalized", symbol, dataset_kind, period_start
    ) as partition_fd:
        temp_name, temp_fd = _open_unique_partial(partition_fd, final_name)
        try:
            with os.fdopen(os.dup(temp_fd), "wb") as output:
                pq.write_table(
                    dataset.table,
                    output,
                    compression="zstd",
                    row_group_size=dataset.row_count,
                )
                output.flush()
            os.fsync(temp_fd)
            checksum = _validate_parquet_partial(temp_fd, dataset)
            _reject_destination_symlink(partition_fd, final_name)
            os.replace(
                temp_name,
                final_name,
                src_dir_fd=partition_fd,
                dst_dir_fd=partition_fd,
            )
            os.fsync(partition_fd)
            return StoredParquet(path=destination, sha256=checksum, row_count=dataset.row_count)
        finally:
            os.close(temp_fd)
            _unlink_at(partition_fd, temp_name)


@contextmanager
def _open_partition(
    data_root: Path,
    layer: str,
    symbol: str,
    dataset: DatasetKind,
    period_start: datetime,
) -> Iterator[int]:
    descriptors = [_open_secure_root(data_root)]
    try:
        for component in _partition_components(layer, symbol, dataset, period_start):
            descriptors.append(_open_or_create_directory(descriptors[-1], component))
        yield descriptors[-1]
    finally:
        for descriptor in reversed(descriptors):
            os.close(descriptor)


def _partition_path(
    data_root: Path,
    layer: str,
    symbol: str,
    dataset: DatasetKind,
    period_start: datetime,
) -> Path:
    return data_root.joinpath(*_partition_components(layer, symbol, dataset, period_start))


def _partition_components(
    layer: str, symbol: str, dataset: DatasetKind, period_start: datetime
) -> tuple[str, ...]:
    period_start = require_utc_midnight(period_start, field="period_start")
    components = (
        layer,
        "binance",
        "usdm",
        normalize_symbol(symbol),
        dataset.value,
        f"date={period_start:%Y-%m-%d}",
    )
    for component in components:
        _require_component(component)
    return components


def _open_secure_root(data_root: Path) -> int:
    try:
        root_stat = os.lstat(data_root)
    except FileNotFoundError:
        with suppress(FileExistsError):
            os.mkdir(data_root, 0o700)
        root_stat = os.lstat(data_root)
    if not stat.S_ISDIR(root_stat.st_mode) or stat.S_ISLNK(root_stat.st_mode):
        raise ValueError("data root must be a non-symlink directory")
    if stat.S_IMODE(root_stat.st_mode) & 0o022:
        raise ValueError("data root must not be group/world-writable")
    try:
        descriptor = os.open(data_root, _DIRECTORY_FLAGS)
    except OSError as error:
        raise ValueError("data root could not be opened securely") from error
    try:
        opened_stat = os.fstat(descriptor)
        if opened_stat.st_uid != os.geteuid():
            raise ValueError("data root owner does not match the effective user")
        if not stat.S_ISDIR(opened_stat.st_mode) or stat.S_IMODE(opened_stat.st_mode) & 0o022:
            raise ValueError("data root is not a secure directory")
        return descriptor
    except Exception:
        os.close(descriptor)
        raise


def _open_or_create_directory(parent_fd: int, component: str) -> int:
    _require_component(component)
    try:
        os.mkdir(component, 0o700, dir_fd=parent_fd)
    except FileExistsError:
        pass
    except OSError as error:
        raise ValueError("secure directory component could not be created") from error
    try:
        descriptor = os.open(component, _DIRECTORY_FLAGS, dir_fd=parent_fd)
    except OSError as error:
        raise ValueError("secure directory component could not be opened") from error
    try:
        opened_stat = os.fstat(descriptor)
        if not stat.S_ISDIR(opened_stat.st_mode):
            raise ValueError("secure directory component is not a directory")
        return descriptor
    except Exception:
        os.close(descriptor)
        raise


def _open_regular_staging(staging: Path) -> int:
    try:
        descriptor = os.open(staging, _READ_FLAGS)
    except OSError as error:
        raise ValueError("staging archive could not be opened safely") from error
    try:
        if not stat.S_ISREG(os.fstat(descriptor).st_mode):
            raise ValueError("staging archive must be a regular file")
        return descriptor
    except Exception:
        os.close(descriptor)
        raise


def _open_existing_regular(partition_fd: int, name: str, label: str) -> int | None:
    _require_component(name)
    try:
        existing_stat = os.stat(name, dir_fd=partition_fd, follow_symlinks=False)
    except FileNotFoundError:
        return None
    if stat.S_ISLNK(existing_stat.st_mode):
        raise ValueError(f"{label} symlink is not allowed")
    try:
        descriptor = os.open(name, _READ_FLAGS, dir_fd=partition_fd)
    except FileNotFoundError:
        return None
    except OSError as error:
        raise ValueError(f"{label} could not be opened safely") from error
    try:
        if not stat.S_ISREG(os.fstat(descriptor).st_mode):
            raise ValueError(f"{label} must be a regular file")
        return descriptor
    except Exception:
        os.close(descriptor)
        raise


def _open_unique_partial(partition_fd: int, final_name: str) -> tuple[str, int]:
    _require_component(final_name)
    for _ in range(32):
        name = f".{final_name}.{secrets.token_hex(16)}.partial"
        try:
            return name, os.open(name, _TEMP_FLAGS, 0o600, dir_fd=partition_fd)
        except FileExistsError:
            continue
        except OSError as error:
            raise ValueError("temporary archive file could not be created safely") from error
    raise ValueError("could not allocate a unique temporary archive file")


def _copy_and_hash(source_fd: int, destination_fd: int) -> str:
    os.lseek(source_fd, 0, os.SEEK_SET)
    digest = hashlib.sha256()
    while chunk := os.read(source_fd, 1024 * 1024):
        digest.update(chunk)
        _write_all(destination_fd, chunk)
    return digest.hexdigest()


def _validate_parquet_partial(descriptor: int, dataset: NormalizedDataset) -> str:
    try:
        with os.fdopen(os.dup(descriptor), "rb") as source:
            source.seek(0)
            parquet = pq.ParquetFile(source)
            metadata = parquet.metadata
            table = parquet.read()
    except Exception as error:
        raise ValueError("Parquet validation failed: unreadable footer") from error
    if metadata is None or metadata.num_row_groups != 1:
        raise ValueError("Parquet validation failed: unexpected row groups")
    if (
        metadata.num_rows != dataset.row_count
        or metadata.row_group(0).num_rows != dataset.row_count
    ):
        raise ValueError("Parquet validation failed: row count mismatch")
    if not parquet.schema_arrow.equals(dataset.table.schema, check_metadata=True):
        raise ValueError("Parquet validation failed: schema mismatch")
    if not table.equals(dataset.table, check_metadata=True):
        raise ValueError("Parquet validation failed: data mismatch")
    return _hash_fd(descriptor)


def _hash_fd(descriptor: int) -> str:
    os.lseek(descriptor, 0, os.SEEK_SET)
    digest = hashlib.sha256()
    while chunk := os.read(descriptor, 1024 * 1024):
        digest.update(chunk)
    return digest.hexdigest()


def _reject_destination_symlink(partition_fd: int, name: str) -> None:
    _require_component(name)
    try:
        destination_stat = os.stat(name, dir_fd=partition_fd, follow_symlinks=False)
    except FileNotFoundError:
        return
    if stat.S_ISLNK(destination_stat.st_mode):
        raise ValueError("destination symlink is not allowed")
    if not stat.S_ISREG(destination_stat.st_mode):
        raise ValueError("destination must be a regular file")


def _unlink_at(partition_fd: int, name: str) -> None:
    with suppress(FileNotFoundError):
        os.unlink(name, dir_fd=partition_fd)


def _write_all(descriptor: int, data: bytes) -> None:
    offset = 0
    while offset < len(data):
        offset += os.write(descriptor, data[offset:])


def _require_component(value: str) -> None:
    if not value or value in {".", ".."} or "/" in value or "\\" in value:
        raise ValueError("storage path component is unsafe")


def _checksum(value: str) -> str:
    if len(value) != 64 or any(character not in "0123456789abcdef" for character in value):
        raise ValueError("source checksum must be a lowercase SHA-256")
    return value
