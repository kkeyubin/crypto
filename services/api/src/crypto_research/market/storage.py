import hashlib
import os
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import pyarrow.parquet as pq

from crypto_research.market.binance.archive_paths import DatasetKind
from crypto_research.market.binance.normalization import NormalizedDataset
from crypto_research.market.validation import normalize_symbol, require_utc_midnight


@dataclass(frozen=True)
class StoredParquet:
    path: Path
    sha256: str
    row_count: int


def raw_archive_path(
    data_root: Path,
    symbol: str,
    dataset: DatasetKind,
    period_start: datetime,
    source_checksum: str,
) -> Path:
    directory = _partition_directory(data_root, "raw", symbol, dataset, period_start)
    return directory / f"{_checksum(source_checksum)}.zip"


def normalized_archive_path(
    data_root: Path, symbol: str, dataset: DatasetKind, period_start: datetime
) -> Path:
    directory = _partition_directory(data_root, "normalized", symbol, dataset, period_start)
    return directory / "data.parquet"


def retain_raw_archive(
    staging: Path,
    data_root: Path,
    symbol: str,
    dataset: DatasetKind,
    period_start: datetime,
    source_checksum: str,
) -> Path:
    """Atomically retain a verified source object under its immutable SHA-256 name."""
    destination = raw_archive_path(data_root, symbol, dataset, period_start, source_checksum)
    destination.parent.mkdir(parents=True, exist_ok=True)
    if destination.exists():
        if _file_sha256(destination) != source_checksum:
            raise ValueError("existing raw object does not match source checksum")
        staging.unlink(missing_ok=True)
        return destination
    if _file_sha256(staging) != source_checksum:
        raise ValueError("staging raw object does not match source checksum")
    os.replace(staging, destination)
    _fsync_directory(destination.parent)
    return destination


def write_normalized_parquet(dataset: NormalizedDataset, destination: Path) -> StoredParquet:
    """Publish valid normalized records through a same-directory temporary file."""
    destination.parent.mkdir(parents=True, exist_ok=True)
    partial = destination.with_suffix(f"{destination.suffix}.partial")
    partial.unlink(missing_ok=True)
    try:
        pq.write_table(dataset.table, partial, compression="zstd")
        with partial.open("rb") as output:
            os.fsync(output.fileno())
        os.replace(partial, destination)
        _fsync_directory(destination.parent)
    finally:
        partial.unlink(missing_ok=True)
    return StoredParquet(
        path=destination,
        sha256=_file_sha256(destination),
        row_count=dataset.row_count,
    )


def _partition_directory(
    data_root: Path,
    layer: str,
    symbol: str,
    dataset: DatasetKind,
    period_start: datetime,
) -> Path:
    period_start = require_utc_midnight(period_start, field="period_start")
    return (
        data_root
        / layer
        / "binance"
        / "usdm"
        / normalize_symbol(symbol)
        / dataset.value
        / f"date={period_start:%Y-%m-%d}"
    )


def _checksum(value: str) -> str:
    if len(value) != 64 or any(character not in "0123456789abcdef" for character in value):
        raise ValueError("source checksum must be a lowercase SHA-256")
    return value


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        while chunk := source.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _fsync_directory(directory: Path) -> None:
    descriptor = os.open(directory, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
