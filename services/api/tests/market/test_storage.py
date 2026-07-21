import hashlib
from datetime import UTC, datetime
from pathlib import Path

import pyarrow.parquet as pq

from crypto_research.market.binance.archive_paths import DatasetKind
from crypto_research.market.binance.normalization import normalize_csv
from crypto_research.market.storage import (
    normalized_archive_path,
    raw_archive_path,
    retain_raw_archive,
    write_normalized_parquet,
)


def test_retains_raw_by_source_checksum_and_writes_parquet_atomically(tmp_path: Path) -> None:
    payload = b"verified archive"
    checksum = hashlib.sha256(payload).hexdigest()
    staging = tmp_path / "staging.zip"
    staging.write_bytes(payload)
    period = datetime(2024, 1, 1, tzinfo=UTC)

    raw = retain_raw_archive(staging, tmp_path, "btcusdt", DatasetKind.KLINES, period, checksum)
    assert raw == raw_archive_path(tmp_path, "BTCUSDT", DatasetKind.KLINES, period, checksum)
    assert raw.read_bytes() == payload
    assert not staging.exists()

    normalized = normalize_csv(
        DatasetKind.KLINES,
        (Path(__file__).parent / "fixtures" / "klines.csv").read_bytes(),
        period,
        datetime(2024, 1, 1, 0, 2, tzinfo=UTC),
    )
    destination = normalized_archive_path(tmp_path, "BTCUSDT", DatasetKind.KLINES, period)
    result = write_normalized_parquet(normalized, destination)

    assert result.path == destination
    assert result.sha256 == hashlib.sha256(destination.read_bytes()).hexdigest()
    assert pq.ParquetFile(destination).read().equals(normalized.table)
    assert not list(destination.parent.glob("*.partial"))
