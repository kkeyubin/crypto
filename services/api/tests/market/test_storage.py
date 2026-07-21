import hashlib
from datetime import UTC, datetime
from pathlib import Path

import pyarrow.parquet as pq
import pytest

import crypto_research.market.storage as storage
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
    result = write_normalized_parquet(normalized, destination, data_root=tmp_path)

    assert result.path == destination
    assert result.sha256 == hashlib.sha256(destination.read_bytes()).hexdigest()
    assert pq.ParquetFile(destination).read().equals(normalized.table)
    assert not list(destination.parent.glob("*.partial"))


def test_rejects_normalized_destination_outside_data_root(tmp_path: Path) -> None:
    normalized = _normalized_dataset()
    root = tmp_path / "root"

    with pytest.raises(ValueError, match="data root"):
        write_normalized_parquet(normalized, tmp_path / "outside.parquet", data_root=root)


def test_rejects_normalized_destination_through_symlink_escape(tmp_path: Path) -> None:
    normalized = _normalized_dataset()
    root = tmp_path / "root"
    outside = tmp_path / "outside"
    root.mkdir()
    outside.mkdir()
    (root / "escape").symlink_to(outside, target_is_directory=True)

    with pytest.raises(ValueError, match="data root"):
        write_normalized_parquet(normalized, root / "escape" / "data.parquet", data_root=root)


@pytest.mark.parametrize("kind", ["corrupt", "schema", "row_count"])
def test_rejects_invalid_parquet_publication_and_preserves_existing_destination(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, kind: str
) -> None:
    normalized = _normalized_dataset()
    destination = normalized_archive_path(
        tmp_path, "BTCUSDT", DatasetKind.KLINES, datetime(2024, 1, 1, tzinfo=UTC)
    )
    destination.parent.mkdir(parents=True)
    destination.write_bytes(b"previous published partition")
    original_write = pq.write_table

    def bad_write(table, path, **kwargs) -> None:
        if kind == "corrupt":
            Path(path).write_bytes(b"not parquet")
        elif kind == "schema":
            original_write(table.drop(["close"]), path, **kwargs)
        else:
            original_write(table.slice(1), path, **kwargs)

    monkeypatch.setattr(storage.pq, "write_table", bad_write)
    with pytest.raises(ValueError, match="Parquet"):
        write_normalized_parquet(normalized, destination, data_root=tmp_path)

    assert destination.read_bytes() == b"previous published partition"
    assert not list(destination.parent.glob("*.partial"))


def test_raw_retention_rejects_symlink_escape(tmp_path: Path) -> None:
    payload = b"verified archive"
    checksum = hashlib.sha256(payload).hexdigest()
    staging = tmp_path / "staging.zip"
    staging.write_bytes(payload)
    root = tmp_path / "root"
    outside = tmp_path / "outside"
    root.mkdir()
    outside.mkdir()
    (root / "raw").symlink_to(outside, target_is_directory=True)

    with pytest.raises(ValueError, match="data root"):
        retain_raw_archive(
            staging,
            root,
            "BTCUSDT",
            DatasetKind.KLINES,
            datetime(2024, 1, 1, tzinfo=UTC),
            checksum,
        )

    assert staging.exists()


def _normalized_dataset():
    return normalize_csv(
        DatasetKind.KLINES,
        (Path(__file__).parent / "fixtures" / "klines.csv").read_bytes(),
        datetime(2024, 1, 1, tzinfo=UTC),
        datetime(2024, 1, 1, 0, 2, tzinfo=UTC),
    )
