import hashlib
import os
import stat
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

PERIOD = datetime(2024, 1, 1, tzinfo=UTC)


def test_retains_regular_raw_by_checksum_without_deleting_staging_and_publishes_parquet(
    tmp_path: Path,
) -> None:
    root = secure_root(tmp_path / "root")
    payload = b"verified archive"
    checksum = hashlib.sha256(payload).hexdigest()
    staging = tmp_path / "staging.zip"
    staging.write_bytes(payload)

    raw = retain_raw_archive(staging, root, "btcusdt", DatasetKind.KLINES, PERIOD, checksum)
    assert raw == raw_archive_path(root, "BTCUSDT", DatasetKind.KLINES, PERIOD, checksum)
    assert raw.read_bytes() == payload
    assert stat.S_ISREG(raw.stat().st_mode)
    assert staging.read_bytes() == payload

    normalized = _normalized_dataset()
    result = write_normalized_parquet(normalized, root, "BTCUSDT", DatasetKind.KLINES, PERIOD)
    destination = normalized_archive_path(root, "BTCUSDT", DatasetKind.KLINES, PERIOD)

    assert result.path == destination
    assert result.sha256 == hashlib.sha256(destination.read_bytes()).hexdigest()
    assert pq.ParquetFile(destination).read().equals(normalized.table)
    assert not partial_names(destination.parent)


@pytest.mark.parametrize("kind", ["corrupt", "schema", "row_count"])
def test_rejects_invalid_parquet_and_preserves_existing_destination(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, kind: str
) -> None:
    root = secure_root(tmp_path / "root")
    normalized = _normalized_dataset()
    destination = normalized_archive_path(root, "BTCUSDT", DatasetKind.KLINES, PERIOD)
    destination.parent.mkdir(parents=True)
    destination.write_bytes(b"previous published partition")
    original_write = pq.write_table

    def bad_write(table, output, **kwargs) -> None:
        if kind == "corrupt":
            output.write(b"not parquet")
        elif kind == "schema":
            original_write(table.drop(["close"]), output, **kwargs)
        else:
            original_write(table.slice(1), output, **kwargs)

    monkeypatch.setattr(storage.pq, "write_table", bad_write)
    with pytest.raises(ValueError, match="Parquet"):
        write_normalized_parquet(normalized, root, "BTCUSDT", DatasetKind.KLINES, PERIOD)

    assert destination.read_bytes() == b"previous published partition"
    assert not partial_names(destination.parent)


def test_rejects_insecure_or_symlinked_data_root(tmp_path: Path) -> None:
    insecure = tmp_path / "insecure"
    insecure.mkdir()
    insecure.chmod(0o777)
    with pytest.raises(ValueError, match="data root"):
        write_normalized_parquet(
            _normalized_dataset(), insecure, "BTCUSDT", DatasetKind.KLINES, PERIOD
        )

    target = secure_root(tmp_path / "target")
    root_link = tmp_path / "root-link"
    root_link.symlink_to(target, target_is_directory=True)
    with pytest.raises(ValueError, match="data root"):
        write_normalized_parquet(
            _normalized_dataset(), root_link, "BTCUSDT", DatasetKind.KLINES, PERIOD
        )


def test_rejects_ancestor_and_existing_destination_symlinks(tmp_path: Path) -> None:
    root = secure_root(tmp_path / "root")
    outside = secure_root(tmp_path / "outside")
    (root / "normalized").symlink_to(outside, target_is_directory=True)
    with pytest.raises(ValueError, match="secure directory"):
        write_normalized_parquet(_normalized_dataset(), root, "BTCUSDT", DatasetKind.KLINES, PERIOD)

    (root / "normalized").unlink()
    destination = normalized_archive_path(root, "BTCUSDT", DatasetKind.KLINES, PERIOD)
    destination.parent.mkdir(parents=True)
    destination.symlink_to(outside / "outside.parquet")
    with pytest.raises(ValueError, match="destination symlink"):
        write_normalized_parquet(_normalized_dataset(), root, "BTCUSDT", DatasetKind.KLINES, PERIOD)


def test_unpredictable_partial_ignores_precreated_predictable_name(tmp_path: Path) -> None:
    root = secure_root(tmp_path / "root")
    destination = normalized_archive_path(root, "BTCUSDT", DatasetKind.KLINES, PERIOD)
    destination.parent.mkdir(parents=True)
    predictable = destination.with_suffix(f"{destination.suffix}.partial")
    predictable.write_bytes(b"attacker content")

    write_normalized_parquet(_normalized_dataset(), root, "BTCUSDT", DatasetKind.KLINES, PERIOD)

    assert predictable.read_bytes() == b"attacker content"
    assert pq.ParquetFile(destination).metadata.num_rows == 2
    assert len(partial_names(destination.parent)) == 1


def test_ancestor_path_swap_does_not_redirect_fd_anchored_publication(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = secure_root(tmp_path / "root")
    outside = secure_root(tmp_path / "outside")
    original_validate = storage._validate_parquet_partial
    original = root / "normalized-original"

    def swap_after_validation(*args, **kwargs) -> None:
        original_validate(*args, **kwargs)
        os.rename(root / "normalized", original)
        (root / "normalized").symlink_to(outside, target_is_directory=True)

    monkeypatch.setattr(storage, "_validate_parquet_partial", swap_after_validation)
    write_normalized_parquet(_normalized_dataset(), root, "BTCUSDT", DatasetKind.KLINES, PERIOD)

    retained = original / "binance/usdm/BTCUSDT/klines/date=2024-01-01/data.parquet"
    assert pq.ParquetFile(retained).metadata.num_rows == 2
    assert not list(outside.rglob("data.parquet"))


def test_raw_retention_rejects_staging_or_existing_destination_symlink(tmp_path: Path) -> None:
    root = secure_root(tmp_path / "root")
    payload = b"verified archive"
    checksum = hashlib.sha256(payload).hexdigest()
    regular = tmp_path / "regular.zip"
    regular.write_bytes(payload)
    staging_link = tmp_path / "staging-link.zip"
    staging_link.symlink_to(regular)
    with pytest.raises(ValueError, match="staging"):
        retain_raw_archive(staging_link, root, "BTCUSDT", DatasetKind.KLINES, PERIOD, checksum)

    destination = raw_archive_path(root, "BTCUSDT", DatasetKind.KLINES, PERIOD, checksum)
    destination.parent.mkdir(parents=True)
    destination.symlink_to(tmp_path / "outside.zip")
    with pytest.raises(ValueError, match="destination symlink"):
        retain_raw_archive(regular, root, "BTCUSDT", DatasetKind.KLINES, PERIOD, checksum)
    assert regular.exists()


def partial_names(directory: Path) -> list[Path]:
    return list(directory.glob("*.partial"))


def secure_root(path: Path) -> Path:
    path.mkdir(mode=0o700)
    path.chmod(0o700)
    return path


def _normalized_dataset():
    return normalize_csv(
        DatasetKind.KLINES,
        (Path(__file__).parent / "fixtures" / "klines.csv").read_bytes(),
        PERIOD,
        datetime(2024, 1, 1, 0, 2, tzinfo=UTC),
    )
