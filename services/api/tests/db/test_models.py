from dataclasses import FrozenInstanceError
from pathlib import Path

import pytest
from sqlalchemy import CheckConstraint, UniqueConstraint

from crypto_research.db.models import (
    DataPartitionRow,
    SourceObjectRow,
    SymbolRow,
    is_legal_job_transition,
)
from crypto_research.db.repositories import DataPartition


def _constraints(table: object, constraint_type: type[object]) -> list[object]:
    constraints = table.constraints  # type: ignore[attr-defined]
    return [constraint for constraint in constraints if isinstance(constraint, constraint_type)]


def test_symbol_identity_is_normalized_to_uppercase() -> None:
    symbol = SymbolRow(symbol="  btcusdt ")

    assert symbol.symbol == "BTCUSDT"


def test_catalog_tables_define_unique_keys_and_state_checks() -> None:
    source_uniques = _constraints(SourceObjectRow.__table__, UniqueConstraint)
    partition_uniques = _constraints(DataPartitionRow.__table__, UniqueConstraint)
    checks = _constraints(DataPartitionRow.__table__, CheckConstraint)

    assert any(
        {column.name for column in item.columns} == {"source_url", "checksum_sha256"}
        for item in source_uniques
    )
    assert any(
        {column.name for column in item.columns}
        == {"symbol", "dataset", "partition_date", "version"}
        for item in partition_uniques
    )
    assert any("approval_status" in str(item.sqltext) for item in checks)


def test_job_transitions_are_explicit_and_reject_terminal_restarts() -> None:
    assert is_legal_job_transition("queued", "running")
    assert is_legal_job_transition("running", "succeeded")
    assert not is_legal_job_transition("succeeded", "running")
    assert not is_legal_job_transition("failed", "queued")


def test_approved_partition_values_are_immutable() -> None:
    partition = DataPartition(
        id="partition-1",
        symbol="BTCUSDT",
        dataset="klines_1m",
        partition_date="2026-07-20",
        version=1,
        approval_status="approved",
        checksum_sha256="a" * 64,
        parquet_path="normalized/binance/usdm/BTCUSDT/klines_1m/date=2026-07-20/data.parquet",
    )

    with pytest.raises(FrozenInstanceError):
        partition.version = 2  # type: ignore[misc]


def test_model_representations_do_not_expose_source_urls() -> None:
    source = SourceObjectRow(
        source_url="https://user:secret@127.0.0.1:17891/archive.zip",
        checksum_sha256="a" * 64,
    )

    assert "secret" not in repr(source)
    assert "127.0.0.1" not in repr(source)


def test_alembic_environment_uses_an_async_database_engine() -> None:
    env_path = Path(__file__).resolve().parents[2] / "migrations" / "env.py"

    assert "async_engine_from_config" in env_path.read_text()
