from dataclasses import FrozenInstanceError
from pathlib import Path

import pytest
from sqlalchemy import CheckConstraint, ForeignKeyConstraint, UniqueConstraint

from crypto_research.db.models import (
    BACKFILL_OBJECT_STATES,
    BackfillObjectRow,
    DataManifestRow,
    DataPartitionRow,
    LiveDataPartitionRow,
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


def test_backfill_objects_define_durable_state_lease_and_source_version_constraints() -> None:
    checks = _constraints(BackfillObjectRow.__table__, CheckConstraint)
    uniques = _constraints(BackfillObjectRow.__table__, UniqueConstraint)

    assert BACKFILL_OBJECT_STATES == (
        "planned",
        "downloading",
        "checksum_verified",
        "normalized",
        "validated",
        "catalog_approved",
        "source_pending",
        "failed",
    )
    assert any("state" in str(item.sqltext) for item in checks)
    assert any("attempt_count" in str(item.sqltext) for item in checks)
    assert any("source_checksum" in str(item.sqltext) for item in checks)
    assert any("source_checksum ~" in str(item.sqltext) for item in checks)
    assert any("normalized_checksum ~" in str(item.sqltext) for item in checks)
    assert any("normalized_path" in str(item.sqltext) for item in checks)
    assert any("partition_id" in str(item.sqltext) for item in checks)
    assert any(
        {column.name for column in item.columns}
        == {"job_id", "source_url"}
        for item in uniques
    )
    assert {"lease_owner", "lease_expires_at", "attempt_count", "last_error"}.issubset(
        BackfillObjectRow.__table__.columns.keys()
    )


def test_manifest_table_persists_full_versioned_manifest_by_partition() -> None:
    uniques = _constraints(DataManifestRow.__table__, UniqueConstraint)

    assert DataManifestRow.__table__.columns["manifest"].nullable is False
    assert any({column.name for column in item.columns} == {"partition_id"} for item in uniques)


def test_live_catalog_is_independent_and_records_query_contract() -> None:
    checks = _constraints(LiveDataPartitionRow.__table__, CheckConstraint)
    uniques = _constraints(LiveDataPartitionRow.__table__, UniqueConstraint)

    assert "source_object_id" not in LiveDataPartitionRow.__table__.columns
    assert {
        "batch_id",
        "layer",
        "relative_path",
        "checksum_sha256",
        "schema_name",
        "sort_keys",
        "unique_keys",
        "min_source_event_time",
        "max_source_event_time",
        "row_count",
        "approval_status",
    }.issubset(LiveDataPartitionRow.__table__.columns.keys())
    assert any("normalized" in str(item.sqltext) for item in checks)
    assert any("row_count > 0" in str(item.sqltext) for item in checks)
    assert any(
        {column.name for column in item.columns} == {"relative_path"}
        for item in uniques
    )


def test_catalog_evidence_uses_composite_relations_and_sha256_checks() -> None:
    object_checks = _constraints(SourceObjectRow.__table__, CheckConstraint)
    partition_checks = _constraints(DataPartitionRow.__table__, CheckConstraint)
    manifest_checks = _constraints(DataManifestRow.__table__, CheckConstraint)
    manifest_foreign_keys = _constraints(DataManifestRow.__table__, ForeignKeyConstraint)
    backfill_foreign_keys = _constraints(BackfillObjectRow.__table__, ForeignKeyConstraint)

    assert any("checksum_sha256" in str(item.sqltext) for item in object_checks)
    assert any("checksum_sha256" in str(item.sqltext) for item in partition_checks)
    assert any("source_checksum" in str(item.sqltext) for item in manifest_checks)
    assert any(
        {column.name for column in item.columns}
        == {"source_object_id", "source_url", "source_checksum"}
        for item in manifest_foreign_keys
    )
    assert any(
        {column.name for column in item.columns} == {"manifest_id", "partition_id"}
        for item in backfill_foreign_keys
    )


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
