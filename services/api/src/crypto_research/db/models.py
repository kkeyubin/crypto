from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from sqlalchemy import (
    JSON,
    BigInteger,
    Boolean,
    CheckConstraint,
    DateTime,
    ForeignKey,
    ForeignKeyConstraint,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import Mapped, mapped_column, validates

from crypto_research.db.base import Base

JOB_STATES = ("queued", "running", "succeeded", "failed", "cancelled")
PARTITION_STATUSES = ("candidate", "approved", "rejected")
GAP_STATUSES = ("open", "repaired")
STREAM_STATUSES = ("connecting", "connected", "degraded", "disconnected")
BACKFILL_OBJECT_STATES = (
    "planned",
    "downloading",
    "checksum_verified",
    "normalized",
    "validated",
    "catalog_approved",
    "source_pending",
    "failed",
)


def utc_now() -> datetime:
    return datetime.now(UTC)


def new_id() -> str:
    return str(uuid4())


def normalize_symbol(symbol: str) -> str:
    normalized = symbol.strip().upper()
    if not normalized:
        raise ValueError("symbol must not be empty")
    return normalized


def is_legal_job_transition(current: str, target: str) -> bool:
    return target in {
        "queued": {"running", "cancelled"},
        "running": {"succeeded", "failed", "cancelled"},
        "succeeded": set(),
        "failed": set(),
        "cancelled": set(),
    }.get(current, set())


class SymbolRow(Base):
    __tablename__ = "symbols"

    symbol: Mapped[str] = mapped_column(String(32), primary_key=True)
    enabled: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    history_start: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    history_end: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    include_agg_trades: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utc_now, nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utc_now, onupdate=utc_now, nullable=False
    )

    @validates("symbol")
    def _normalize_symbol(self, _key: str, value: str) -> str:
        return normalize_symbol(value)


class SymbolMetadataSnapshotRow(Base):
    __tablename__ = "symbol_metadata_snapshots"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    symbol: Mapped[str] = mapped_column(ForeignKey("symbols.symbol"), nullable=False, index=True)
    source: Mapped[str] = mapped_column(String(32), nullable=False)
    payload: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict, nullable=False)
    captured_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utc_now, nullable=False
    )


class IngestionJobRow(Base):
    __tablename__ = "ingestion_jobs"
    __table_args__ = (
        CheckConstraint(f"status IN {JOB_STATES!r}", name="ck_ingestion_jobs_status"),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    symbol: Mapped[str] = mapped_column(ForeignKey("symbols.symbol"), nullable=False, index=True)
    dataset: Mapped[str] = mapped_column(String(64), nullable=False)
    status: Mapped[str] = mapped_column(String(16), default="queued", nullable=False)
    requested_start: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    requested_end: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    details: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utc_now, nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utc_now, onupdate=utc_now, nullable=False
    )


class SourceObjectRow(Base):
    __tablename__ = "source_objects"
    __table_args__ = (
        UniqueConstraint("source_url", "checksum_sha256", name="uq_source_objects_url_checksum"),
        UniqueConstraint(
            "id",
            "source_url",
            "checksum_sha256",
            name="uq_source_objects_identity_url_checksum",
        ),
        CheckConstraint(
            "checksum_sha256 ~ '^[0-9a-f]{64}$'",
            name="ck_source_objects_checksum_sha256",
        ),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    source_url: Mapped[str] = mapped_column(Text, nullable=False)
    checksum_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    size_bytes: Mapped[int | None] = mapped_column(Integer)
    fetched_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utc_now, nullable=False
    )


class BackfillObjectRow(Base):
    __tablename__ = "backfill_objects"
    __table_args__ = (
        UniqueConstraint(
            "job_id",
            "source_url",
            name="uq_backfill_objects_job_url",
        ),
        CheckConstraint(
            f"state IN {BACKFILL_OBJECT_STATES!r}",
            name="ck_backfill_objects_state",
        ),
        CheckConstraint(
            "attempt_count >= 0",
            name="ck_backfill_objects_attempt_count_nonnegative",
        ),
        CheckConstraint(
            "end_at > start_at",
            name="ck_backfill_objects_time_order",
        ),
        CheckConstraint(
            "(lease_owner IS NULL) = (lease_expires_at IS NULL)",
            name="ck_backfill_objects_complete_lease",
        ),
        CheckConstraint(
            "source_checksum = '' OR source_checksum ~ '^[0-9a-f]{64}$'",
            name="ck_backfill_objects_source_checksum",
        ),
        CheckConstraint(
            "normalized_checksum IS NULL OR "
            "normalized_checksum ~ '^[0-9a-f]{64}$'",
            name="ck_backfill_objects_normalized_checksum",
        ),
        CheckConstraint(
            "state IN ('planned', 'downloading', 'source_pending', 'failed') OR "
            "(length(source_checksum) = 64 AND raw_path IS NOT NULL)",
            name="ck_backfill_objects_verified_evidence",
        ),
        CheckConstraint(
            "state NOT IN ('normalized', 'validated', 'catalog_approved') OR "
            "(normalized_path IS NOT NULL AND length(normalized_checksum) = 64 "
            "AND row_count > 0)",
            name="ck_backfill_objects_normalized_evidence",
        ),
        CheckConstraint(
            "state <> 'catalog_approved' OR "
            "(partition_id IS NOT NULL AND manifest_id IS NOT NULL)",
            name="ck_backfill_objects_catalog_evidence",
        ),
        ForeignKeyConstraint(
            ("manifest_id", "partition_id"),
            ("data_manifests.manifest_id", "data_manifests.partition_id"),
            name="fk_backfill_objects_manifest_partition",
        ),
        Index(
            "ix_backfill_objects_claim",
            "state",
            "lease_expires_at",
            "created_at",
        ),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    job_id: Mapped[str] = mapped_column(
        ForeignKey("ingestion_jobs.id"), nullable=False, index=True
    )
    source_url: Mapped[str] = mapped_column(Text, nullable=False)
    source_checksum: Mapped[str] = mapped_column(String(64), default="", nullable=False)
    start_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    end_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    state: Mapped[str] = mapped_column(String(24), default="planned", nullable=False)
    raw_path: Mapped[str | None] = mapped_column(Text)
    normalized_path: Mapped[str | None] = mapped_column(Text)
    normalized_checksum: Mapped[str | None] = mapped_column(String(64))
    row_count: Mapped[int | None] = mapped_column(Integer)
    partition_id: Mapped[str | None] = mapped_column(ForeignKey("data_partitions.id"))
    manifest_id: Mapped[str | None] = mapped_column(ForeignKey("data_manifests.manifest_id"))
    lease_owner: Mapped[str | None] = mapped_column(String(128))
    lease_expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    attempt_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    last_error: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utc_now, nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utc_now, onupdate=utc_now, nullable=False
    )
    downloading_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    checksum_verified_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    normalized_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    validated_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    catalog_approved_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    terminal_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

class DataPartitionRow(Base):
    __tablename__ = "data_partitions"
    __table_args__ = (
        UniqueConstraint(
            "symbol",
            "dataset",
            "partition_date",
            "version",
            name="uq_data_partitions_symbol_dataset_date_version",
        ),
        UniqueConstraint(
            "id",
            "source_object_id",
            name="uq_data_partitions_id_source_object",
        ),
        CheckConstraint(
            f"approval_status IN {PARTITION_STATUSES!r}",
            name="ck_data_partitions_approval_status",
        ),
        CheckConstraint("version > 0", name="ck_data_partitions_version_positive"),
        CheckConstraint(
            "checksum_sha256 ~ '^[0-9a-f]{64}$'",
            name="ck_data_partitions_checksum_sha256",
        ),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    symbol: Mapped[str] = mapped_column(ForeignKey("symbols.symbol"), nullable=False, index=True)
    dataset: Mapped[str] = mapped_column(String(64), nullable=False)
    partition_date: Mapped[str] = mapped_column(String(10), nullable=False)
    version: Mapped[int] = mapped_column(Integer, nullable=False)
    source_object_id: Mapped[str | None] = mapped_column(ForeignKey("source_objects.id"))
    checksum_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    parquet_path: Mapped[str] = mapped_column(Text, nullable=False)
    approval_status: Mapped[str] = mapped_column(String(16), default="candidate", nullable=False)
    validation_details: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utc_now, nullable=False
    )
    approved_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class DataManifestRow(Base):
    __tablename__ = "data_manifests"
    __table_args__ = (
        UniqueConstraint("partition_id", name="uq_data_manifests_partition_id"),
        UniqueConstraint(
            "manifest_id",
            "partition_id",
            name="uq_data_manifests_manifest_partition",
        ),
        CheckConstraint(
            "source_checksum ~ '^[0-9a-f]{64}$'",
            name="ck_data_manifests_source_checksum",
        ),
        ForeignKeyConstraint(
            ("partition_id", "source_object_id"),
            ("data_partitions.id", "data_partitions.source_object_id"),
            name="fk_data_manifests_partition_source",
        ),
        ForeignKeyConstraint(
            ("source_object_id", "source_url", "source_checksum"),
            (
                "source_objects.id",
                "source_objects.source_url",
                "source_objects.checksum_sha256",
            ),
            name="fk_data_manifests_source_identity",
        ),
    )

    manifest_id: Mapped[str] = mapped_column(String(36), primary_key=True)
    partition_id: Mapped[str] = mapped_column(
        ForeignKey("data_partitions.id"), nullable=False, index=True
    )
    source_object_id: Mapped[str] = mapped_column(
        ForeignKey("source_objects.id"), nullable=False, index=True
    )
    source_url: Mapped[str] = mapped_column(Text, nullable=False)
    source_checksum: Mapped[str] = mapped_column(String(64), nullable=False)
    manifest: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utc_now, nullable=False
    )


class LiveDataPartitionRow(Base):
    """Approved live artifacts, independent of archive source-object evidence."""

    __tablename__ = "live_data_partitions"
    __table_args__ = (
        UniqueConstraint(
            "batch_id",
            "layer",
            "relative_path",
            name="uq_live_data_partitions_batch_layer_path",
        ),
        UniqueConstraint(
            "relative_path", name="uq_live_data_partitions_relative_path"
        ),
        CheckConstraint(
            "layer IN ('raw', 'normalized')",
            name="ck_live_data_partitions_layer",
        ),
        CheckConstraint(
            "approval_status IN ('approved', 'rejected')",
            name="ck_live_data_partitions_approval_status",
        ),
        CheckConstraint(
            "checksum_sha256 ~ '^[0-9a-f]{64}$'",
            name="ck_live_data_partitions_checksum_sha256",
        ),
        CheckConstraint(
            "partition_date ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'",
            name="ck_live_data_partitions_partition_date",
        ),
        CheckConstraint(
            "dataset IN ('klines', 'agg_trades', 'mark_price', 'book_ticker')",
            name="ck_live_data_partitions_dataset",
        ),
        CheckConstraint(
            "CASE WHEN json_typeof(sort_keys) = 'array' "
            "THEN json_array_length(sort_keys) > 0 ELSE FALSE END",
            name="ck_live_data_partitions_sort_keys_shape",
        ),
        CheckConstraint(
            "CASE WHEN json_typeof(unique_keys) = 'array' "
            "THEN json_array_length(unique_keys) > 0 ELSE FALSE END",
            name="ck_live_data_partitions_unique_keys_shape",
        ),
        CheckConstraint(
            "(layer = 'raw' AND schema_name = "
            "'ndjson/binance-stream-event-v1') OR "
            "(layer = 'normalized' AND ("
            "(dataset = 'klines' AND schema_name = "
            "'parquet/binance-kline-1m-v1') OR "
            "(dataset = 'agg_trades' AND schema_name = "
            "'parquet/binance-aggregate-trade-v1') OR "
            "(dataset = 'mark_price' AND schema_name = "
            "'parquet/binance-mark-price-v1') OR "
            "(dataset = 'book_ticker' AND schema_name = "
            "'parquet/binance-book-ticker-v1'))) ",
            name="ck_live_data_partitions_schema_contract",
        ),
        CheckConstraint(
            "row_count > 0", name="ck_live_data_partitions_row_count_positive"
        ),
        CheckConstraint(
            "max_source_event_time >= min_source_event_time",
            name="ck_live_data_partitions_event_time_order",
        ),
        CheckConstraint(
            "relative_path !~ '(^/|(^|/)\\.\\.(/|$))'",
            name="ck_live_data_partitions_relative_path",
        ),
        CheckConstraint(
            "(layer = 'raw' AND relative_path LIKE 'raw/%.ndjson.gz') OR "
            "(layer = 'normalized' AND relative_path LIKE 'normalized/%.parquet')",
            name="ck_live_data_partitions_layer_path",
        ),
        CheckConstraint(
            "relative_path ~ ('^' || layer || "
            "'/binance/usdm/[A-Z0-9]{3,32}/' || dataset || '/date=' || "
            "partition_date || '/part-' || left(checksum_sha256, 24) || "
            "CASE WHEN layer = 'raw' THEN '\\.ndjson\\.gz$' "
            "ELSE '\\.parquet$' END)",
            name="ck_live_data_partitions_checksum_path",
        ),
        Index(
            "ix_live_data_partitions_approved_query",
            "symbol",
            "dataset",
            "layer",
            "approval_status",
            "min_source_event_time",
            "max_source_event_time",
        ),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    batch_id: Mapped[str] = mapped_column(String(64), nullable=False)
    layer: Mapped[str] = mapped_column(String(16), nullable=False)
    symbol: Mapped[str] = mapped_column(
        ForeignKey("symbols.symbol"), nullable=False, index=True
    )
    dataset: Mapped[str] = mapped_column(String(64), nullable=False)
    partition_date: Mapped[str] = mapped_column(String(10), nullable=False)
    relative_path: Mapped[str] = mapped_column(Text, nullable=False)
    checksum_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    schema_name: Mapped[str] = mapped_column(String(128), nullable=False)
    sort_keys: Mapped[list[str]] = mapped_column(JSON, nullable=False)
    unique_keys: Mapped[list[str]] = mapped_column(JSON, nullable=False)
    min_source_event_time: Mapped[int] = mapped_column(BigInteger, nullable=False)
    max_source_event_time: Mapped[int] = mapped_column(BigInteger, nullable=False)
    row_count: Mapped[int] = mapped_column(BigInteger, nullable=False)
    approval_status: Mapped[str] = mapped_column(String(16), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utc_now, nullable=False
    )
    approved_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utc_now, nullable=False
    )


class DataGapRow(Base):
    __tablename__ = "data_gaps"
    __table_args__ = (
        CheckConstraint(f"status IN {GAP_STATUSES!r}", name="ck_data_gaps_status"),
        CheckConstraint("end_at > start_at", name="ck_data_gaps_time_order"),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    symbol: Mapped[str] = mapped_column(ForeignKey("symbols.symbol"), nullable=False, index=True)
    dataset: Mapped[str] = mapped_column(String(64), nullable=False)
    start_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    end_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    reason: Mapped[str] = mapped_column(String(128), nullable=False)
    status: Mapped[str] = mapped_column(String(16), default="open", nullable=False)
    repair_details: Mapped[dict[str, Any] | None] = mapped_column(JSON)
    opened_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utc_now, nullable=False
    )
    repaired_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class StreamStateRow(Base):
    __tablename__ = "stream_states"
    __table_args__ = (
        CheckConstraint(f"status IN {STREAM_STATUSES!r}", name="ck_stream_states_status"),
    )

    symbol: Mapped[str] = mapped_column(ForeignKey("symbols.symbol"), primary_key=True)
    stream_name: Mapped[str] = mapped_column(String(64), primary_key=True)
    status: Mapped[str] = mapped_column(String(16), nullable=False)
    last_event_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utc_now, onupdate=utc_now, nullable=False
    )
    details: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict, nullable=False)


class WorkerHeartbeatRow(Base):
    __tablename__ = "worker_heartbeats"

    worker_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    heartbeat_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    details: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict, nullable=False)


class AuditEventRow(Base):
    __tablename__ = "audit_events"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    action: Mapped[str] = mapped_column(String(64), nullable=False)
    subject_type: Mapped[str] = mapped_column(String(64), nullable=False)
    subject_id: Mapped[str] = mapped_column(String(128), nullable=False)
    details: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utc_now, nullable=False
    )
