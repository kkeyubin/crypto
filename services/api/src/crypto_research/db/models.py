from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from sqlalchemy import (
    JSON,
    Boolean,
    CheckConstraint,
    DateTime,
    ForeignKey,
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
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    source_url: Mapped[str] = mapped_column(Text, nullable=False)
    checksum_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    size_bytes: Mapped[int | None] = mapped_column(Integer)
    fetched_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utc_now, nullable=False
    )


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
        CheckConstraint(
            f"approval_status IN {PARTITION_STATUSES!r}",
            name="ck_data_partitions_approval_status",
        ),
        CheckConstraint("version > 0", name="ck_data_partitions_version_positive"),
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
