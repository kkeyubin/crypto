"""add Phase 1 market data state

Revision ID: 20260721_0001
Revises:
Create Date: 2026-07-21
"""

import sqlalchemy as sa
from alembic import op

revision = "20260721_0001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "symbols",
        sa.Column("symbol", sa.String(length=32), primary_key=True),
        sa.Column("enabled", sa.Boolean(), nullable=False),
        sa.Column("history_start", sa.DateTime(timezone=True)),
        sa.Column("history_end", sa.DateTime(timezone=True)),
        sa.Column("include_agg_trades", sa.Boolean(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_table(
        "symbol_metadata_snapshots",
        sa.Column("id", sa.String(length=36), primary_key=True),
        sa.Column("symbol", sa.String(length=32), sa.ForeignKey("symbols.symbol"), nullable=False),
        sa.Column("source", sa.String(length=32), nullable=False),
        sa.Column("payload", sa.JSON(), nullable=False),
        sa.Column("captured_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_table(
        "ingestion_jobs",
        sa.Column("id", sa.String(length=36), primary_key=True),
        sa.Column("symbol", sa.String(length=32), sa.ForeignKey("symbols.symbol"), nullable=False),
        sa.Column("dataset", sa.String(length=64), nullable=False),
        sa.Column("status", sa.String(length=16), nullable=False),
        sa.Column("requested_start", sa.DateTime(timezone=True)),
        sa.Column("requested_end", sa.DateTime(timezone=True)),
        sa.Column("details", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint("status IN ('queued', 'running', 'succeeded', 'failed', 'cancelled')"),
    )
    op.create_table(
        "source_objects",
        sa.Column("id", sa.String(length=36), primary_key=True),
        sa.Column("source_url", sa.Text(), nullable=False),
        sa.Column("checksum_sha256", sa.String(length=64), nullable=False),
        sa.Column("size_bytes", sa.Integer()),
        sa.Column("fetched_at", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint("source_url", "checksum_sha256"),
    )
    op.create_table(
        "data_partitions",
        sa.Column("id", sa.String(length=36), primary_key=True),
        sa.Column("symbol", sa.String(length=32), sa.ForeignKey("symbols.symbol"), nullable=False),
        sa.Column("dataset", sa.String(length=64), nullable=False),
        sa.Column("partition_date", sa.String(length=10), nullable=False),
        sa.Column("version", sa.Integer(), nullable=False),
        sa.Column("source_object_id", sa.String(length=36), sa.ForeignKey("source_objects.id")),
        sa.Column("checksum_sha256", sa.String(length=64), nullable=False),
        sa.Column("parquet_path", sa.Text(), nullable=False),
        sa.Column("approval_status", sa.String(length=16), nullable=False),
        sa.Column("validation_details", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("approved_at", sa.DateTime(timezone=True)),
        sa.CheckConstraint("approval_status IN ('candidate', 'approved', 'rejected')"),
        sa.CheckConstraint("version > 0"),
        sa.UniqueConstraint("symbol", "dataset", "partition_date", "version"),
    )
    op.create_table(
        "data_gaps",
        sa.Column("id", sa.String(length=36), primary_key=True),
        sa.Column("symbol", sa.String(length=32), sa.ForeignKey("symbols.symbol"), nullable=False),
        sa.Column("dataset", sa.String(length=64), nullable=False),
        sa.Column("start_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("end_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("reason", sa.String(length=128), nullable=False),
        sa.Column("status", sa.String(length=16), nullable=False),
        sa.Column("repair_details", sa.JSON()),
        sa.Column("opened_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("repaired_at", sa.DateTime(timezone=True)),
        sa.CheckConstraint("status IN ('open', 'repaired')"),
        sa.CheckConstraint("end_at > start_at"),
    )
    op.create_table(
        "stream_states",
        sa.Column(
            "symbol",
            sa.String(length=32),
            sa.ForeignKey("symbols.symbol"),
            primary_key=True,
        ),
        sa.Column("stream_name", sa.String(length=64), primary_key=True),
        sa.Column("status", sa.String(length=16), nullable=False),
        sa.Column("last_event_at", sa.DateTime(timezone=True)),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("details", sa.JSON(), nullable=False),
        sa.CheckConstraint("status IN ('connecting', 'connected', 'degraded', 'disconnected')"),
    )
    op.create_table(
        "worker_heartbeats",
        sa.Column("worker_id", sa.String(length=128), primary_key=True),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("heartbeat_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("details", sa.JSON(), nullable=False),
    )
    op.create_table(
        "audit_events",
        sa.Column("id", sa.String(length=36), primary_key=True),
        sa.Column("action", sa.String(length=64), nullable=False),
        sa.Column("subject_type", sa.String(length=64), nullable=False),
        sa.Column("subject_id", sa.String(length=128), nullable=False),
        sa.Column("details", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    )


def downgrade() -> None:
    op.drop_table("audit_events")
    op.drop_table("worker_heartbeats")
    op.drop_table("stream_states")
    op.drop_table("data_gaps")
    op.drop_table("data_partitions")
    op.drop_table("source_objects")
    op.drop_table("ingestion_jobs")
    op.drop_table("symbol_metadata_snapshots")
    op.drop_table("symbols")
