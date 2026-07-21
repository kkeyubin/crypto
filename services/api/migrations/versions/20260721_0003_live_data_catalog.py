"""add independent approved live-data catalog

Revision ID: 20260721_0003
Revises: 20260721_0002
Create Date: 2026-07-21
"""

import sqlalchemy as sa
from alembic import op

revision = "20260721_0003"
down_revision = "20260721_0002"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "live_data_partitions",
        sa.Column("id", sa.String(length=36), primary_key=True),
        sa.Column("batch_id", sa.String(length=64), nullable=False),
        sa.Column("layer", sa.String(length=16), nullable=False),
        sa.Column(
            "symbol",
            sa.String(length=32),
            sa.ForeignKey("symbols.symbol"),
            nullable=False,
        ),
        sa.Column("dataset", sa.String(length=64), nullable=False),
        sa.Column("partition_date", sa.String(length=10), nullable=False),
        sa.Column("relative_path", sa.Text(), nullable=False),
        sa.Column("checksum_sha256", sa.String(length=64), nullable=False),
        sa.Column("schema_name", sa.String(length=128), nullable=False),
        sa.Column("sort_keys", sa.JSON(), nullable=False),
        sa.Column("unique_keys", sa.JSON(), nullable=False),
        sa.Column("min_source_event_time", sa.BigInteger(), nullable=False),
        sa.Column("max_source_event_time", sa.BigInteger(), nullable=False),
        sa.Column("row_count", sa.BigInteger(), nullable=False),
        sa.Column("approval_status", sa.String(length=16), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column(
            "approved_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.UniqueConstraint(
            "batch_id",
            "layer",
            "relative_path",
            name="uq_live_data_partitions_batch_layer_path",
        ),
        sa.UniqueConstraint(
            "relative_path", name="uq_live_data_partitions_relative_path"
        ),
        sa.CheckConstraint(
            "layer IN ('raw', 'normalized')",
            name="ck_live_data_partitions_layer",
        ),
        sa.CheckConstraint(
            "approval_status IN ('approved', 'rejected')",
            name="ck_live_data_partitions_approval_status",
        ),
        sa.CheckConstraint(
            "checksum_sha256 ~ '^[0-9a-f]{64}$'",
            name="ck_live_data_partitions_checksum_sha256",
        ),
        sa.CheckConstraint(
            "row_count > 0", name="ck_live_data_partitions_row_count_positive"
        ),
        sa.CheckConstraint(
            "max_source_event_time >= min_source_event_time",
            name="ck_live_data_partitions_event_time_order",
        ),
        sa.CheckConstraint(
            "relative_path !~ '(^/|(^|/)\\.\\.(/|$))'",
            name="ck_live_data_partitions_relative_path",
        ),
        sa.CheckConstraint(
            "(layer = 'raw' AND relative_path LIKE 'raw/%.ndjson.gz') OR "
            "(layer = 'normalized' AND relative_path LIKE 'normalized/%.parquet')",
            name="ck_live_data_partitions_layer_path",
        ),
    )
    op.create_index(
        "ix_live_data_partitions_symbol",
        "live_data_partitions",
        ["symbol"],
    )
    op.create_index(
        "ix_live_data_partitions_approved_query",
        "live_data_partitions",
        [
            "symbol",
            "dataset",
            "layer",
            "approval_status",
            "min_source_event_time",
            "max_source_event_time",
        ],
    )


def downgrade() -> None:
    op.drop_index(
        "ix_live_data_partitions_approved_query",
        table_name="live_data_partitions",
    )
    op.drop_index(
        "ix_live_data_partitions_symbol", table_name="live_data_partitions"
    )
    op.drop_table("live_data_partitions")
