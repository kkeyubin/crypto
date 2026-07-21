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
            "partition_date ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'",
            name="ck_live_data_partitions_partition_date",
        ),
        sa.CheckConstraint(
            "dataset IN ('klines', 'agg_trades', 'mark_price', 'book_ticker')",
            name="ck_live_data_partitions_dataset",
        ),
        sa.CheckConstraint(
            "CASE WHEN json_typeof(sort_keys) = 'array' "
            "THEN json_array_length(sort_keys) > 0 ELSE FALSE END",
            name="ck_live_data_partitions_sort_keys_shape",
        ),
        sa.CheckConstraint(
            "CASE WHEN json_typeof(unique_keys) = 'array' "
            "THEN json_array_length(unique_keys) > 0 ELSE FALSE END",
            name="ck_live_data_partitions_unique_keys_shape",
        ),
        sa.CheckConstraint(
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
        sa.CheckConstraint(
            "relative_path ~ ('^' || layer || "
            "'/binance/usdm/[A-Z0-9]{3,32}/' || dataset || '/date=' || "
            "partition_date || '/part-' || left(checksum_sha256, 24) || "
            "CASE WHEN layer = 'raw' THEN '\\.ndjson\\.gz$' "
            "ELSE '\\.parquet$' END)",
            name="ck_live_data_partitions_checksum_path",
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
