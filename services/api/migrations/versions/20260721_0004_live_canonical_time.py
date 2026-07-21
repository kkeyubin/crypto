"""add canonical live query ranges and catalog constraints

Revision ID: 20260721_0004
Revises: 20260721_0003
Create Date: 2026-07-21
"""

import sqlalchemy as sa
from alembic import op

revision = "20260721_0004"
down_revision = "20260721_0003"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "live_data_partitions",
        sa.Column("min_canonical_time", sa.BigInteger(), nullable=True),
    )
    op.add_column(
        "live_data_partitions",
        sa.Column("max_canonical_time", sa.BigInteger(), nullable=True),
    )
    op.execute(
        "DO $$\n"
        "BEGIN\n"
        "    IF EXISTS (SELECT 1 FROM live_data_partitions) THEN\n"
        "        RAISE EXCEPTION 'live_data_partitions contains rows without "
        "canonical ranges. Back up catalog and artifacts, run DELETE FROM "
        "live_data_partitions, rerun migration, then re-register verified "
        "manifests.';\n"
        "    END IF;\n"
        "END\n"
        "$$"
    )
    op.alter_column(
        "live_data_partitions", "min_canonical_time", nullable=False
    )
    op.alter_column(
        "live_data_partitions", "max_canonical_time", nullable=False
    )
    op.drop_index(
        "ix_live_data_partitions_approved_query",
        table_name="live_data_partitions",
    )
    op.create_index(
        "ix_live_data_partitions_approved_query",
        "live_data_partitions",
        [
            "symbol",
            "dataset",
            "layer",
            "approval_status",
            "min_canonical_time",
            "max_canonical_time",
        ],
    )
    op.create_check_constraint(
        "ck_live_data_partitions_partition_date",
        "live_data_partitions",
        "partition_date ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'",
    )
    op.create_check_constraint(
        "ck_live_data_partitions_dataset",
        "live_data_partitions",
        "dataset IN ('klines', 'agg_trades', 'mark_price', 'book_ticker')",
    )
    op.create_check_constraint(
        "ck_live_data_partitions_sort_keys_shape",
        "live_data_partitions",
        "CASE WHEN json_typeof(sort_keys) = 'array' "
        "THEN json_array_length(sort_keys) > 0 ELSE FALSE END",
    )
    op.create_check_constraint(
        "ck_live_data_partitions_unique_keys_shape",
        "live_data_partitions",
        "CASE WHEN json_typeof(unique_keys) = 'array' "
        "THEN json_array_length(unique_keys) > 0 ELSE FALSE END",
    )
    op.create_check_constraint(
        "ck_live_data_partitions_schema_contract",
        "live_data_partitions",
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
    )
    op.create_check_constraint(
        "ck_live_data_partitions_checksum_path",
        "live_data_partitions",
        "relative_path ~ ('^' || layer || "
        "'/binance/usdm/[A-Z0-9]{3,32}/' || dataset || '/date=' || "
        "partition_date || '/part-' || left(checksum_sha256, 24) || "
        "CASE WHEN layer = 'raw' THEN '\\.ndjson\\.gz$' "
        "ELSE '\\.parquet$' END)",
    )
    op.create_check_constraint(
        "ck_live_data_partitions_canonical_time_order",
        "live_data_partitions",
        "min_canonical_time >= 0 AND "
        "max_canonical_time >= min_canonical_time",
    )


def downgrade() -> None:
    for name in (
        "ck_live_data_partitions_canonical_time_order",
        "ck_live_data_partitions_checksum_path",
        "ck_live_data_partitions_schema_contract",
        "ck_live_data_partitions_unique_keys_shape",
        "ck_live_data_partitions_sort_keys_shape",
        "ck_live_data_partitions_dataset",
        "ck_live_data_partitions_partition_date",
    ):
        op.drop_constraint(name, "live_data_partitions", type_="check")
    op.drop_index(
        "ix_live_data_partitions_approved_query",
        table_name="live_data_partitions",
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
    op.drop_column("live_data_partitions", "max_canonical_time")
    op.drop_column("live_data_partitions", "min_canonical_time")
