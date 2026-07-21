"""add durable backfill objects and manifest catalog

Revision ID: 20260721_0002
Revises: 20260721_0001
Create Date: 2026-07-21
"""

import sqlalchemy as sa
from alembic import op

revision = "20260721_0002"
down_revision = "20260721_0001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_unique_constraint(
        "uq_source_objects_identity_url_checksum",
        "source_objects",
        ["id", "source_url", "checksum_sha256"],
    )
    op.create_check_constraint(
        "ck_source_objects_checksum_sha256",
        "source_objects",
        "checksum_sha256 ~ '^[0-9a-f]{64}$'",
    )
    op.create_unique_constraint(
        "uq_data_partitions_id_source_object",
        "data_partitions",
        ["id", "source_object_id"],
    )
    op.create_check_constraint(
        "ck_data_partitions_checksum_sha256",
        "data_partitions",
        "checksum_sha256 ~ '^[0-9a-f]{64}$'",
    )
    op.create_table(
        "backfill_objects",
        sa.Column("id", sa.String(length=36), primary_key=True),
        sa.Column(
            "job_id",
            sa.String(length=36),
            sa.ForeignKey("ingestion_jobs.id"),
            nullable=False,
        ),
        sa.Column("source_url", sa.Text(), nullable=False),
        sa.Column("source_checksum", sa.String(length=64), nullable=False),
        sa.Column("start_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("end_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("state", sa.String(length=24), nullable=False),
        sa.Column("raw_path", sa.Text()),
        sa.Column("normalized_path", sa.Text()),
        sa.Column("normalized_checksum", sa.String(length=64)),
        sa.Column("row_count", sa.Integer()),
        sa.Column("partition_id", sa.String(length=36), sa.ForeignKey("data_partitions.id")),
        sa.Column("manifest_id", sa.String(length=36)),
        sa.Column("lease_owner", sa.String(length=128)),
        sa.Column("lease_expires_at", sa.DateTime(timezone=True)),
        sa.Column("attempt_count", sa.Integer(), nullable=False),
        sa.Column("last_error", sa.Text()),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("downloading_at", sa.DateTime(timezone=True)),
        sa.Column("checksum_verified_at", sa.DateTime(timezone=True)),
        sa.Column("normalized_at", sa.DateTime(timezone=True)),
        sa.Column("validated_at", sa.DateTime(timezone=True)),
        sa.Column("catalog_approved_at", sa.DateTime(timezone=True)),
        sa.Column("terminal_at", sa.DateTime(timezone=True)),
        sa.CheckConstraint(
            "state IN ('planned', 'downloading', 'checksum_verified', 'normalized', "
            "'validated', 'catalog_approved', 'source_pending', 'failed')",
            name="ck_backfill_objects_state",
        ),
        sa.CheckConstraint(
            "attempt_count >= 0",
            name="ck_backfill_objects_attempt_count_nonnegative",
        ),
        sa.CheckConstraint("end_at > start_at", name="ck_backfill_objects_time_order"),
        sa.CheckConstraint(
            "(lease_owner IS NULL) = (lease_expires_at IS NULL)",
            name="ck_backfill_objects_complete_lease",
        ),
        sa.CheckConstraint(
            "source_checksum = '' OR source_checksum ~ '^[0-9a-f]{64}$'",
            name="ck_backfill_objects_source_checksum",
        ),
        sa.CheckConstraint(
            "normalized_checksum IS NULL OR "
            "normalized_checksum ~ '^[0-9a-f]{64}$'",
            name="ck_backfill_objects_normalized_checksum",
        ),
        sa.CheckConstraint(
            "state IN ('planned', 'downloading', 'source_pending', 'failed') OR "
            "(length(source_checksum) = 64 AND raw_path IS NOT NULL)",
            name="ck_backfill_objects_verified_evidence",
        ),
        sa.CheckConstraint(
            "state NOT IN ('normalized', 'validated', 'catalog_approved') OR "
            "(normalized_path IS NOT NULL AND length(normalized_checksum) = 64 "
            "AND row_count > 0)",
            name="ck_backfill_objects_normalized_evidence",
        ),
        sa.CheckConstraint(
            "state <> 'catalog_approved' OR "
            "(partition_id IS NOT NULL AND manifest_id IS NOT NULL)",
            name="ck_backfill_objects_catalog_evidence",
        ),
        sa.UniqueConstraint(
            "job_id",
            "source_url",
            name="uq_backfill_objects_job_url",
        ),
    )
    op.create_index("ix_backfill_objects_job_id", "backfill_objects", ["job_id"])
    op.create_index(
        "ix_backfill_objects_claim",
        "backfill_objects",
        ["state", "lease_expires_at", "created_at"],
    )
    op.create_table(
        "data_manifests",
        sa.Column("manifest_id", sa.String(length=36), primary_key=True),
        sa.Column(
            "partition_id",
            sa.String(length=36),
            sa.ForeignKey("data_partitions.id"),
            nullable=False,
        ),
        sa.Column(
            "source_object_id",
            sa.String(length=36),
            sa.ForeignKey("source_objects.id"),
            nullable=False,
        ),
        sa.Column("source_url", sa.Text(), nullable=False),
        sa.Column("source_checksum", sa.String(length=64), nullable=False),
        sa.Column("manifest", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint("partition_id", name="uq_data_manifests_partition_id"),
        sa.UniqueConstraint(
            "manifest_id",
            "partition_id",
            name="uq_data_manifests_manifest_partition",
        ),
        sa.CheckConstraint(
            "source_checksum ~ '^[0-9a-f]{64}$'",
            name="ck_data_manifests_source_checksum",
        ),
        sa.ForeignKeyConstraint(
            ["partition_id", "source_object_id"],
            ["data_partitions.id", "data_partitions.source_object_id"],
            name="fk_data_manifests_partition_source",
        ),
        sa.ForeignKeyConstraint(
            ["source_object_id", "source_url", "source_checksum"],
            [
                "source_objects.id",
                "source_objects.source_url",
                "source_objects.checksum_sha256",
            ],
            name="fk_data_manifests_source_identity",
        ),
    )
    op.create_index("ix_data_manifests_partition_id", "data_manifests", ["partition_id"])
    op.create_index("ix_data_manifests_source_object_id", "data_manifests", ["source_object_id"])
    op.create_foreign_key(
        "fk_backfill_objects_manifest_id",
        "backfill_objects",
        "data_manifests",
        ["manifest_id"],
        ["manifest_id"],
    )
    op.create_foreign_key(
        "fk_backfill_objects_manifest_partition",
        "backfill_objects",
        "data_manifests",
        ["manifest_id", "partition_id"],
        ["manifest_id", "partition_id"],
    )


def downgrade() -> None:
    op.drop_constraint(
        "fk_backfill_objects_manifest_partition", "backfill_objects", type_="foreignkey"
    )
    op.drop_constraint(
        "fk_backfill_objects_manifest_id", "backfill_objects", type_="foreignkey"
    )
    op.drop_index("ix_data_manifests_source_object_id", table_name="data_manifests")
    op.drop_index("ix_data_manifests_partition_id", table_name="data_manifests")
    op.drop_table("data_manifests")
    op.drop_index("ix_backfill_objects_claim", table_name="backfill_objects")
    op.drop_index("ix_backfill_objects_job_id", table_name="backfill_objects")
    op.drop_table("backfill_objects")
    op.drop_constraint(
        "ck_data_partitions_checksum_sha256", "data_partitions", type_="check"
    )
    op.drop_constraint(
        "uq_data_partitions_id_source_object", "data_partitions", type_="unique"
    )
    op.drop_constraint(
        "ck_source_objects_checksum_sha256", "source_objects", type_="check"
    )
    op.drop_constraint(
        "uq_source_objects_identity_url_checksum", "source_objects", type_="unique"
    )
