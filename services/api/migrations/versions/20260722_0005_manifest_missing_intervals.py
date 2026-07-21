"""backfill accepted archive manifest content gaps

Revision ID: 20260722_0005
Revises: 20260721_0004
Create Date: 2026-07-22
"""

from alembic import op

revision = "20260722_0005"
down_revision = "20260721_0004"
branch_labels = None
depends_on = None


BACKFILL_SQL = """
WITH eligible AS (
    SELECT DISTINCT
        m.manifest_id,
        m.partition_id,
        g.start_at,
        g.end_at
    FROM data_manifests AS m
    JOIN data_partitions AS p
      ON p.id = m.partition_id
    JOIN data_gaps AS g
      ON g.symbol = p.symbol
     AND g.dataset = p.dataset
    WHERE p.approval_status = 'approved'
      AND m.manifest->'source'->>'kind' = 'binance_archive'
      AND COALESCE(
          m.manifest::jsonb->'missing_intervals',
          '[]'::jsonb
      ) = '[]'::jsonb
      AND g.reason IN (
          'missing_minute_open_time',
          'aggregate_trade_id_discontinuity'
      )
      AND g.end_at > g.start_at
      AND g.start_at >= (m.manifest->>'start')::timestamptz
      AND g.end_at <= (m.manifest->>'end')::timestamptz
), with_previous_end AS (
    SELECT
        eligible.*,
        max(end_at) OVER (
            PARTITION BY manifest_id, partition_id
            ORDER BY start_at, end_at
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ) AS previous_end
    FROM eligible
), grouped AS (
    SELECT
        with_previous_end.*,
        sum(
            CASE
                WHEN previous_end IS NULL OR start_at > previous_end THEN 1
                ELSE 0
            END
        ) OVER (
            PARTITION BY manifest_id, partition_id
            ORDER BY start_at, end_at
        ) AS interval_group
    FROM with_previous_end
), merged AS (
    SELECT
        manifest_id,
        partition_id,
        min(start_at) AS start_at,
        max(end_at) AS end_at
    FROM grouped
    GROUP BY manifest_id, partition_id, interval_group
), serialized AS (
    SELECT
        manifest_id,
        partition_id,
        jsonb_agg(
            jsonb_build_object('start', start_at, 'end', end_at)
            ORDER BY start_at, end_at
        ) AS missing_intervals
    FROM merged
    GROUP BY manifest_id, partition_id
)
UPDATE data_manifests AS m
SET manifest = jsonb_set(
    m.manifest::jsonb,
    '{missing_intervals}',
    serialized.missing_intervals,
    true
)::json
FROM serialized
WHERE m.manifest_id = serialized.manifest_id
  AND m.partition_id = serialized.partition_id
"""


def upgrade() -> None:
    op.execute(BACKFILL_SQL)


def downgrade() -> None:
    raise RuntimeError(
        "fail-closed: migrated manifest holes are immutable provenance and cannot "
        "be distinguished safely from later validated intervals"
    )
