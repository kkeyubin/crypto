# Market Data Recovery Runbook

## Detection

For each symbol and stream, track exchange event time, receive time, sequence/trade ID where available, last finalized bar, reconnect count, and data age. Raise `SYSTEM` when freshness or continuity crosses the configured limit.

## Automatic Response

1. Preserve the fixed-bucket SQLite WAL spool under `spool/binance/usdm/bucket=00..3f/` and inspect non-`cataloged` batches; do not delete `journal.sqlite3`, `-wal`, or `-shm` files while the writer is running.
2. Restart the single authoritative worker on the same `CRYPTO_DATA_ROOT`. It must reconstruct/verify `prepared` artifacts and resubmit `published` batches to PostgreSQL before accepting them as cataloged.
3. Mark the affected stream `degraded` and block new strategy entries requiring that stream.
4. Continue recording unaffected data and manage existing paper positions under the declared degraded-data policy.
5. Reconnect WebSocket with bounded exponential backoff. Persist every direct/proxy source-mode change.
6. Confirm the `worker_restart` or disconnect gap was committed before treating its anchor as closed.
7. Query Binance REST/archive data for the exact missing interval.
8. Normalize and validate repaired rows.
9. Update the manifest and checksum history without rewriting the original audit record.
10. Resume entries only after continuity, approved-catalog, and freshness checks pass.

Only rows in PostgreSQL `live_data_partitions` with `layer=normalized` and `approval_status=approved` may enter the DuckDB live query boundary. A Parquet file existing on disk is not approval evidence by itself.

## Legacy Spool and Migration 0004

The reviewed `20260721_0003` catalog migration and intermediate Task 5 commits were never deployed to the server. Migration `20260721_0004` therefore expects `live_data_partitions` to be empty. It deliberately aborts if rows exist because source event `E` cannot be used to infer the canonical query range.

With the worker stopped, detect the obsolete per-day layout without following directory symlinks:

```bash
data_root=/srv/crypto-research/data
find -P "$data_root/spool/binance/usdm" \
  -type f -path '*/date=*/journal.sqlite3' -print
```

The worker performs its own descriptor-relative check while acquiring the authoritative writer lock. If it reports a legacy or unsafe spool, keep the worker stopped. Preserve the complete tree before any recovery decision:

```bash
data_root=/srv/crypto-research/data
recovery_root=/srv/crypto-research/recovery/task-5-legacy-spool
mkdir -p "$recovery_root"
cp -a "$data_root/spool/binance/usdm" "$recovery_root/usdm"
```

Check whether an unexpected catalog exists and export it before migration:

```bash
cd /srv/crypto-research/repo/deploy
docker compose \
  --env-file /srv/crypto-research/config/runtime.env \
  -f compose.yaml exec -T postgres \
  psql -U crypto -d crypto_research -Atc \
  'SELECT count(*) FROM live_data_partitions;'
docker compose \
  --env-file /srv/crypto-research/config/runtime.env \
  -f compose.yaml exec -T postgres \
  pg_dump -U crypto -d crypto_research \
  --data-only --table=live_data_partitions \
  > /srv/crypto-research/recovery/live_data_partitions.sql
```

These values match `deploy/compose.yaml`: `POSTGRES_USER=crypto` and `POSTGRES_DB=crypto_research`; only `POSTGRES_PASSWORD` comes from `runtime.env`. Run `psql` and `pg_dump` inside the `postgres` service so the host needs neither libpq tools nor a `CRYPTO_DATABASE_URL`. In particular, never pass SQLAlchemy's `postgresql+asyncpg://` URL to libpq.

If the count is nonzero, do not copy source-event min/max into canonical columns and do not delete the only evidence. Either restore the pre-`0004` application while an audited recovery derives canonical min/max from each checksum-verified Parquet file, or confirm that the never-deployed catalog is disposable, retain the dump above, then clear and re-register verified artifacts:

```bash
cd /srv/crypto-research/repo/deploy
docker compose \
  --env-file /srv/crypto-research/config/runtime.env \
  -f compose.yaml exec -T postgres \
  psql -U crypto -d crypto_research \
  -c 'DELETE FROM live_data_partitions;'
docker compose \
  --env-file /srv/crypto-research/config/runtime.env \
  -f compose.yaml run --rm api true
```

The final command intentionally preserves the API image's default `api-entrypoint.py`. The entrypoint reads `POSTGRES_PASSWORD`, constructs `CRYPTO_DATABASE_URL`, runs `alembic upgrade head`, and only then executes `true` so the one-off container exits successfully. Do not override this entrypoint for migrations.

Only when the preserved legacy journal has been proved disposable or migrated by an audited tool may it be quarantined. Never merge its SQLite files into a bucket journal by filesystem copy:

```bash
data_root=/srv/crypto-research/data
mv "$data_root/spool/binance/usdm" \
  "$data_root/spool/binance/usdm.legacy"
mkdir -m 700 -p "$data_root/spool/binance/usdm"
```

After restart, verify new journals appear only as `bucket=00..3f/journal.sqlite3`, then reconcile the approved catalog and stream checkpoints before treating capture as healthy.

## Manual Investigation

Compare UTC boundaries, symbol/contract status, duplicates, gaps, trade IDs, and exchange maintenance notices. Determine whether direct access failed before enabling proxy port `17891`. Record the incident, affected datasets, repair source, and verification result.

## Fail-Closed Conditions

- Required data is stale or missing.
- Repaired data conflicts with already processed events.
- Contract metadata changed without a new manifest version.
- Paper-ledger state cannot be reconciled after replay.
- A legacy/symlinked spool layout is present, or migration `0004` finds catalog rows without independently verified canonical ranges.

In these cases keep ingestion or repair running, but leave new paper entries disabled and issue a Feishu `SYSTEM` or `RISK` notification.
