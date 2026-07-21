# Market Data Recovery Runbook

## Detection

For each symbol and stream, track exchange event time, receive time, sequence/trade ID where available, last finalized bar, reconnect count, and data age. Raise `SYSTEM` when freshness or continuity crosses the configured limit.

## Automatic Response

1. Preserve the per-day SQLite WAL spool and inspect non-`cataloged` batches; do not delete `journal.sqlite3`, `-wal`, or `-shm` files while the writer is running.
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

## Manual Investigation

Compare UTC boundaries, symbol/contract status, duplicates, gaps, trade IDs, and exchange maintenance notices. Determine whether direct access failed before enabling proxy port `17891`. Record the incident, affected datasets, repair source, and verification result.

## Fail-Closed Conditions

- Required data is stale or missing.
- Repaired data conflicts with already processed events.
- Contract metadata changed without a new manifest version.
- Paper-ledger state cannot be reconciled after replay.

In these cases keep ingestion or repair running, but leave new paper entries disabled and issue a Feishu `SYSTEM` or `RISK` notification.
