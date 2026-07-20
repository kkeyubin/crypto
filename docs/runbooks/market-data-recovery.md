# Market Data Recovery Runbook

## Detection

For each symbol and stream, track exchange event time, receive time, sequence/trade ID where available, last finalized bar, reconnect count, and data age. Raise `SYSTEM` when freshness or continuity crosses the configured limit.

## Automatic Response

1. Mark the affected stream `degraded`.
2. Block new strategy entries requiring that stream.
3. Continue recording unaffected data and manage existing paper positions under the declared degraded-data policy.
4. Reconnect WebSocket with bounded exponential backoff.
5. Query Binance REST/archive data for the exact missing interval.
6. Normalize and validate repaired rows.
7. Update the manifest and checksum history without rewriting the original audit record.
8. Resume entries only after continuity and freshness checks pass.

## Manual Investigation

Compare UTC boundaries, symbol/contract status, duplicates, gaps, trade IDs, and exchange maintenance notices. Determine whether direct access failed before enabling proxy port `17891`. Record the incident, affected datasets, repair source, and verification result.

## Fail-Closed Conditions

- Required data is stale or missing.
- Repaired data conflicts with already processed events.
- Contract metadata changed without a new manifest version.
- Paper-ledger state cannot be reconciled after replay.

In these cases keep ingestion or repair running, but leave new paper entries disabled and issue a Feishu `SYSTEM` or `RISK` notification.
