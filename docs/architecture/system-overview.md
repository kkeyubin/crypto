# System Overview

## Phase 1 Deployment

Docker Compose runs on `192.168.1.4` under a systemd unit. The `server` profile starts four services:

| Component | Network boundary | Responsibility |
|---|---|---|
| `postgres` | private Compose network plus `127.0.0.1:55432` | operational state, manifests, gaps, leases, and audit history |
| `api` | private Compose network only | typed symbol, backfill, profile, eligibility, and health APIs; migrations |
| `market-worker` | host network, no listening port | archive backfill and routed WebSocket capture |
| `web` | `127.0.0.1:8088` only | Chinese-first personal data console and API proxy |

The host network is required only because the server's proxy listens on `127.0.0.1:17891`. It also prevents the worker from resolving Compose DNS, so its validated database endpoint is `127.0.0.1:55432`; API uses `postgres:5432`. Both processes mount the same `/srv/crypto-research/data`. No process-wide proxy or exchange credential is accepted.

## Data and Trust Flow

```text
official archive + sibling .CHECKSUM       routed Binance WebSocket
             |                                      |
       raw checksum object                    durable SQLite spool
             |                                      |
       normalized Parquet                    raw + normalized shards
             +------------------+-------------------+
                                |
                  PostgreSQL manifest/catalog approval
                                |
             gaps + freshness + per-symbol empirical profile
                                |
                    fail-closed eligibility + Web API
```

PostgreSQL stores bounded structured state. Immutable raw objects and partitioned Parquet live under `/srv/crypto-research/data`. DuckDB may open only descriptor-safe paths from approved catalog rows and rechecks bytes before query. A file on disk is not trusted without matching catalog, manifest, checksum, and validation evidence.

BTCUSDT and PEPEUSDT have independent configuration, partitions, gaps, stream freshness, profile sample counts, and eligibility reasons. A healthy proxy-fed stream is not itself degraded; REST metadata failure is reported separately as `metadata_unverified`.

## Failure and Recovery Semantics

- Recent archive 404s become `source_pending`; older missing objects fail and open a gap.
- Disconnects and restarts create explicit gap evidence; stale or incomplete required streams fail closed.
- The worker durably accepts live events before acknowledgement and recovers uncataloged batches after restart.
- Source checksum changes create immutable versions; they never overwrite prior evidence.
- API migration failure stops API startup, and the worker waits for both PostgreSQL and API health.

## Later-Phase Boundary

Research replay/backtests are Phase 2. Paper orders and Feishu signals are Phase 3. AI shadow assessment is Phase 4. None runs in the Phase 1 deployment, and the market worker cannot place orders or access account APIs.
