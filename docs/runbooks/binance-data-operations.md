# Binance Data Operations Runbook

This runbook operates Phase 1 public Binance USDⓈ-M data only. It must not be used for account endpoints, API keys, orders, backtests, simulated trading, or notifications. All request ranges are explicit UTC half-open intervals `[start, end)`, contain only closed days, and are capped at 366 days.

## Pre-change Inventory and Baseline

Before changing an existing installation, record container names/status/restart policy, loopback bindings, network/volume names, bind mounts, data size, database size, migration revision, filesystem capacity, checkout identity, and only the **names** of runtime variables. Do not print `runtime.env` values or container environments.

The read-only baseline captured on `2026-07-22T02:03+08:00` for `/home/keyubin/crypto-research-phase0-smoke` was:

- `crypto-research-postgres-1`, `crypto-research-api-1`, and `crypto-research-web-1` healthy with `restart=unless-stopped`;
- Web bound only to `127.0.0.1:8088`; API and PostgreSQL had no host port;
- network `crypto-research_default`, volume `crypto-research_postgres-data`;
- API bind `/home/keyubin/crypto-research-phase0-smoke/data` → `/srv/crypto-research/data`; data size `4.0K`;
- `runtime.env` mode `0600` with keys `POSTGRES_PASSWORD`, `CRYPTO_SESSION_SECRET`, and `CRYPTO_DATA_ROOT` only;
- database size `7518kB`, no `alembic_version` table, `688G` filesystem free;
- copied source identified in README as `0226feba8bbefec207c7eee5b40c93c68a22e922`, without a `.git` directory.

This baseline is evidence, not a backup. Keep the Phase 0 stack unchanged until the Phase 1 image/configuration passes review. Then follow the consistent backup order in [Market Data Recovery](market-data-recovery.md).

## Validate and Start Phase 1

```bash
cd /srv/crypto-research/repo
docker compose --env-file /srv/crypto-research/config/runtime.env \
  -f deploy/compose.yaml --profile server config --quiet
docker compose --env-file /srv/crypto-research/config/runtime.env \
  -f deploy/compose.yaml --profile server up -d --build
docker compose --env-file /srv/crypto-research/config/runtime.env \
  -f deploy/compose.yaml --profile server ps
curl --fail http://127.0.0.1:8088/api/health/ready
curl --fail http://127.0.0.1:8088/api/operations/market-data
```

The API container owns `alembic upgrade head`; `market-worker` waits for healthy PostgreSQL/API and never races migrations. Its host-network database address is validated as `127.0.0.1:55432`. The only UI listener is `127.0.0.1:8088`. The scoped proxy is `CRYPTO_HTTP_PROXY_URL=http://127.0.0.1:17891` with `CRYPTO_PROXY_MODE=auto`; direct access is attempted first. Do not add generic `HTTP_PROXY`, `HTTPS_PROXY`, or `ALL_PROXY` variables.

## Add Symbols and Request Bounded History

The following acceptance ranges are deliberately small, different, and fully closed. BTC opts into one aggregate-trade download; PEPE does not.

```bash
curl --fail-with-body -H 'Content-Type: application/json' \
  -d '{"symbol":"BTCUSDT","history_start":"2026-07-18T00:00:00Z","history_end":"2026-07-20T00:00:00Z","include_agg_trades":true}' \
  http://127.0.0.1:8088/api/symbols
curl --fail-with-body -H 'Content-Type: application/json' \
  -d '{"symbol":"PEPEUSDT","history_start":"2026-07-19T00:00:00Z","history_end":"2026-07-20T00:00:00Z","include_agg_trades":false}' \
  http://127.0.0.1:8088/api/symbols

curl --fail-with-body -H 'Content-Type: application/json' \
  -d '{"symbol":"BTCUSDT","data_types":["kline_1m","mark_price","funding"],"start":"2026-07-18T00:00:00Z","end":"2026-07-20T00:00:00Z","include_agg_trades":false}' \
  http://127.0.0.1:8088/api/symbols/BTCUSDT/backfills
curl --fail-with-body -H 'Content-Type: application/json' \
  -d '{"symbol":"BTCUSDT","data_types":["agg_trade"],"start":"2026-07-19T00:00:00Z","end":"2026-07-20T00:00:00Z","include_agg_trades":true}' \
  http://127.0.0.1:8088/api/symbols/BTCUSDT/backfills
curl --fail-with-body -H 'Content-Type: application/json' \
  -d '{"symbol":"PEPEUSDT","data_types":["kline_1m","mark_price","funding"],"start":"2026-07-19T00:00:00Z","end":"2026-07-20T00:00:00Z","include_agg_trades":false}' \
  http://127.0.0.1:8088/api/symbols/PEPEUSDT/backfills
```

Save returned job IDs and poll `GET /api/backfills/{job_id}`. Inspect each symbol independently:

```bash
curl --fail 'http://127.0.0.1:8088/api/symbols/BTCUSDT/partitions?limit=100&offset=0'
curl --fail 'http://127.0.0.1:8088/api/symbols/PEPEUSDT/partitions?limit=100&offset=0'
curl --fail 'http://127.0.0.1:8088/api/symbols/BTCUSDT/gaps?limit=100&offset=0'
curl --fail 'http://127.0.0.1:8088/api/symbols/PEPEUSDT/gaps?limit=100&offset=0'
curl --fail http://127.0.0.1:8088/api/symbols/BTCUSDT/profile
curl --fail http://127.0.0.1:8088/api/symbols/PEPEUSDT/profile
curl --fail http://127.0.0.1:8088/api/symbols/BTCUSDT/eligibility
curl --fail http://127.0.0.1:8088/api/symbols/PEPEUSDT/eligibility
```

Do not infer PEPE parameters from BTC. Record exact row ranges, sample counts, coverage, manifest IDs, partition IDs, and reason codes per symbol.

## Verify Official Archive Checksums

Every archive is trusted only after the downloaded ZIP SHA-256 matches its official sibling `.CHECKSUM`. The worker persists the source checksum and normalized checksum separately. For acceptance, query approved object evidence and independently compare all three values on the server:

```bash
cd /srv/crypto-research/repo
while IFS='|' read -r source_url source_checksum raw_path; do
  official_checksum=$(curl --fail --silent --show-error "${source_url}.CHECKSUM" | awk 'NR==1 {print tolower($1)}')
  stored_checksum=$(sha256sum "/srv/crypto-research/data/${raw_path}" | awk '{print $1}')
  test "$official_checksum" = "$source_checksum"
  test "$stored_checksum" = "$source_checksum"
  printf '%s %s\n' "$source_checksum" "$source_url"
done < <(docker compose --env-file /srv/crypto-research/config/runtime.env \
  -f deploy/compose.yaml --profile server exec -T postgres \
  psql -U crypto -d crypto_research -At -F '|' -c \
  "SELECT source_url, source_checksum, raw_path FROM backfill_objects WHERE state='catalog_approved' ORDER BY source_url")
```

Never publish a checksum mismatch. Retain the conflicting bytes in quarantine and record the URL, official checksum, actual checksum, UTC observation time, and affected job without secrets.

## Pending Days, Recheck, and Source Replacement

An archive 404 for either of the two most recently closed UTC days is `source_pending`; it is not a valid empty partition. An older 404 fails and opens a gap. Recheck the exact official URL and sibling `.CHECKSUM` after Binance publishes it. Phase 1 has no public mutation that force-promotes a pending/failed object, so do not reset state with ad hoc SQL. If it remains pending after the source appears, keep eligibility fail-closed and use a reviewed maintenance change that preserves the audit trail.

For periodic archive recheck, compare the current official `.CHECKSUM` with `data_manifests.source_checksum`. A source replacement (same official URL, new checksum) must produce a new immutable raw object, Parquet path, manifest, and partition version. Never overwrite the prior files or edit the old checksum. If no reviewed re-ingestion path is available, open an incident and leave the old version active/degraded rather than silently substituting bytes.

## Disconnect Gap Triage

After a reconnect or restart, inspect operations health, per-symbol streams, and gaps:

```bash
curl --fail http://127.0.0.1:8088/api/operations/market-data
curl --fail 'http://127.0.0.1:8088/api/symbols/BTCUSDT/streams?limit=100&offset=0'
curl --fail 'http://127.0.0.1:8088/api/symbols/BTCUSDT/gaps?limit=100&offset=0'
```

Classify each disconnect gap by symbol, stream, UTC start/end, last source ID, and reconnect reason. Repair archival datasets only with an exact bounded backfill request. Do not mark a live gap repaired merely because new messages resumed; continuity or approved replacement evidence is required. Keep affected eligibility blocked while any required stream is stale, degraded, or has an open gap.

## Expected Metadata Degradation

Binance REST can return HTTP 451 in the current server network. This must appear as REST/metadata degraded and `metadata_unverified`; it must not stop verified archives or fresh routed WebSockets. Never synthesize `exchangeInfo`, tick size, status, or contract metadata. Archive/live health and metadata health are separate gates.

## Restart and Idempotency Check

Before restart, record job/partition/manifest/live-partition counts. Restart API and worker, wait for health, then compare counts and identities; retries may add newly completed evidence but must not duplicate an existing immutable identity.

```bash
cd /srv/crypto-research/repo
docker compose --env-file /srv/crypto-research/config/runtime.env \
  -f deploy/compose.yaml --profile server restart api market-worker
docker compose --env-file /srv/crypto-research/config/runtime.env \
  -f deploy/compose.yaml --profile server ps
curl --retry 12 --retry-delay 5 --retry-connrefused --fail \
  http://127.0.0.1:8088/api/operations/market-data
```

The worker must recover its fixed `bucket=00..3f` SQLite spool and catalog all previously published batches exactly once. Follow [Market Data Recovery](market-data-recovery.md) for backup and rollback before any destructive recovery.

## Access and Exposure Check

From the server, verify no application or database socket listens on a non-loopback address:

```bash
ss -lntp | grep -E ':(8088|55432)\b'
```

Expected listeners are exactly `127.0.0.1:8088` and `127.0.0.1:55432`. API and worker expose no host port. From the Mac:

```bash
ssh -N -L 8088:127.0.0.1:8088 keyubin@192.168.1.4
```

Open `http://127.0.0.1:8088`. Record a Chinese-console screenshot showing independent BTCUSDT/PEPEUSDT state, source mode, freshness, gaps, profile sample counts, and eligibility reasons.

## Acceptance Record

Record sanitized UTC timestamps, application commit, job/manifest/partition IDs, source and normalized checksums, exact row ranges/counts, stream message types, direct-failure classification, active source mode, metadata reason codes, restart comparisons, and `ss` output. Do not record proxy traffic, secrets, chat IDs, or bulk market data. Phase 1 remains in progress until every server acceptance item passes.
