# Binance Data Operations Runbook

This runbook operates Phase 1 public Binance USDⓈ-M data only. It does not authorize account endpoints, credentials, orders, backtests, simulated trading, notifications, or AI decisions. All history ranges are UTC half-open intervals `[start, end)`, contain only closed days, and are capped at 366 days. Funding archives are monthly-only, so onboarding history must contain one or more complete UTC calendar months.

## Select the Exact Installation

Never mix a checkout, environment file, or data root from different installations. For the existing Phase 0 smoke stack use:

```bash
export CRYPTO_INSTALL_ROOT=/home/keyubin/crypto-research-phase0-smoke
export CRYPTO_CHECKOUT=/home/keyubin/crypto-research-phase0-smoke/repo
export CRYPTO_ENV_FILE=/home/keyubin/crypto-research-phase0-smoke/config/runtime.env
export CRYPTO_DATA_ROOT=/home/keyubin/crypto-research-phase0-smoke/data
export CRYPTO_COMPOSE_FILE="$CRYPTO_CHECKOUT/deploy/compose.yaml"
```

For a fresh formal installation use:

```bash
export CRYPTO_INSTALL_ROOT=/srv/crypto-research
export CRYPTO_CHECKOUT=/srv/crypto-research/repo
export CRYPTO_ENV_FILE=/srv/crypto-research/config/runtime.env
export CRYPTO_DATA_ROOT=/srv/crypto-research/data
export CRYPTO_COMPOSE_FILE="$CRYPTO_CHECKOUT/deploy/compose.yaml"
```

Confirm the selection before every change:

```bash
set -euo pipefail
test -d "$CRYPTO_CHECKOUT"
test -f "$CRYPTO_ENV_FILE"
test -d "$CRYPTO_DATA_ROOT"
test -f "$CRYPTO_COMPOSE_FILE"
test "$(stat -c %a "$CRYPTO_ENV_FILE")" = 600
runtime_uid=$(awk -F= '$1 == "CRYPTO_RUNTIME_UID" {print $2}' "$CRYPTO_ENV_FILE")
runtime_gid=$(awk -F= '$1 == "CRYPTO_RUNTIME_GID" {print $2}' "$CRYPTO_ENV_FILE")
runtime_uid=${runtime_uid:-1000}
runtime_gid=${runtime_gid:-1000}
case "$runtime_uid" in ''|*[!0-9]*) exit 1 ;; esac
case "$runtime_gid" in ''|*[!0-9]*) exit 1 ;; esac
test "$runtime_uid" -gt 0
test "$runtime_gid" -gt 0
data_owner=$(stat -c '%u:%g' "$CRYPTO_DATA_ROOT")
test "$data_owner" = "$runtime_uid:$runtime_gid"
docker compose --env-file "$CRYPTO_ENV_FILE" -f "$CRYPTO_COMPOSE_FILE" config --quiet
```

Compose runs both API and `market-worker` as this validated non-root owner. The same UID is required for API DuckDB reads; do not fix a mismatch by making the data tree world-readable or by reverting either service to root.

The read-only inventory captured on `2026-07-22T02:03+08:00` found installation root `/home/keyubin/crypto-research-phase0-smoke`, checkout `/home/keyubin/crypto-research-phase0-smoke/repo`, data `/home/keyubin/crypto-research-phase0-smoke/data`, and mode-`0600` environment `/home/keyubin/crypto-research-phase0-smoke/config/runtime.env`. PostgreSQL/API/Web were healthy, only `127.0.0.1:8088` was exposed, the volume was `crypto-research_postgres-data`, the database was `7518kB`, and source identity was `0226feba8bbefec207c7eee5b40c93c68a22e922`. This evidence is not a backup. Before replacing or upgrading that stack, complete the `VERIFIED` backup gate in [Market Data Recovery](market-data-recovery.md).

## Validate and Start Phase 1

```bash
cd "$CRYPTO_CHECKOUT"
docker compose --env-file "$CRYPTO_ENV_FILE" -f "$CRYPTO_COMPOSE_FILE" \
  --profile server config --quiet
docker compose --env-file "$CRYPTO_ENV_FILE" -f "$CRYPTO_COMPOSE_FILE" \
  --profile server up -d --build
docker compose --env-file "$CRYPTO_ENV_FILE" -f "$CRYPTO_COMPOSE_FILE" \
  --profile server ps
curl --fail http://127.0.0.1:8088/api/health/ready
curl --fail http://127.0.0.1:8088/api/operations/market-data
```

The API owns `alembic upgrade head`; `market-worker` waits for healthy PostgreSQL/API. PostgreSQL is bound only to `127.0.0.1:55432`, Web only to `127.0.0.1:8088`, and API has no host port. The worker tries direct access before the scoped `http://127.0.0.1:17891` proxy. Do not add generic proxy variables.

## Preflight Official Acceptance Objects

An acceptance date is usable only when its ZIP and official sibling `.CHECKSUM` are published. Probe every required object before mutating symbol or job state:

```bash
set -euo pipefail
preflight_archive_checksum() {
  source_url=$1
  archive_name=${source_url##*/}
  checksum_line=$(curl --fail --silent --show-error "${source_url}.CHECKSUM") || return 1
  checksum=$(printf '%s\n' "$checksum_line" | awk 'NR==1 {print tolower($1)}')
  listed_name=$(printf '%s\n' "$checksum_line" | awk 'NR==1 {print $2}' | sed 's/^\*//')
  printf '%s' "$checksum" | grep -Eq '^[0-9a-f]{64}$' || return 1
  test "$listed_name" = "$archive_name" || return 1
  printf 'published %s %s\n' "$checksum" "$source_url"
}

archive_root=https://data.binance.vision/data/futures/um
preflight_archive_checksum "$archive_root/monthly/klines/BTCUSDT/1m/BTCUSDT-1m-2026-06.zip"
preflight_archive_checksum "$archive_root/monthly/markPriceKlines/BTCUSDT/1m/BTCUSDT-1m-2026-06.zip"
preflight_archive_checksum "$archive_root/monthly/fundingRate/BTCUSDT/BTCUSDT-fundingRate-2026-06.zip"
preflight_archive_checksum "$archive_root/daily/aggTrades/BTCUSDT/BTCUSDT-aggTrades-2026-06-14.zip"
preflight_archive_checksum "$archive_root/monthly/klines/1000PEPEUSDT/1m/1000PEPEUSDT-1m-2026-05.zip"
preflight_archive_checksum "$archive_root/monthly/markPriceKlines/1000PEPEUSDT/1m/1000PEPEUSDT-1m-2026-05.zip"
preflight_archive_checksum "$archive_root/monthly/fundingRate/1000PEPEUSDT/1000PEPEUSDT-fundingRate-2026-05.zip"
```

These candidates were observed published on 2026-07-22, but the live probes remain mandatory. Daily `fundingRate` archives do not exist. `PEPEUSDT` is not the Binance USDⓈ-M venue symbol; inputs `PEPE`, `PEPEUSDT`, and `1000PEPEUSDT` all operate on canonical `1000PEPEUSDT` state. Intermediate Phase 1 alias-row builds were never deployed, so there is no legacy alias-row migration or fallback.

The observed Binance kline/mark-price CSV header uses `quote_volume`, `count`, `taker_buy_volume`, and `taker_buy_quote_volume`; these are strict source fields and normalize to the repository's longer canonical names. Live stream identifiers are also case-sensitive: `aggTrade`, `bookTicker`, `kline_1m`, and `markPrice@1s`. Do not lowercase the suffixes. Routed connections use `/market` for aggTrade, kline, and mark price, and `/public` for bookTicker.

**Do not POST any symbol or backfill request unless every probe succeeds.**

## Add Symbols and Request Bounded History

Use the canonical venue symbol in operational scripts. BTC uses the complete June month for core data and one separate closed aggregate-trade day; 1000PEPE uses the complete May month without aggregate trades.

```bash
curl --fail-with-body -H 'Content-Type: application/json' \
  -d '{"symbol":"BTCUSDT","history_start":"2026-06-01T00:00:00Z","history_end":"2026-07-01T00:00:00Z","include_agg_trades":true}' \
  http://127.0.0.1:8088/api/symbols
curl --fail-with-body -H 'Content-Type: application/json' \
  -d '{"symbol":"1000PEPEUSDT","history_start":"2026-05-01T00:00:00Z","history_end":"2026-06-01T00:00:00Z","include_agg_trades":false}' \
  http://127.0.0.1:8088/api/symbols

curl --fail-with-body -H 'Content-Type: application/json' \
  -d '{"symbol":"BTCUSDT","data_types":["kline_1m","mark_price","funding"],"start":"2026-06-01T00:00:00Z","end":"2026-07-01T00:00:00Z","include_agg_trades":false}' \
  http://127.0.0.1:8088/api/symbols/BTCUSDT/backfills
curl --fail-with-body -H 'Content-Type: application/json' \
  -d '{"symbol":"BTCUSDT","data_types":["agg_trade"],"start":"2026-06-14T00:00:00Z","end":"2026-06-15T00:00:00Z","include_agg_trades":true}' \
  http://127.0.0.1:8088/api/symbols/BTCUSDT/backfills
curl --fail-with-body -H 'Content-Type: application/json' \
  -d '{"symbol":"1000PEPEUSDT","data_types":["kline_1m","mark_price","funding"],"start":"2026-05-01T00:00:00Z","end":"2026-06-01T00:00:00Z","include_agg_trades":false}' \
  http://127.0.0.1:8088/api/symbols/1000PEPEUSDT/backfills
```

Save every job ID and poll `GET /api/backfills/{job_id}`. Inspect partitions, gaps, profile, eligibility, and streams under each symbol independently. Never infer 1000PEPE parameters from BTC.

## Verify Downloaded Archive Checksums

Every approved archive must match both the durable source checksum and the official checksum:

```bash
set -euo pipefail
cd "$CRYPTO_CHECKOUT"
catalog_rows=$(
  docker compose --env-file "$CRYPTO_ENV_FILE" -f "$CRYPTO_COMPOSE_FILE" \
    --profile server exec -T postgres \
    psql -v ON_ERROR_STOP=1 -U crypto -d crypto_research -At -F '|' -c \
    "SELECT source_url, source_checksum, raw_path FROM backfill_objects WHERE state='catalog_approved' ORDER BY source_url"
)
test -n "$catalog_rows"

while IFS='|' read -r source_url source_checksum raw_path; do
  official_checksum=$(curl --fail --silent --show-error "${source_url}.CHECKSUM" | awk 'NR==1 {print tolower($1)}')
  stored_checksum=$(sha256sum "$CRYPTO_DATA_ROOT/$raw_path" | awk '{print $1}')
  printf '%s\n' "$official_checksum" | grep -Eq '^[0-9a-f]{64}$'
  test "$official_checksum" = "$source_checksum"
  test "$stored_checksum" = "$source_checksum"
  printf '%s %s\n' "$source_checksum" "$source_url"
done <<< "$catalog_rows"
```

Never publish a mismatch. Quarantine conflicting bytes and record only sanitized URL, checksums, UTC time, and job identity.

## Pending Sources, Replacements, and Gaps

A recent archive 404 is `source_pending`, not an empty partition. An older 404 fails and opens a gap. Recheck the exact official URL and `.CHECKSUM`; do not reset durable state with ad hoc SQL. A source replacement at the same URL requires new immutable raw data, Parquet, manifest, and partition versions. Never overwrite prior evidence.

Use only the bounded operator commands. They accept no URL, checksum, or time range from the caller:

```bash
curl --fail-with-body -H 'Content-Type: application/json' -d '{}' \
  -X POST "http://127.0.0.1:8088/api/backfills/$JOB_ID/retry"

curl --fail-with-body -H 'Content-Type: application/json' \
  -d "{\"partition_id\":\"$APPROVED_PARTITION_ID\"}" \
  -X POST "http://127.0.0.1:8088/api/backfills/$JOB_ID/recheck"

curl --fail-with-body -H 'Content-Type: application/json' \
  -d "{\"partition_ids\":[\"$APPROVED_PARTITION_ID\"]}" \
  -X POST "http://127.0.0.1:8088/api/gaps/$GAP_ID/reconcile"
```

Retry changes only failed or `source_pending` objects back to planned. Recheck reads the structured approved archive identity and official sibling checksum; unchanged bytes are a no-op, while a new checksum creates a separate immutable job/object and lets the catalog publish the next version. Reconcile accepts 1–100 unique approved partition IDs and leaves the gap open unless their validated coverage fully repairs it.

After reconnect or restart, inspect `/api/operations/market-data`, per-symbol streams, and gaps. A disconnect gap is repaired only by continuity or approved replacement evidence; new messages alone are insufficient. Keep eligibility false while a required stream is stale/degraded or a required gap remains open. `metadata_unverified` caused by unavailable Binance REST metadata is a separate fail-closed gate and must never be filled with invented values.

The `PublicBinanceRestAdapter` is retained as an inactive boundary. Current server acceptance observed direct timeout and proxy HTTP 451 for USDⓈ-M REST, so no repeated REST repair/metadata probe is enabled and `/api/operations/market-data` deliberately reports `rest_healthy=false`. Activation requires a reachable public endpoint and a separately reviewed acceptance gate.

## Restart, Exposure, and Acceptance Record

Record job/partition/manifest/live-partition counts, restart API and worker, and verify identities are not duplicated:

```bash
docker compose --env-file "$CRYPTO_ENV_FILE" -f "$CRYPTO_COMPOSE_FILE" \
  --profile server restart api market-worker
docker compose --env-file "$CRYPTO_ENV_FILE" -f "$CRYPTO_COMPOSE_FILE" \
  --profile server ps
curl --retry 12 --retry-delay 5 --retry-connrefused --fail \
  http://127.0.0.1:8088/api/operations/market-data
ss -lntp | grep -E ':(8088|55432)\b'
```

Expected listeners are exactly `127.0.0.1:8088` and `127.0.0.1:55432`. From the Mac use `ssh -N -L 8088:127.0.0.1:8088 keyubin@192.168.1.4` and open `http://127.0.0.1:8088`.

Record sanitized UTC timestamps, application commit, job/manifest/partition IDs, checksums, row ranges/counts, source mode, gaps, eligibility reasons, restart comparison, and listener output. Do not record secrets, proxy traffic, chat IDs, or bulk market data.

## Observed Acceptance — 2026-07-22

The accepted deployment preserved `/home/keyubin/crypto-research-phase0-smoke/config/runtime.env` and `/home/keyubin/crypto-research-phase0-smoke/data`, used a verified backup at `/home/keyubin/crypto-research-backups/phase0-20260721T193438Z`, and staged candidates separately from the installation root. API/worker acceptance used candidate `4672152`; the browser-found canonical stream-name display fix used Web candidate `b7676c2`.

Seven jobs, source objects, manifests, and archive partitions remained stable across restart. Fourteen archive/Parquet checksum comparisons passed, both BTCUSDT and 1000PEPEUSDT exposed all four required live streams, the scoped proxy was active after recorded direct failures, and listeners were exactly `127.0.0.1:55432` and `127.0.0.1:8088`. The full sanitized record is in `.superpowers/sdd/task-8-report.md`.

Do not promote this accepted foundation directly into research replay. Open upstream, restart, and proxy-disconnect gaps plus `metadata_unverified` keep both symbols ineligible. Reconnect churn may make `bookTicker` transiently disconnected even when the latest snapshot is healthy; require a sustained stable observation window and repaired gaps before Phase 2.
