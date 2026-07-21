# Phase 1 Binance Data Foundation Design

**Status:** Approved for execution by the owner's 2026-07-21 Phase 1 directive

**Depends on:** Phase 0 contracts and application foundation at `9d80c99`

**Deployment target:** `keyubin@192.168.1.4`

## 1. Outcome and Boundary

Phase 1 makes Binance USDⓈ-M public market data reproducible, observable, and safe to consume. The owner can add `BTCUSDT`, `PEPEUSDT`, or another perpetual symbol; request an explicit UTC history range; observe live data; inspect gaps, freshness, checksums, and per-symbol eligibility; and remove the symbol from active monitoring without deleting its audit history.

This phase does not implement strategies, backtests, paper orders, signals, Feishu notifications, or AI. It supplies the verified data boundary those later phases require.

## 2. Verified Source Constraints

The implementation must describe the environment as it is, not as Binance normally behaves:

| Source | Local Mac | Server | Phase 1 use |
|---|---|---|---|
| `data.binance.vision` archives | direct works | direct expected and acceptance-tested | authoritative historical bulk and next-day reconciliation |
| USDⓈ-M REST `fapi.binance.com` | HTTP 451 direct and through `17891` | direct timeout; proxy HTTP 451 | optional adapter only; never required for a healthy archive/live state |
| Routed USDⓈ-M WebSocket | direct works | direct handshake timeout; server loopback proxy works | authoritative live capture with direct-first, proxy-on-restriction behavior |

The worker uses the current routed WebSocket bases: `/public` for high-frequency streams and `/market` for regular market streams. It rotates connections before Binance's 24-hour limit, honors ping/pong, reconnects with bounded exponential backoff and jitter, and records every source transition.

No API key or exchange credential is accepted. Because `exchangeInfo` cannot currently be verified, metadata-dependent eligibility fails closed as `metadata_unverified`. Historical and live datasets may still become `data_ready`; they cannot become strategy- or paper-eligible until a documented public metadata source succeeds.

## 3. Source and Reconciliation Model

The historical planner prefers complete monthly archives, then daily archives for the remaining closed UTC days. It supports:

- `klines/<symbol>/1m` for trade-price 1-minute bars;
- `markPriceKlines/<symbol>/1m` for mark-price 1-minute bars;
- `fundingRate/<symbol>` for funding history;
- `aggTrades/<symbol>` only when explicitly selected because of its size.

Every ZIP is downloaded to a temporary file, checked against the sibling `.CHECKSUM`, inspected for one safe CSV member, normalized, and atomically promoted. A 404 for the most recently closed day is `source_pending`, not a silent empty dataset. Archives can be replaced upstream, so reconciliation rechecks source checksums and versions any replacement.

The live worker subscribes per active symbol to completed and in-progress 1-minute klines, mark-price updates, aggregate trades, and `bookTicker` best bid/ask. Binance's `markPrice@1s` fields `r` and `T` are retained explicitly as a **provisional next-funding observation**; they are not a realized funding payment, do not enter `FUNDING_SCHEMA` or `DataType.FUNDING`, and do not replace archive/REST funding history. Likewise, mark-price ticks are not synthesized into archive-compatible OHLC bars; official `markPriceKlines` remain the source for that schema.

The worker acknowledges a message only after one of 64 fixed SQLite WAL bucket journals durably commits it. A stable `symbol|dataset` uniqueness domain selects `bucket=00..3f`, so source and normalized keys remain unique across UTC dates without scanning historical partitions. Each event also records its source `partition_key`; a prepared batch contains only one symbol/dataset/source-date partition and advances deterministically through `prepared -> published -> cataloged`. Batch IDs encode the bucket, allowing acknowledgement to locate one journal directly. A descriptor-backed cross-process writer lock enforces one authoritative writer for the shared data root. Startup enumerates only the 64 bucket paths and separately performs descriptor-safe legacy-layout detection; any former per-day journal or symlinked spool fails closed with recovery guidance. Prepared manifests make immutable gzip NDJSON raw shards and compressed normalized Parquet shards recoverable across process failure; in-progress klines remain raw-only. After an unknown disconnect, planned rotation, subscription regroup, or worker restart, the worker records the exact observed interval with the authoritative `DataType`; a gap anchor is cleared only after its database transaction commits. REST repair may fill an interval only when the adapter is reachable and its returned range passes the same validators.

## 4. Storage, Catalog, and Integrity

Large data stays outside Git under `CRYPTO_DATA_ROOT`:

```text
raw/binance/usdm/<SYMBOL>/<dataset>/date=YYYY-MM-DD/
normalized/binance/usdm/<SYMBOL>/<dataset>/date=YYYY-MM-DD/
spool/binance/usdm/bucket=00..3f/journal.sqlite3
```

Closed partitions are immutable Parquet files written through a same-filesystem temporary path. Decimal values remain decimal/string-derived values, never binary floating-point inputs; any numeric zero, including signed scientific zero with a large exponent, is canonicalized to Arrow-safe `0` before durable acceptance. UTC millisecond timestamps, source event IDs where present, schema version, normalization version, and ingestion time are retained. Duplicate primary keys are rejected or deterministically deduplicated and counted.

PostgreSQL stores symbol configuration, metadata state, ingestion jobs, archive partition manifests, the independent `live_data_partitions` registry, source objects/checksums, gaps, stream state, worker heartbeats, and audit events. It does not store high-volume ticks. For each live batch, catalog registration and stream-state checkpoints commit in one transaction; only then is the SQLite batch acknowledged. Every live artifact records both source-event min/max for provenance and dataset-canonical min/max for querying: kline `open_time`, aggregate-trade and book-ticker `transact_time`, and mark-price `event_time`. Repository shard overlap, approved-row guards, and fixed DuckDB filters/orders use only that canonical axis; Binance source `E` never selects a query shard. DuckDB reads approved normalized Parquet through checksum-revalidated file descriptors, and callers cannot supply paths or SQL. Raw, rejected, missing, or merely present-but-unregistered shards are not queryable. Approval records checksum, schema, ordering, uniqueness, both ranges, and row-count evidence.

## 5. Symbol-Independent State

Each symbol progresses independently:

```text
requested -> backfilling -> data_ready | degraded | failed
metadata_unverified | profile_building -> eligible | ineligible
```

The empirical profile includes realized volatility, jump frequency, spread distribution, volume/liquidity by hour, funding distribution, observed gaps, staleness, and data coverage. BTC metrics never seed PEPE thresholds or approval. Eligibility exposes separate reasons such as `insufficient_history`, `stale_live_data`, `unrepaired_gap`, `insufficient_liquidity`, and `metadata_unverified`.

Adding a symbol requires an explicit history start/end and opt-in for historical aggregate trades. Default API limits prevent accidental unbounded downloads. Removing a symbol disables new subscriptions and jobs but retains data, manifests, and audit records.

## 6. API and Console

The API provides typed endpoints for symbol list/detail, add/disable, bounded backfill creation, job status, datasets/partitions, gaps, profile, eligibility, and source/worker health. Mutations are idempotent and create audit events. Dates are accepted and returned as UTC ISO 8601 values.

The Chinese-first console replaces Phase 0 placeholders only for the symbol/data surfaces. It shows independent symbol cards, add-symbol form, backfill progress, data coverage, live freshness, source mode (`direct`, `proxy`, or `degraded`), gaps, profile metrics, and eligibility reasons. No control suggests backtest, paper, or signal functionality is available yet.

## 7. Deployment and Network Boundary

`market-worker` runs with Linux host networking so it can reach the server's loopback-only proxy at `127.0.0.1:17891`. PostgreSQL is additionally published on a non-default loopback-only host port for that worker; the API continues using the private Compose network. Web remains bound to `127.0.0.1:8088`. No market, database, or application port becomes publicly reachable.

Proxy mode is `auto`: attempt direct access first; switch only after a classified timeout, DNS failure, TLS/connect failure, or geographic restriction; periodically probe direct recovery. Configuration validation rejects a non-loopback proxy in production unless a later security decision explicitly allows it.

## 8. Acceptance Gate

Phase 1 is complete only when automated tests and a server smoke test demonstrate all of the following:

1. Add `BTCUSDT` and `PEPEUSDT` with separate bounded ranges and independent states.
2. Download at least one closed partition for kline, mark-price kline, and funding; verify official SHA-256 files and write validated Parquet/manifests.
3. Opt in to a bounded `aggTrades` partition and prove a failed or pending object is not published as valid.
4. Receive and persist routed live kline, mark price, aggregate trade, and best bid/ask events through the server's measured network path.
5. Detect a fixture gap, block eligibility, repair/reconcile it, and retain repair history.
6. Restart worker/API containers without duplicating valid rows or losing job/stream state.
7. Show both symbols, freshness, coverage, source mode, gap state, profile, and fail-closed eligibility in the Chinese UI.
8. Confirm REST unavailability leaves the system honestly degraded only for REST/metadata functions; verified archive and live data remain usable.

Later phases may consume only catalog-approved partitions and fresh live state. They must continue to block on stale data, unrepaired gaps, and unresolved eligibility reasons.
