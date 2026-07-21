# Task 5: Routed WebSocket Capture and Market Worker

## Status

Task 5 implementation and both independent-review remediation rounds are
complete.
This report does not declare Phase 1 server acceptance complete; routed
connectivity, the shared production volume, PostgreSQL restart recovery, and
long-running operation remain Task 8 acceptance gates.

## Delivered behavior

- `RoutedStream` constructs only the committed structured Binance sources.
  Aggregate trade and best bid/ask use `/public`; kline and mark price use
  `/market`. Stream names are canonical lowercase, combined groups are
  deterministic, and no group exceeds 1024 streams.
- Pure parsers are transport-independent and fail closed for malformed JSON,
  unknown events, mismatched combined-stream identity, non-UTC receipt time,
  invalid fields, non-finite decimals, unsigned-int64 overflow, invalid
  price/quantity signs, inconsistent OHLC/trade-ID/book relationships, and
  invalid 1-minute kline boundaries. Decimal strings remain exact. Kline and mark-price events correctly retain
  `source_id=None`; only genuine aggregate-trade and book update IDs populate
  source ID.
- Closed trade klines and aggregate trades use the existing archive Arrow
  schemas. In-progress klines are retained raw and are not normalized.
  `bookTicker` uses a live best-bid/ask schema.
- A mark-price WebSocket update is a tick, not an archive mark-price OHLC bar.
  Its `r/T` fields are named and stored as a provisional next-funding
  observation. They are never written to realized `FUNDING_SCHEMA` or
  `DataType.FUNDING`, and ticks are never synthesized into OHLC. Official
  archive/REST sources remain authoritative for mark-price klines and realized
  funding history.
- `handle_message` returns only after a per-symbol/dataset/day SQLite journal
  commits the event with WAL and `synchronous=FULL`. Indexed identity and
  payload-hash lookup is logarithmic; no daily JSON ledger is read or rewritten.
  The in-memory signal only wakes the publisher and is never the durable source
  of truth.
- Each prepared batch records event membership, deterministic raw/normalized
  paths, SHA-256, schema name, sort/unique keys, event-time range, and row count
  before publishing immutable gzip NDJSON and zstd Parquet shards. The journal
  advances through `prepared`, `published`, and `cataloged`; restart recovery
  reconstructs or verifies the same objects and resumes any unacknowledged
  batch.
- Replay identity excludes local `receive_time`. Replaying the same source
  identity and payload is idempotent across process restarts and batch
  boundaries; a different payload for that identity fails closed. Closed
  klines additionally reserve `open_time` and aggregate trades reserve
  `aggregate_trade_id` as normalized primary keys across batch shards. Same-key
  equivalent raw observations remain raw-only; same-key changed normalized
  payloads fail closed. In-progress klines never reserve a normalized key.
- A private descriptor-backed `fcntl` lock beneath `CRYPTO_DATA_ROOT` permits
  one authoritative live writer across instances/processes. The lease remains
  held for the worker lifetime and is force-released during shutdown. All
  immutable-object paths are structured beneath the secure root and opened
  without following symlinks; prepared-object checksums are reverified on
  recovery. Lease close and every storage mutation share the same re-entrant
  lock, so a close cannot race a newly accepted or published event.
- Direct WebSocket attempts explicitly use `proxy=None`. Auto fallback occurs
  only after a classified, awaited direct-failure audit record; only loopback
  proxies are accepted. Proxy activation and direct recovery are audited. If a
  transition audit fails after opening a socket, that new socket is closed and
  the prior connection mode remains authoritative.
- Ping/pong, planned pre-24-hour rotation, bounded exponential backoff with a
  true hard maximum, subscription refresh, periodic direct recovery, and
  graceful SIGTERM are deterministic and covered without live Binance.
  Shutdown and discard close connections best-effort and remove every managed
  connection even when an individual close fails.
- `MarketWorker` reloads enabled symbols independently. A publisher drains the
  durable spool, then approves every `LiveWriteResult` and updates its stream
  states in one PostgreSQL transaction. Only after that commit succeeds does it
  acknowledge the SQLite batch. Transient SQLAlchemy/asyncpg failures retry the
  same published batch with bounded backoff; retry exhaustion leaves it durable
  for process restart.
- Startup reloads stream states, preserves `last_event_at`, details, and
  `source_mode`, and opens deterministic `worker_restart` gap anchors from the
  last durable timestamp. Every direct/proxy change is persisted even if the
  stream was already connected. Disconnect anchors are removed only after the
  corresponding gap transaction commits.
- Reader tasks that finish through disconnect, close, parse, or database error
  are collected and rebuilt when their managed connection remains active.
  Source-mode details are merged with per-message dataset details instead of
  being overwritten, while stale/disconnected transient reasons are cleared on
  recovery.
- Planned rotation, subscription regroup, and unknown disconnect expose
  explicit disconnect/reconnect lifecycle windows. Gaps use the existing
  authoritative `DataType` values: `kline_1m`, `mark_price`, `agg_trade`, and
  `best_bid_ask`. The mark-price provisional funding observation does not create
  a false realized-funding gap.
- Archive backfill leasing runs as an independent task so a slow download cannot
  block WebSocket supervision, heartbeat refresh, or SIGTERM. Cleanup attempts
  readers, backfill, force-flush, all sockets, stopped heartbeat, and writer
  release independently; cleanup failures do not mask the primary worker error.
- Migration `20260721_0003` adds the independent `live_data_partitions`
  registry. It has no archive `source_object` coupling and records immutable
  batch/layer/path identity plus checksum, schema, ordering, uniqueness, range,
  row count, approval state, and timestamps. The DuckDB-facing boundary returns
  only approved normalized Parquet paths under the fixed data root; raw or
  unregistered shards cannot enter that boundary.
- No trading, exchange credential, wallet, public port, strategy, paper
  execution, notification, or AI behavior was added.

## Review-driven TDD evidence

Every remediation below was first represented by a failing focused test:

- parser unsigned-int64, signed/unsigned decimal, OHLC, trade-ID, and book
  relationship enforcement;
- WAL-before-return acceptance, immutable batch shards replacing the daily JSON
  ledger, batch-boundary replay, normalized-primary-key conflict detection, and
  same-batch duplicate replay;
- prepared-batch crash injection, deterministic restart recovery, and
  published-before-catalog acknowledgement;
- live-catalog completeness, immutable idempotent registration, migration head,
  and approved-normalized-only DuckDB path resolution;
- two storage instances and a separate Python process contending for the writer
  lock;
- durable acceptance before batch publication, atomic catalog/state commit,
  transient DB retry of one published batch, and shutdown draining;
- genuine versus unavailable source IDs and a jittered backoff hard cap;
- provisional funding naming and separation from realized funding;
- transition-audit socket leaks, shutdown close failure, planned rotation, and
  subscription-regroup lifecycle events;
- restart gap recovery, source-mode changes while connected, gap-anchor retry,
  completed reader cleanup/rebuild, blocking backfill cancellation, and cleanup
  errors attempting to mask a primary failure.

The final focused market suite is recorded in the verification section below.

## Verification

The final verification commands are:

```text
cd services/api
.venv/bin/pytest -q tests/market
# 207 passed

.venv/bin/pytest -q
# 436 passed, 1 skipped

.venv/bin/ruff check src tests migrations
# All checks passed!

.venv/bin/python scripts/export_schemas.py --check
# passed

.venv/bin/python -m alembic -c alembic.ini heads
# 20260721_0003 (head)

source /Users/kyle/.nvm/nvm.sh && nvm use
# Node v24.15.0, npm v11.12.1
npm run contracts:test-generation
npm run contracts:check-types
npm run contracts:types
git diff --exit-code contracts
# passed

npm run web:test -- --run
# 2 files, 10 tests passed

npm run web:build
# passed

git diff --check
# passed
```

The former wall-clock-sensitive 60ms backfill heartbeat test now uses a fixed
clock and explicit publish/renew synchronization. Its exact test was repeated
30 times successfully before the two complete market-suite runs; production
lease expiry behavior was not relaxed.

The skipped test is the opt-in PostgreSQL integration suite because
`CRYPTO_TEST_DATABASE_URL` was not set.

No real Binance endpoint, mutable production data, credential, live fund, LAN
server, or production database was used.

## Remaining acceptance boundary

- The cross-process lock assumes all worker replicas use the same POSIX shared
  data root. Task 8 must verify that deployment invariant and prove restart
  replay on the actual mounted volume.
- WAL acceptance, prepared immutable publication, and catalog retry are covered
  under injected failures and process contention locally. Task 8 still owns
  power-loss/container-restart smoke testing and operational orphan inspection.
- Routed direct/proxy behavior and 24-hour stability remain real-environment
  acceptance checks; mocked connector health is not reported as production
  connectivity.
