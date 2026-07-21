# Task 5: Routed WebSocket Capture and Market Worker

## Status

Task 5 implementation and the independent-review remediation are complete.
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
  invalid fields, non-finite decimals, and invalid 1-minute kline boundaries.
  Decimal strings remain exact. Kline and mark-price events correctly retain
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
- Live capture uses a bounded queue and batch roller. Each batch publishes
  immutable gzip NDJSON raw `part-*` shards and zstd Parquet normalized
  `part-*` shards; it never reads and rewrites a whole daily data file per
  message. A partition event index records approved immutable objects.
- Replay identity excludes local `receive_time`. Replaying the same source
  identity and payload is idempotent across process restarts and batch
  boundaries; a different payload for that identity fails closed. Duplicate
  identities inside one batch are written once.
- A private descriptor-backed `fcntl` lock beneath `CRYPTO_DATA_ROOT` permits
  one authoritative live writer across instances/processes. The lease remains
  held for the worker lifetime and is force-released during shutdown. All
  object/index paths are structured beneath the secure root and opened without
  following symlinks; indexed object checksums are reverified on replay.
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
- `MarketWorker` reloads enabled symbols independently and applies queue
  backpressure. A storage batch completes before its stream states are committed
  together in one repository transaction; there is no database commit per
  event. Transient batch-checkpoint failures retain and replay the pending
  batch with bounded backoff. Shutdown force-flushes all accepted events.
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
- Existing database tables cover Task 5 state, so no `0003` migration was
  required. No trading, exchange credential, wallet, public port, strategy,
  paper execution, notification, or AI behavior was added.

## Review-driven TDD evidence

Every remediation below was first represented by a failing focused test:

- immutable batch shards replacing daily read/merge/rewrite;
- batch-boundary and changed-receive-time replay, same-batch duplicate replay,
  and conflicting payload detection;
- two storage instances and a separate Python process contending for the writer
  lock;
- bounded buffer backpressure, batch state commit, transient DB retry, and
  shutdown force-flush;
- genuine versus unavailable source IDs and a jittered backoff hard cap;
- provisional funding naming and separation from realized funding;
- transition-audit socket leaks, shutdown close failure, planned rotation, and
  subscription-regroup lifecycle events;
- completed reader cleanup/rebuild, blocking backfill cancellation, and cleanup
  errors attempting to mask a primary failure.

The final focused market suite is recorded in the verification section below.

## Verification

The final verification commands are:

```text
cd services/api
.venv/bin/pytest -q tests/market
# 177 passed

.venv/bin/pytest -q
# 405 passed, 1 skipped

.venv/bin/ruff check src tests migrations
# All checks passed!

.venv/bin/python scripts/export_schemas.py --check
# passed

source /Users/kyle/.nvm/nvm.sh && nvm use
# Node v24.15.0, npm v11.12.1
npm run contracts:test-generation
npm run contracts:check-types
npm run contracts:types
git diff --exit-code contracts
# passed

git diff --check
# passed
```

The skipped test is the opt-in PostgreSQL integration suite because
`CRYPTO_TEST_DATABASE_URL` was not set.

No real Binance endpoint, mutable production data, credential, live fund, LAN
server, or production database was used.

## Remaining acceptance boundary

- The cross-process lock assumes all worker replicas use the same POSIX shared
  data root. Task 8 must verify that deployment invariant and prove restart
  replay on the actual mounted volume.
- Immutable object publication and the partition event index are covered under
  injected failures and process contention locally. Task 8 still owns
  power-loss/container-restart smoke testing and operational orphan inspection.
- Routed direct/proxy behavior and 24-hour stability remain real-environment
  acceptance checks; mocked connector health is not reported as production
  connectivity.
