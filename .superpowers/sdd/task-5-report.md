# Task 5: Routed WebSocket Capture and Market Worker

## Status

Task 5 implementation is complete. This report does not declare Phase 1
complete; API, UI, deployment, and server acceptance remain later tasks.

## Delivered behavior

- `RoutedStream` is backed by the existing structured
  `BinanceWebSocketSource`; callers cannot supply a URL or base URL. Canonical
  stream names are lowercase. Aggregate trade and best bid/ask use `/public`;
  1-minute kline and 1-second mark price/funding use `/market`. Combined stream
  groups are deterministic and never exceed Binance's 1024-stream limit.
- Pure parsing is independent of transport and fails closed for invalid JSON,
  unknown event types, stream/event identity mismatches, non-UTC receive time,
  malformed fields, non-finite decimals, and invalid kline intervals. Parsed
  events retain venue, market, symbol, dataset, event/receive time, source ID,
  close state, and the exact source decimal strings.
- Closed live klines and aggregate trades use the same Arrow schemas as archive
  normalization. In-progress klines are retained raw but not normalized. Mark
  price/funding and `bookTicker` have exact `decimal128(38,18)` live schemas.
- Raw event evidence is written before normalization. All paths are derived
  from structured descriptors beneath `CRYPTO_DATA_ROOT`; dir-fd traversal,
  `O_NOFOLLOW`, fixed names, validated Parquet reads, `fsync`, and atomic
  `os.replace` publication are used. Restart replay is idempotent. Per-partition
  locks prevent overlapping reader threads from losing rows during read/merge/
  replace.
- The transport uses the installed `websockets` 16 asyncio API. Direct
  connections explicitly pass `proxy=None`; auto fallback is unlocked only
  after an awaited, classified direct failure record. Only loopback proxy URLs
  are accepted. Successful proxy activation and direct recovery are separately
  audited.
- Protocol ping/pong settings, explicit ping checks, planned 23h55 rotation,
  bounded exponential backoff with jitter, subscription refresh, and graceful
  shutdown are covered deterministically. Existing proxy connections are
  actively probed at the configured recovery interval and all groups migrate
  back to direct after a successful probe.
- `MarketWorker` reloads enabled symbols independently, reconciles routed
  groups, consumes managed WebSocket messages, persists live evidence before
  freshness state, runs one archive backfill lease beside live capture, and
  updates worker heartbeats. Initial handshakes and established-reader losses
  both reconnect with bounded backoff; SIGTERM interrupts an in-progress
  backoff immediately.
- Stream state records connecting, connected, degraded/stale, and disconnected
  states. A connected stream with no first event also becomes stale. Disconnect
  state retains its UTC anchor; reconnect creates deterministic, idempotent
  per-stream `source_unknown_disconnect` gaps.
- Runtime repositories use a session per operation so concurrent stream readers
  never share a SQLAlchemy `AsyncSession`. The archive scheduler reconstructs
  an official archive descriptor from each persisted planned object and rejects
  any URL that does not exactly equal that structured plan.
- Existing tables already cover the required state, so no `0003` migration was
  added. No API, UI, strategy, backtest, trading, credential, wallet,
  notification, or AI behavior was introduced.

## TDD evidence

Initial RED runs failed because `streams.py`, `live_storage.py`, `worker.py`,
and `market.__main__` did not exist. Later focused RED cases reproduced and then
guarded:

- stale state missing when a connection received no first event;
- existing proxy connections never running their periodic direct probe;
- initial handshake failure terminating the worker instead of retrying;
- SIGTERM waiting behind a long reconnect backoff;
- 24 concurrent live events collapsing to only 3 rows through a lost-update
  race;
- disconnect state omitting its persisted UTC anchor;
- the prior structured WebSocket contract routing aggregate trades through
  `/market` with mixed-case stream suffixes.

Every case was observed failing before the corresponding production change.
The final focused market result is `159 passed`.

## Verification

All commands below completed with exit code 0 after the final changes:

```text
cd services/api
.venv/bin/pytest -q tests/market
# 159 passed

.venv/bin/pytest -q
# 387 passed, 1 skipped

.venv/bin/ruff check src tests migrations
# All checks passed!

.venv/bin/python scripts/export_schemas.py --check

source /Users/kyle/.nvm/nvm.sh && nvm use
# Node v24.15.0, npm v11.12.1
npm run contracts:test-generation
npm run contracts:check-types
npm run contracts:types
git diff --exit-code contracts

git diff --check
```

The skipped test is the opt-in PostgreSQL integration suite because
`CRYPTO_TEST_DATABASE_URL` was not set. No real Binance endpoint, mutable
production data, credential, live fund, or server deployment was used.

## Remaining risks and acceptance boundary

- Live daily Parquet publication currently performs a deterministic whole-file
  merge. It is correct and protected against in-process overlap, but sustained
  high-rate production load should move to larger buffered batches/immutable
  shards before broad symbol expansion.
- The per-partition lock is process-local. Deployment must keep one authoritative
  live writer per partition unless a later task adds an inter-process lease or
  append-only shard protocol.
- Routed connectivity, loopback proxy behavior, 24-hour operation, PostgreSQL
  persistence across container restart, and archive/live reconciliation still
  require the Phase 1 server smoke/acceptance gate. They were not inferred from
  mocked tests.
