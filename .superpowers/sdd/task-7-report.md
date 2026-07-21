# Task 7 Report: Chinese-First Symbol and Data Console

## Scope

Implemented and review-hardened the Phase 1 read-only data console in `apps/web`. It provides Chinese-first navigation, independent symbol evidence, centralized operations health, archive-compatible symbol onboarding, idempotent backfill recovery, disable/re-enable controls, and explicit bounded-data semantics. It does not add trading, orders, credentials, notifications, or LLM behavior.

## TDD evidence

The initial implementation and every review correction were driven by a failing behavior assertion before the production change. Material RED checkpoints included:

1. Symbols navigation, loading, empty, safe-error, retry, BTC/PEPE independence, filtering, responsive layout, focus styles, and backfill progress were absent.
2. The add form exposed minute-level closed timestamps that the archive planner rejects instead of inclusive UTC calendar days mapped to a half-open range.
3. A successful symbol POST followed by a failed backfill POST was reported as one failed action and had no safe resume path.
4. One symbol evidence failure or operations-health failure rejected the entire dashboard.
5. Freshness used the newest arbitrary stream event rather than all four exact required streams and their oldest event.
6. First-page partition and gap counts were presented as totals, and approved catalog rows were incorrectly labeled as archive-only rows.
7. The runtime eligibility decoder accepted `eligible=true` with blockers and `eligible=false` without blockers.
8. The disable dialog did not trap focus or return focus after Escape, cancel, or confirmation.
9. Disabled symbols had no immutable-identity re-enable action.
10. A non-matching filter rendered an unexplained blank grid.
11. Initial rendering waited for every per-symbol evidence request, so one hung symbol also hid ready symbols and operations health.
12. Symbol and health retries had no abort signal or temporal fence, allowing an older response to overwrite a newer retry or full reload.
13. Disable and re-enable replaced only `SymbolView` while retaining stale profile, eligibility, partitions, gaps, and streams.
14. Successful initial and retried backfill creation left the symbol card at its pre-backfill status.
15. The 100-symbol notice incorrectly implied that the client-side filter could search configurations that were not loaded.
16. A slow disable request disabled the focused confirmation control and could break the modal keyboard/focus lifecycle.
17. A successful disable mutation and its evidence refresh shared one awaited promise, so a hung refresh kept the confirmation modal locked indefinitely.
18. After symbol creation succeeded, the add form could still be collapsed while backfill creation or partial-success recovery was unresolved, discarding the only visible retry path.

Focused tests were observed failing for the missing behavior and then passing after each minimal correction.

## Backend-compatible range contract

- The form accepts date-only inclusive UTC days.
- The selected start day is submitted as `00:00Z`; the inclusive end day is converted to the next day at `00:00Z`, producing the backend half-open `[start, end)` range of complete closed UTC days.
- Exactly 366 inclusive days is allowed; 367 days is rejected before any POST.
- The explanation is explicit in Chinese and English.
- A real `MarketDataControlService` plus archive-planner test submits the same July 1 through inclusive July 20 UI range as `[2026-07-01T00:00Z, 2026-07-21T00:00Z)` and verifies the persisted plan covers those midnight UTC boundaries.

## Failure isolation and recovery

- Symbol configuration and backfill creation are separate API operations and separate UI outcomes.
- If configuration succeeds but backfill creation fails, the configured symbol is reloaded into the grid and the form shows a partial-success state.
- The recovery button retries only the deterministic backfill POST; it never repeats symbol creation and therefore preserves backend idempotency.
- Symbol evidence requests resolve independently. A PEPE failure produces a PEPE retry card while BTC remains available; the retry reloads only PEPE.
- Once the bounded symbol list is available, the dashboard publishes loading cards immediately. Operations health and each symbol then settle independently, so a hung PEPE request cannot hide ready BTC evidence or health.
- Operations health has its own unavailable/loading/retry state and does not hide symbol evidence.
- Every list, health, and per-symbol evidence load has an `AbortController`. A monotonically increasing dashboard generation fences full reloads and unmounts; per-resource operation tokens fence overlapping symbol and health retries. Stale promises cannot commit even when a transport mock ignores abort.
- Unmount aborts outstanding work, suppresses every later state commit, and prevents a retained refresh callback from starting another request.
- Post-mutation evidence refresh is independent from the committed mutation and bounded to 10 seconds. Rejection or timeout replaces stale evidence with a truthful symbol-scoped recovery card; retry refreshes evidence only and cannot repeat the disable POST.
- Server error bodies are never rendered; all exposed errors remain localized and generic.

## Evidence invariants

- Runtime eligibility decoding enforces the backend invariant: eligible results have zero reason codes, and ineligible results have at least one valid blocking reason.
- Per-symbol freshness requires the exact four Binance streams: `aggtrade`, `bookticker`, `kline_1m`, and `markprice@1s` for that symbol.
- The card reports 4/4, missing, no-event, disconnected/degraded, and stale states explicitly.
- When all required event timestamps exist, the displayed timestamp is their oldest value. Staleness reuses the backend `stale_live_data` eligibility decision rather than inventing a second threshold.
- Operations `source_mode` remains the backend-owned global heartbeat and active-stream health decision.

## Bounded data and counts

- The API page limit remains 100. A full page is treated conservatively as truncated.
- Partition and gap values are labeled as visible counts; truncated values use “at least” and include a not-total notice, so zero or total is never implied when more records may exist.
- The partition metric is named “approved catalog rows” because the public API does not distinguish archive and live partitions.
- A full 100-symbol page displays a bounded-list notice rather than claiming the complete configured universe.
- The filter label and bounded-list notice state that filtering searches only the loaded first 100 items; it never implies that unloaded configurations can be found.
- A filter with no matches has its own state and clear-filter action. When the list is truncated, the empty result explicitly says that remaining configurations were not searched; otherwise it does not imply that monitored symbols were deleted.

## Controlled actions and accessibility

- `agg_trade` history still requires symbol-level and backfill-level explicit opt-ins.
- Backfill status remains manually refreshable through `GET /api/backfills/{job_id}`.
- Disablement explains history preservation and uses a modal focus trap. Tab and Shift+Tab stay inside; Escape and cancel close it before submission; focus returns to the opener.
- During a slow disable request, the focused confirmation control remains focusable with `aria-disabled`, repeat activation is guarded, Tab and Shift+Tab remain trapped, and Escape cannot dismiss an in-flight action.
- As soon as the disable POST succeeds, the confirmation closes and focus moves to a stable symbol-owned refreshing surface; the committed mutation is no longer coupled to the evidence request.
- A fresh evidence bundle is installed atomically before reporting the completed evidence state. Old eligibility, profile, partitions, gaps, or streams are never carried forward. Refresh failure or timeout becomes a focused symbol-scoped retry state and does not emit a false success notice. After a successful refresh, focus moves to the replacement action.
- Re-enable POSTs the card's immutable `history_start`, `history_end`, and `include_agg_trades` values through the existing `/api/symbols` endpoint. It never creates backfills automatically, and both the copy and completion notice say so.
- Every successful `createBackfills` path triggers a fresh dashboard load, including partial-success retry. A failed backfill still leaves the newly configured symbol visible and retryable.
- Once a valid add submission starts, the form cannot be collapsed while the add request is busy or a backfill recovery remains pending. Partial failure preserves the retry-only state and lock; successful recovery releases it. Unmount aborts active add/backfill requests and fences all later state writes.
- Chinese remains the default; English translations cover every added state and action.

## Browser visual self-review

The initial desktop and 390 x 844 browser review confirmed the full-width evidence grid. It also found and fixed an inherited mobile `flex-basis` that made the filter control 280 px tall. After correction, the 390 px viewport had a 390 px document width, 65 px filter control, 42 px search input, and 343 px symbol cards with no horizontal overflow.

Review-hardening states reuse the same surface, border, focus, warning, and responsive tokens. Partial success, health failure, truncated evidence, freshness, disabled/re-enable, and filtered-empty states do not depend on color alone.

## Final verification

All commands below were run after the final review corrections:

- `npm run contracts:check-types` — passed.
- `npm run web:test -- --run` — 4 files, 40 tests passed.
- `npm run web:build` — passed; 56 modules transformed.
- `cd services/api && .venv/bin/pytest -q tests/routes/test_symbols.py tests/routes/test_data.py tests/routes/test_operations.py tests/routes/test_control.py` — 26 tests passed, including the real archive-planner range test.
- `cd services/api && .venv/bin/ruff check src tests` — passed.
- `git diff --check` — passed before commit.

## Known boundaries

- The dashboard intentionally shows one bounded page of up to 100 symbols and up to 100 catalog/gap/stream records per symbol. The UI labels truncation honestly; a larger universe still needs an aggregate endpoint or real pagination to reduce request fan-out.
- Backfill progress is deliberately user-refreshed. Continuous polling, push notifications, and background alerts remain outside Task 7.
- Re-enabling collection deliberately does not infer that historical backfills should be duplicated; any later backfill is a separate explicit action.
- The post-mutation evidence refresh has a deliberate 10-second client timeout; timeout is recoverable through the symbol-scoped retry action without replaying the mutation.
