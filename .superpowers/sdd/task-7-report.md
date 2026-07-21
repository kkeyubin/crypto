# Task 7 Report: Chinese-First Symbol and Data Console

## Scope

Implemented the Phase 1 read-only data console in `apps/web`. The console now provides Chinese-first navigation, configured-symbol inspection, per-symbol data evidence, centralized market-data health, symbol onboarding with explicit UTC backfill ranges, manual backfill progress refresh, and reversible symbol disablement. It does not add trading, order, notification, or LLM behavior.

## TDD evidence

Each material behavior began with a failing assertion before implementation. The RED checkpoints covered:

1. The Symbols navigation item was disabled instead of opening a route.
2. Loading, empty, safe-error, and retry states were absent.
3. BTC and PEPE did not have independent profile, partition, gap, eligibility, and stream evidence cards.
4. The symbol form did not require a closed UTC range and double opt-in for high-volume `aggTrades` data.
5. Symbol disablement had no explicit confirmation or preserved-history explanation.
6. Filtering, full-width responsive layout, visible focus, and reduced-motion styling were absent.
7. A 390 px browser review exposed an inherited `flex-basis` that made the filter row 280 px tall.
8. Submitted backfill jobs had no explicit progress refresh control.

The implementation then made those focused tests pass without weakening their assertions.

## Implementation

### Trust boundary and API access

- Added a runtime-validating API client for symbol, partition, gap, profile, eligibility, stream, operations-health, and backfill endpoints.
- Response bodies from failed requests are never rendered. Errors are localized, generic, and contain no server-provided HTML, paths, URLs, or arbitrary detail.
- Symbols and job identifiers are validated before use in encoded URL segments.
- The UI consumes Task 6 `source_mode` as the centralized heartbeat, exact-stream, and freshness decision instead of reimplementing those rules.

### Console states and evidence

- Added loading, empty, error, retry, and ready states.
- BTC and PEPE evidence is requested and rendered independently rather than sharing profile or eligibility state.
- Cards show approved sample rows, sample count, coverage, gap count, latest UTC event time, data and metadata status, and eligibility reasons.
- Health cells distinguish source path, archive, live, and REST/metadata observations with text and icons; color is not the sole state signal.

### Controlled actions

- Adding a symbol requires an explicit closed UTC start/end range and creates data-only backfills for `kline_1m`, `mark_price`, and `funding`.
- `agg_trade` requires both an enable checkbox and a separate high-volume acknowledgement.
- Submitted job identifiers remain visible and can be refreshed manually through `GET /api/backfills/{job_id}`.
- Disablement uses a keyboard-accessible confirmation dialog, explains that history is preserved, supports Escape/cancel, and calls only the symbol disable endpoint.

### Language, accessibility, and layout

- Chinese is the default language; English is available and persisted.
- Navigation, page copy, filters, statuses, confirmations, and actions are localized.
- The Evidence Lab visual tokens are used consistently with tabular numerals, visible focus, responsive grids, and reduced-motion handling.
- Disabled Phase 1 destinations remain visibly unavailable while Overview and Symbols expose active navigation state.

## Browser visual self-review

The desktop view used the available width for the evidence grid and health row. A 390 x 844 review initially found excessive whitespace above the filter input because `.filter-control` inherited a desktop `flex-basis: 280px`. A failing CSS regression assertion was added, then the narrow breakpoint was corrected to `flex: 0 1 auto`.

After the fix, browser measurements were:

- viewport width: 390 px;
- document scroll width: 390 px (no horizontal overflow);
- filter control height: 65 px;
- search input height: 42 px;
- BTC and PEPE card widths: 343 px each.

The narrow add-symbol form, cards, localized copy, controls, and confirmation affordances remained readable and operable.

## Verification

All verification was run after the final backfill-refresh change:

- `npm run contracts:check-types` — passed.
- `npm run web:test -- --run` — 3 files, 21 tests passed.
- `npm run web:build` — passed; 56 modules transformed.
- `cd services/api && .venv/bin/pytest -q tests/routes/test_symbols.py tests/routes/test_data.py tests/routes/test_operations.py` — 16 tests passed.
- `git diff --check` — passed before commit.

## Known boundaries and follow-up risks

- The page currently loads at most the first 100 configured symbols and performs several evidence reads per symbol. A larger universe will need a backend aggregate endpoint or pagination to avoid request fan-out.
- Backfill progress is deliberately refreshed by the user; continuous polling, push notifications, and background alerts are outside this task.
- The console reports backend evidence and policy decisions but does not authorize trading, simulate orders, manage credentials, or expose public ports.
