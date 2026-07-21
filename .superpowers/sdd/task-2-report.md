# Task 2: Phase 1 API Contracts and Generated TypeScript

## Status

Complete. The public Binance USD-M data boundary now has strict Pydantic input
and read models, `DataManifest` v2 provenance and validation metadata, and
committed JSON Schema/TypeScript declarations for every new root contract.

## RED evidence

Tests were added before implementation in
`services/api/tests/contracts/test_data.py` and the existing manifest runtime
tests were extended. Running:

```bash
cd services/api && .venv/bin/pytest -q tests/contracts/test_data.py tests/contracts/test_runtime_contracts.py
```

failed as intended during collection: `crypto_research.contracts.data` and the
new manifest metadata enums did not yet exist.

## GREEN evidence

Added strict immutable request/view contracts for symbols, bounded backfills,
jobs, approved partitions, gaps, profiles, eligibility, streams, and source
health. All timestamps reject naive, non-UTC, and future values; history ranges
are limited to 366 days; `agg_trade` history requires explicit opt-in; and all
externally visible status/reason values are enums. Eligibility carries an
explicit boolean plus a tuple of machine-readable reason codes, requiring at
least one code when it is ineligible.

`DataManifest` now requires `schema_version="2.0.0"`, source kind/object URL,
raw and normalized relative paths, source and normalized SHA-256 values, row
count, validation state, primary-key fields, and deterministic deduplication
metadata. Its former v1 `checksum` input is intentionally not accepted.

The focused GREEN command passed with `60 passed in 0.69s`:

```bash
cd services/api && .venv/bin/pytest -q tests/contracts/test_data.py tests/contracts/test_runtime_contracts.py
```

## Generated files

Added JSON Schemas and generated TypeScript declarations for
`AddSymbolRequest`, `BackfillRequest`, `SymbolView`, `IngestionJobView`,
`DataPartitionView`, `DataGapView`, `SymbolProfileView`, `EligibilityView`,
`StreamStateView`, and `MarketDataHealthView`. Updated `DataManifest` schema
and declaration, the generated type index, the schema exporter, and the
generator's exact-root safety test.

## Verification

```bash
cd services/api && .venv/bin/pytest -q
# 168 passed, 1 skipped in 11.11s
cd services/api && .venv/bin/ruff check src tests
# All checks passed!
cd services/api && .venv/bin/python scripts/export_schemas.py --check
source /Users/kyle/.nvm/nvm.sh && nvm use
npm run contracts:types
npm run contracts:test-generation
npm run contracts:check-types
git diff --check
```

All listed verification commands completed successfully. A full-suite timing
failure in the future-timestamp test was traced to computing a one-second
future fixture during module import; the fixture now uses a five-minute future
window and the full suite is stable.

## Self-review and concerns

Reviewed source restrictions, enum-only external states, UTC/range/count/finite
validation, generated-schema root lists, deterministic type generation, and
the generated `DataManifest` required fields. No API routes, persistence,
archive implementation, trading, signals, credentials, or AI behavior were
added. No outstanding concerns.

## Review-fix evidence

### RED

Added source-kind URL relation tests, closed `RepairRecord` enum tests,
explicit-v2 omission coverage, numeric-string coverage, Schema condition
assertions, and a generated-output atomicity regression. The first runtime
contract command failed as intended because `RepairResult` and `RepairSource`
did not exist:

```bash
cd services/api && .venv/bin/pytest -q tests/contracts/test_runtime_contracts.py tests/contracts/test_data.py tests/contracts/test_schema_export.py
# collection error: cannot import name 'RepairResult'
```

Before schema regeneration, the conditional schema assertion also failed as
intended with `KeyError: 'allOf'`.

The first regeneration exposed a generator bug: `allOf` causes
`json-schema-to-typescript` to emit `export type EligibilityView = ...`, while
the wrapper expected an interface. More importantly, the generator deleted old
generated files before compiling all roots. The new regression corrupted one
expected temporary schema, seeded all old generated outputs, and failed with an
`AssertionError` showing `AIAssessment.ts` had already been replaced after the
generation failure.

### GREEN

`DataManifest` now parses and validates source URLs against the exact archive,
REST, and routed WebSocket allowlists. It rejects cross-kind URLs, noncanonical
hosts/schemes/ports/userinfo/fragments, traversal (including encoded traversal),
and empty or malformed stream identifiers. `RepairRecord` now uses the exact
`RepairSource` and `RepairResult` enums. `schema_version` is an explicit required
`Literal["2.0.0"]` field.

The schema exporter owns `EligibilityView`'s two JSON Schema `if`/`then`
conditions for `reason_codes` cardinality. The type generator now compiles every
root in memory before deleting any generated file, preserves manual files, and
wraps either a generated root interface or root type alias in `DeepReadonly`.
The generator regression confirms every pre-existing generated output remains
byte-identical on a compile failure.

Focused contracts passed:

```bash
cd services/api && .venv/bin/pytest -q tests/contracts
# 152 passed in 3.22s
```

Final verification passed:

```bash
cd services/api && .venv/bin/pytest -q
# 202 passed, 1 skipped in 7.31s
cd services/api && .venv/bin/ruff check src tests
# All checks passed!
cd services/api && .venv/bin/python scripts/export_schemas.py --check
source /Users/kyle/.nvm/nvm.sh && nvm use
npm run contracts:types
npm run contracts:test-generation
npm run contracts:check-types
git diff --check
```

No expected generated TypeScript files are deleted; `DataManifest.ts` and
`EligibilityView.ts` are regenerated with the reviewed changes. No concerns
remain.

## Second review-fix evidence

### RED

Added exact public REST endpoint/query tests and a generator assertion that no
generated root declaration includes `[k: string]: unknown;`. Running:

```bash
cd services/api && .venv/bin/pytest -q tests/contracts/test_runtime_contracts.py
```

failed as intended: all 14 private, trailing-slash, unknown, repeated, blank,
lowercase-symbol, wrong-interval, and credential-bearing REST URLs were still
accepted by the prior `/fapi/v1/` prefix check. The generator safety test also
exposed the `EligibilityView` root index-signature artifact.

### GREEN

REST provenance now accepts only `klines`, `markPriceKlines`, `aggTrades`, and
`fundingRate` with their exact per-endpoint parameter allowlists. It requires
one uppercase `symbol`, requires exactly `interval=1m` for both kline endpoints,
and rejects private/trading paths, suffixes, unknown/repeated/blank parameters,
and case-insensitive credential names.

For schemas with `additionalProperties: false`, the type generator strips only
the exact json-schema-to-typescript root intersection artifact before applying
the root `DeepReadonly` wrapper. It leaves nested mappings untouched and the
generator test rejects an index signature in every generated root declaration.

Final verification:

```bash
cd services/api && .venv/bin/pytest -q tests/contracts
# 170 passed in 6.24s
cd services/api && .venv/bin/pytest -q
# 220 passed, 1 skipped in 12.15s
cd services/api && .venv/bin/ruff check src tests
# All checks passed!
cd services/api && .venv/bin/python scripts/export_schemas.py --check
source /Users/kyle/.nvm/nvm.sh && nvm use
npm run contracts:types
npm run contracts:test-generation
npm run contracts:check-types
git diff --check
```

All checks passed; `rg` confirms no generated TypeScript declaration retains
the root unknown index signature. No concerns remain.

## Third review-fix evidence

### RED

Added table-driven endpoint/query cases for canonical unsigned numeric values,
int64 overflow, endpoint limit maxima, timestamp ordering, aggTrades cursor/time
mutual exclusion and time span, plus strict combined-WebSocket query cases.

```bash
cd services/api && .venv/bin/pytest -q tests/contracts/test_runtime_contracts.py
```

failed as intended with 18 failures: the prior exact REST path/key validation
still accepted noncanonical/overflow numeric fields, invalid endpoint limits,
inconsistent time/cursor combinations, and combined stream queries containing
credentials, duplicate streams, or unrelated keys.

### GREEN

Replaced the mutable endpoint key map with immutable `RestEndpointSpec` entries
that declare allowed keys, limit maxima, one-minute interval requirements,
cursor/time exclusion, and maximum time span. Shared helpers parse canonical
ASCII unsigned integers, enforce int64 bounds and relationships, and reject
sensitive query names. Combined WebSocket URLs now require exactly one exact,
nonblank `streams` pair and no other query data.

Final verification:

```bash
cd services/api && .venv/bin/pytest -q tests/contracts
# 194 passed in 5.81s
cd services/api && .venv/bin/pytest -q
# 244 passed, 1 skipped in 10.71s
cd services/api && .venv/bin/ruff check src tests
# All checks passed!
cd services/api && .venv/bin/python scripts/export_schemas.py --check
source /Users/kyle/.nvm/nvm.sh && nvm use
npm run contracts:types
npm run contracts:test-generation
npm run contracts:check-types
git diff --check
```

All checks passed. No concerns remain.
