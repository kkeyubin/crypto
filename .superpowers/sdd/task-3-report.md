# Task 3: Verified Binance Archive Ingestion Report

## Scope delivered

Implemented the Phase 1 USD-M archive boundary only. The planner creates
structured `BinanceArchiveSource` objects and derives every request URL from
`resolved_url`; it does not accept an arbitrary upstream URL. It chooses full
monthly archives first, followed by daily edge ranges. The adapter streams to a
same-filesystem `.partial` file, fsyncs it, verifies the sibling official
SHA-256 before opening it, rejects unsafe ZIP structures, and returns typed
not-found without assigning orchestration policy.

Raw ZIPs are retained by verified source checksum. Normalized Parquet files are
written to a temporary sibling and atomically published. Kline, mark-price,
funding, and aggregate-trade CSVs have deterministic Arrow schemas with
`decimal128(38, 18)` price/quantity/funding fields derived directly from source
strings. Range, ordering, primary key, duplicate-count, header, column-count,
and one-minute kline-close invariants are enforced.

No REST calls, live workers, database orchestration, catalog approval, API/UI,
backtest, trading, or signal work was added.

## TDD evidence

### RED

1. Before production modules existed:

   ```text
   services/api/.venv/bin/pytest -q services/api/tests/market
   ```

   Result: collection failed with four expected
   `ModuleNotFoundError: No module named 'crypto_research.market'` errors.

2. After adding the URL-override adversarial test but before its guard:

   ```text
   services/api/.venv/bin/pytest -q services/api/tests/market/test_archive_paths.py::test_archive_object_cannot_override_the_structured_official_source_url
   ```

   Result: failed as expected with `Failed: DID NOT RAISE <class 'ValueError'>`.

### GREEN

After the smallest corresponding implementations:

```text
services/api/.venv/bin/pytest -q services/api/tests/market
```

Result: `28 passed`.

Focused adversarial coverage includes official dataset URL layouts, monthly and
daily boundaries, archive URL override rejection, mocked checksum/download
handling, 404, checksum mismatch, truncated/multi-member/absolute/traversal
ZIPs, compressed/uncompressed/ratio bounds, unexpected CSV headers and column
counts, source-range violations, unsorted and duplicate timestamps/IDs,
one-minute close times, Decimal preservation, and atomic raw/Parquet storage.
ZIP bytes are generated in test helpers from committed textual CSV fixtures; no
opaque ZIP fixture is committed.

## Verification

```text
cd services/api
.venv/bin/pytest -q
```

Result: `243 passed, 1 skipped`.

```text
cd services/api
.venv/bin/ruff check src tests migrations
.venv/bin/python scripts/export_schemas.py --check
```

Result: Ruff passed and the schema drift check exited successfully.

```text
source /Users/kyle/.nvm/nvm.sh && nvm use
npm run contracts:test-generation
npm run contracts:check-types
git diff --check
```

Result: Node v24.15.0 selected; both TypeScript contract checks and whitespace
diff check passed.

## Self-review

- Every archive object's URL and checksum sibling are checked against its
  structured official source, so a caller cannot substitute an arbitrary host.
- No price, quantity, or funding value is parsed through `float`; Arrow receives
  `Decimal` values created from source text.
- ZIP inspection happens only after checksum verification, reads no arbitrary
  member path, disallows symlinks, and leaves no failed staging file.
- Raw and normalized paths retain the required
  `raw|normalized/binance/usdm/<SYMBOL>/<dataset>/date=YYYY-MM-DD/` layout.
- The task intentionally stops before orchestration: current typed 404 remains
  an adapter fact; Task 4 owns `source_pending` versus gap handling and catalog
  approval.

## Concerns / follow-up boundary

The exact accepted CSV headers are deliberately strict and fixture-tested. If
Binance changes a published archive header, ingestion fails closed instead of
silently mapping a changed schema; a future compatibility change must add an
explicit versioned mapping and fixture. Task 4 still must decide per-object
pending/gap state and record manifests/catalog approval after this boundary.

## Review hardening follow-up

### RED

Review findings were first added as deterministic tests for the current/open UTC
day, non-UTC `as_of`, strict checksum records, empty and non-regular ZIP
members, EOCD count/size/offset and many-entry archives, destination traversal
and symlink escapes, corrupt/schema/row-count Parquet writes, and raw-retention
symlink escape.

```text
services/api/.venv/bin/pytest -q services/api/tests/market
```

Result before the fixes: `24 failed, 28 passed`. The expected failures included
missing `as_of` and `data_root` arguments, permissive checksum parsing,
acceptance of empty CSV members, EOCD-derived malformed ZIP paths reaching
`zipfile`, and publication/retention escapes.

### GREEN

The corresponding changes now enforce:

- planner ranges ending no later than deterministic current UTC midnight;
- one exact official checksum record for the URL basename;
- single non-empty regular CSV member with bounded, preflighted EOCD/central
  directory before `ZipFile` construction;
- resolved-root containment and same-filesystem raw retention;
- Parquet footer/schema/row-group/row-count/data equality validation before
  rename, preserving an existing published file on every validation failure.

```text
services/api/.venv/bin/pytest -q services/api/tests/market
```

Result: `52 passed`.

## Descriptor-anchored storage hardening

### RED

Added regressions for a canonical ZIP64 locator beside an otherwise safe classic
EOCD, immutable staging-descriptor handling, insecure/symlinked roots, ancestor
and final-entry symlinks, unpredictable temporary files, path swap during
publication, validation failure preservation, and raw destination symlinks.

```text
services/api/.venv/bin/pytest -q services/api/tests/market/test_storage.py services/api/tests/market/test_archive_download.py
```

Result before implementation: `10 failed, 24 passed`. The failures demonstrated
the former pathname `replace`/unlink behavior, unavailable derived-path API,
and ZIP64 locator reaching `ZipFile` construction.

### GREEN

Storage now derives output paths internally and performs all security-sensitive
operations through a 0700, non-symlink, non-group/world-writable root directory
descriptor. It creates each partition component relative to an opened parent
descriptor with `O_NOFOLLOW`; creates unpredictable 0600 temporary names;
validates Parquet through the temporary descriptor; hashes it through that same
descriptor; and renames only by `src_dir_fd`/`dst_dir_fd` before fsyncing the
opened directory. A simulated ancestor path replacement therefore publishes to
the originally opened directory, never to the replacement path.

Raw retention opens staging once with `O_NOFOLLOW`, requires a regular file,
copies rather than moves it into a descriptor-anchored temporary file, verifies
the hash, and leaves staging cleanup to its caller. Existing raw files are
opened and verified through `dir_fd` with `O_NOFOLLOW`.

The ZIP preflight now rejects a ZIP64 locator immediately preceding classic
EOCD (and adjacent ZIP64 EOCD signatures) before `ZipFile` is constructed.

```text
services/api/.venv/bin/pytest -q services/api/tests/market
```

Result: `55 passed`.
