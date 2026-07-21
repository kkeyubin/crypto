# Task 4: Deterministic Cross-Language Contract Generation

## Status

Complete. The four Pydantic root contracts are exported in serialization mode as
committed, deterministically formatted JSON Schemas, and each is compiled to an
isolated TypeScript declaration module with exact root-only re-exports.

## RED evidence

Created `services/api/tests/contracts/test_schema_export.py` before the export
script. Running:

```bash
cd services/api && .venv/bin/pytest tests/contracts/test_schema_export.py -q
```

failed as intended with exit code 1 because
`scripts/export_schemas.py` did not exist (`[Errno 2] No such file or directory`).

## GREEN evidence

Added `services/api/scripts/export_schemas.py`, generated the schemas, then ran
the focused test again. It passed: `1 passed in 0.68s`.

`export_schemas.py --check` passes when committed files match the serialization
schemas and reports drift with a nonzero exit code when they do not.

## Dependencies resolved

- Node: `v24.15.0` from `.nvmrc` (`24`)
- npm: `11.12.1`
- `json-schema-to-typescript`: `15.0.4`
- `typescript`: `5.9.3`

The durable Task 4 plan now includes `typescript` and
`contracts:check-types`. This narrow compatibility correction is needed because
the transitive `@types/lodash` declarations require an ES2015 library when
compiled by TypeScript; the script uses `--lib es2015`.

## Generated files

- `contracts/jsonschema/AIAssessment.schema.json`
- `contracts/jsonschema/DataManifest.schema.json`
- `contracts/jsonschema/MarketSnapshot.schema.json`
- `contracts/jsonschema/StrategySpec.schema.json`
- `contracts/types/AIAssessment.ts`
- `contracts/types/DataManifest.ts`
- `contracts/types/MarketSnapshot.ts`
- `contracts/types/StrategySpec.ts`
- `contracts/types/index.ts`

`index.ts` contains only the four exact root type re-exports. Each root was
compiled independently, and the per-module duplicate exported-name scan found no
duplicates.

## Verification

```bash
cd services/api && .venv/bin/python scripts/export_schemas.py --check
cd ../.. && source /Users/kyle/.nvm/nvm.sh && nvm use
npm install
npm run contracts:types
npm run contracts:check-types
npm run contracts:types  # second run; SHA-256 manifest unchanged
cd services/api && .venv/bin/pytest -q
.venv/bin/ruff check src tests
```

Results: schema check passed; type generation passed twice with identical
SHA-256 manifests; TypeScript check passed; backend suite passed (`51 passed in
2.21s`); Ruff reported `All checks passed!`.

## Self-review and concerns

The exporter uses `model_json_schema(mode="serialization")`, stable model order,
sorted JSON keys, two-space indentation, and a trailing newline. The generator
sorts input schemas, removes only prior `.ts` generated outputs, and rebuilds the
index deterministically. No concerns remain.

## Review-fix evidence

The schema exporter now accepts `--output` for isolated verification. In
`--check` mode it neither creates the output directory nor writes any file, and
it compares the actual `*.schema.json` names with the exact four expected roots.
Its diagnostics identify every missing, modified, and stale file on separate
lines. Normal mode owns the generated-only `contracts/jsonschema/` boundary:
it creates the directory, writes all four current schemas, and removes stale
`*.schema.json` artifacts.

Focused tests cover a missing schema (and the no-directory-creation guarantee),
a modified schema, and a stale extra schema without changing committed files:
`4 passed in 3.80s`.

`contracts/generate-types.mjs` now requires that its input has exactly the four
schema roots. It removes only `.ts` files whose first line is exactly the
generated header. `npm run contracts:test-generation` uses temporary input and
output directories to prove a manual `.ts` survives, a header-marked stale file
is removed, the exact root-only index is rebuilt, and a second generation is
byte-identical.

Fresh review-fix verification ran the focused schema tests, schema `--check`,
the generator safety test, two TypeScript generations with identical SHA-256
manifests, `contracts:check-types`, full API tests (`54 passed in 6.30s`), and
Ruff (`All checks passed!`). No warnings or concerns remain.
