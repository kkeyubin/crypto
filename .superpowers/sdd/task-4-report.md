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
