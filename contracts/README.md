# Generated contract artifacts

`jsonschema/` is generated-only. Run
`services/api/.venv/bin/python services/api/scripts/export_schemas.py` to
replace its complete `*.schema.json` set, or use `--check` in CI to verify the
set and contents without writing anything.

The five roots separate author input from persisted output: `StrategySpec`
omits `content_hash`, while flat `StrategySpecRecord` requires and validates
the canonical SHA-256 before it can round-trip through JSON. The other roots
are `AIAssessment`, `DataManifest`, and `MarketSnapshot`.

`types/` is generated from that exact five-schema set. Every exported root is
a `DeepReadonly` view of an internal generated shape, including nested objects,
arrays, tuples, and index signatures. The generator removes only `.ts` files
whose first line is exactly `// Generated. Do not edit.`; manual TypeScript
files are preserved.
