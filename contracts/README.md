# Generated contract artifacts

`jsonschema/` is generated-only. Run
`services/api/.venv/bin/python services/api/scripts/export_schemas.py` to
replace its complete `*.schema.json` set, or use `--check` in CI to verify the
set and contents without writing anything.

`types/` is generated from that exact four-schema set. The generator removes
only `.ts` files whose first line is exactly `// Generated. Do not edit.`;
manual TypeScript files are preserved.
