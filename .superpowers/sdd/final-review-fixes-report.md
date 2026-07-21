# Phase 0 Whole-Branch Review Fix Report

## Reviewed design correction

The final review found that a computed Pydantic field made `StrategySpec`
serialization impossible to feed back into its own closed input contract. The
corrected lifecycle keeps `StrategySpec` as the ergonomic author/input name and
adds flat `StrategySpecRecord` persisted/output data with a required,
revalidated canonical SHA-256. No wrapper key or later-phase runtime behavior
was introduced.

The review also found that generated TypeScript roots exposed mutable nested
shapes and that Python scalar coercion was looser than the JSON Schemas. The
generator now emits each root as a `DeepReadonly` alias of an internal shape,
and all Pydantic runtime contracts inherit strict frozen configuration. JSON
parsing still accepts the JSON representations of UUIDs, UTC datetimes, and
string enums.

## RED evidence

The focused Python run after adding tests failed exactly at the missing
behaviors: `18 failed, 55 passed`. Failures covered serialized input hash
leakage, the absent record contract/factory, scalar coercion, missing interval
and repair/manifest range invariants, the fifth schema root, and lifecycle Skill
wording.

`npm run contracts:test-generation` failed because the root-only index lacked
`StrategySpecRecord`. `npm run contracts:check-types` failed with three
`TS2578` errors because `@ts-expect-error` was unused for nested identity,
chronology tuple, and fixed-parameter index mutations.

## GREEN evidence

- Focused contract/schema/Skill suite: `73 passed`.
- Full API suite: `87 passed`.
- Ruff, including `deploy/api-entrypoint.py`: `All checks passed!`.
- JSON Schema export `--check`: passed for the exact five-root set.
- Type generation safety/idempotence and TypeScript checking: passed.
- Regeneration followed by `git diff --exit-code -- contracts`: passed with no drift.
- Web suite: `10 passed`; production build: passed.
- `git diff --check` and staged diff whitespace checks: passed.

## Boundary and artifact review

- `StrategySpec` dumps omit `content_hash` and reject caller-provided hashes.
- `StrategySpecRecord.from_spec(...)` computes the hash, serializes the same
  flat payload plus the hash, round-trips through JSON, and rejects tampering.
- Boolean integer inputs and numeric-string OHLC/risk inputs are rejected.
- `MissingInterval`, `RepairRecord`, and `DataManifest` enforce their reviewed
  time-containment rules.
- Generated JSON Schemas and TypeScript declarations include both strategy
  lifecycle roots; `types/index.ts` exports roots only.
- Observation workflow omits execution and risk; only executable BB/RB declares
  both. Phase 0 still contains no ingestion, backtest, paper-order, credential,
  or trading-authority implementation.

## Concerns

None.

## Final non-finite review follow-up

A final re-review identified JSON-noncompliant `NaN`, `Infinity`, and
`-Infinity` as a remaining scalar gap. Test-first coverage produced `21 failed,
69 passed`: unconstrained strategy parameters and funding accepted all three;
some constrained financial fields accepted positive infinity or emitted only a
range error; canonical hashing serialized the non-standard constants.

`StrictFrozenModel` now sets `allow_inf_nan=False`, yielding deterministic
`finite_number` validation errors across parameter, funding, OHLC, fee, and risk
examples. Canonical hashing also sets `allow_nan=False` as defense in depth.
Parameterized coverage proves every valid executable BB/RB and observation
family record constructed by the contract round-trips through JSON.

Final GREEN verification: focused contracts `90 passed`; full API `117
passed`; Ruff including the deploy entrypoint, schema drift, contract generation
and types/no-drift, Web `10 passed`, production build, and whitespace checks all
passed.
