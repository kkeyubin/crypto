# Task 5 — Unified Crypto Research Skill Report

## RED

Command:

```bash
cd services/api && .venv/bin/pytest tests/repository/test_unified_skill.py -q
```

Result: `2 failed`. Both tests raised `FileNotFoundError` for the absent
`skills/crypto-trading-research/SKILL.md`, the intended missing-Skill baseline.
The behavior baseline and its contract audit are preserved in
`task-5-baseline-report.md`.

Review-hardening RED used the same focused command and produced
`4 failed, 3 passed`. Those failures proved the previous token/link tests did
not enforce the seven-family mode matrix, the complete per-symbol gate, the
exact runtime schema and authority boundary, or the presence of a reusable
three-case eval contract.

## GREEN and Verification

```bash
cd services/api
.venv/bin/python - <<'PY'
from pathlib import Path
import yaml

path = Path('../../skills/crypto-trading-research/agents/openai.yaml')
value = yaml.safe_load(path.read_text())
assert set(value) == {'interface'}
assert set(value['interface']) == {'display_name', 'short_description', 'default_prompt'}
assert all(isinstance(item, str) and item for item in value['interface'].values())
PY
.venv/bin/pytest tests/repository/test_unified_skill.py -q
.venv/bin/pytest -q
.venv/bin/ruff check .
```

Results:

- YAML metadata: valid `interface` structure.
- Focused repository policy test: `7 passed`.
- Full API suite: `61 passed`.
- Ruff: `All checks passed!`.
- Markdown link policy is exercised by the focused test; every Skill/reference
  link is local and resolves.
- `evals/cases.json` is parsed and checked for the three exact prompts,
  non-empty expected/forbidden behaviors, and critical safety expectations.

## Files

- `skills/crypto-trading-research/SKILL.md`
- `skills/crypto-trading-research/agents/openai.yaml`
- `skills/crypto-trading-research/references/strategy-workflow.md`
- `skills/crypto-trading-research/references/runtime-ai.md`
- `skills/crypto-trading-research/references/source-map.md`
- `skills/crypto-trading-research/evals/cases.json`
- `services/api/tests/repository/test_unified_skill.py`
- `docs/superpowers/plans/2026-07-21-phase-0-foundation-and-contracts.md`
- `.superpowers/sdd/task-5-baseline-report.md`
- `.superpowers/sdd/task-5-green-report.md`

## Commit

- Initial implementation: `3bd4e45 feat: add unified crypto research skill`.
- Review hardening: `test: harden unified skill boundaries`.

## Concerns

- The Skill is a research and shadow-analysis contract, not a runtime
  implementation. Actual object validation and order isolation remain enforced
  by the existing Pydantic contracts and deterministic runtime boundary.
- `evals/cases.json` is deliberately a fixed pressure-test contract. It does not
  invoke or score an LLM; the RED and GREEN reports preserve the actual answers
  and human audits separately.
- In the current `MarketSnapshot`, `deterministic_signal_id` is only an optional
  UUID identifier. It has no signal payload or timestamp; adding either requires
  a future contract extension.
- The unchanged `.superpowers/sdd/task-5-brief.md` is an SDD ledger artifact;
  the executable plan was corrected only in the project plan document.
