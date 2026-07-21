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
- Focused repository policy test: `2 passed`.
- Full API suite: `56 passed`.
- Ruff: `All checks passed!`.
- Markdown link policy is exercised by the focused test; every Skill/reference
  link is local and resolves.

## Files

- `skills/crypto-trading-research/SKILL.md`
- `skills/crypto-trading-research/agents/openai.yaml`
- `skills/crypto-trading-research/references/strategy-workflow.md`
- `skills/crypto-trading-research/references/runtime-ai.md`
- `skills/crypto-trading-research/references/source-map.md`
- `services/api/tests/repository/test_unified_skill.py`
- `docs/superpowers/plans/2026-07-21-phase-0-foundation-and-contracts.md`

## Commit

Commit message: `feat: add unified crypto research skill`.

## Concerns

- The Skill is a research and shadow-analysis contract, not a runtime
  implementation. Actual object validation and order isolation remain enforced
  by the existing Pydantic contracts and deterministic runtime boundary.
- The unchanged `.superpowers/sdd/task-5-brief.md` is an SDD ledger artifact;
  the executable plan was corrected only in the project plan document.
