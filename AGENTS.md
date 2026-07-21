# Repository Guidelines

## Project Structure & Module Organization

The API and Binance market-data worker are in `services/api/src/crypto_research/`; tests, migrations, and schema tooling are under `services/api/`. The React console is `apps/web/`, generated contracts are `contracts/`, Skills are `skills/`, documentation is `docs/`, and deployment assets are `deploy/`. Do not commit source books, runtime data, reports, and artifacts; also exclude dependencies, secrets, and temporary `.superpowers/` sessions. Reviewed `.superpowers/sdd/*-report.md` files may be committed.

## Build, Test, and Development Commands

Node.js is managed by NVM. Before npm commands, run `source /Users/kyle/.nvm/nvm.sh && nvm use`.

- `cd services/api && .venv/bin/pytest -q` runs all Python tests, including repository policy checks.
- `cd services/api && .venv/bin/ruff check src tests` lints the API.
- `cd services/api && .venv/bin/python scripts/export_schemas.py --check` rejects stale JSON Schemas.
- `npm run contracts:test-generation && npm run contracts:check-types` verifies generated TypeScript contract tooling.
- `npm run contracts:types && git diff --exit-code contracts` regenerates and rejects uncommitted contract drift.
- `npm run web:test -- --run` runs Web tests once; `npm run web:build` type-checks and builds the console.

When a Pydantic contract changes, regenerate and commit both `contracts/jsonschema/` and `contracts/types/`. Validate server configuration with `docker compose --env-file /srv/crypto-research/config/runtime.env -f deploy/compose.yaml --profile server config --quiet` first.

## Commit and Pull Request Guidelines

Use concise imperative Conventional Commits, for example `feat: add price feed adapter` or `fix: reject stale quotes`. Pull requests state motivation, verification commands, configuration impact, risks, linked issues, and user-visible screenshots or logs when applicable.

## Coding, Testing, and Security

Follow committed formatters and two-space indentation for JSON, YAML, JavaScript, and TypeScript. Use `camelCase`, `PascalCase`, and `kebab-case` appropriately. Every behavior change needs focused tests plus the relevant full suite. Pytest and Vitest have no numeric coverage threshold. Network, exchange, wallet, and market-data tests use mocks or sandbox endpoints, never live funds or mutable production data.

Never commit seed phrases, private keys, wallet files, exchange/API secrets, or populated `.env` files. Keep runtime secrets in `/srv/crypto-research/config/runtime.env`, validate at startup, and redact logs and fixtures. Use no generic process-wide proxy. Phase 1 authorizes public market-data ingestion only; Phase 2 research/backtesting and all simulated trading, notifications, AI, exchange credentials, and public ports remain out of scope.

Do not use Claude for this project. Codex handles book analysis, skill generation, strategy formalization, and backtesting. Analyze each book and create its separate Skill before considering a combined cross-book Skill. Keep copyrighted source books out of published commits.
