# Repository Guidelines

## Project Structure & Module Organization

Keep the root focused on configuration and documentation. Place application code in `src/`, tests in `tests/`, static files in `assets/`, documentation in `docs/`, and repository-owned Codex skills in `skills/<skill-name>/`. Mirror source paths in tests where practical. Do not commit generated output, dependencies, or editor state.

## Build, Test, and Development Commands

No build system is configured yet. Document a new toolchain in `README.md` and expose repeatable scripts. For Node.js, activate the repository's NVM version, then standardize scripts such as:

- `npm install` — install locked dependencies.
- `npm run dev` — start the local development process.
- `npm test` — run the complete automated test suite.
- `npm run lint` — check formatting and static-analysis rules.
- `npm run build` — produce a release-ready build.

Commit the lockfile and an `.nvmrc` when Node.js is adopted.

## Coding Style & Naming Conventions

Follow committed formatters and linters, not editor-only settings. Use two-space indentation for JSON, YAML, JavaScript, and TypeScript. Prefer `camelCase` for variables and functions, `PascalCase` for classes and components, and `kebab-case` for filenames. Keep modules small and document non-obvious business or cryptographic assumptions.

## Testing Guidelines

Every feature or bug fix should include tests. Name tests `*.test.*` or `*.spec.*` consistently and keep fixtures under `tests/fixtures/`. Cover normal behavior, boundary conditions, invalid input, and failure paths. Tests involving networks, exchanges, wallets, or market data must use mocks or sandbox endpoints by default; never depend on live funds or mutable production data.

## Commit & Pull Request Guidelines

There is no existing commit convention. Use concise, imperative Conventional Commits such as `feat: add price feed adapter` or `fix: reject stale quotes`. Pull requests should explain motivation, verification commands, configuration impact, and risks. Link issues and include screenshots or logs for user-visible behavior.

## Security & Configuration

Never commit seed phrases, private keys, API secrets, wallet files, or populated `.env` files. Provide `.env.example` with placeholder values, validate configuration at startup, and redact identifiers and credentials from logs and test fixtures.

## Agent-Specific Instructions

Do not use Claude for this project. Codex handles book analysis, skill generation, strategy formalization, and backtesting. For each book, create an analysis and a separate skill; only build a combined cross-book skill after all individual books are complete. Keep copyrighted source books out of published commits.
