# Task 6 Report: Chinese-First Evidence Lab Web Shell

## Scope

Implemented only the Phase 0 React shell and API liveness presentation. The shell deliberately does not claim to show market data, strategy performance, paper orders, or AI state.

## RED

1. With `apps/web/src/App.test.tsx` written before application code, ran:

   ```bash
   source /Users/kyle/.nvm/nvm.sh
   nvm use
   npm --workspace apps/web test -- --run
   ```

   This failed as expected with `No workspaces found: --workspace=apps/web`.

2. Added only the npm workspace/test-runner configuration and dependencies, then ran the focused test. It failed as expected because Vite could not resolve `./App` from `src/App.test.tsx`.

3. Controller regression check after the Web dependencies were installed:

   ```bash
   npm run contracts:check-types
   ```

   exited `2` with `TS2304` errors in `node_modules/@types/react-dom/index.d.ts` for `AbortSignal`, `ReferrerPolicy`, and `RequestDestination`. The generated contract declarations were not the source of the errors: TypeScript was automatically discovering the React ambient types from the workspace.

4. Controller CDP emulation at `390x844` found no document-width overflow (`innerWidth`, `clientWidth`, and `scrollWidth` were all `390`) but measured `.content-frame.top` at `337.48px`. The desktop two-row grid left the sidebar in the remaining row and placed main content in an implicit third row. A new static layout contract initially failed because the mobile media rule did not define `grid-template-rows: 64px auto minmax(0, 1fr)`.

5. Controller ran `npm run web:dev -- --host 127.0.0.1 --port 4173`; the root npm script swallowed the arguments and started Vite on its default port `5173` instead of `4173`.

6. Final controller verification found that the filesystem-backed layout assertion crossed the browser TypeScript boundary. `npm run web:build` failed with `TS2307` for `node:fs` and `node:path`, plus `TS2591` for `process`, because the browser `tsconfig` deliberately does not load Node ambient types.

## GREEN

- Added a Chinese-first, responsive Evidence Lab shell with a normal desktop sidebar/main grid, a full-width bounded main content frame, narrow-screen stacked layout, keyboard focus styles, and reduced-motion overrides.
- The only persisted locale that starts in English is `crypto-locale=en`; all other values fall back to `zh-CN`. `setLocale` updates both persisted selection and `<html lang>`.
- Health probes `GET /api/health/live` with an `AbortController`, ignores aborts/unmounted updates, and presents the textual status plus an accessible label, not color alone.
- `apps/web/src/contracts.ts` re-exports the generated contract types from `contracts/types/index.ts`.
- Added component assertions for Chinese default/health accessibility, English persistence/document language, and accessible current-page navigation.
- Corrected the root `web:test` script to forward `--run` to Vitest. The Task 6 brief and implementation plan now show the required trailing `--` form.
- Isolated the generated-contract check in `contracts/tsconfig.json` with `types: []`, `lib: ["ES2015"]`, `strict`, and `noEmit`. `contracts:check-types` now runs `tsc -p contracts/tsconfig.json`; it neither needs DOM types nor suppresses declaration checking.
- The mobile media rule now uses the three explicit grid rows `64px auto minmax(0, 1fr)`. The navigation remains horizontally scrollable while `scrollbar-width: none` and the WebKit scrollbar selector hide only the decorative native scrollbar.
- Root `web:dev` now includes a trailing `--`, forwarding host and port options to the Web workspace's Vite process.
- The layout test now consumes `styles.css?raw` through Vite rather than Node filesystem APIs. `vite/client` supplies the raw-asset module declaration, and Vitest enables its CSS transform for the static assertion. No Node types were added and tests remain included in the browser build type-check.

## Controller validation after the responsive fix

- Independent headless Chrome CDP mobile emulation used `Emulation.setDeviceMetricsOverride` with `width: 390`, `height: 844`, and `mobile: true` against the local Vite server.
- Final controller measurement: `innerWidth: 390`, `clientWidth: 390`, `scrollWidth: 390`, `.sidebar.bottom: 127`, `.main-content.top: 127`. The content now follows the navigation instead of starting at the prior `337.48px` implicit-row gap; there is no whole-document horizontal overflow. The earlier `navBottom: 116` and `contentTop: 151` values were from a preliminary measurement and are superseded by this final controller result.
- `npm run web:dev -- --host 127.0.0.1 --port 4173` listened on `127.0.0.1:4173` during verification and was terminated afterwards; a subsequent listener check was empty.

## Environment and verification

- Node `v24.15.0`, npm `11.12.1` via `/Users/kyle/.nvm/nvm.sh` and `.nvmrc`.
- Installed Web dependencies without proxy use; npm audit reported `0 vulnerabilities`.

Passed:

```bash
npm run web:test -- --run
npm run web:build
npm run contracts:test-generation
npm run contracts:types
npm run contracts:check-types
services/api/.venv/bin/pytest services/api/tests -q
services/api/.venv/bin/ruff check services/api/src services/api/tests
git diff --check
```

Results: Web Vitest `10/10` across the shell and health suites passed after test TypeScript checking; production Vite build emitted `apps/web/dist`; generated contract safety/type checks passed without drift; API regression suite `61 passed`; Ruff passed.

Final browser-compatible regression verification reran:

```bash
npm --workspace @crypto-research/web exec -- vitest run src/App.test.tsx --reporter=verbose
npm run web:build
npm run contracts:test-generation
npm run contracts:types
npm run contracts:check-types
services/api/.venv/bin/pytest services/api/tests -q
services/api/.venv/bin/ruff check services/api/src services/api/tests
git diff --check
```

All commands passed: focused Web tests `10/10` after test type-checking, Vite build, generated-contract safety/type checks, API `61 passed`, Ruff, and whitespace diff check.

## Review hardening follow-up

- Health now accepts a liveness response only when JSON is an object with `status: "ok"`, `service: "api"`, and a non-empty semantic version. Non-2xx responses, network failures, invalid JSON, malformed payloads, and invalid versions all produce the accessible unavailable state; abort and active guards remain in place.
- The overview link is the only Phase 0 navigation target. Other navigation controls are localized disabled buttons whose accessible labels state that the destination is not available, avoiding fabricated hash routes.
- Header and language-control accessible text now comes from translations. Locale initialization is exercised through `resolveInitialLocale`, allowing only stored `en` to choose English.
- Shell CSS scopes navigation, controls, typography, cards, and responsive navigation selectors to the console regions rather than global element selectors.
- The Web workspace now self-declares TypeScript and separates browser production checking from test type checking: `tsconfig.json` has only Vite browser types and excludes test files, while `tsconfig.test.json` includes test globals and is run before Vitest.

## Files

- `.gitignore`, `package.json`, `package-lock.json`, `contracts/tsconfig.json`
- `apps/web/package.json`, `apps/web/tsconfig.json`, `apps/web/vite.config.ts`, `apps/web/index.html`
- `apps/web/src/App.tsx`, `i18n.ts`, `useHealth.ts`, `contracts.ts`, `main.tsx`, `styles.css`, `setupTests.ts`, `App.test.tsx`
- `docs/superpowers/plans/2026-07-21-phase-0-foundation-and-contracts.md`
- `.superpowers/sdd/task-6-report.md`

## Concerns

- The API liveness route is the only live integration by design. Data health, strategy outcomes, paper trading, and AI state remain future phases rather than static placeholders.
- Vitest prints an informational i18next/Locize sponsorship line in the test runner; it is third-party stdout, not a warning or test failure.
