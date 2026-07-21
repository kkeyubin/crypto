# Task 7 Report: Reproducible Phase 0 Operations

## Scope

Implemented only Phase 0 operations scaffolding: Docker build definitions, Compose, Nginx proxying, systemd commands, CI gates, repository policy tests, and operator documentation. The reviewed commit was also smoke-tested on the personal server in an isolated user-owned directory; no existing service or systemd configuration was changed.

## RED

1. Added `services/api/tests/repository/test_no_source_books_tracked.py` before deployment code. It passed immediately (`1 passed`), as designed: the repository had no tracked `.pdf`, `.epub`, `.mobi`, or `.azw*` files before the permanent guard was introduced.
2. Added `test_operations_scaffold.py` before the operations files. It failed as expected with `FileNotFoundError` for the missing root `.dockerignore`. The test requires Docker definitions, loopback-only Compose, explicit systemd runtime env-file invocation, CI contract/schema/Web gates, README boundaries, and project-specific `AGENTS.md` guidance.

## GREEN

- Added root `.dockerignore` for Git/worktree/Superpowers state, `.env*` with `.env.example` re-included, source-book formats, data/reports/artifacts, dependencies, build output, virtual environments, and caches.
- Added API and workspace-compatible Web Dockerfiles. The Web build uses the root lockfile and checked generated contracts without host dependencies.
- Added Nginx `/api/` proxying and a Compose stack with PostgreSQL/API/Web health checks and dependencies; only `127.0.0.1:8088:80` is exposed.
- Added a systemd unit that explicitly uses `/srv/crypto-research/config/runtime.env` for both actions and an absolute compose-file path.
- Added CI gates for backend tests/Ruff/schema drift, contract generation safety/type checking/generated diff, and Web tests/build.
- Added exact local/server commands and Phase 0 boundaries to `README.md`; updated `AGENTS.md`; synchronized the Task 7 local checklist without changing the progress ledger.

## Pre-review local verification

Before the review hardening, repository-focused tests were `2 passed` and the full API suite was `63 passed`; Ruff, JSON Schema `--check`, Node `v24.15.0`/npm `11.12.1`, `npm ci` (178 packages, `0 vulnerabilities`), contract generation safety/type checks/no drift, Web tests `10 passed`, the production Web build, and `git diff --check` also passed.

Static deployment review found no exchange, credential, paper-broker, or order-service configuration and confirmed the sole port mapping is `127.0.0.1:8088:80`. `git ls-files '*.pdf' '*.epub' '*.mobi' '*.azw*'` returned no files.

## Docker and remote boundary

The local Docker CLI is unavailable, so no local containers were started and no existing local services were affected. Before the remote smoke run, read-only checks confirmed that the target directory, Compose project containers, and volume did not exist and that port `8088` was free. Because `keyubin` has no passwordless `sudo`, the smoke copy was installed under `/home/keyubin/crypto-research-phase0-smoke`; the formal `/srv/crypto-research` systemd installation remains an administrator action.

## Review hardening follow-up

The review identified three important gaps: PostgreSQL URI encoding had relied on unsafe Compose interpolation, the book guard depended on pytest's working directory, and operations checks were only token searches. New RED tests reproduced the absent entrypoint, the missing Compose project/CI container structure, and root plus nested source books in a temporary Git repository. GREEN adds a raw-password entrypoint using SQLAlchemy `URL.create`, a `git -C` book helper, structured PyYAML assertions, clean-image CI steps, and clarified operator/agent guidance. Local Docker remains unavailable; remote smoke evidence is recorded below.

The follow-up also excludes editable-install `*.egg-info/` metadata from Git and Docker contexts. README safety language now limits the ban on data/reports/artifacts to runtime outputs and explicitly preserves committed contracts and reviewed SDD reports.

## Post-review local verification

After the review fixes, `pip install -e '.[dev]'` confirmed PyYAML `6.0.3`; the focused repository suite passed `8 passed`; the full API suite passed `69 passed`; Ruff and JSON Schema `--check` passed. `npm ci` completed with 178 packages and no vulnerabilities; contract generation safety/type/no-drift checks, Web tests `10 passed`, and the production Web build passed. Docker CLI remains unavailable locally, so Compose config, clean Docker builds, and startup are remote-controller results rather than claimed local results.

## Remote smoke verification — 2026-07-21

The committed tree was transferred with `git archive` to the isolated server path. Runtime secrets were generated directly into a mode-`0600` env file and were not printed or committed. Compose configuration validation passed.

The first API image build could not resolve PyPI from Docker's default build network. Only after that access restriction appeared, the server's existing loopback proxy on port `17891` was supplied as a predefined build argument with host networking. API and Web image builds then passed; the proxy is absent from the image runtime environments and Compose configuration.

`docker compose up -d --no-build` started all three services. Controller verification returned:

- `postgres`, `api`, and `web`: `healthy`, each with restart policy `unless-stopped`;
- API PID 1: raw `POSTGRES_PASSWORD` absent and decoded `CRYPTO_DATABASE_URL` password present;
- Web mapping: `127.0.0.1:8088:80`, with no public bind;
- `GET /api/health/live`: `{"status":"ok","service":"api","version":"0.1.0"}`;
- `GET /api/health/ready`: `{"status":"ready","service":"api"}`;
- Web root: HTTP `200`.

The isolated smoke stack was deliberately left running with `unless-stopped`. Installing the documented `/srv/crypto-research` checkout and systemd unit still requires administrator approval and is not claimed here.
