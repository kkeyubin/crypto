from pathlib import Path

REPOSITORY_ROOT = Path(__file__).resolve().parents[4]


def test_phase_zero_operations_scaffold_is_reproducible() -> None:
    required_files = {
        ".dockerignore": [".git", ".worktrees", ".superpowers", ".env*", "!.env.example"],
        "deploy/api.Dockerfile": [
            "FROM python:3.12-slim",
            "pip install --no-cache-dir .",
            "uvicorn",
        ],
        "deploy/web.Dockerfile": [
            "FROM node:24-alpine AS build",
            "npm ci",
            "npm run web:build",
            "FROM nginx:1.27-alpine",
        ],
        "deploy/nginx.conf": [
            "location /api/",
            "proxy_pass http://api:8000",
            "try_files $uri /index.html",
        ],
        "deploy/compose.yaml": [
            "postgres:",
            "api:",
            "web:",
            '"127.0.0.1:8088:80"',
            "condition: service_healthy",
            "CRYPTO_SESSION_SECRET: ${CRYPTO_SESSION_SECRET:?set CRYPTO_SESSION_SECRET}",
        ],
        "deploy/crypto-research.service": [
            "WorkingDirectory=/srv/crypto-research/repo/deploy",
            "--env-file /srv/crypto-research/config/runtime.env",
        ],
        ".github/workflows/ci.yml": [
            "contracts:test-generation",
            "contracts:check-types",
            "export_schemas.py --check",
            "web:test -- --run",
            "web:build",
            "git diff --exit-code contracts",
        ],
        "README.md": [
            "No market ingestion, backtest execution, paper order service, or signal notifications",
            "docs/roadmap.md",
            "docker compose --env-file /srv/crypto-research/config/runtime.env",
        ],
        "AGENTS.md": [
            "services/api/src/crypto_research/",
            "apps/web/",
            "Do not use Claude for this project.",
        ],
    }

    for relative_path, expected_fragments in required_files.items():
        content = (REPOSITORY_ROOT / relative_path).read_text()
        for fragment in expected_fragments:
            assert fragment in content, f"{relative_path} is missing {fragment!r}"
