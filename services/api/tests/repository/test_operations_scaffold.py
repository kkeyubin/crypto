from pathlib import Path

import yaml

REPOSITORY_ROOT = Path(__file__).resolve().parents[4]
RUNTIME_ENV_FILE = "/srv/crypto-research/config/runtime.env"
COMPOSE_FILE = "/srv/crypto-research/repo/deploy/compose.yaml"


def read_repository_file(relative_path: str) -> str:
    return (REPOSITORY_ROOT / relative_path).read_text()


def test_compose_uses_exact_phase_zero_services_and_safe_database_password_handoff() -> None:
    compose = yaml.safe_load(read_repository_file("deploy/compose.yaml"))
    services = compose["services"]

    assert compose["name"] == "crypto-research"
    assert set(services) == {"postgres", "api", "web"}
    assert services["web"]["ports"] == ["127.0.0.1:8088:80"]
    assert "ports" not in services["postgres"]
    assert "ports" not in services["api"]
    assert services["api"]["environment"]["POSTGRES_PASSWORD"] == (
        "${POSTGRES_PASSWORD:?set POSTGRES_PASSWORD}"
    )
    assert "CRYPTO_DATABASE_URL" not in services["api"]["environment"]
    assert services["api"]["depends_on"] == {"postgres": {"condition": "service_healthy"}}
    assert services["web"]["depends_on"] == {"api": {"condition": "service_healthy"}}
    assert "healthcheck" in services["postgres"]
    assert "healthcheck" in services["api"]
    assert "healthcheck" in services["web"]


def test_container_files_use_entrypoint_and_exact_systemd_runtime_arguments() -> None:
    dockerfile = read_repository_file("deploy/api.Dockerfile")
    systemd_lines = read_repository_file("deploy/crypto-research.service").splitlines()

    assert "COPY deploy/api-entrypoint.py /app/deploy/api-entrypoint.py" in dockerfile
    assert 'ENTRYPOINT ["python", "/app/deploy/api-entrypoint.py"]' in dockerfile
    assert (
        "ExecStart=/usr/bin/docker compose "
        f"--env-file {RUNTIME_ENV_FILE} -f {COMPOSE_FILE} up -d --build"
    ) in systemd_lines
    assert (
        "ExecStop=/usr/bin/docker compose "
        f"--env-file {RUNTIME_ENV_FILE} -f {COMPOSE_FILE} down"
    ) in systemd_lines


def test_market_data_recovery_migration_preserves_api_entrypoint() -> None:
    runbook = read_repository_file("docs/runbooks/market-data-recovery.md")

    assert "-f compose.yaml run --rm api true" in runbook
    assert "--entrypoint" not in runbook


def test_ci_has_backend_web_and_clean_container_safety_gates() -> None:
    workflow = yaml.safe_load(read_repository_file(".github/workflows/ci.yml"))
    jobs = workflow["jobs"]
    backend_commands = [step["run"] for step in jobs["backend"]["steps"] if "run" in step]
    web_commands = [step["run"] for step in jobs["web"]["steps"] if "run" in step]
    container_commands = [step["run"] for step in jobs["containers"]["steps"] if "run" in step]

    assert "pytest -q" in backend_commands
    assert "python scripts/export_schemas.py --check" in backend_commands
    assert "npm run contracts:test-generation" in web_commands
    assert "npm run contracts:check-types" in web_commands
    assert "git diff --exit-code contracts" in web_commands
    assert "npm run web:test -- --run" in web_commands
    assert "npm run web:build" in web_commands
    assert jobs["containers"]["env"] == {
        "POSTGRES_PASSWORD": "ci-postgres-password",
        "CRYPTO_SESSION_SECRET": "ci-session-secret-that-is-at-least-32-characters",
    }
    assert container_commands == [
        "docker compose -f deploy/compose.yaml config --quiet",
        "docker build --no-cache -f deploy/api.Dockerfile .",
        "docker build --no-cache -f deploy/web.Dockerfile .",
    ]


def test_operator_docs_and_agent_policy_preserve_phase_zero_boundaries() -> None:
    gitignore = read_repository_file(".gitignore")
    dockerignore = read_repository_file(".dockerignore")
    readme = read_repository_file("README.md")
    agents = read_repository_file("AGENTS.md")

    for ignore_file in [gitignore, dockerignore]:
        assert "*.egg-info/" in ignore_file
        assert "**/*.egg-info/" in ignore_file
    assert all(fragment in dockerignore for fragment in [".git", ".env*", "!.env.example"])
    assert "git clone" in readme
    assert "cp /srv/crypto-research/repo/.env.example" in readme
    assert "openssl rand -hex" in readme
    assert (
        "No market ingestion, backtest execution, paper order service, or signal notifications"
        in readme
    )
    assert "runtime outputs" in readme
    assert "Committed contracts and reviewed SDD reports are exceptions" in readme
    assert "runtime data, reports, and artifacts" in agents
    assert "reviewed `.superpowers/sdd/*-report.md`" in agents.lower()
    assert "no numeric coverage threshold" in agents
    assert "Do not use Claude for this project." in agents
