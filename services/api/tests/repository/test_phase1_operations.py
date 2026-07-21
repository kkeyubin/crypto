from pathlib import Path

import yaml

REPOSITORY_ROOT = Path(__file__).resolve().parents[4]
FORBIDDEN_PROXY_KEYS = {
    "HTTP_PROXY",
    "HTTPS_PROXY",
    "ALL_PROXY",
    "http_proxy",
    "https_proxy",
    "all_proxy",
}
FORBIDDEN_EXCHANGE_KEY_FRAGMENTS = ("API_KEY", "API_SECRET", "BINANCE_KEY", "BINANCE_SECRET")


def read_text(relative_path: str) -> str:
    return (REPOSITORY_ROOT / relative_path).read_text()


def read_compose() -> dict:
    return yaml.safe_load(read_text("deploy/compose.yaml"))


def test_server_profile_supervises_worker_with_loopback_ports_and_shared_data() -> None:
    services = read_compose()["services"]

    assert set(services) == {"postgres", "api", "market-worker", "web"}
    assert services["postgres"]["ports"] == ["127.0.0.1:55432:5432"]
    assert services["web"]["ports"] == ["127.0.0.1:8088:80"]
    assert "ports" not in services["api"]

    worker = services["market-worker"]
    assert worker["profiles"] == ["server"]
    assert worker["network_mode"] == "host"
    assert worker["command"] == ["python", "-m", "crypto_research.market"]
    assert worker["restart"] == "unless-stopped"
    assert worker["depends_on"] == {
        "postgres": {"condition": "service_healthy"},
        "api": {"condition": "service_healthy"},
    }
    assert "healthcheck" in worker

    expected_mount = (
        "${CRYPTO_DATA_ROOT:-/srv/crypto-research/data}:/srv/crypto-research/data"
    )
    assert services["api"]["volumes"] == [expected_mount]
    assert worker["volumes"] == [expected_mount]


def test_api_and_worker_share_the_validated_nonroot_data_owner() -> None:
    services = read_compose()["services"]
    expected_user = "${CRYPTO_RUNTIME_UID:-1000}:${CRYPTO_RUNTIME_GID:-1000}"

    assert services["api"]["user"] == expected_user
    assert services["market-worker"]["user"] == expected_user

    example = read_text(".env.example")
    assert "CRYPTO_RUNTIME_UID=1000" in example
    assert "CRYPTO_RUNTIME_GID=1000" in example


def test_api_and_host_worker_use_only_validated_database_endpoints() -> None:
    services = read_compose()["services"]
    api_environment = services["api"]["environment"]
    worker_environment = services["market-worker"]["environment"]

    assert api_environment["CRYPTO_DATABASE_HOST"] == "postgres"
    assert api_environment["CRYPTO_DATABASE_PORT"] == "5432"
    assert api_environment["CRYPTO_RUN_MIGRATIONS"] == "true"
    assert worker_environment["CRYPTO_DATABASE_HOST"] == "127.0.0.1"
    assert worker_environment["CRYPTO_DATABASE_PORT"] == "55432"
    assert worker_environment["CRYPTO_RUN_MIGRATIONS"] == "false"
    assert "CRYPTO_DATABASE_URL" not in api_environment
    assert "CRYPTO_DATABASE_URL" not in worker_environment

    assert api_environment["POSTGRES_PASSWORD"] == "${POSTGRES_PASSWORD:?set POSTGRES_PASSWORD}"
    assert worker_environment["POSTGRES_PASSWORD"] == (
        "${POSTGRES_PASSWORD:?set POSTGRES_PASSWORD}"
    )


def test_worker_has_scoped_validated_proxy_and_no_exchange_credentials() -> None:
    services = read_compose()["services"]
    worker_environment = services["market-worker"]["environment"]

    assert worker_environment["CRYPTO_HTTP_PROXY_URL"] == "http://127.0.0.1:17891"
    assert worker_environment["CRYPTO_PROXY_MODE"] == "auto"

    for service in services.values():
        environment = service.get("environment", {})
        assert FORBIDDEN_PROXY_KEYS.isdisjoint(environment)
        assert not any(
            fragment in key.upper()
            for key in environment
            for fragment in FORBIDDEN_EXCHANGE_KEY_FRAGMENTS
        )


def test_image_ci_and_systemd_include_phase1_runtime_gates() -> None:
    dockerfile = read_text("deploy/api.Dockerfile")
    unit = read_text("deploy/crypto-research.service")
    workflow = yaml.safe_load(read_text(".github/workflows/ci.yml"))

    assert "COPY services/api/src ./src" in dockerfile
    assert "COPY deploy/market-worker-healthcheck.py" in dockerfile
    assert "--profile server" in unit

    backend_commands = [
        step["run"] for step in workflow["jobs"]["backend"]["steps"] if "run" in step
    ]
    container_commands = [
        step["run"] for step in workflow["jobs"]["containers"]["steps"] if "run" in step
    ]
    assert "alembic -c alembic.ini heads" in backend_commands
    assert any("import crypto_research.market.__main__" in command for command in backend_commands)
    assert any(
        "docker compose --profile server" in command and "config --quiet" in command
        for command in container_commands
    )
    assert any("run --rm api true" in command for command in container_commands)


def test_operations_docs_cover_phase1_recovery_and_keep_later_phases_closed() -> None:
    operations = read_text("docs/runbooks/binance-data-operations.md")
    recovery = read_text("docs/runbooks/market-data-recovery.md")
    readme = read_text("README.md")
    architecture = read_text("docs/architecture/system-overview.md")
    roadmap = read_text("docs/roadmap.md")
    agents = read_text("AGENTS.md")

    required_operations_terms = (
        "UTC",
        "366",
        ".CHECKSUM",
        "source_pending",
        "source replacement",
        "disconnect gap",
        "metadata_unverified",
        "127.0.0.1:55432",
        "127.0.0.1:8088",
        "ssh -N -L 8088:127.0.0.1:8088",
    )
    for term in required_operations_terms:
        assert term in operations
    assert "PostgreSQL catalog" in recovery
    assert "Parquet" in recovery
    assert "rollback" in recovery.lower()
    assert "--profile server" in readme
    assert "host network" in architecture.lower()
    assert "server acceptance" in roadmap.lower()
    assert "Do not use Claude for this project." in agents
    assert "Phase 2" in agents


def test_runbooks_parameterize_the_real_checkout_and_verify_private_backup_first() -> None:
    operations = read_text("docs/runbooks/binance-data-operations.md")
    recovery = read_text("docs/runbooks/market-data-recovery.md")

    for document in (operations, recovery):
        for variable in (
            "CRYPTO_CHECKOUT",
            "CRYPTO_ENV_FILE",
            "CRYPTO_DATA_ROOT",
        ):
            assert variable in document

    assert "CRYPTO_BACKUP_ROOT" in recovery
    assert 'install -m 0600 "$CRYPTO_ENV_FILE"' in recovery
    assert 'cmp --silent "$CRYPTO_ENV_FILE"' in recovery
    assert "pg_restore --list" in recovery
    assert 'tar -tf "$backup_root/data.tar"' in recovery
    assert 'sha256sum --check "$backup_root/backup.sha256"' in recovery
    assert 'touch "$backup_root/VERIFIED"' in recovery
    assert "Do not start an upgrade unless `VERIFIED` exists" in recovery
    assert '"$CRYPTO_DATA_ROOT/$raw_path"' in operations
    assert '"/srv/crypto-research/data/${raw_path}"' not in operations


def test_smoke_upgrade_uses_the_observed_install_checkout_env_and_data_layout() -> None:
    operations = read_text("docs/runbooks/binance-data-operations.md")
    recovery = read_text("docs/runbooks/market-data-recovery.md")
    readme = read_text("README.md")
    report = read_text(".superpowers/sdd/task-8-report.md")
    documents = (operations, recovery, readme, report)
    install_root = "/home/keyubin/crypto-research-phase0-smoke"
    checkout = f"{install_root}/repo"
    environment = f"{install_root}/config/runtime.env"
    data_root = f"{install_root}/data"

    for document in documents:
        assert install_root in document
        assert checkout in document
        assert environment in document
        assert data_root in document
        assert f"{install_root}/runtime.env" not in document
        assert 'CRYPTO_ENV_FILE="$CRYPTO_CHECKOUT/runtime.env"' not in document
        assert 'CRYPTO_DATA_ROOT="$CRYPTO_CHECKOUT/data"' not in document

    for document in (operations, recovery, readme):
        assert f"export CRYPTO_CHECKOUT={checkout}" in document
        assert f"export CRYPTO_CHECKOUT={install_root}\n" not in document


def test_runtime_docs_verify_configured_ids_match_the_data_root_owner() -> None:
    operations = read_text("docs/runbooks/binance-data-operations.md")
    recovery = read_text("docs/runbooks/market-data-recovery.md")
    readme = read_text("README.md")

    for document in (operations, recovery, readme):
        assert "CRYPTO_RUNTIME_UID" in document
        assert "CRYPTO_RUNTIME_GID" in document
        assert 'stat -c \'%u:%g\' "$CRYPTO_DATA_ROOT"' in document
        assert 'test "$data_owner" = "$runtime_uid:$runtime_gid"' in document

    assert 'install -d -m 0750 -o "$runtime_uid" -g "$runtime_gid"' in readme


def test_task8_report_records_the_sanitized_runtime_owner_failure_state() -> None:
    report = read_text(".superpowers/sdd/task-8-report.md")

    assert "/home/keyubin/crypto-research-phase0-smoke/data" in report
    assert "0750" in report
    assert "1000:1000" in report
    assert "data root owner does not match the effective user" in report
    assert "worker is stopped" in report
    assert "API, Web, and PostgreSQL remain healthy" in report
    assert "VERIFIED backup exists" in report


def test_acceptance_candidates_require_all_official_checksums_before_posts() -> None:
    operations = read_text("docs/runbooks/binance-data-operations.md")

    assert "preflight_archive_checksum" in operations
    assert "Do not POST any symbol or backfill request unless every probe succeeds" in operations
    assert "1000PEPEUSDT" in operations
    assert "2026-05-01T00:00:00Z" in operations
    assert "2026-06-01T00:00:00Z" in operations
    assert "2026-07-01T00:00:00Z" in operations
    assert "2026-06-14T00:00:00Z" in operations
    assert "2026-06-15T00:00:00Z" in operations
    assert '"symbol":"PEPEUSDT"' not in operations
    assert "2026-07-18T00:00:00Z" not in operations
    assert "2026-07-19T00:00:00Z" not in operations


def test_checksum_verification_aborts_on_query_download_hash_or_match_failure() -> None:
    operations = read_text("docs/runbooks/binance-data-operations.md")
    verification = operations.split("## Verify Downloaded Archive Checksums", 1)[1]

    assert "set -euo pipefail" in verification
    assert "catalog_rows=$(" in verification
    assert 'psql -v ON_ERROR_STOP=1' in verification
    assert 'test -n "$catalog_rows"' in verification
    assert 'done <<< "$catalog_rows"' in verification
    assert "done < <(" not in verification


def test_backup_discovers_and_stops_server_profile_services() -> None:
    recovery = read_text("docs/runbooks/market-data-recovery.md")
    backup = recovery.split("## Verified Backup Gate Before Upgrade", 1)[1].split(
        "## Detection and Automatic Response", 1
    )[0]

    assert "set -euo pipefail" in backup
    assert backup.count("--profile server") >= 5
    assert "--profile server config --services" in backup
    assert '--profile server stop "$service"' in backup


def test_phase1_recovery_does_not_claim_future_trading_or_notification_actions() -> None:
    recovery = read_text("docs/runbooks/market-data-recovery.md")

    for forbidden in (
        "strategy entries",
        "paper positions",
        "resume entries",
        "Paper-ledger",
        "paper entries",
        "Feishu",
    ):
        assert forbidden not in recovery
    assert "eligibility" in recovery
    assert "audit" in recovery.lower()
    assert "gap" in recovery.lower()
