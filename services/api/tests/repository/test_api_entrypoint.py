import importlib.util
import os
import subprocess
import sys
from pathlib import Path

import pytest

REPOSITORY_ROOT = Path(__file__).resolve().parents[4]
ENTRYPOINT = REPOSITORY_ROOT / "deploy" / "api-entrypoint.py"


def load_entrypoint_module() -> object:
    specification = importlib.util.spec_from_file_location("api_entrypoint", ENTRYPOINT)
    assert specification is not None
    assert specification.loader is not None
    module = importlib.util.module_from_spec(specification)
    specification.loader.exec_module(module)
    return module


def test_entrypoint_percent_encodes_postgres_password_without_leaking_raw_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    password = "a@b:c/d?e#f"
    module = load_entrypoint_module()
    launched: list[tuple[str, list[str]]] = []

    monkeypatch.setenv("POSTGRES_PASSWORD", password)
    monkeypatch.setenv("CRYPTO_DATABASE_HOST", "postgres")
    monkeypatch.setenv("CRYPTO_DATABASE_PORT", "5432")
    monkeypatch.setenv("CRYPTO_RUN_MIGRATIONS", "true")
    monkeypatch.setattr(module, "run_migrations", lambda: None)
    monkeypatch.setattr(
        module.os,
        "execvp",
        lambda program, command: launched.append((program, command)),
    )

    module.main(["uvicorn", "crypto_research.api:app"])

    from sqlalchemy.engine import make_url

    database_url = make_url(os.environ["CRYPTO_DATABASE_URL"])
    assert os.environ.get("POSTGRES_PASSWORD") is None
    assert database_url.password == password
    assert database_url.host == "postgres"
    assert database_url.port == 5432
    assert launched == [("uvicorn", ["uvicorn", "crypto_research.api:app"])]


def test_worker_uses_loopback_host_database_without_running_migrations(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = load_entrypoint_module()
    launched: list[tuple[str, list[str]]] = []
    migrations: list[bool] = []
    monkeypatch.setenv("POSTGRES_PASSWORD", "worker-password")
    monkeypatch.setenv("CRYPTO_DATABASE_HOST", "127.0.0.1")
    monkeypatch.setenv("CRYPTO_DATABASE_PORT", "55432")
    monkeypatch.setenv("CRYPTO_RUN_MIGRATIONS", "false")
    monkeypatch.setattr(module, "run_migrations", lambda: migrations.append(True))
    monkeypatch.setattr(
        module.os,
        "execvp",
        lambda program, command: launched.append((program, command)),
    )

    module.main(["python", "-m", "crypto_research.market"])

    from sqlalchemy.engine import make_url

    database_url = make_url(os.environ["CRYPTO_DATABASE_URL"])
    assert database_url.host == "127.0.0.1"
    assert database_url.port == 55432
    assert migrations == []
    assert launched == [
        ("python", ["python", "-m", "crypto_research.market"]),
    ]


@pytest.mark.parametrize(
    ("host", "port", "run_migrations"),
    [
        ("database.example", "5432", "true"),
        ("postgres", "55432", "true"),
        ("postgres", "5432", "false"),
        ("127.0.0.1", "5432", "false"),
        ("127.0.0.1", "55432", "true"),
        ("127.0.0.1", "55432", "sometimes"),
    ],
)
def test_entrypoint_rejects_unapproved_database_or_migration_settings(
    monkeypatch: pytest.MonkeyPatch,
    host: str,
    port: str,
    run_migrations: str,
) -> None:
    module = load_entrypoint_module()
    monkeypatch.setenv("POSTGRES_PASSWORD", "must-not-appear")
    monkeypatch.setenv("CRYPTO_DATABASE_HOST", host)
    monkeypatch.setenv("CRYPTO_DATABASE_PORT", port)
    monkeypatch.setenv("CRYPTO_RUN_MIGRATIONS", run_migrations)
    monkeypatch.setattr(
        module.os,
        "execvp",
        lambda _program, _command: (_ for _ in ()).throw(
            AssertionError("invalid deployment configuration executed a command")
        ),
    )
    monkeypatch.setattr(module, "run_migrations", lambda: None)

    with pytest.raises(SystemExit) as error:
        module.main(["true"])

    assert error.value.code == 2


def test_entrypoint_fails_explicitly_without_password_or_command() -> None:
    without_password = subprocess.run(
        [sys.executable, str(ENTRYPOINT), sys.executable, "-c", "raise SystemExit(0)"],
        capture_output=True,
        env=os.environ | {"POSTGRES_PASSWORD": ""},
        text=True,
    )
    without_command = subprocess.run(
        [sys.executable, str(ENTRYPOINT)],
        capture_output=True,
        env=os.environ | {"POSTGRES_PASSWORD": "must-not-appear"},
        text=True,
    )

    assert without_password.returncode != 0
    assert "POSTGRES_PASSWORD" in without_password.stderr
    assert without_command.returncode != 0
    assert "command" in without_command.stderr.lower()
    assert "must-not-appear" not in without_command.stdout + without_command.stderr


def test_entrypoint_fails_explicitly_when_migration_config_is_missing(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    module = load_entrypoint_module()
    monkeypatch.setattr(module, "MIGRATION_CONFIG", Path("/missing/alembic.ini"))

    with pytest.raises(SystemExit) as error:
        module.run_migrations()

    assert error.value.code == 2
    assert "alembic.ini" in capsys.readouterr().err
