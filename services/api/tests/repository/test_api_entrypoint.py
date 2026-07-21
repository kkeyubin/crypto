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
    assert launched == [("uvicorn", ["uvicorn", "crypto_research.api:app"])]


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
