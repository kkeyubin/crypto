import os
import subprocess
import sys
from pathlib import Path

REPOSITORY_ROOT = Path(__file__).resolve().parents[4]
ENTRYPOINT = REPOSITORY_ROOT / "deploy" / "api-entrypoint.py"


def test_entrypoint_percent_encodes_postgres_password_without_leaking_raw_environment() -> None:
    password = "a@b:c/d?e#f"
    child = """
import os
from sqlalchemy.engine import make_url
url = make_url(os.environ["CRYPTO_DATABASE_URL"])
print(os.environ.get("POSTGRES_PASSWORD", "absent"))
print(url.password)
print(url.host)
"""
    environment = os.environ | {"POSTGRES_PASSWORD": password}

    result = subprocess.run(
        [sys.executable, str(ENTRYPOINT), sys.executable, "-c", child],
        capture_output=True,
        check=True,
        env=environment,
        text=True,
    )

    assert result.stdout.splitlines() == ["absent", password, "postgres"]
    assert password not in result.stderr


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
