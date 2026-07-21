import os
import subprocess
import sys
from pathlib import Path

from sqlalchemy import URL

MIGRATION_CONFIG = Path("/app/alembic.ini")
APPROVED_DATABASE_ENDPOINTS = {
    ("postgres", 5432),
    ("127.0.0.1", 55432),
}
APPROVED_RUNTIME_DATABASE_SETTINGS = {
    ("postgres", 5432, True),
    ("127.0.0.1", 55432, False),
}


def run_migrations() -> None:
    if not MIGRATION_CONFIG.exists():
        print(f"required Alembic configuration is missing: {MIGRATION_CONFIG}", file=sys.stderr)
        raise SystemExit(2)
    subprocess.run(["alembic", "-c", str(MIGRATION_CONFIG), "upgrade", "head"], check=True)


def database_endpoint(host: str, port_value: str) -> tuple[str, int]:
    try:
        port = int(port_value)
    except ValueError:
        port = -1
    endpoint = (host, port)
    if endpoint not in APPROVED_DATABASE_ENDPOINTS:
        print("database host/port is not an approved deployment endpoint", file=sys.stderr)
        raise SystemExit(2)
    return endpoint


def migration_enabled(value: str) -> bool:
    normalized = value.lower()
    if normalized == "true":
        return True
    if normalized == "false":
        return False
    print("CRYPTO_RUN_MIGRATIONS must be true or false", file=sys.stderr)
    raise SystemExit(2)


def main(command: list[str]) -> None:
    password = os.environ.pop("POSTGRES_PASSWORD", "")
    if not password:
        print("POSTGRES_PASSWORD must be set", file=sys.stderr)
        raise SystemExit(2)
    if not command:
        print("container command is required", file=sys.stderr)
        raise SystemExit(2)
    host, port = database_endpoint(
        os.environ.pop("CRYPTO_DATABASE_HOST", "postgres"),
        os.environ.pop("CRYPTO_DATABASE_PORT", "5432"),
    )
    should_migrate = migration_enabled(
        os.environ.pop("CRYPTO_RUN_MIGRATIONS", "true")
    )
    if (host, port, should_migrate) not in APPROVED_RUNTIME_DATABASE_SETTINGS:
        print("database endpoint and migration mode are not an approved pair", file=sys.stderr)
        raise SystemExit(2)

    database_url = URL.create(
        "postgresql+asyncpg",
        username="crypto",
        password=password,
        host=host,
        port=port,
        database="crypto_research",
    )
    os.environ["CRYPTO_DATABASE_URL"] = database_url.render_as_string(hide_password=False)
    if should_migrate:
        run_migrations()
    os.execvp(command[0], command)


if __name__ == "__main__":
    main(sys.argv[1:])
