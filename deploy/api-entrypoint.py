import os
import subprocess
import sys
from pathlib import Path

from sqlalchemy import URL

MIGRATION_CONFIG = Path("/app/alembic.ini")


def run_migrations() -> None:
    if not MIGRATION_CONFIG.exists():
        print(f"required Alembic configuration is missing: {MIGRATION_CONFIG}", file=sys.stderr)
        raise SystemExit(2)
    subprocess.run(["alembic", "-c", str(MIGRATION_CONFIG), "upgrade", "head"], check=True)


def main(command: list[str]) -> None:
    password = os.environ.pop("POSTGRES_PASSWORD", "")
    if not password:
        print("POSTGRES_PASSWORD must be set", file=sys.stderr)
        raise SystemExit(2)
    if not command:
        print("container command is required", file=sys.stderr)
        raise SystemExit(2)

    database_url = URL.create(
        "postgresql+asyncpg",
        username="crypto",
        password=password,
        host="postgres",
        port=5432,
        database="crypto_research",
    )
    os.environ["CRYPTO_DATABASE_URL"] = database_url.render_as_string(hide_password=False)
    run_migrations()
    os.execvp(command[0], command)


if __name__ == "__main__":
    main(sys.argv[1:])
