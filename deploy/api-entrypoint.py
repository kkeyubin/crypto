import os
import sys

from sqlalchemy import URL


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
    os.execvp(command[0], command)


if __name__ == "__main__":
    main(sys.argv[1:])
