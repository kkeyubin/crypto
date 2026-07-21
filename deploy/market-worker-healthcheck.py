import asyncio
import os
from datetime import UTC, datetime, timedelta

import asyncpg

HEARTBEAT_MAX_AGE = timedelta(seconds=120)


def database_endpoint(host: str, port_value: str) -> tuple[str, int]:
    try:
        port = int(port_value)
    except ValueError:
        port = -1
    if (host, port) != ("127.0.0.1", 55432):
        raise ValueError("worker database endpoint must be 127.0.0.1:55432")
    return host, port


def heartbeat_is_healthy(
    status: str | None,
    heartbeat_at: datetime | None,
    now: datetime,
) -> bool:
    if status not in {"running", "degraded"} or heartbeat_at is None:
        return False
    if heartbeat_at.tzinfo is None:
        return False
    age = now - heartbeat_at.astimezone(UTC)
    return timedelta(0) <= age <= HEARTBEAT_MAX_AGE


async def check() -> bool:
    host, port = database_endpoint(
        os.environ.get("CRYPTO_DATABASE_HOST", ""),
        os.environ.get("CRYPTO_DATABASE_PORT", ""),
    )
    password = os.environ.get("POSTGRES_PASSWORD", "")
    worker_id = os.environ.get("CRYPTO_MARKET_WORKER_ID", "market-worker")
    if not password or not worker_id:
        return False
    connection = await asyncpg.connect(
        host=host,
        port=port,
        user="crypto",
        password=password,
        database="crypto_research",
        timeout=5,
        command_timeout=5,
    )
    try:
        row = await connection.fetchrow(
            "SELECT status, heartbeat_at FROM worker_heartbeats WHERE worker_id = $1",
            worker_id,
        )
    finally:
        await connection.close()
    return row is not None and heartbeat_is_healthy(
        row["status"], row["heartbeat_at"], datetime.now(UTC)
    )


def main() -> None:
    try:
        healthy = asyncio.run(check())
    except Exception:
        healthy = False
    raise SystemExit(0 if healthy else 1)


if __name__ == "__main__":
    main()
