import asyncio
import os
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path

import pytest
from sqlalchemy import inspect
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from crypto_research.db.models import DataPartitionRow, IngestionJobRow, SourceObjectRow, SymbolRow
from crypto_research.db.repositories import (
    AddSymbolCommand,
    BackfillCommand,
    SqlAlchemyDataStateRepository,
)

TEST_DATABASE_URL = os.environ.get("CRYPTO_TEST_DATABASE_URL")
if TEST_DATABASE_URL is None:
    pytest.skip(
        "set CRYPTO_TEST_DATABASE_URL to run PostgreSQL persistence integration tests",
        allow_module_level=True,
    )

API_ROOT = Path(__file__).resolve().parents[2]


def _upgrade_test_database() -> None:
    environment = os.environ | {"CRYPTO_DATABASE_URL": TEST_DATABASE_URL}
    subprocess.run(
        [sys.executable, "-m", "alembic", "-c", str(API_ROOT / "alembic.ini"), "upgrade", "head"],
        check=True,
        cwd=API_ROOT,
        env=environment,
    )


def test_postgres_persistence_invariants() -> None:
    _upgrade_test_database()

    async def scenario() -> None:
        engine = create_async_engine(TEST_DATABASE_URL)
        session_factory = async_sessionmaker(engine, expire_on_commit=False)
        try:
            async with engine.connect() as connection:
                table_names = await connection.run_sync(
                    lambda sync_connection: inspect(sync_connection).get_table_names()
                )
            assert {
                "symbols",
                "source_objects",
                "data_partitions",
                "ingestion_jobs",
                "stream_states",
            }.issubset(table_names)

            async with session_factory() as session:
                repository = SqlAlchemyDataStateRepository(session)
                await repository.add_symbol(
                    AddSymbolCommand(
                        "BTCUSDT",
                        datetime(2026, 1, 1, tzinfo=UTC),
                        datetime(2026, 1, 2, tzinfo=UTC),
                    )
                )
                await session.commit()

            async with session_factory() as session:
                stored_symbol = await session.get(SymbolRow, "BTCUSDT")
                assert stored_symbol is not None
                assert stored_symbol.history_start == datetime(2026, 1, 1, tzinfo=UTC)
                assert stored_symbol.history_start.tzinfo is not None
                assert stored_symbol.history_start.utcoffset() == UTC.utcoffset(None)

                repository = SqlAlchemyDataStateRepository(session)
                with pytest.raises(ValueError, match="UTC-aware"):
                    await repository.create_backfill(
                        BackfillCommand(
                            id="00000000-0000-0000-0000-000000000101",
                            symbol="BTCUSDT",
                            dataset="klines_1m",
                            requested_start=datetime(2026, 1, 1),
                        )
                    )
                assert (
                    await session.get(IngestionJobRow, "00000000-0000-0000-0000-000000000101")
                    is None
                )

                session.add(
                    SourceObjectRow(
                        id="00000000-0000-0000-0000-000000000102",
                        source_url="https://data.binance.vision/file.zip",
                        checksum_sha256="a" * 64,
                    )
                )
                await session.commit()

                session.add(
                    SourceObjectRow(
                        id="00000000-0000-0000-0000-000000000103",
                        source_url="https://data.binance.vision/file.zip",
                        checksum_sha256="a" * 64,
                    )
                )
                with pytest.raises(IntegrityError):
                    await session.flush()
                await session.rollback()

                session.add(
                    DataPartitionRow(
                        id="00000000-0000-0000-0000-000000000104",
                        symbol="BTCUSDT",
                        dataset="klines_1m",
                        partition_date="2026-01-01",
                        version=1,
                        checksum_sha256="b" * 64,
                        parquet_path="normalized/btc-1.parquet",
                        approval_status="approved",
                    )
                )
                await session.commit()

                session.add(
                    DataPartitionRow(
                        id="00000000-0000-0000-0000-000000000105",
                        symbol="BTCUSDT",
                        dataset="klines_1m",
                        partition_date="2026-01-01",
                        version=1,
                        checksum_sha256="c" * 64,
                        parquet_path="normalized/btc-duplicate.parquet",
                        approval_status="approved",
                    )
                )
                with pytest.raises(IntegrityError):
                    await session.flush()
                await session.rollback()

                session.add(
                    IngestionJobRow(
                        id="00000000-0000-0000-0000-000000000106",
                        symbol="BTCUSDT",
                        dataset="klines_1m",
                        status="not-a-job-state",
                    )
                )
                with pytest.raises(IntegrityError):
                    await session.flush()
                await session.rollback()
        finally:
            await engine.dispose()

    asyncio.run(scenario())
