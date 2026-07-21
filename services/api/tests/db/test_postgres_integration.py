import asyncio
import hashlib
import json
import os
import runpy
import subprocess
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import UUID, uuid4

import pytest
from sqlalchemy import func, inspect, select, text, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from crypto_research.config import Settings
from crypto_research.contracts.data import EligibilityReasonCode, SymbolDataStatus
from crypto_research.contracts.manifest import (
    ArchiveCadence,
    ArchiveDataset,
    BinanceArchiveSource,
    BinanceRestEndpoint,
    BinanceRestSource,
    DataManifest,
    DataType,
    DeduplicationMethod,
    MissingInterval,
    ValidationState,
)
from crypto_research.contracts.strategy import InstrumentRef
from crypto_research.db.models import (
    AuditEventRow,
    BackfillObjectRow,
    DataGapRow,
    DataManifestRow,
    DataPartitionRow,
    IngestionJobRow,
    LiveDataPartitionRow,
    SourceObjectRow,
    SymbolRow,
)
from crypto_research.db.repositories import (
    AddSymbolCommand,
    ApprovedPartitionEvidenceConflict,
    ApprovedPartitionEvidenceNotFound,
    BackfillCommand,
    GapRecord,
    MutationIdentityConflict,
    SqlAlchemyApprovedCoverageResolver,
    SqlAlchemyDataStateRepository,
    StreamState,
)
from crypto_research.market.backfill import (
    BackfillObject,
    BackfillRunner,
    BackfillState,
    DownloadEvidence,
    NormalizeEvidence,
    PublishEvidence,
)
from crypto_research.market.binance.streams import streams_for_symbols
from crypto_research.market.catalog import (
    CatalogCandidate,
    SqlAlchemyCatalogRepository,
)
from crypto_research.market.control import MarketDataControlService
from crypto_research.market.live_catalog import (
    LiveCatalogError,
    SqlAlchemyLiveCatalogRepository,
)
from crypto_research.market.live_storage import LiveWriteResult, StoredLivePartition
from crypto_research.market.profile import ProfileMetric, SymbolProfile

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
                "data_manifests",
                "live_data_partitions",
                "backfill_objects",
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

                live_result = LiveWriteResult(
                    raw=(
                        _live_part(
                            "raw",
                            "a" * 64,
                            "ndjson/binance-stream-event-v1",
                            ("source_event_time", "source_id", "event_key"),
                            ("event_key",),
                        ),
                    ),
                    normalized=(
                        _live_part(
                            "normalized",
                            "b" * 64,
                            "parquet/binance-aggregate-trade-v1",
                            ("aggregate_trade_id",),
                            ("aggregate_trade_id",),
                        ),
                    ),
                    batch_id="live-batch-1",
                )
                live_catalog = SqlAlchemyLiveCatalogRepository(session)
                first_live = await live_catalog.register_batch(live_result)
                retried_live = await live_catalog.register_batch(live_result)
                await session.commit()
                assert [row.id for row in retried_live] == [
                    row.id for row in first_live
                ]
                assert await session.scalar(
                    select(func.count()).select_from(LiveDataPartitionRow)
                ) == 2
                original_normalized = live_result.normalized[0]
                conflicting_normalized = _live_part(
                    original_normalized.layer,
                    "c" * 64,
                    original_normalized.schema_name,
                    original_normalized.sort_keys,
                    original_normalized.unique_keys,
                )
                conflicting_live = LiveWriteResult(
                    raw=live_result.raw,
                    normalized=(conflicting_normalized,),
                    batch_id=live_result.batch_id,
                )
                with pytest.raises(LiveCatalogError, match="immutable"):
                    await live_catalog.register_batch(conflicting_live)
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

            concurrent_first = LiveWriteResult(
                raw=(
                    _live_part(
                        "raw",
                        "d" * 64,
                        "ndjson/binance-stream-event-v1",
                        ("source_event_time", "source_id", "event_key"),
                        ("event_key",),
                    ),
                ),
                normalized=(
                    _live_part(
                        "normalized",
                        "e" * 64,
                        "parquet/binance-aggregate-trade-v1",
                        ("aggregate_trade_id",),
                        ("aggregate_trade_id",),
                    ),
                ),
                batch_id="live-batch-concurrent",
            )
            concurrent_conflict = LiveWriteResult(
                raw=(
                    _live_part(
                        "raw",
                        "f" * 64,
                        "ndjson/binance-stream-event-v1",
                        ("source_event_time", "source_id", "event_key"),
                        ("event_key",),
                    ),
                ),
                normalized=(
                    _live_part(
                        "normalized",
                        "0" * 64,
                        "parquet/binance-aggregate-trade-v1",
                        ("aggregate_trade_id",),
                        ("aggregate_trade_id",),
                    ),
                ),
                batch_id=concurrent_first.batch_id,
            )
            first_live_session = session_factory()
            second_live_session = session_factory()
            conflicting_task = None
            try:
                await SqlAlchemyLiveCatalogRepository(
                    first_live_session
                ).register_batch(concurrent_first)
                conflicting_task = asyncio.create_task(
                    SqlAlchemyLiveCatalogRepository(
                        second_live_session
                    ).register_batch(concurrent_conflict)
                )
                await asyncio.sleep(0.05)
                assert not conflicting_task.done()

                await first_live_session.commit()
                with pytest.raises(LiveCatalogError, match="immutable"):
                    await asyncio.wait_for(conflicting_task, timeout=5)
                await second_live_session.rollback()
            finally:
                if conflicting_task is not None:
                    if not conflicting_task.done():
                        conflicting_task.cancel()
                    await asyncio.gather(conflicting_task, return_exceptions=True)
                await first_live_session.close()
                await second_live_session.close()

            async with session_factory() as verification_session:
                concurrent_rows = (
                    await verification_session.execute(
                        select(LiveDataPartitionRow).where(
                            LiveDataPartitionRow.batch_id
                            == concurrent_first.batch_id
                        )
                    )
                ).scalars().all()
                assert {row.checksum_sha256 for row in concurrent_rows} == {
                    "d" * 64,
                    "e" * 64,
                }

            lease_start = datetime(2026, 1, 3, tzinfo=UTC)
            async with session_factory() as setup_session:
                repository = SqlAlchemyDataStateRepository(setup_session)
                await repository.create_backfill(
                    BackfillCommand(
                        id="00000000-0000-0000-0000-000000000107",
                        symbol="BTCUSDT",
                        dataset="kline_1m",
                        requested_start=lease_start,
                        requested_end=lease_start.replace(day=4),
                    )
                )
                await repository.plan(
                    BackfillObject(
                        object_id="00000000-0000-0000-0000-000000000108",
                        job_id="00000000-0000-0000-0000-000000000107",
                        source_url="https://data.binance.vision/lease.zip",
                        start=lease_start,
                        end=lease_start.replace(day=4),
                    )
                )
                await setup_session.commit()

            first_session = session_factory()
            second_session = session_factory()
            try:
                first_repository = SqlAlchemyDataStateRepository(first_session)
                second_repository = SqlAlchemyDataStateRepository(second_session)
                first = await first_repository.claim(
                    "worker-a", lease_start, timedelta(minutes=1)
                )
                assert first is not None
                assert await second_repository.claim(
                    "worker-b", lease_start, timedelta(minutes=1)
                ) is None
                await first_session.commit()
                await second_session.rollback()
                async with session_factory() as expiry_session:
                    await expiry_session.execute(
                        update(BackfillObjectRow)
                        .where(
                            BackfillObjectRow.id
                            == "00000000-0000-0000-0000-000000000108"
                        )
                        .values(lease_expires_at=func.now() - timedelta(seconds=1))
                    )
                    await expiry_session.commit()
                restarted = await second_repository.claim(
                    "worker-b", lease_start + timedelta(minutes=2), timedelta(minutes=1)
                )
                assert restarted is not None
                assert restarted.attempt_count == 2
                await second_session.commit()
            finally:
                await first_session.close()
                await second_session.close()

            async with session_factory() as catalog_session:
                catalog = SqlAlchemyCatalogRepository(catalog_session)
                first_manifest = _manifest(
                    "00000000-0000-0000-0000-000000000109",
                    lease_start,
                    "c" * 64,
                    "d" * 64,
                    "normalized/btc-c.parquet",
                )
                first = await catalog.approve(
                    CatalogCandidate(first_manifest, _validations())
                )
                retry = await catalog.approve(
                    CatalogCandidate(first_manifest, _validations())
                )
                replacement = await catalog.approve(
                    CatalogCandidate(
                        _manifest(
                            "00000000-0000-0000-0000-000000000111",
                            lease_start,
                            "e" * 64,
                            "f" * 64,
                            "normalized/btc-e.parquet",
                        ),
                        _validations(),
                    )
                )
                reversion = await catalog.approve(
                    CatalogCandidate(
                        _manifest(
                            "00000000-0000-0000-0000-000000000110",
                            lease_start,
                            "c" * 64,
                            "d" * 64,
                            "normalized/btc-c.parquet",
                        ),
                        _validations(),
                    )
                )
                state_repository = SqlAlchemyDataStateRepository(catalog_session)
                repair_gap = await state_repository.record_gap(
                    GapRecord(
                        id="00000000-0000-0000-0000-000000000112",
                        symbol="BTCUSDT",
                        dataset="kline_1m",
                        start_at=lease_start,
                        end_at=lease_start.replace(day=4),
                        reason="partition_coverage",
                    )
                )
                repaired_gap = await state_repository.reconcile_gap(
                    repair_gap.id,
                    (reversion.partition_id,),
                    lease_start.replace(day=5),
                    "catalog",
                )
                fabricated_gap = await state_repository.record_gap(
                    GapRecord(
                        id="00000000-0000-0000-0000-000000000113",
                        symbol="BTCUSDT",
                        dataset="kline_1m",
                        start_at=lease_start,
                        end_at=lease_start.replace(day=4),
                        reason="partition_coverage",
                    )
                )
                with pytest.raises(ApprovedPartitionEvidenceNotFound):
                    await state_repository.reconcile_gap(
                        fabricated_gap.id,
                        ("00000000-0000-0000-0000-000000999999",),
                        lease_start.replace(day=5),
                        "catalog",
                    )
                await catalog_session.commit()
                assert retry.partition_id == first.partition_id
                assert replacement.version == first.version + 1
                assert reversion.version == replacement.version + 1
                assert repaired_gap.status == "repaired"
                assert fabricated_gap.status == "open"
                approved = await catalog.approved(
                    "BTCUSDT", DataType.KLINE_1M, lease_start, lease_start.replace(day=4)
                )
                assert [item.version for item in approved] == [
                    first.version,
                    replacement.version,
                    reversion.version,
                ]

            publish_start = datetime(2026, 1, 4, tzinfo=UTC)
            async with session_factory() as setup_session:
                setup_repository = SqlAlchemyDataStateRepository(setup_session)
                await setup_repository.create_backfill(
                    BackfillCommand(
                        id="00000000-0000-0000-0000-000000000201",
                        symbol="BTCUSDT",
                        dataset="kline_1m",
                        requested_start=publish_start,
                        requested_end=publish_start.replace(day=5),
                    )
                )
                await setup_repository.plan(
                    BackfillObject(
                        object_id="00000000-0000-0000-0000-000000000202",
                        job_id="00000000-0000-0000-0000-000000000201",
                        source_url=(
                            "https://data.binance.vision/data/futures/um/daily/klines/"
                            "BTCUSDT/1m/BTCUSDT-1m-2026-01-04.zip"
                        ),
                        start=publish_start,
                        end=publish_start.replace(day=5),
                    )
                )
                await setup_session.commit()

            main_session = session_factory()
            heartbeat_session = session_factory()
            try:
                main_repository = SqlAlchemyDataStateRepository(main_session)
                heartbeat_repository = SqlAlchemyDataStateRepository(heartbeat_session)
                publish_catalog = SqlAlchemyCatalogRepository(main_session)

                class SlowPublishStages:
                    async def download(self, _work: BackfillObject) -> DownloadEvidence:
                        await asyncio.sleep(0.025)
                        return DownloadEvidence("1" * 64, "raw/1.zip")

                    async def normalize(self, _work: BackfillObject) -> NormalizeEvidence:
                        await asyncio.sleep(0.025)
                        return NormalizeEvidence(
                            "normalized/publish-heartbeat.parquet", "2" * 64, 2
                        )

                    async def validate(self, _work: BackfillObject) -> dict[str, bool]:
                        await asyncio.sleep(0.025)
                        return _validations()

                    async def publish(self, _work: BackfillObject) -> PublishEvidence:
                        approved = await publish_catalog.approve(
                            CatalogCandidate(
                                _manifest(
                                    "00000000-0000-0000-0000-000000000203",
                                    publish_start,
                                    "1" * 64,
                                    "2" * 64,
                                    "normalized/publish-heartbeat.parquet",
                                ),
                                _validations(),
                            )
                        )
                        await asyncio.sleep(0.08)
                        return PublishEvidence(
                            approved.partition_id, approved.manifest_id
                        )

                completed = await BackfillRunner(
                    main_repository,
                    SlowPublishStages(),
                    clock=lambda: datetime.now(UTC),
                    heartbeat_repository=heartbeat_repository,
                ).run_once("worker-publish", timedelta(milliseconds=100))
                assert completed is not None
                assert completed.state is BackfillState.CATALOG_APPROVED
            finally:
                await main_session.close()
                await heartbeat_session.close()
        finally:
            await engine.dispose()

    asyncio.run(scenario())


def test_postgres_task6_control_invariants() -> None:
    _upgrade_test_database()

    async def scenario() -> None:
        engine = create_async_engine(TEST_DATABASE_URL)
        session_factory = async_sessionmaker(engine, expire_on_commit=False)
        suffix = uuid4().hex[:8].upper()
        symbol = f"T6{suffix}USDT"
        coverage_symbol = f"C6{suffix}USDT"
        start = datetime(2026, 4, 1, tzinfo=UTC)
        end = start + timedelta(days=2)
        command = AddSymbolCommand(symbol, start, end)
        try:
            first_session = session_factory()
            second_session = session_factory()
            try:
                first_repository = SqlAlchemyDataStateRepository(first_session)
                second_repository = SqlAlchemyDataStateRepository(second_session)
                await first_repository.add_symbol(command)
                second_task = asyncio.create_task(
                    second_repository.add_symbol(command)
                )
                await asyncio.sleep(0.05)
                assert not second_task.done()
                await first_session.commit()
                assert (await asyncio.wait_for(second_task, timeout=5)).symbol == symbol
                await second_session.commit()
            finally:
                await first_session.close()
                await second_session.close()

            async with session_factory() as verification:
                assert await verification.scalar(
                    select(func.count())
                    .select_from(SymbolRow)
                    .where(SymbolRow.symbol == symbol)
                ) == 1
                assert await _audit_count(verification, "symbol_added", symbol) == 1

            for enabled, action in (
                (False, "symbol_disabled"),
                (True, "symbol_enabled"),
            ):
                first_session = session_factory()
                second_session = session_factory()
                try:
                    first_repository = SqlAlchemyDataStateRepository(first_session)
                    second_repository = SqlAlchemyDataStateRepository(second_session)
                    await first_repository.set_symbol_enabled(symbol, enabled)
                    second_task = asyncio.create_task(
                        second_repository.set_symbol_enabled(symbol, enabled)
                    )
                    await asyncio.sleep(0.05)
                    assert not second_task.done()
                    await first_session.commit()
                    assert (
                        await asyncio.wait_for(second_task, timeout=5)
                    ).enabled is enabled
                    await second_session.commit()
                finally:
                    await first_session.close()
                    await second_session.close()
                async with session_factory() as verification:
                    assert await _audit_count(verification, action, symbol) == 1

            job_id = str(uuid4())
            job_command = BackfillCommand(
                id=job_id,
                symbol=symbol,
                dataset=DataType.KLINE_1M.value,
                requested_start=start,
                requested_end=end,
            )
            first_session = session_factory()
            second_session = session_factory()
            try:
                first_repository = SqlAlchemyDataStateRepository(first_session)
                second_repository = SqlAlchemyDataStateRepository(second_session)
                await first_repository.create_backfill(job_command)
                second_task = asyncio.create_task(
                    second_repository.create_backfill(job_command)
                )
                await asyncio.sleep(0.05)
                assert not second_task.done()
                await first_session.commit()
                assert (await asyncio.wait_for(second_task, timeout=5)).id == job_id
                await second_session.commit()
            finally:
                await first_session.close()
                await second_session.close()

            async with session_factory() as verification:
                assert await verification.scalar(
                    select(func.count())
                    .select_from(IngestionJobRow)
                    .where(IngestionJobRow.id == job_id)
                ) == 1
                assert await _audit_count(
                    verification, "backfill_created", job_id
                ) == 1

            object_id = str(uuid4())
            work = BackfillObject(
                object_id=object_id,
                job_id=job_id,
                source_url=f"https://data.binance.vision/{symbol}/status.zip",
                start=start,
                end=end,
            )
            first_session = session_factory()
            second_session = session_factory()
            try:
                first_repository = SqlAlchemyDataStateRepository(first_session)
                second_repository = SqlAlchemyDataStateRepository(second_session)
                await first_repository.plan(work)
                second_task = asyncio.create_task(second_repository.plan(work))
                await asyncio.sleep(0.05)
                assert not second_task.done()
                await first_session.commit()
                assert (
                    await asyncio.wait_for(second_task, timeout=5)
                ).object_id == object_id
                await second_session.commit()
            finally:
                await first_session.close()
                await second_session.close()

            async with session_factory() as status_session:
                await status_session.execute(
                    update(IngestionJobRow)
                    .where(IngestionJobRow.id == job_id)
                    .values(status="succeeded")
                )
                await status_session.execute(
                    update(BackfillObjectRow)
                    .where(BackfillObjectRow.id == object_id)
                    .values(state=BackfillState.FAILED.value)
                )
                await status_session.commit()
                repository = SqlAlchemyDataStateRepository(status_session)
                projected = await repository.get_backfill(job_id)
                assert projected is not None
                assert projected.status == "failed"
                assert await repository.count_failed_jobs() >= 1

            claim_job_id = str(uuid4())
            claim_object_id = str(uuid4())
            async with session_factory() as setup_session:
                repository = SqlAlchemyDataStateRepository(setup_session)
                await repository.create_backfill(
                    BackfillCommand(
                        id=claim_job_id,
                        symbol=symbol,
                        dataset=DataType.FUNDING.value,
                        requested_start=start,
                        requested_end=end,
                    )
                )
                await repository.plan(
                    BackfillObject(
                        object_id=claim_object_id,
                        job_id=claim_job_id,
                        source_url=(
                            f"https://data.binance.vision/{symbol}/claim.zip"
                        ),
                        start=start,
                        end=end,
                    )
                )
                await setup_session.execute(
                    update(BackfillObjectRow)
                    .where(BackfillObjectRow.id == claim_object_id)
                    .values(created_at=datetime(2000, 1, 1, tzinfo=UTC))
                )
                await repository.set_symbol_enabled(symbol, False)
                await setup_session.commit()

            async with session_factory() as disabled_session:
                repository = SqlAlchemyDataStateRepository(disabled_session)
                disabled_claim = await repository.claim(
                    "task6-worker", datetime.now(UTC), timedelta(minutes=1)
                )
                assert disabled_claim is None or disabled_claim.object_id != claim_object_id
                await disabled_session.rollback()

            async with session_factory() as enabled_session:
                repository = SqlAlchemyDataStateRepository(enabled_session)
                await repository.set_symbol_enabled(symbol, True)
                await enabled_session.commit()
                claimed = await repository.claim(
                    "task6-worker", datetime.now(UTC), timedelta(minutes=1)
                )
                assert claimed is not None
                assert claimed.object_id == claim_object_id
                assert claimed.attempt_count == 1
                await enabled_session.commit()

            async with session_factory() as coverage_session:
                repository = SqlAlchemyDataStateRepository(coverage_session)
                await repository.add_symbol(
                    AddSymbolCommand(coverage_symbol, start, end)
                )
                catalog = SqlAlchemyCatalogRepository(coverage_session)
                manifests: dict[tuple[DataType, int], DataManifest] = {}
                for data_type in (
                    DataType.KLINE_1M,
                    DataType.MARK_PRICE,
                    DataType.FUNDING,
                ):
                    for day in range(2):
                        item = _task6_manifest(
                            coverage_symbol,
                            data_type,
                            start + timedelta(days=day),
                        )
                        manifests[(data_type, day)] = item
                        if data_type is not DataType.MARK_PRICE or day == 0:
                            await catalog.approve(
                                CatalogCandidate(item, _validations())
                            )
                await coverage_session.commit()

                class Profiles:
                    async def compute(
                        self,
                        requested_symbol: str,
                        requested_start: datetime,
                        requested_end: datetime,
                        *,
                        calculated_at: datetime,
                    ) -> SymbolProfile:
                        metric = ProfileMetric(1.0, 10, 1.0)
                        return SymbolProfile(
                            symbol=requested_symbol,
                            calculated_at=calculated_at,
                            coverage_start=requested_start,
                            coverage_end=requested_end,
                            realized_volatility=metric,
                            jump_frequency=metric,
                            median_spread_bps=metric,
                            median_hourly_volume=metric,
                            funding_rate_mean=metric,
                        )

                control = MarketDataControlService(
                    repository,
                    Profiles(),
                    Settings(_env_file=None),
                    clock=lambda: datetime.now(UTC),
                )
                assert (
                    await control.get_symbol(coverage_symbol)
                ).data_status is not SymbolDataStatus.DATA_READY

                await catalog.approve(
                    CatalogCandidate(
                        manifests[(DataType.MARK_PRICE, 1)], _validations()
                    )
                )
                await coverage_session.commit()
                assert (
                    await control.get_symbol(coverage_symbol)
                ).data_status is SymbolDataStatus.DATA_READY

                expected_streams = streams_for_symbols((coverage_symbol,))
                for stream in expected_streams[:-1]:
                    await repository.update_stream(
                        StreamState(
                            coverage_symbol,
                            stream.name,
                            datetime.now(UTC),
                            "connected",
                            {"source_mode": "direct"},
                        )
                    )
                await coverage_session.commit()
                incomplete = await control.get_eligibility(coverage_symbol)
                assert EligibilityReasonCode.STALE_LIVE_DATA in incomplete.reason_codes
                assert EligibilityReasonCode.SOURCE_DEGRADED in incomplete.reason_codes

                last_stream = expected_streams[-1]
                await repository.update_stream(
                    StreamState(
                        coverage_symbol,
                        last_stream.name,
                        datetime.now(UTC),
                        "connected",
                        {"source_mode": "direct"},
                    )
                )
                await coverage_session.commit()
                complete = await control.get_eligibility(coverage_symbol)
                assert EligibilityReasonCode.STALE_LIVE_DATA not in complete.reason_codes
                assert EligibilityReasonCode.SOURCE_DEGRADED not in complete.reason_codes

            async with session_factory() as conflict_setup:
                conflict_symbol = f"X6{suffix}USDT"
                first = SqlAlchemyDataStateRepository(conflict_setup)
                await first.add_symbol(
                    AddSymbolCommand(conflict_symbol, start, end)
                )
                await conflict_setup.commit()
            async with session_factory() as conflict_session:
                with pytest.raises(MutationIdentityConflict, match="immutable"):
                    await SqlAlchemyDataStateRepository(conflict_session).add_symbol(
                        AddSymbolCommand(
                            conflict_symbol, start + timedelta(days=1), end
                        )
                    )
                await conflict_session.rollback()
        finally:
            await engine.dispose()

    asyncio.run(scenario())


def test_postgres_retry_and_recheck_are_concurrency_safe_and_immutable() -> None:
    _upgrade_test_database()

    async def scenario() -> None:
        engine = create_async_engine(TEST_DATABASE_URL)
        session_factory = async_sessionmaker(engine, expire_on_commit=False)
        suffix = uuid4().hex[:8].upper()
        symbol = f"R{suffix}USDT"
        start = datetime(2026, 3, 1, tzinfo=UTC)
        end = start + timedelta(days=1)
        retry_job_id = str(uuid4())
        retry_object_id = str(uuid4())
        approved_job_id = str(uuid4())
        approved_object_id = str(uuid4())
        try:
            async with session_factory() as setup:
                repository = SqlAlchemyDataStateRepository(setup)
                await repository.add_symbol(AddSymbolCommand(symbol, start, end))
                await repository.create_backfill(
                    BackfillCommand(retry_job_id, symbol, "kline_1m", start, end)
                )
                await repository.plan(
                    BackfillObject(
                        retry_object_id,
                        retry_job_id,
                        f"https://data.binance.vision/{symbol}/retry.zip",
                        start,
                        end,
                    )
                )
                await setup.execute(
                    update(BackfillObjectRow)
                    .where(BackfillObjectRow.id == retry_object_id)
                    .values(state=BackfillState.SOURCE_PENDING.value)
                )
                await repository.create_backfill(
                    BackfillCommand(approved_job_id, symbol, "kline_1m", start, end)
                )
                manifest = _task6_manifest(symbol, DataType.KLINE_1M, start)
                approved = await SqlAlchemyCatalogRepository(setup).approve(
                    CatalogCandidate(manifest, _validations())
                )
                await repository.plan(
                    BackfillObject(
                        approved_object_id,
                        approved_job_id,
                        manifest.source.resolved_url,
                        start,
                        end,
                    )
                )
                await setup.execute(
                    update(BackfillObjectRow)
                    .where(BackfillObjectRow.id == approved_object_id)
                    .values(
                        state=BackfillState.CATALOG_APPROVED.value,
                        source_checksum=manifest.source_checksum,
                        raw_path=manifest.raw_path,
                        normalized_path=manifest.normalized_path,
                        normalized_checksum=manifest.normalized_checksum,
                        row_count=manifest.row_count,
                        partition_id=approved.partition_id,
                        manifest_id=approved.manifest_id,
                    )
                )
                summary = await repository.get_symbol_summary(symbol)
                assert "source_pending" in summary.job_statuses
                await setup.commit()

            first = session_factory()
            second = session_factory()
            try:
                first_repository = SqlAlchemyDataStateRepository(first)
                second_repository = SqlAlchemyDataStateRepository(second)
                await first_repository.retry_backfill_job(retry_job_id, end)
                retry_task = asyncio.create_task(
                    second_repository.retry_backfill_job(retry_job_id, end)
                )
                await asyncio.sleep(0.05)
                assert not retry_task.done()
                await first.commit()
                assert (await asyncio.wait_for(retry_task, timeout=5)).status == "queued"
                await second.commit()
            finally:
                await first.close()
                await second.close()

            replacement_checksum = "f" * 64
            async with session_factory() as conflicts:
                repository = SqlAlchemyDataStateRepository(conflicts)
                with pytest.raises(MutationIdentityConflict, match="approved"):
                    await repository.retry_backfill_job(approved_job_id, end)
                await conflicts.rollback()
                await conflicts.execute(
                    update(IngestionJobRow)
                    .where(IngestionJobRow.id == retry_job_id)
                    .values(status="cancelled")
                )
                with pytest.raises(MutationIdentityConflict, match="cancelled"):
                    await repository.retry_backfill_job(retry_job_id, end)
                await conflicts.rollback()

            first = session_factory()
            second = session_factory()
            try:
                first_repository = SqlAlchemyDataStateRepository(first)
                second_repository = SqlAlchemyDataStateRepository(second)
                first_decision = await first_repository.register_archive_recheck(
                    job_id=approved_job_id,
                    object_id=approved_object_id,
                    partition_id=approved.partition_id,
                    observed_checksum=replacement_checksum,
                    checked_at=end,
                )
                recheck_task = asyncio.create_task(
                    second_repository.register_archive_recheck(
                        job_id=approved_job_id,
                        object_id=approved_object_id,
                        partition_id=approved.partition_id,
                        observed_checksum=replacement_checksum,
                        checked_at=end,
                    )
                )
                await asyncio.sleep(0.05)
                assert not recheck_task.done()
                await first.commit()
                second_decision = await asyncio.wait_for(recheck_task, timeout=5)
                await second.commit()
            finally:
                await first.close()
                await second.close()

            assert first_decision.created is True
            assert second_decision.created is False
            assert first_decision.replacement_job == second_decision.replacement_job
            assert first_decision.replacement_object == second_decision.replacement_object
            async with session_factory() as verification:
                original = await verification.get(BackfillObjectRow, approved_object_id)
                assert original is not None
                assert original.source_checksum == manifest.source_checksum
                assert original.partition_id == approved.partition_id
                assert await verification.scalar(
                    select(func.count())
                    .select_from(IngestionJobRow)
                    .where(IngestionJobRow.id == first_decision.replacement_job.id)
                ) == 1
                replacement_row = await verification.get(
                    BackfillObjectRow, first_decision.replacement_object.object_id
                )
                assert replacement_row is not None
                assert replacement_row.source_checksum == replacement_checksum
                assert replacement_row.state == BackfillState.PLANNED.value
        finally:
            await engine.dispose()

    asyncio.run(scenario())


def test_postgres_manifest_gap_migration_and_safe_repair_coverage() -> None:
    _upgrade_test_database()

    async def scenario() -> None:
        engine = create_async_engine(TEST_DATABASE_URL)
        session_factory = async_sessionmaker(engine, expire_on_commit=False)
        suffix = uuid4().hex[:8].upper()
        symbol = f"MG{suffix}USDT"
        migration_symbol = f"MA{suffix}USDT"
        start = datetime(2026, 2, 1, tzinfo=UTC)
        end = start + timedelta(hours=3)
        try:
            async with session_factory() as session:
                repository = SqlAlchemyDataStateRepository(session)
                await repository.add_symbol(AddSymbolCommand(symbol, start, end))
                await repository.add_symbol(
                    AddSymbolCommand(
                        migration_symbol,
                        start,
                        start + timedelta(days=1),
                        include_agg_trades=True,
                    )
                )
                catalog = SqlAlchemyCatalogRepository(session)
                repair_interval = MissingInterval(
                    start=start + timedelta(hours=1),
                    end=start + timedelta(hours=2),
                )
                manifest = _rest_manifest(
                    symbol,
                    DataType.KLINE_1M,
                    start,
                    end,
                    missing_intervals=(repair_interval,),
                )
                approved = await catalog.approve(
                    CatalogCandidate(manifest, _validations())
                )
                repair_gap = await repository.record_gap(
                    GapRecord(
                        id=str(uuid4()),
                        symbol=symbol,
                        dataset=DataType.KLINE_1M.value,
                        start_at=repair_interval.start,
                        end_at=repair_interval.end,
                        reason="missing_minute_open_time",
                    )
                )
                migration_manifest = _migration_archive_manifest(
                    migration_symbol,
                    DataType.KLINE_1M,
                    start,
                )
                await catalog.approve(
                    CatalogCandidate(migration_manifest, _validations())
                )
                for reason, gap_start, gap_end in (
                    (
                        "missing_minute_open_time",
                        start + timedelta(hours=4),
                        start + timedelta(hours=5),
                    ),
                    (
                        "worker_restart",
                        start + timedelta(minutes=5),
                        start + timedelta(minutes=10),
                    ),
                    (
                        "source_unknown_disconnect",
                        start + timedelta(minutes=10),
                        start + timedelta(minutes=15),
                    ),
                    (
                        "missing_minute_open_time",
                        start - timedelta(hours=1),
                        start,
                    ),
                ):
                    await repository.record_gap(
                        GapRecord(
                            id=str(uuid4()),
                            symbol=migration_symbol,
                            dataset=DataType.KLINE_1M.value,
                            start_at=gap_start,
                            end_at=gap_end,
                            reason=reason,
                        )
                    )

                aggregate_manifest = _migration_archive_manifest(
                    migration_symbol,
                    DataType.AGG_TRADE,
                    start,
                )
                aggregate_partition = await catalog.approve(
                    CatalogCandidate(aggregate_manifest, _validations())
                )
                aggregate_gap = MissingInterval(
                    start=start + timedelta(minutes=20),
                    end=start + timedelta(minutes=25),
                )
                await repository.record_gap(
                    GapRecord(
                        id=str(uuid4()),
                        symbol=migration_symbol,
                        dataset=DataType.AGG_TRADE.value,
                        start_at=aggregate_gap.start,
                        end_at=aggregate_gap.end,
                        reason="aggregate_trade_id_discontinuity",
                    )
                )
                await session.flush()

                stored_before = await session.get(
                    DataManifestRow, str(migration_manifest.manifest_id)
                )
                assert stored_before is not None
                identity_before = (
                    stored_before.partition_id,
                    stored_before.source_object_id,
                    stored_before.source_url,
                    stored_before.source_checksum,
                )
                migration = runpy.run_path(
                    API_ROOT
                    / "migrations/versions/20260722_0005_manifest_missing_intervals.py"
                )
                await session.execute(text(migration["BACKFILL_SQL"]))
                session.expire_all()

                stored_after = await session.get(
                    DataManifestRow, str(migration_manifest.manifest_id)
                )
                assert stored_after is not None
                assert (
                    stored_after.partition_id,
                    stored_after.source_object_id,
                    stored_after.source_url,
                    stored_after.source_checksum,
                ) == identity_before
                migrated = DataManifest.model_validate_json(
                    json.dumps(stored_after.manifest)
                )
                assert migrated.missing_intervals == (
                    MissingInterval(
                        start=start + timedelta(hours=4),
                        end=start + timedelta(hours=5),
                    ),
                )

                stored_aggregate = await session.get(
                    DataManifestRow, str(aggregate_manifest.manifest_id)
                )
                assert stored_aggregate is not None
                assert (
                    DataManifest.model_validate_json(
                        json.dumps(stored_aggregate.manifest)
                    ).missing_intervals
                    == (aggregate_gap,)
                )

                coverage = await SqlAlchemyApprovedCoverageResolver(session).resolve(
                    (approved.partition_id,),
                    symbol=symbol,
                    data_type=DataType.KLINE_1M,
                )
                assert [
                    (item.time_range.start, item.time_range.end) for item in coverage
                ] == [
                    (start, start + timedelta(hours=1)),
                    (start + timedelta(hours=2), end),
                ]

                still_open = await repository.reconcile_gap(
                    repair_gap.id,
                    (approved.partition_id,),
                    end + timedelta(hours=1),
                    "catalog",
                )
                assert still_open.status == "open"
                assert still_open.repair_details is not None
                assert still_open.repair_details["history"][-1]["result"] == "partial"
                assert aggregate_partition.partition_id != approved.partition_id

                with pytest.raises(RuntimeError, match="fail-closed"):
                    migration["downgrade"]()
                await session.rollback()
        finally:
            await engine.dispose()

    asyncio.run(scenario())


def test_postgres_gap_repair_requires_current_exact_coverage_version() -> None:
    _upgrade_test_database()

    async def scenario() -> None:
        engine = create_async_engine(TEST_DATABASE_URL)
        session_factory = async_sessionmaker(engine, expire_on_commit=False)
        suffix = uuid4().hex[:8].upper()
        symbol = f"CV{suffix}USDT"
        foreign_symbol = f"FV{suffix}USDT"
        start = datetime(2026, 3, 1, tzinfo=UTC)
        end = start + timedelta(hours=3)
        hole = MissingInterval(
            start=start + timedelta(hours=1),
            end=start + timedelta(hours=2),
        )

        def revision(
            base: DataManifest,
            label: str,
            *,
            missing_intervals: tuple[MissingInterval, ...],
        ) -> DataManifest:
            source_checksum = hashlib.sha256(f"source|{label}".encode()).hexdigest()
            normalized_checksum = hashlib.sha256(
                f"normalized|{label}".encode()
            ).hexdigest()
            return base.model_copy(
                update={
                    "manifest_id": uuid4(),
                    "raw_path": f"raw/{symbol}/{label}-{source_checksum}.json",
                    "normalized_path": (
                        f"normalized/{symbol}/{label}-{normalized_checksum}.parquet"
                    ),
                    "source_checksum": source_checksum,
                    "normalized_checksum": normalized_checksum,
                    "missing_intervals": missing_intervals,
                }
            )

        async def gap(
            repository: SqlAlchemyDataStateRepository,
            gap_start: datetime,
            gap_end: datetime,
        ):
            return await repository.record_gap(
                GapRecord(
                    id=str(uuid4()),
                    symbol=symbol,
                    dataset=DataType.KLINE_1M.value,
                    start_at=gap_start,
                    end_at=gap_end,
                    reason="missing_minute_open_time",
                )
            )

        try:
            async with session_factory() as session:
                repository = SqlAlchemyDataStateRepository(session)
                await repository.add_symbol(AddSymbolCommand(symbol, start, end))
                await repository.add_symbol(
                    AddSymbolCommand(foreign_symbol, start, end)
                )
                catalog = SqlAlchemyCatalogRepository(session)
                first_manifest = _rest_manifest(
                    symbol, DataType.KLINE_1M, start, end
                )
                first = await catalog.approve(
                    CatalogCandidate(first_manifest, _validations())
                )
                replay = await catalog.approve(
                    CatalogCandidate(first_manifest, _validations())
                )
                second_manifest = revision(
                    first_manifest, "current-hole", missing_intervals=(hole,)
                )
                second = await catalog.approve(
                    CatalogCandidate(second_manifest, _validations())
                )
                foreign = await catalog.approve(
                    CatalogCandidate(
                        _rest_manifest(
                            foreign_symbol, DataType.KLINE_1M, start, end
                        ),
                        _validations(),
                    )
                )

                with pytest.raises(ApprovedPartitionEvidenceConflict):
                    await SqlAlchemyApprovedCoverageResolver(session).resolve(
                        (first.partition_id, second.partition_id),
                        symbol=symbol,
                        data_type=DataType.KLINE_1M,
                    )

                complete_gap = await gap(repository, start, hole.start)
                hole_gap = await gap(repository, hole.start, hole.end)
                superseded_gap = await gap(repository, hole.start, hole.end)
                foreign_gap = await gap(repository, hole.start, hole.end)
                unknown_gap = await gap(repository, hole.start, hole.end)

                complete = await repository.reconcile_gap(
                    complete_gap.id,
                    (second.partition_id,),
                    end + timedelta(hours=1),
                    "catalog",
                )
                current_hole = await repository.reconcile_gap(
                    hole_gap.id,
                    (second.partition_id,),
                    end + timedelta(hours=2),
                    "catalog",
                )
                with pytest.raises(ApprovedPartitionEvidenceConflict):
                    await repository.reconcile_gap(
                        superseded_gap.id,
                        (first.partition_id,),
                        end + timedelta(hours=3),
                        "catalog",
                    )
                with pytest.raises(ApprovedPartitionEvidenceNotFound):
                    await repository.reconcile_gap(
                        foreign_gap.id,
                        (foreign.partition_id,),
                        end + timedelta(hours=4),
                        "catalog",
                    )
                unknown_id = str(uuid4())
                with pytest.raises(ApprovedPartitionEvidenceNotFound):
                    await repository.reconcile_gap(
                        unknown_gap.id,
                        (unknown_id,),
                        end + timedelta(hours=5),
                        "catalog",
                    )

                reverted_manifest = first_manifest.model_copy(
                    update={"manifest_id": uuid4()}
                )
                reverted = await catalog.approve(
                    CatalogCandidate(reverted_manifest, _validations())
                )
                reversion_gap = await gap(repository, hole.start, hole.end)
                reversion = await repository.reconcile_gap(
                    reversion_gap.id,
                    (reverted.partition_id,),
                    end + timedelta(hours=6),
                    "catalog",
                )
                await session.commit()

                assert replay.partition_id == first.partition_id
                assert [first.version, second.version, reverted.version] == [1, 2, 3]
                assert complete.status == "repaired"
                assert current_hole.status == "open"
                assert reversion.status == "repaired"

                expected_ids = {
                    complete_gap.id: [second.partition_id],
                    hole_gap.id: [second.partition_id],
                    reversion_gap.id: [reverted.partition_id],
                }
                for repaired_gap in (
                    complete,
                    current_hole,
                    reversion,
                ):
                    assert repaired_gap.repair_details is not None
                    assert repaired_gap.repair_details["history"][-1][
                        "partition_ids"
                    ] == expected_ids[repaired_gap.id]
                invalid_gap_ids = {
                    superseded_gap.id,
                    foreign_gap.id,
                    unknown_gap.id,
                }
                for invalid_gap_id in invalid_gap_ids:
                    invalid_gap = await session.get(DataGapRow, invalid_gap_id)
                    assert invalid_gap is not None
                    assert invalid_gap.repair_details == {"gap": {}, "history": []}

                all_gap_ids = set(expected_ids) | invalid_gap_ids
                audit_rows = (
                    await session.execute(
                        select(AuditEventRow).where(
                            AuditEventRow.subject_id.in_(all_gap_ids),
                            AuditEventRow.action.in_(
                                ("gap_repaired", "gap_repair_attempted")
                            ),
                        )
                    )
                ).scalars().all()
                assert {
                    row.subject_id: row.details["partition_ids"] for row in audit_rows
                } == expected_ids

            concurrent_start = end + timedelta(days=1)
            concurrent_end = concurrent_start + timedelta(hours=3)
            concurrent_hole = MissingInterval(
                start=concurrent_start + timedelta(hours=1),
                end=concurrent_start + timedelta(hours=2),
            )
            async with session_factory() as setup:
                setup_repository = SqlAlchemyDataStateRepository(setup)
                first_concurrent_manifest = _rest_manifest(
                    symbol,
                    DataType.KLINE_1M,
                    concurrent_start,
                    concurrent_end,
                )
                first_concurrent = await SqlAlchemyCatalogRepository(setup).approve(
                    CatalogCandidate(first_concurrent_manifest, _validations())
                )
                concurrent_gap = await gap(
                    setup_repository, concurrent_hole.start, concurrent_hole.end
                )
                await setup.commit()

            publisher = session_factory()
            reconciler = session_factory()
            reconcile_task: asyncio.Task | None = None
            try:
                current_concurrent_manifest = revision(
                    first_concurrent_manifest,
                    "concurrent-hole",
                    missing_intervals=(concurrent_hole,),
                )
                current_concurrent = await SqlAlchemyCatalogRepository(
                    publisher
                ).approve(
                    CatalogCandidate(current_concurrent_manifest, _validations())
                )
                reconcile_task = asyncio.create_task(
                    SqlAlchemyDataStateRepository(reconciler).reconcile_gap(
                        concurrent_gap.id,
                        (first_concurrent.partition_id,),
                        concurrent_end + timedelta(hours=1),
                        "catalog",
                    )
                )
                with pytest.raises(asyncio.TimeoutError):
                    await asyncio.wait_for(asyncio.shield(reconcile_task), timeout=0.25)
                await publisher.commit()
                with pytest.raises(ApprovedPartitionEvidenceConflict):
                    await asyncio.wait_for(reconcile_task, timeout=5)
                await reconciler.commit()

                assert current_concurrent.version == first_concurrent.version + 1
                concurrent_row = await reconciler.get(
                    DataGapRow, concurrent_gap.id, populate_existing=True
                )
                assert concurrent_row is not None
                assert concurrent_row.repair_details == {"gap": {}, "history": []}
                assert await _audit_count(
                    reconciler, "gap_repair_attempted", concurrent_gap.id
                ) == 0
            finally:
                if reconcile_task is not None and not reconcile_task.done():
                    reconcile_task.cancel()
                await publisher.rollback()
                await reconciler.rollback()
                await publisher.close()
                await reconciler.close()
        finally:
            await engine.dispose()

    asyncio.run(scenario())


async def _audit_count(session, action: str, subject_id: str) -> int:
    return int(
        (
            await session.scalar(
                select(func.count())
                .select_from(AuditEventRow)
                .where(
                    AuditEventRow.action == action,
                    AuditEventRow.subject_id == subject_id,
                )
            )
        )
        or 0
    )


def _rest_manifest(
    symbol: str,
    data_type: DataType,
    start: datetime,
    end: datetime,
    *,
    missing_intervals: tuple[MissingInterval, ...] = (),
) -> DataManifest:
    endpoint = {
        DataType.KLINE_1M: BinanceRestEndpoint.KLINES,
        DataType.AGG_TRADE: BinanceRestEndpoint.AGG_TRADES,
    }[data_type]
    identity = f"{symbol}|{data_type.value}|{start.isoformat()}|{end.isoformat()}"
    source_checksum = hashlib.sha256(f"source|{identity}".encode()).hexdigest()
    normalized_checksum = hashlib.sha256(
        f"normalized|{identity}".encode()
    ).hexdigest()
    start_ms = int(start.timestamp() * 1000)
    end_ms = int(end.timestamp() * 1000)
    return DataManifest(
        manifest_id=uuid4(),
        instrument=InstrumentRef(
            venue="BINANCE", market="USD_M_PERPETUAL", symbol=symbol
        ),
        data_type=data_type,
        start=start,
        end=end,
        retrieved_at=end + timedelta(days=1),
        schema_version="2.0.0",
        normalization_version="1.0.0",
        source=BinanceRestSource(
            kind="binance_rest",
            endpoint=endpoint,
            symbol=symbol,
            interval="1m" if data_type is DataType.KLINE_1M else None,
            start_time=start_ms,
            end_time=end_ms - 1,
            limit=180 if data_type is DataType.KLINE_1M else 1000,
        ),
        raw_path=f"raw/{symbol}/{data_type.value}/{source_checksum}.json",
        normalized_path=(
            f"normalized/{symbol}/{data_type.value}/{normalized_checksum}.parquet"
        ),
        source_checksum=source_checksum,
        normalized_checksum=normalized_checksum,
        row_count=2,
        validation_state=ValidationState.VALIDATED,
        primary_key_fields=("open_time",),
        deduplication_method=DeduplicationMethod.REJECT_DUPLICATES,
        duplicates_removed=0,
        missing_intervals=missing_intervals,
    )


def _migration_archive_manifest(
    symbol: str,
    data_type: DataType,
    start: datetime,
) -> DataManifest:
    archive_dataset = {
        DataType.KLINE_1M: ArchiveDataset.KLINES,
        DataType.AGG_TRADE: ArchiveDataset.AGG_TRADES,
    }[data_type]
    identity = f"migration|{symbol}|{data_type.value}|{start.isoformat()}"
    source_checksum = hashlib.sha256(f"source|{identity}".encode()).hexdigest()
    normalized_checksum = hashlib.sha256(
        f"normalized|{identity}".encode()
    ).hexdigest()
    end = start + timedelta(days=1)
    return DataManifest(
        manifest_id=uuid4(),
        instrument=InstrumentRef(
            venue="BINANCE", market="USD_M_PERPETUAL", symbol=symbol
        ),
        data_type=data_type,
        start=start,
        end=end,
        retrieved_at=end + timedelta(days=1),
        schema_version="2.0.0",
        normalization_version="1.0.0",
        source=BinanceArchiveSource(
            kind="binance_archive",
            cadence=ArchiveCadence.DAILY,
            dataset=archive_dataset,
            symbol=symbol,
            interval="1m" if data_type is DataType.KLINE_1M else None,
            period_start=start,
        ),
        raw_path=f"raw/{symbol}/{data_type.value}/{source_checksum}.zip",
        normalized_path=(
            f"normalized/{symbol}/{data_type.value}/{normalized_checksum}.parquet"
        ),
        source_checksum=source_checksum,
        normalized_checksum=normalized_checksum,
        row_count=2,
        validation_state=ValidationState.VALIDATED,
        primary_key_fields=(
            "open_time" if data_type is DataType.KLINE_1M else "aggregate_trade_id",
        ),
        deduplication_method=DeduplicationMethod.REJECT_DUPLICATES,
        duplicates_removed=0,
    )


def _task6_manifest(
    symbol: str, data_type: DataType, start: datetime
) -> DataManifest:
    archive_dataset = {
        DataType.KLINE_1M: ArchiveDataset.KLINES,
        DataType.MARK_PRICE: ArchiveDataset.MARK_PRICE_KLINES,
        DataType.FUNDING: ArchiveDataset.FUNDING_RATE,
    }[data_type]
    identity = f"{symbol}|{data_type.value}|{start.isoformat()}"
    source_checksum = hashlib.sha256(f"source|{identity}".encode()).hexdigest()
    normalized_checksum = hashlib.sha256(
        f"normalized|{identity}".encode()
    ).hexdigest()
    end = start + timedelta(days=1)
    return DataManifest(
        manifest_id=uuid4(),
        instrument=InstrumentRef(
            venue="BINANCE", market="USD_M_PERPETUAL", symbol=symbol
        ),
        data_type=data_type,
        start=start,
        end=end,
        retrieved_at=end + timedelta(days=1),
        schema_version="2.0.0",
        normalization_version="1.0.0",
        source=BinanceArchiveSource(
            kind="binance_archive",
            cadence=ArchiveCadence.DAILY,
            dataset=archive_dataset,
            symbol=symbol,
            interval=None if data_type is DataType.FUNDING else "1m",
            period_start=start,
        ),
        raw_path=f"raw/{symbol}/{data_type.value}/{source_checksum}.zip",
        normalized_path=(
            f"normalized/{symbol}/{data_type.value}/{normalized_checksum}.parquet"
        ),
        source_checksum=source_checksum,
        normalized_checksum=normalized_checksum,
        row_count=1,
        validation_state=ValidationState.VALIDATED,
        primary_key_fields=("event_time",),
        deduplication_method=DeduplicationMethod.REJECT_DUPLICATES,
        duplicates_removed=0,
    )


def _validations() -> dict[str, bool]:
    return {
        "checksum": True,
        "schema": True,
        "ordering": True,
        "uniqueness": True,
        "range": True,
        "row_count": True,
    }


def _live_part(
    layer: str,
    checksum: str,
    schema_name: str,
    sort_keys: tuple[str, ...],
    unique_keys: tuple[str, ...],
) -> StoredLivePartition:
    suffix = ".parquet" if layer == "normalized" else ".ndjson.gz"
    relative = (
        f"{layer}/binance/usdm/BTCUSDT/agg_trades/date=2026-01-01/"
        f"part-{checksum[:24]}{suffix}"
    )
    return StoredLivePartition(
        path=Path("/tmp") / relative,
        sha256=checksum,
        row_count=1,
        layer=layer,
        symbol="BTCUSDT",
        dataset="agg_trades",
        partition_date="2026-01-01",
        schema_name=schema_name,
        sort_keys=sort_keys,
        unique_keys=unique_keys,
        min_source_event_time=1,
        max_source_event_time=2,
        min_canonical_time=1,
        max_canonical_time=2,
        relative_path=relative,
    )


def _manifest(
    manifest_id: str,
    start: datetime,
    source_checksum: str,
    normalized_checksum: str,
    normalized_path: str,
) -> DataManifest:
    end = start.replace(day=start.day + 1)
    return DataManifest(
        manifest_id=UUID(manifest_id),
        instrument=InstrumentRef(
            venue="BINANCE", market="USD_M_PERPETUAL", symbol="BTCUSDT"
        ),
        data_type=DataType.KLINE_1M,
        start=start,
        end=end,
        retrieved_at=end + timedelta(days=1),
        schema_version="2.0.0",
        normalization_version="1.0.0",
        source=BinanceArchiveSource(
            kind="binance_archive",
            cadence=ArchiveCadence.DAILY,
            dataset=ArchiveDataset.KLINES,
            symbol="BTCUSDT",
            interval="1m",
            period_start=start,
        ),
        raw_path=f"raw/{source_checksum}.zip",
        normalized_path=normalized_path,
        source_checksum=source_checksum,
        normalized_checksum=normalized_checksum,
        row_count=2,
        validation_state=ValidationState.VALIDATED,
        primary_key_fields=("open_time",),
        deduplication_method=DeduplicationMethod.REJECT_DUPLICATES,
        duplicates_removed=0,
    )
