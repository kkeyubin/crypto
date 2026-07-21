from __future__ import annotations

import asyncio
import signal
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path
from urllib.parse import urlparse

import httpx2
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from websockets.asyncio.client import connect as websocket_connect

from crypto_research.config import Settings, get_settings
from crypto_research.database import Database
from crypto_research.db.models import BackfillObjectRow
from crypto_research.db.repositories import (
    DataGap,
    GapRecord,
    SourceTransition,
    SqlAlchemyDataStateRepository,
    StreamState,
    SymbolState,
    WorkerHeartbeat,
)
from crypto_research.market.backfill import (
    ArchiveBackfillStages,
    BackfillRunner,
    BackfillState,
)
from crypto_research.market.backfill_planning import (
    DetectedGapRecorder,
    MissingArchiveGapRecorder,
)
from crypto_research.market.binance.archive_paths import (
    ArchiveObject,
    DatasetKind,
    plan_archives,
)
from crypto_research.market.catalog import SqlAlchemyCatalogRepository
from crypto_research.market.live_storage import LiveStorage
from crypto_research.market.worker import (
    ConnectionModeTransition,
    ConnectionPolicy,
    DirectConnectionFailure,
    MarketWorker,
    RoutedConnectionFactory,
    StreamConnectionSupervisor,
)

_ACTIVE_BACKFILL_STATES = tuple(
    state.value
    for state in (
        BackfillState.PLANNED,
        BackfillState.DOWNLOADING,
        BackfillState.CHECKSUM_VERIFIED,
        BackfillState.NORMALIZED,
        BackfillState.VALIDATED,
    )
)
_ARCHIVE_PATH_DATASETS = {
    "klines": DatasetKind.KLINES,
    "markPriceKlines": DatasetKind.MARK_PRICE_KLINES,
    "fundingRate": DatasetKind.FUNDING_RATE,
    "aggTrades": DatasetKind.AGG_TRADES,
}


class SessionWorkerRepository:
    """Give concurrent stream readers independent transaction-scoped sessions."""

    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self._session_factory = session_factory

    async def list_active_symbols(self) -> tuple[SymbolState, ...]:
        async with self._session_factory() as session, session.begin():
            return await SqlAlchemyDataStateRepository(session).list_active_symbols()

    async def update_stream(self, state: StreamState) -> None:
        async with self._session_factory() as session, session.begin():
            await SqlAlchemyDataStateRepository(session).update_stream(state)

    async def update_streams(self, states: tuple[StreamState, ...]) -> None:
        """Commit one durable stream-state checkpoint for an entire live batch."""
        async with self._session_factory() as session, session.begin():
            repository = SqlAlchemyDataStateRepository(session)
            for state in states:
                await repository.update_stream(state)

    async def record_gap(self, gap: GapRecord) -> DataGap:
        async with self._session_factory() as session, session.begin():
            return await SqlAlchemyDataStateRepository(session).record_gap(gap)

    async def update_worker_heartbeat(self, heartbeat: WorkerHeartbeat) -> None:
        async with self._session_factory() as session, session.begin():
            await SqlAlchemyDataStateRepository(session).update_worker_heartbeat(heartbeat)

    async def record_source_transition(self, transition: SourceTransition) -> None:
        async with self._session_factory() as session, session.begin():
            await SqlAlchemyDataStateRepository(session).record_source_transition(transition)

    async def checkpoint(self) -> None:
        # Each operation above owns and commits its transaction before returning.
        return None


class DatabaseBackfillScheduler:
    """Run at most one existing structured archive lease beside live capture."""

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        data_root: Path,
        client: httpx2.AsyncClient,
        *,
        clock: Callable[[], datetime],
    ) -> None:
        self._session_factory = session_factory
        self._data_root = data_root
        self._client = client
        self._clock = clock

    async def run_once(
        self, worker_id: str, lease_duration: timedelta
    ) -> object | None:
        async with (
            self._session_factory() as session,
            self._session_factory() as heartbeat_session,
        ):
            statement = select(BackfillObjectRow).where(
                BackfillObjectRow.state.in_(_ACTIVE_BACKFILL_STATES)
            )
            rows = (await session.execute(statement)).scalars().all()
            if not rows:
                await session.rollback()
                return None
            now = self._clock()
            archives = {
                row.source_url: resolve_planned_archive(row, now) for row in rows
            }
            repository = SqlAlchemyDataStateRepository(session)
            heartbeat_repository = SqlAlchemyDataStateRepository(heartbeat_session)
            detected_gaps = DetectedGapRecorder(repository)
            stages = ArchiveBackfillStages(
                archives,
                self._data_root,
                self._data_root / ".staging" / "archives",
                self._client,
                SqlAlchemyCatalogRepository(session),
                detected_gaps,
                clock=self._clock,
            )
            runner = BackfillRunner(
                repository,
                stages,
                clock=self._clock,
                gap_sink=MissingArchiveGapRecorder(archives, repository),
                heartbeat_repository=heartbeat_repository,
            )
            return await runner.run_once(worker_id, lease_duration)


def resolve_planned_archive(row: BackfillObjectRow, as_of: datetime) -> ArchiveObject:
    """Recreate an official descriptor and require exact persisted URL equality."""
    try:
        parsed = urlparse(row.source_url)
        if (
            parsed.scheme != "https"
            or parsed.hostname != "data.binance.vision"
            or parsed.query
            or parsed.fragment
        ):
            raise ValueError
        parts = tuple(part for part in parsed.path.split("/") if part)
        dataset_component = next(
            component for component in parts if component in _ARCHIVE_PATH_DATASETS
        )
        dataset_index = parts.index(dataset_component)
        symbol = parts[dataset_index + 1]
        planned = plan_archives(
            _ARCHIVE_PATH_DATASETS[dataset_component],
            symbol,
            row.start_at,
            row.end_at,
            as_of=as_of.astimezone(UTC),
        )
        if len(planned) != 1 or planned[0].url != row.source_url:
            raise ValueError
        return planned[0]
    except (IndexError, StopIteration, ValueError) as error:
        raise ValueError(
            "backfill source does not match an approved archive plan"
        ) from error


async def run_market_worker(
    settings: Settings | None = None,
    *,
    connector=websocket_connect,
) -> None:
    settings = settings or get_settings()
    def clock() -> datetime:
        return datetime.now(UTC)

    database = Database(settings.database_url)
    client = httpx2.AsyncClient(follow_redirects=False, timeout=30)
    repository = SessionWorkerRepository(database.session_factory)

    async def record_direct_failure(failure: DirectConnectionFailure) -> None:
        await repository.record_source_transition(
            SourceTransition(
                settings.market_worker_id,
                failure.at,
                "direct",
                "degraded",
                failure.kind.value,
                {
                    "route": failure.route,
                    "error_type": failure.error_type,
                    "detail": failure.detail,
                },
            )
        )

    async def record_mode_transition(transition: ConnectionModeTransition) -> None:
        await repository.record_source_transition(
            SourceTransition(
                settings.market_worker_id,
                transition.at,
                transition.from_mode.value,
                transition.to_mode.value,
                transition.reason,
                {"route": transition.route},
            )
        )

    policy = ConnectionPolicy(
        proxy_mode=settings.proxy_mode,
        proxy_url=settings.http_proxy_url,
    )
    factory = RoutedConnectionFactory(
        connector,
        policy,
        record_direct_failure=record_direct_failure,
        record_mode_transition=record_mode_transition,
    )
    supervisor = StreamConnectionSupervisor(factory)
    backfill = DatabaseBackfillScheduler(
        database.session_factory,
        settings.data_root,
        client,
        clock=clock,
    )
    worker = MarketWorker(
        repository,
        supervisor,
        LiveStorage(settings.data_root),
        backfill_runner=backfill,
        worker_id=settings.market_worker_id,
        clock=clock,
        stale_after=timedelta(seconds=settings.live_stale_after_seconds),
        refresh_interval=30,
        lease_duration=timedelta(minutes=5),
    )
    loop = asyncio.get_running_loop()
    loop.add_signal_handler(signal.SIGTERM, worker.request_shutdown)
    try:
        await worker.run()
    finally:
        loop.remove_signal_handler(signal.SIGTERM)
        await client.aclose()
        await database.close()


def main() -> None:
    asyncio.run(run_market_worker())


if __name__ == "__main__":
    main()
