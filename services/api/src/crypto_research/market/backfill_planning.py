from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Protocol
from uuid import NAMESPACE_URL, uuid5

from crypto_research.contracts.manifest import DataType
from crypto_research.db.repositories import GapRecord
from crypto_research.market.backfill import BackfillObject
from crypto_research.market.binance.archive_paths import ArchiveObject, DatasetKind
from crypto_research.market.gaps import DetectedGap


class PlanningRepository(Protocol):
    async def plan(self, work: BackfillObject) -> BackfillObject: ...


class GapRepository(Protocol):
    async def record_gap(self, gap: GapRecord) -> object: ...


class ArchiveBackfillPlanner:
    """Persist deterministic work objects from an official archive plan."""

    def __init__(self, repository: PlanningRepository) -> None:
        self._repository = repository

    async def plan(
        self, job_id: str, archives: Iterable[ArchiveObject]
    ) -> tuple[BackfillObject, ...]:
        planned: list[BackfillObject] = []
        for archive in archives:
            identity = "|".join(
                (job_id, archive.url, archive.start.isoformat(), archive.end.isoformat())
            )
            work = BackfillObject(
                object_id=str(uuid5(NAMESPACE_URL, identity)),
                job_id=job_id,
                source_url=archive.url,
                start=archive.start,
                end=archive.end,
            )
            planned.append(await self._repository.plan(work))
        return tuple(planned)


class MissingArchiveGapRecorder:
    """Persist old official 404s as typed, half-open coverage gaps."""

    def __init__(
        self,
        archives: Mapping[str, ArchiveObject],
        repository: GapRepository,
    ) -> None:
        self._archives = dict(archives)
        self._repository = repository

    async def record_missing_archive(self, work: BackfillObject) -> None:
        try:
            archive = self._archives[work.source_url]
        except KeyError as error:
            raise ValueError("missing archive is not in the approved archive plan") from error
        if (work.start, work.end) != (archive.start, archive.end):
            raise ValueError("missing archive range does not match the approved plan")
        identity = (
            f"archive-missing|{archive.symbol}|{archive.dataset.value}|"
            f"{archive.start.isoformat()}|{archive.end.isoformat()}"
        )
        await self._repository.record_gap(
            GapRecord(
                id=str(uuid5(NAMESPACE_URL, identity)),
                symbol=archive.symbol,
                dataset=_data_type(archive.dataset).value,
                start_at=archive.start,
                end_at=archive.end,
                reason="archive_missing",
                details={"source_url": archive.url},
            )
        )


class DetectedGapRecorder:
    """Persist content detectors' typed evidence before catalog approval."""

    def __init__(self, repository: GapRepository) -> None:
        self._repository = repository

    async def record_gap(self, gap: DetectedGap) -> None:
        await self._repository.record_gap(
            GapRecord(
                id=gap.gap_id,
                symbol=gap.symbol,
                dataset=gap.data_type.value,
                start_at=gap.start,
                end_at=gap.end,
                reason=gap.reason.value,
                details=dict(gap.details),
            )
        )


def _data_type(dataset: DatasetKind) -> DataType:
    return {
        DatasetKind.KLINES: DataType.KLINE_1M,
        DatasetKind.MARK_PRICE_KLINES: DataType.MARK_PRICE,
        DatasetKind.FUNDING_RATE: DataType.FUNDING,
        DatasetKind.AGG_TRADES: DataType.AGG_TRADE,
    }[dataset]
