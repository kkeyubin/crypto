import asyncio
import hashlib
import io
import zipfile
from datetime import UTC, datetime, timedelta
from pathlib import Path

import httpx2
import pytest

from crypto_research.contracts.manifest import DataType
from crypto_research.market.backfill import (
    ArchiveBackfillStages,
    BackfillObject,
    BackfillRunner,
    BackfillState,
    DownloadEvidence,
    InMemoryBackfillRepository,
    MissingArchiveDisposition,
    NormalizeEvidence,
    PublishEvidence,
    classify_missing_archive,
)
from crypto_research.market.backfill_planning import (
    ArchiveBackfillPlanner,
    MissingArchiveGapRecorder,
)
from crypto_research.market.binance.archive import ArchiveNotFoundError
from crypto_research.market.binance.archive_paths import DatasetKind, plan_archives
from crypto_research.market.catalog import InMemoryCatalogRepository

NOW = datetime(2026, 7, 21, 12, tzinfo=UTC)


def object_for(day: int = 20) -> BackfillObject:
    start = datetime(2026, 7, day, tzinfo=UTC)
    return BackfillObject(
        object_id=f"object-{day}",
        job_id="job-1",
        source_url=f"https://data.binance.vision/{day}.zip",
        start=start,
        end=start + timedelta(days=1),
    )


def test_durable_sequence_rejects_backwards_transitions_and_preserves_evidence() -> None:
    async def scenario() -> None:
        repository = InMemoryBackfillRepository()
        await repository.plan(object_for())
        leased = await repository.claim("worker-a", NOW, timedelta(minutes=5))
        assert leased is not None
        assert leased.attempt_count == 1

        states = [
            BackfillState.DOWNLOADING,
            BackfillState.CHECKSUM_VERIFIED,
            BackfillState.NORMALIZED,
            BackfillState.VALIDATED,
            BackfillState.CATALOG_APPROVED,
        ]
        for state in states:
            leased = await repository.advance(
                leased.object_id,
                "worker-a",
                leased.attempt_count,
                NOW,
                state,
                evidence={"last_error": "prior diagnostic"} if state is states[0] else None,
            )
        assert leased.state is BackfillState.CATALOG_APPROVED
        assert leased.last_error == "prior diagnostic"
        assert leased.state_timestamps[BackfillState.VALIDATED] == NOW
        with pytest.raises(ValueError, match="transition"):
            await repository.advance(
                leased.object_id,
                "worker-a",
                leased.attempt_count,
                NOW,
                BackfillState.NORMALIZED,
            )

    asyncio.run(scenario())


def test_expired_lease_resumes_last_durable_state_and_fences_old_worker() -> None:
    async def scenario() -> None:
        repository = InMemoryBackfillRepository()
        await repository.plan(object_for())
        first = await repository.claim("worker-a", NOW, timedelta(minutes=1))
        assert first is not None
        downloading = await repository.advance(
            first.object_id,
            "worker-a",
            first.attempt_count,
            NOW,
            BackfillState.DOWNLOADING,
        )

        assert await repository.claim("worker-b", NOW, timedelta(minutes=1)) is None
        restarted = await repository.claim(
            "worker-b", NOW + timedelta(minutes=2), timedelta(minutes=1)
        )
        assert restarted is not None
        assert restarted.state is BackfillState.DOWNLOADING
        assert restarted.attempt_count == 2
        with pytest.raises(ValueError, match="lease"):
            await repository.renew(
                downloading.object_id,
                "worker-a",
                downloading.attempt_count,
                NOW + timedelta(minutes=2),
                timedelta(minutes=1),
            )

    asyncio.run(scenario())


def test_attempt_count_fences_stale_work_even_when_worker_id_is_reused() -> None:
    async def scenario() -> None:
        repository = InMemoryBackfillRepository()
        await repository.plan(object_for())
        stale = await repository.claim("worker-a", NOW, timedelta(minutes=1))
        assert stale is not None
        current = await repository.claim(
            "worker-a", NOW + timedelta(minutes=2), timedelta(minutes=5)
        )
        assert current is not None and current.attempt_count == 2

        with pytest.raises(ValueError, match="lease"):
            await repository.advance(
                stale.object_id,
                "worker-a",
                stale.attempt_count,
                NOW + timedelta(minutes=2),
                BackfillState.DOWNLOADING,
            )

    asyncio.run(scenario())


def test_only_failed_or_pending_objects_can_retry_explicitly() -> None:
    async def scenario() -> None:
        repository = InMemoryBackfillRepository()
        await repository.plan(object_for())
        leased = await repository.claim("worker-a", NOW, timedelta(minutes=5))
        assert leased is not None
        failed = await repository.advance(
            leased.object_id,
            "worker-a",
            leased.attempt_count,
            NOW,
            BackfillState.FAILED,
            evidence={"last_error": "old archive missing"},
        )
        retried = await repository.retry(failed.object_id, NOW + timedelta(minutes=1))
        assert retried.state is BackfillState.PLANNED
        assert retried.attempt_count == 1
        assert retried.last_error == "old archive missing"

        with pytest.raises(ValueError, match="retry"):
            await repository.retry(retried.object_id, NOW + timedelta(minutes=2))

    asyncio.run(scenario())


def test_replanning_after_checksum_discovery_is_idempotent() -> None:
    async def scenario() -> None:
        repository = InMemoryBackfillRepository()
        original = object_for()
        await repository.plan(original)
        leased = await repository.claim("worker-a", NOW, timedelta(minutes=5))
        assert leased is not None
        downloading = await repository.advance(
            leased.object_id,
            "worker-a",
            leased.attempt_count,
            NOW,
            BackfillState.DOWNLOADING,
        )
        await repository.advance(
            leased.object_id,
            "worker-a",
            leased.attempt_count,
            NOW,
            BackfillState.CHECKSUM_VERIFIED,
            evidence={"source_checksum": "a" * 64, "raw_path": "raw/a.zip"},
        )

        replanned = await repository.plan(original)
        assert replanned.object_id == downloading.object_id
        assert replanned.source_checksum == "a" * 64

    asyncio.run(scenario())


@pytest.mark.parametrize("day", [19, 20])
def test_recent_closed_days_are_source_pending(day: int) -> None:
    work = object_for(day)
    assert classify_missing_archive(work.start, work.end, NOW) is MissingArchiveDisposition.PENDING


def test_older_missing_archive_fails_with_explicit_half_open_gap() -> None:
    work = object_for(18)
    result = classify_missing_archive(work.start, work.end, NOW)

    assert result is MissingArchiveDisposition.FAILED_WITH_GAP
    assert (work.start, work.end) == (
        datetime(2026, 7, 18, tzinfo=UTC),
        datetime(2026, 7, 19, tzinfo=UTC),
    )


def test_runner_persists_every_stage_before_catalog_approval() -> None:
    class Stages:
        def __init__(self) -> None:
            self.calls: list[str] = []

        async def download(self, _work: BackfillObject) -> DownloadEvidence:
            self.calls.append("download")
            return DownloadEvidence("a" * 64, "raw/a.zip")

        async def normalize(self, work: BackfillObject) -> NormalizeEvidence:
            assert work.state is BackfillState.CHECKSUM_VERIFIED
            self.calls.append("normalize")
            return NormalizeEvidence("normalized/v1.parquet", "b" * 64, 2)

        async def validate(self, work: BackfillObject) -> dict[str, bool]:
            assert work.state is BackfillState.NORMALIZED
            self.calls.append("validate")
            return {
                "checksum": True,
                "schema": True,
                "ordering": True,
                "uniqueness": True,
                "range": True,
                "row_count": True,
            }

        async def publish(self, work: BackfillObject) -> PublishEvidence:
            assert work.state is BackfillState.VALIDATED
            self.calls.append("publish")
            return PublishEvidence("partition-1", "manifest-1")

    async def scenario() -> None:
        repository = InMemoryBackfillRepository()
        stages = Stages()
        await repository.plan(object_for())
        runner = BackfillRunner(repository, stages, clock=lambda: NOW)

        completed = await runner.run_once("worker-a", timedelta(minutes=5))

        assert completed is not None
        assert completed.state is BackfillState.CATALOG_APPROVED
        assert completed.partition_id == "partition-1"
        assert completed.manifest_id == "manifest-1"
        assert stages.calls == ["download", "normalize", "validate", "publish"]

    asyncio.run(scenario())


def test_runner_renews_lease_while_publish_is_still_running() -> None:
    class Repository(InMemoryBackfillRepository):
        renewals = 0

        async def renew(self, *args, **kwargs):
            self.renewals += 1
            return await super().renew(*args, **kwargs)

    class SlowStages:
        async def download(self, _work: BackfillObject) -> DownloadEvidence:
            return DownloadEvidence("a" * 64, "raw/a.zip")

        async def normalize(self, _work: BackfillObject) -> NormalizeEvidence:
            return NormalizeEvidence("normalized/a.parquet", "b" * 64, 1)

        async def validate(self, _work: BackfillObject) -> dict[str, bool]:
            return {name: True for name in (
                "checksum", "schema", "ordering", "uniqueness", "range", "row_count"
            )}

        async def publish(self, _work: BackfillObject) -> PublishEvidence:
            await asyncio.sleep(0.08)
            return PublishEvidence("partition-1", "manifest-1")

    async def scenario() -> None:
        repository = Repository()
        await repository.plan(object_for())
        completed = await BackfillRunner(
            repository, SlowStages(), clock=lambda: datetime.now(UTC)
        ).run_once("worker-a", timedelta(milliseconds=60))

        assert completed is not None
        assert completed.state is BackfillState.CATALOG_APPROVED
        assert repository.renewals >= 1

    asyncio.run(scenario())


def test_runner_renews_before_each_short_stage_when_cumulative_time_exceeds_lease() -> None:
    class Repository(InMemoryBackfillRepository):
        renewals = 0

        async def renew(self, *args, **kwargs):
            self.renewals += 1
            return await super().renew(*args, **kwargs)

    class ShortStages:
        async def download(self, _work: BackfillObject) -> DownloadEvidence:
            await asyncio.sleep(0.025)
            return DownloadEvidence("a" * 64, "raw/a.zip")

        async def normalize(self, _work: BackfillObject) -> NormalizeEvidence:
            await asyncio.sleep(0.025)
            return NormalizeEvidence("normalized/a.parquet", "b" * 64, 1)

        async def validate(self, _work: BackfillObject) -> dict[str, bool]:
            await asyncio.sleep(0.025)
            return {
                name: True
                for name in (
                    "checksum",
                    "schema",
                    "ordering",
                    "uniqueness",
                    "range",
                    "row_count",
                )
            }

        async def publish(self, _work: BackfillObject) -> PublishEvidence:
            await asyncio.sleep(0.025)
            return PublishEvidence("partition-1", "manifest-1")

    async def scenario() -> None:
        repository = Repository()
        await repository.plan(object_for())
        completed = await BackfillRunner(
            repository, ShortStages(), clock=lambda: datetime.now(UTC)
        ).run_once("worker-a", timedelta(milliseconds=90))

        assert completed is not None
        assert completed.state is BackfillState.CATALOG_APPROVED
        assert repository.renewals >= 4

    asyncio.run(scenario())


def test_failed_lease_heartbeat_cancels_the_inflight_stage() -> None:
    cancelled = asyncio.Event()

    class Repository(InMemoryBackfillRepository):
        renewal_attempts = 0

        async def renew(self, *args, **kwargs):
            self.renewal_attempts += 1
            if self.renewal_attempts == 1:
                return await super().renew(*args, **kwargs)
            raise ValueError("lease heartbeat rejected")

    class Stages:
        async def download(self, _work: BackfillObject) -> DownloadEvidence:
            try:
                await asyncio.sleep(10)
            except asyncio.CancelledError:
                cancelled.set()
                raise

    async def scenario() -> None:
        repository = Repository()
        await repository.plan(object_for())
        result = await BackfillRunner(
            repository, Stages(), clock=lambda: datetime.now(UTC)
        ).run_once("worker-a", timedelta(milliseconds=60))

        assert result is not None and result.state is BackfillState.FAILED
        assert cancelled.is_set()

    asyncio.run(scenario())


def test_failed_pre_stage_renewal_does_not_create_the_operation_coroutine() -> None:
    started = False

    class Repository(InMemoryBackfillRepository):
        async def renew(self, *args, **kwargs):
            raise ValueError("pre-stage lease renewal rejected")

    class Stages:
        async def download(self, _work: BackfillObject) -> DownloadEvidence:
            nonlocal started
            started = True
            return DownloadEvidence("a" * 64, "raw/a.zip")

    async def scenario() -> None:
        repository = Repository()
        await repository.plan(object_for())
        result = await BackfillRunner(
            repository, Stages(), clock=lambda: datetime.now(UTC)
        ).run_once("worker-a", timedelta(seconds=1))

        assert result is not None and result.state is BackfillState.FAILED
        assert started is False

    asyncio.run(scenario())


def test_runner_records_old_404_as_gap_but_keeps_recent_404_pending() -> None:
    class MissingStages:
        async def download(self, _work: BackfillObject) -> DownloadEvidence:
            raise ArchiveNotFoundError("missing")

    class GapSink:
        def __init__(self) -> None:
            self.objects: list[BackfillObject] = []

        async def record_missing_archive(self, work: BackfillObject) -> None:
            self.objects.append(work)

    async def scenario() -> None:
        repository = InMemoryBackfillRepository()
        sink = GapSink()
        recent = object_for(20)
        old = object_for(18)
        await repository.plan(recent)
        await repository.plan(old)
        runner = BackfillRunner(repository, MissingStages(), gap_sink=sink, clock=lambda: NOW)

        first = await runner.run_once("worker-a", timedelta(minutes=5))
        second = await runner.run_once("worker-a", timedelta(minutes=5))

        assert first is not None and first.state is BackfillState.FAILED
        assert second is not None and second.state is BackfillState.SOURCE_PENDING
        assert [work.object_id for work in sink.objects] == [old.object_id]

    asyncio.run(scenario())


def test_production_archive_stages_execute_task3_pipeline_and_publish_manifest(
    tmp_path: Path,
) -> None:
    csv_payload = (Path(__file__).parent / "fixtures" / "klines.csv").read_bytes()
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("BTCUSDT-1m-2024-01-01.csv", csv_payload)
    body = buffer.getvalue()
    checksum = hashlib.sha256(body).hexdigest()
    archive_object = plan_archives(
        DatasetKind.KLINES,
        "BTCUSDT",
        datetime(2024, 1, 1, tzinfo=UTC),
        datetime(2024, 1, 2, tzinfo=UTC),
        as_of=NOW,
    )[0]

    def handler(request: httpx2.Request) -> httpx2.Response:
        if request.url.path.endswith(".CHECKSUM"):
            name = Path(request.url.path.removesuffix(".CHECKSUM")).name
            return httpx2.Response(200, text=f"{checksum}  {name}\n")
        return httpx2.Response(200, content=body)

    async def scenario() -> None:
        data_root = tmp_path / "data"
        data_root.mkdir(mode=0o700)
        staging_root = tmp_path / "staging"
        detected_gaps: list[object] = []

        class GapSink:
            async def record_gap(self, gap: object) -> None:
                detected_gaps.append(gap)

        class Catalog(InMemoryCatalogRepository):
            async def approve(self, candidate):
                assert detected_gaps
                return await super().approve(candidate)

        catalog = Catalog()
        repository = InMemoryBackfillRepository()
        work = BackfillObject(
            object_id="object-production",
            job_id="job-production",
            source_url=archive_object.url,
            start=archive_object.start,
            end=archive_object.end,
        )
        await repository.plan(work)
        async with httpx2.AsyncClient(transport=httpx2.MockTransport(handler)) as client:
            stages = ArchiveBackfillStages(
                {archive_object.url: archive_object},
                data_root,
                staging_root,
                client,
                catalog,
                GapSink(),
                clock=lambda: datetime(2024, 1, 3, tzinfo=UTC),
            )
            completed = await BackfillRunner(
                repository, stages, clock=lambda: datetime(2024, 1, 3, tzinfo=UTC)
            ).run_once("worker-a", timedelta(minutes=5))

        assert completed is not None
        assert completed.state is BackfillState.CATALOG_APPROVED
        assert completed.normalized_path is not None
        assert checksum in completed.normalized_path
        assert (data_root / completed.raw_path).is_file()  # type: ignore[arg-type]
        assert (data_root / completed.normalized_path).is_file()
        approved = await catalog.approved(
            "BTCUSDT", DataType.KLINE_1M, archive_object.start, archive_object.end
        )
        assert len(approved) == 1
        assert approved[0].manifest.source_checksum == checksum
        assert detected_gaps
        assert detected_gaps[0].reason.value == "missing_minute_open_time"  # type: ignore[attr-defined]

    asyncio.run(scenario())


def test_archive_planner_and_missing_gap_recorder_are_deterministic() -> None:
    class StateRepository:
        def __init__(self) -> None:
            self.work: list[BackfillObject] = []
            self.gaps: list[object] = []

        async def plan(self, work: BackfillObject) -> BackfillObject:
            self.work.append(work)
            return work

        async def record_gap(self, gap: object) -> object:
            self.gaps.append(gap)
            return gap

    async def scenario() -> None:
        archive = plan_archives(
            DatasetKind.KLINES,
            "BTCUSDT",
            datetime(2024, 1, 1, tzinfo=UTC),
            datetime(2024, 1, 2, tzinfo=UTC),
            as_of=NOW,
        )[0]
        state = StateRepository()
        planner = ArchiveBackfillPlanner(state)
        first = await planner.plan("job-1", (archive,))
        second = await planner.plan("job-1", (archive,))
        assert first[0].object_id == second[0].object_id

        recorder = MissingArchiveGapRecorder({archive.url: archive}, state)
        await recorder.record_missing_archive(first[0])
        gap = state.gaps[0]
        assert gap.symbol == "BTCUSDT"  # type: ignore[attr-defined]
        assert gap.dataset == "kline_1m"  # type: ignore[attr-defined]
        assert gap.reason == "archive_missing"  # type: ignore[attr-defined]

    asyncio.run(scenario())
