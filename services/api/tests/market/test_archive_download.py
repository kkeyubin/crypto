import asyncio
import hashlib
import io
import zipfile
from datetime import UTC, datetime
from pathlib import Path

import httpx2
import pytest

from crypto_research.market.binance.archive import (
    ArchiveChecksumError,
    ArchiveNotFoundError,
    ArchiveSafetyError,
    ArchiveSizeLimits,
    fetch_archive,
)
from crypto_research.market.binance.archive_paths import DatasetKind, plan_archives


def zip_bytes(members: dict[str, bytes], compression: int = zipfile.ZIP_DEFLATED) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", compression=compression) as archive:
        for name, content in members.items():
            archive.writestr(name, content)
    return buffer.getvalue()


def object_for_test():
    return plan_archives(
        DatasetKind.KLINES,
        "BTCUSDT",
        datetime(2024, 1, 1, tzinfo=UTC),
        datetime(2024, 1, 2, tzinfo=UTC),
    )[0]


def client_for(body: bytes, checksum: str | None = None, status: int = 200) -> httpx2.AsyncClient:
    def handler(request: httpx2.Request) -> httpx2.Response:
        if request.url.path.endswith(".CHECKSUM"):
            digest = checksum if checksum is not None else hashlib.sha256(body).hexdigest()
            return httpx2.Response(200, text=f"{digest}  source.zip\n")
        return httpx2.Response(status, content=body)

    return httpx2.AsyncClient(transport=httpx2.MockTransport(handler))


def test_fetches_checksum_verified_single_csv_and_fsyncs_target(tmp_path: Path) -> None:
    body = zip_bytes({"BTCUSDT-1m-2024-01-01.csv": b"header\nrow\n"})

    async def scenario() -> None:
        async with client_for(body) as client:
            result = await fetch_archive(object_for_test(), tmp_path / "download.zip", client)
        assert result.sha256 == hashlib.sha256(body).hexdigest()
        assert result.member_name == "BTCUSDT-1m-2024-01-01.csv"
        assert result.path.read_bytes() == body
        assert not (tmp_path / "download.zip.partial").exists()

    asyncio.run(scenario())


def test_rejects_bad_checksum_and_removes_partial_file(tmp_path: Path) -> None:
    body = zip_bytes({"data.csv": b"one\n"})

    async def scenario() -> None:
        async with client_for(body, checksum="0" * 64) as client:
            with pytest.raises(ArchiveChecksumError, match="checksum"):
                await fetch_archive(object_for_test(), tmp_path / "download.zip", client)
        assert not list(tmp_path.iterdir())

    asyncio.run(scenario())


@pytest.mark.parametrize(
    ("body", "limits", "message"),
    [
        (b"not a zip", ArchiveSizeLimits(), "ZIP"),
        (zip_bytes({"first.csv": b"a", "second.csv": b"b"}), ArchiveSizeLimits(), "exactly one"),
        (zip_bytes({"/absolute.csv": b"a"}), ArchiveSizeLimits(), "safe relative"),
        (zip_bytes({"../traversal.csv": b"a"}), ArchiveSizeLimits(), "safe relative"),
        (
            zip_bytes({"data.csv": b"x" * 256}),
            ArchiveSizeLimits(max_uncompressed_bytes=10),
            "uncompressed",
        ),
        (
            zip_bytes({"data.csv": b"x" * 10_000}),
            ArchiveSizeLimits(max_compression_ratio=2),
            "compression ratio",
        ),
    ],
)
def test_rejects_adversarial_archives(
    tmp_path: Path, body: bytes, limits: ArchiveSizeLimits, message: str
) -> None:
    async def scenario() -> None:
        async with client_for(body) as client:
            with pytest.raises(ArchiveSafetyError, match=message):
                await fetch_archive(
                    object_for_test(), tmp_path / "download.zip", client, limits=limits
                )
        assert not list(tmp_path.iterdir())

    asyncio.run(scenario())


def test_returns_typed_not_found_without_pending_policy(tmp_path: Path) -> None:
    async def scenario() -> None:
        async with client_for(b"", status=404) as client:
            with pytest.raises(ArchiveNotFoundError):
                await fetch_archive(object_for_test(), tmp_path / "download.zip", client)

    asyncio.run(scenario())


def test_rejects_archive_larger_than_compressed_limit(tmp_path: Path) -> None:
    body = zip_bytes({"data.csv": b"incompressible enough" * 16}, compression=zipfile.ZIP_STORED)

    async def scenario() -> None:
        async with client_for(body) as client:
            with pytest.raises(ArchiveSafetyError, match="compressed"):
                await fetch_archive(
                    object_for_test(),
                    tmp_path / "download.zip",
                    client,
                    limits=ArchiveSizeLimits(max_compressed_bytes=10),
                )

    asyncio.run(scenario())
