import asyncio
import hashlib
import io
import stat
import struct
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
    _parse_checksum,
    fetch_archive,
    fetch_archive_checksum,
)
from crypto_research.market.binance.archive_paths import DatasetKind, plan_archives


def zip_bytes(members: dict[str, bytes], compression: int = zipfile.ZIP_DEFLATED) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", compression=compression) as archive:
        for name, content in members.items():
            archive.writestr(name, content)
    return buffer.getvalue()


def special_zip(name: str, content: bytes, external_attr: int, create_system: int = 3) -> bytes:
    buffer = io.BytesIO()
    info = zipfile.ZipInfo(name)
    info.create_system = create_system
    info.external_attr = external_attr
    with zipfile.ZipFile(buffer, "w", compression=zipfile.ZIP_STORED) as archive:
        archive.writestr(info, content)
    return buffer.getvalue()


def patch_eocd(
    body: bytes,
    *,
    entries: int | None = None,
    size: int | None = None,
    offset: int | None = None,
) -> bytes:
    patched = bytearray(body)
    position = patched.rfind(b"PK\x05\x06")
    assert position >= 0
    if entries is not None:
        struct.pack_into("<H", patched, position + 8, entries)
        struct.pack_into("<H", patched, position + 10, entries)
    if size is not None:
        struct.pack_into("<L", patched, position + 12, size)
    if offset is not None:
        struct.pack_into("<L", patched, position + 16, offset)
    return bytes(patched)


def append_zip64_locator(body: bytes) -> bytes:
    position = body.rfind(b"PK\x05\x06")
    assert position >= 0
    locator = b"PK\x06\x07" + struct.pack("<LQL", 0, 1, 1)
    return body[:position] + locator + body[position:]


def zip64_locator_with_maximum_comment(body: bytes) -> bytes:
    position = body.rfind(b"PK\x05\x06")
    assert position >= 0
    locator = b"PK\x06\x07" + struct.pack("<LQL", 0, 1, 1)
    patched = bytearray(body[:position] + locator + body[position:])
    eocd_position = position + len(locator)
    struct.pack_into("<H", patched, eocd_position + 20, 65_535)
    return bytes(patched) + b"x" * 65_535


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
            name = Path(request.url.path.removesuffix(".CHECKSUM")).name
            return httpx2.Response(200, text=f"{digest}  {name}\n")
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


def test_checksum_probe_reads_only_the_structured_sibling_record() -> None:
    async def scenario() -> None:
        async with client_for(b"unused", checksum="a" * 64) as client:
            checksum = await fetch_archive_checksum(object_for_test(), client)
        assert checksum == "a" * 64

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
    "payload",
    [
        "",
        "0" * 64,
        f"{'0' * 64}  another.zip\n",
        f"{'0' * 64}  path/archive.zip\n",
        f"{'0' * 64}  archive.zip\nextra prose",
        f"{'0' * 64}  archive.zip\n{'1' * 64}  archive.zip\n",
        f"error page {'0' * 64} archive.zip",
    ],
)
def test_checksum_parser_rejects_everything_except_one_exact_official_record(payload: str) -> None:
    with pytest.raises(ArchiveChecksumError, match="checksum"):
        _parse_checksum(payload, "archive.zip")


def test_checksum_parser_accepts_optional_asterisk_and_single_trailing_newline() -> None:
    assert _parse_checksum(f"{'a' * 64} *archive.zip\n", "archive.zip") == "a" * 64


@pytest.mark.parametrize(
    ("body", "limits", "message"),
    [
        (b"not a zip", ArchiveSizeLimits(), "ZIP"),
        (zip_bytes({"first.csv": b"a", "second.csv": b"b"}), ArchiveSizeLimits(), "exactly one"),
        (zip_bytes({"/absolute.csv": b"a"}), ArchiveSizeLimits(), "safe relative"),
        (zip_bytes({"../traversal.csv": b"a"}), ArchiveSizeLimits(), "safe relative"),
        (zip_bytes({"empty.csv": b""}), ArchiveSizeLimits(), "non-empty"),
        (special_zip("symlink.csv", b"x", stat.S_IFLNK << 16), ArchiveSizeLimits(), "regular"),
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


@pytest.mark.parametrize(
    "body",
    [
        patch_eocd(zip_bytes({"data.csv": b"x"}), entries=2),
        patch_eocd(zip_bytes({"data.csv": b"x"}), size=65_537),
        patch_eocd(zip_bytes({"data.csv": b"x"}), offset=999_999),
        zip_bytes({f"{index}.csv": b"x" for index in range(2_000)}),
    ],
)
def test_rejects_invalid_or_unbounded_central_directory_before_zip_parsing(
    tmp_path: Path, body: bytes
) -> None:
    async def scenario() -> None:
        async with client_for(body) as client:
            with pytest.raises(ArchiveSafetyError, match="central directory|exactly one"):
                await fetch_archive(object_for_test(), tmp_path / "download.zip", client)

    asyncio.run(scenario())


def test_rejects_zip64_locator_with_unsaturated_classic_eocd_before_zipfile(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    body = append_zip64_locator(zip_bytes({"data.csv": b"x"}))

    def zipfile_must_not_run(*args, **kwargs):
        raise AssertionError("ZipFile construction must not run after ZIP64 locator detection")

    monkeypatch.setattr(
        "crypto_research.market.binance.archive.zipfile.ZipFile", zipfile_must_not_run
    )

    async def scenario() -> None:
        async with client_for(body) as client:
            with pytest.raises(ArchiveSafetyError, match="ZIP64"):
                await fetch_archive(object_for_test(), tmp_path / "download.zip", client)

    asyncio.run(scenario())


def test_rejects_zip64_locator_hidden_before_maximum_classic_comment(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    body = zip64_locator_with_maximum_comment(zip_bytes({"data.csv": b"x"}))

    def zipfile_must_not_run(*args, **kwargs):
        raise AssertionError("ZipFile construction must not run after ZIP64 locator detection")

    monkeypatch.setattr(
        "crypto_research.market.binance.archive.zipfile.ZipFile", zipfile_must_not_run
    )

    async def scenario() -> None:
        async with client_for(body) as client:
            with pytest.raises(ArchiveSafetyError, match="ZIP64"):
                await fetch_archive(object_for_test(), tmp_path / "download.zip", client)

    asyncio.run(scenario())
