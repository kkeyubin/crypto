import hashlib
import os
import re
import stat
import struct
import zipfile
from contextlib import suppress
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Protocol
from urllib.parse import urlparse

from crypto_research.market.binance.archive_paths import ArchiveObject


class ArchiveError(RuntimeError):
    """Base class for a rejected official archive object."""


class ArchiveNotFoundError(ArchiveError):
    """The archive object was not published by the upstream source."""


class ArchiveChecksumError(ArchiveError):
    """The official checksum does not describe the downloaded bytes."""


class ArchiveSafetyError(ArchiveError):
    """The downloaded archive cannot be safely parsed."""


class HttpClient(Protocol):
    def stream(self, method: str, url: str): ...

    async def get(self, url: str): ...


@dataclass(frozen=True)
class ArchiveSizeLimits:
    max_compressed_bytes: int = 128 * 1024 * 1024
    max_uncompressed_bytes: int = 1024 * 1024 * 1024
    max_central_directory_bytes: int = 64 * 1024
    max_compression_ratio: int = 100


@dataclass(frozen=True)
class VerifiedArchive:
    object: ArchiveObject
    path: Path
    sha256: str
    member_name: str
    compressed_bytes: int
    uncompressed_bytes: int


async def fetch_archive(
    obj: ArchiveObject,
    target: Path,
    client: HttpClient,
    *,
    limits: ArchiveSizeLimits | None = None,
) -> VerifiedArchive:
    """Download a single official ZIP object through a same-filesystem staging file."""
    limits = limits or ArchiveSizeLimits()
    target.parent.mkdir(parents=True, exist_ok=True)
    partial = target.with_suffix(f"{target.suffix}.partial")
    with suppress(FileNotFoundError):
        partial.unlink()
    try:
        digest = hashlib.sha256()
        written = 0
        async with client.stream("GET", obj.url) as response:
            if response.status_code == 404:
                raise ArchiveNotFoundError(f"official archive not found: {obj.url}")
            _raise_for_bad_status(response.status_code, obj.url)
            with partial.open("xb") as output:
                async for chunk in response.aiter_bytes():
                    written += len(chunk)
                    if written > limits.max_compressed_bytes:
                        raise ArchiveSafetyError("compressed archive exceeds byte limit")
                    digest.update(chunk)
                    output.write(chunk)
                output.flush()
                os.fsync(output.fileno())

        checksum_response = await client.get(obj.checksum_url)
        _raise_for_bad_status(checksum_response.status_code, obj.checksum_url)
        expected = _parse_checksum(
            checksum_response.text,
            Path(urlparse(obj.url).path).name,
        )
        actual = digest.hexdigest()
        if actual != expected:
            raise ArchiveChecksumError("official archive checksum mismatch")
        member_name, uncompressed = _inspect_zip(partial, limits)
        os.replace(partial, target)
        _fsync_directory(target.parent)
        return VerifiedArchive(
            object=obj,
            path=target,
            sha256=actual,
            member_name=member_name,
            compressed_bytes=written,
            uncompressed_bytes=uncompressed,
        )
    except Exception:
        with suppress(FileNotFoundError):
            partial.unlink()
        raise


def _raise_for_bad_status(status_code: int, url: str) -> None:
    if not 200 <= status_code < 300:
        raise ArchiveError(f"archive request failed with HTTP {status_code}: {url}")


def _parse_checksum(payload: str, expected_basename: str) -> str:
    pattern = rf"([0-9a-fA-F]{{64}})[ \t]+\*?{re.escape(expected_basename)}\n?"
    match = re.fullmatch(pattern, payload)
    if match is None:
        raise ArchiveChecksumError("official checksum record is invalid")
    return match.group(1).lower()


def _inspect_zip(path: Path, limits: ArchiveSizeLimits) -> tuple[str, int]:
    try:
        _preflight_central_directory(path, limits)
        with zipfile.ZipFile(path) as archive:
            members = archive.infolist()
            if len(members) != 1:
                raise ArchiveSafetyError("archive must contain exactly one CSV member")
            member = members[0]
            _validate_member(member, limits)
            with archive.open(member) as input_file:
                consumed = 0
                while chunk := input_file.read(1024 * 1024):
                    consumed += len(chunk)
                    if consumed > limits.max_uncompressed_bytes:
                        raise ArchiveSafetyError("archive uncompressed size exceeds byte limit")
            if consumed != member.file_size:
                raise ArchiveSafetyError("archive member size changed while reading")
            return member.filename, consumed
    except (OSError, zipfile.BadZipFile) as error:
        raise ArchiveSafetyError("archive is not a valid ZIP") from error


def _validate_member(member: zipfile.ZipInfo, limits: ArchiveSizeLimits) -> None:
    name = member.filename
    path = PurePosixPath(name)
    unix_mode = member.external_attr >> 16
    unix_file_type = stat.S_IFMT(unix_mode)
    dos_is_directory = member.external_attr & 0x10 != 0
    is_regular = (
        unix_file_type in {0, stat.S_IFREG}
        if member.create_system == 3
        else not dos_is_directory
    )
    if (
        path.is_absolute()
        or ".." in path.parts
        or "\\" in name
        or name.endswith("/")
        or not is_regular
        or path.suffix.lower() != ".csv"
    ):
        raise ArchiveSafetyError("archive member must be one safe relative regular CSV path")
    if member.file_size == 0:
        raise ArchiveSafetyError("archive CSV member must be non-empty")
    if member.file_size > limits.max_uncompressed_bytes:
        raise ArchiveSafetyError("archive uncompressed size exceeds byte limit")
    if member.compress_size == 0:
        if member.file_size:
            raise ArchiveSafetyError("archive compression ratio is unsafe")
        return
    if member.file_size / member.compress_size > limits.max_compression_ratio:
        raise ArchiveSafetyError("archive compression ratio exceeds limit")


def _preflight_central_directory(path: Path, limits: ArchiveSizeLimits) -> None:
    file_size = path.stat().st_size
    eocd_size = 22
    maximum_comment_size = 65_535
    if file_size < eocd_size:
        raise ArchiveSafetyError("archive is not a valid ZIP")
    tail_size = min(file_size, eocd_size + maximum_comment_size)
    with path.open("rb") as source:
        source.seek(file_size - tail_size)
        tail = source.read(tail_size)
    position = tail.rfind(b"PK\x05\x06")
    if position < 0 or position + eocd_size > len(tail):
        raise ArchiveSafetyError("archive is not a valid ZIP")
    (
        _signature,
        disk_number,
        central_directory_disk,
        entries_on_disk,
        total_entries,
        central_directory_size,
        central_directory_offset,
        comment_length,
    ) = struct.unpack_from("<4s4H2LH", tail, position)
    eocd_offset = file_size - tail_size + position
    if position + eocd_size + comment_length != len(tail):
        raise ArchiveSafetyError("archive has an invalid final EOCD record")
    if (
        disk_number != 0
        or central_directory_disk != 0
        or entries_on_disk == 0xFFFF
        or total_entries == 0xFFFF
        or central_directory_size == 0xFFFFFFFF
        or central_directory_offset == 0xFFFFFFFF
    ):
        raise ArchiveSafetyError("archive multi-disk or ZIP64 records are not supported")
    if entries_on_disk != 1 or total_entries != 1:
        raise ArchiveSafetyError("archive must contain exactly one central-directory member")
    central_directory_end = central_directory_offset + central_directory_size
    if (
        central_directory_size > limits.max_central_directory_bytes
        or central_directory_end > eocd_offset
        or central_directory_end > file_size
    ):
        raise ArchiveSafetyError("archive central directory exceeds safe bounds")


def _fsync_directory(directory: Path) -> None:
    descriptor = os.open(directory, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
