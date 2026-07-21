import hashlib
import os
import re
import zipfile
from contextlib import suppress
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Protocol

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
    max_members: int = 1
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

        expected = _parse_checksum((await client.get(obj.checksum_url)).text)
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


def _parse_checksum(payload: str) -> str:
    matches = re.findall(r"(?<![0-9a-fA-F])[0-9a-fA-F]{64}(?![0-9a-fA-F])", payload)
    if len(matches) != 1:
        raise ArchiveChecksumError("official checksum response must contain exactly one SHA-256")
    return matches[0].lower()


def _inspect_zip(path: Path, limits: ArchiveSizeLimits) -> tuple[str, int]:
    try:
        with zipfile.ZipFile(path) as archive:
            members = archive.infolist()
            if len(members) != limits.max_members:
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
    except zipfile.BadZipFile as error:
        raise ArchiveSafetyError("archive is not a valid ZIP") from error


def _validate_member(member: zipfile.ZipInfo, limits: ArchiveSizeLimits) -> None:
    name = member.filename
    path = PurePosixPath(name)
    is_symlink = (member.external_attr >> 16) & 0o170000 == 0o120000
    if (
        path.is_absolute()
        or ".." in path.parts
        or "\\" in name
        or name.endswith("/")
        or is_symlink
        or path.suffix.lower() != ".csv"
    ):
        raise ArchiveSafetyError("archive member must be one safe relative CSV path")
    if member.file_size > limits.max_uncompressed_bytes:
        raise ArchiveSafetyError("archive uncompressed size exceeds byte limit")
    if member.compress_size == 0:
        if member.file_size:
            raise ArchiveSafetyError("archive compression ratio is unsafe")
        return
    if member.file_size / member.compress_size > limits.max_compression_ratio:
        raise ArchiveSafetyError("archive compression ratio exceeds limit")


def _fsync_directory(directory: Path) -> None:
    descriptor = os.open(directory, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
