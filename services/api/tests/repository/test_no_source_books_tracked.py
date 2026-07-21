import subprocess
from pathlib import Path

REPOSITORY_ROOT = Path(__file__).resolve().parents[4]
SOURCE_BOOK_PATTERNS = ("*.pdf", "*.epub", "*.mobi", "*.azw*")


def tracked_source_books(repository_root: Path = REPOSITORY_ROOT) -> list[str]:
    result = subprocess.run(
        ["git", "-C", str(repository_root), "ls-files", "--", *SOURCE_BOOK_PATTERNS],
        capture_output=True,
        text=True,
        check=True,
    )
    return result.stdout.splitlines()


def test_copyrighted_source_books_are_not_tracked() -> None:
    assert tracked_source_books() == []


def test_book_guard_finds_root_and_nested_books_from_any_pytest_directory(tmp_path: Path) -> None:
    (tmp_path / "docs").mkdir()
    (tmp_path / "source.pdf").touch()
    (tmp_path / "docs" / "source.epub").touch()
    subprocess.run(["git", "init", str(tmp_path)], check=True, capture_output=True)
    subprocess.run(
        ["git", "-C", str(tmp_path), "add", "source.pdf", "docs/source.epub"],
        check=True,
        capture_output=True,
    )

    assert tracked_source_books(tmp_path) == ["docs/source.epub", "source.pdf"]
