import subprocess


def test_copyrighted_source_books_are_not_tracked() -> None:
    result = subprocess.run(
        ["git", "ls-files", "*.pdf", "*.epub", "*.mobi", "*.azw*"],
        capture_output=True,
        text=True,
        check=True,
    )

    assert result.stdout.strip() == ""
