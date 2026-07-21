import subprocess
import sys
from pathlib import Path


def test_committed_json_schemas_match_models() -> None:
    api_root = Path(__file__).resolve().parents[2]
    result = subprocess.run(
        [sys.executable, "scripts/export_schemas.py", "--check"],
        cwd=api_root,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr
