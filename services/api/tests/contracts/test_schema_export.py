import json
import subprocess
import sys
from pathlib import Path

EXPECTED_ROOTS = {
    "AIAssessment.schema.json",
    "AddSymbolRequest.schema.json",
    "BackfillRequest.schema.json",
    "BackfillRecheckRequest.schema.json",
    "BackfillRecheckView.schema.json",
    "DataGapView.schema.json",
    "DataPartitionView.schema.json",
    "DataManifest.schema.json",
    "EligibilityView.schema.json",
    "GapReconcileRequest.schema.json",
    "IngestionJobView.schema.json",
    "MarketDataHealthView.schema.json",
    "MarketSnapshot.schema.json",
    "StrategySpec.schema.json",
    "StrategySpecRecord.schema.json",
    "StreamStateView.schema.json",
    "SymbolProfileView.schema.json",
    "SymbolView.schema.json",
}


def run_export(api_root: Path, output: Path, *, check: bool) -> subprocess.CompletedProcess[str]:
    command = [sys.executable, "scripts/export_schemas.py", "--output", str(output)]
    if check:
        command.append("--check")
    return subprocess.run(
        command,
        cwd=api_root,
        capture_output=True,
        text=True,
        check=False,
    )


def test_committed_json_schemas_match_models() -> None:
    api_root = Path(__file__).resolve().parents[2]
    output = api_root.parents[1] / "contracts" / "jsonschema"
    result = run_export(api_root, output, check=True)
    assert result.returncode == 0, result.stdout + result.stderr


def test_committed_json_schema_set_has_input_and_record_contracts() -> None:
    api_root = Path(__file__).resolve().parents[2]
    output = api_root.parents[1] / "contracts" / "jsonschema"

    assert {path.name for path in output.glob("*.schema.json")} == EXPECTED_ROOTS


def test_eligibility_schema_matches_reason_code_cardinality_to_eligibility() -> None:
    api_root = Path(__file__).resolve().parents[2]
    schema_path = api_root.parents[1] / "contracts" / "jsonschema" / "EligibilityView.schema.json"
    schema = json.loads(schema_path.read_text())

    assert schema["allOf"] == [
        {
            "if": {"properties": {"eligible": {"const": False}}, "required": ["eligible"]},
            "then": {"properties": {"reason_codes": {"minItems": 1}}},
        },
        {
            "if": {"properties": {"eligible": {"const": True}}, "required": ["eligible"]},
            "then": {"properties": {"reason_codes": {"maxItems": 0}}},
        },
    ]


def test_manifest_schema_requires_v2_and_exposes_closed_structured_sources() -> None:
    api_root = Path(__file__).resolve().parents[2]
    schema_path = api_root.parents[1] / "contracts" / "jsonschema" / "DataManifest.schema.json"
    schema = json.loads(schema_path.read_text())

    assert "schema_version" in schema["required"]
    assert schema["properties"]["schema_version"]["const"] == "2.0.0"
    assert "source_kind" not in schema["properties"]
    assert "source_object_url" not in schema["properties"]

    source_schema = schema["properties"]["source"]
    assert source_schema["discriminator"]["propertyName"] == "kind"
    assert set(source_schema["discriminator"]["mapping"]) == {
        "binance_archive",
        "binance_rest",
        "binance_websocket",
    }
    for variant in source_schema["oneOf"]:
        name = variant["$ref"].removeprefix("#/$defs/")
        definition = schema["$defs"][name]
        assert definition["additionalProperties"] is False
        assert definition["properties"]["resolved_url"]["readOnly"] is True


def test_check_reports_missing_schema_without_creating_output(
    tmp_path: Path,
) -> None:
    api_root = Path(__file__).resolve().parents[2]
    output = tmp_path / "jsonschema"

    result = run_export(api_root, output, check=True)

    assert result.returncode == 1
    for name in EXPECTED_ROOTS:
        assert f"missing: {name}" in result.stderr
    assert not output.exists()


def test_check_reports_modified_schema(tmp_path: Path) -> None:
    api_root = Path(__file__).resolve().parents[2]
    output = tmp_path / "jsonschema"
    output.mkdir()
    for name in EXPECTED_ROOTS:
        (output / name).write_text("{}\n")

    result = run_export(api_root, output, check=True)

    assert result.returncode == 1
    assert "modified: AIAssessment.schema.json" in result.stderr


def test_check_reports_stale_schema(tmp_path: Path) -> None:
    api_root = Path(__file__).resolve().parents[2]
    output = tmp_path / "jsonschema"
    output.mkdir()
    (output / "Legacy.schema.json").write_text("{}\n")

    result = run_export(api_root, output, check=True)

    assert result.returncode == 1
    assert "stale: Legacy.schema.json" in result.stderr
