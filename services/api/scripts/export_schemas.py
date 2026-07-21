import argparse
import json
import sys
from pathlib import Path

from crypto_research.contracts import (
    AIAssessment,
    AddSymbolRequest,
    BackfillRequest,
    DataGapView,
    DataPartitionView,
    DataManifest,
    EligibilityView,
    IngestionJobView,
    MarketDataHealthView,
    MarketSnapshot,
    StrategySpec,
    StrategySpecRecord,
    StreamStateView,
    SymbolProfileView,
    SymbolView,
)

MODELS = [
    AIAssessment,
    AddSymbolRequest,
    BackfillRequest,
    DataGapView,
    DataPartitionView,
    DataManifest,
    EligibilityView,
    IngestionJobView,
    MarketDataHealthView,
    MarketSnapshot,
    StrategySpec,
    StrategySpecRecord,
    StreamStateView,
    SymbolProfileView,
    SymbolView,
]
ROOT = Path(__file__).resolve().parents[3]
OUTPUT = ROOT / "contracts" / "jsonschema"


def render(model: type) -> str:
    schema = model.model_json_schema(mode="serialization")
    if model is EligibilityView:
        schema["allOf"] = [
            {
                "if": {"properties": {"eligible": {"const": False}}, "required": ["eligible"]},
                "then": {"properties": {"reason_codes": {"minItems": 1}}},
            },
            {
                "if": {"properties": {"eligible": {"const": True}}, "required": ["eligible"]},
                "then": {"properties": {"reason_codes": {"maxItems": 0}}},
            },
        ]
    return json.dumps(schema, ensure_ascii=False, indent=2, sort_keys=True) + "\n"


def expected_schemas() -> dict[str, str]:
    return {f"{model.__name__}.schema.json": render(model) for model in MODELS}


def check(output: Path, expected: dict[str, str]) -> list[str]:
    actual = {path.name for path in output.glob("*.schema.json")} if output.exists() else set()
    expected_names = set(expected)
    diagnostics = [f"missing: {name}" for name in sorted(expected_names - actual)]
    diagnostics.extend(f"stale: {name}" for name in sorted(actual - expected_names))
    diagnostics.extend(
        f"modified: {name}"
        for name in sorted(expected_names & actual)
        if (output / name).read_text() != expected[name]
    )
    return diagnostics


def write(output: Path, expected: dict[str, str]) -> None:
    output.mkdir(parents=True, exist_ok=True)
    expected_names = set(expected)
    for stale in output.glob("*.schema.json"):
        if stale.name not in expected_names:
            stale.unlink()
    for name, contents in expected.items():
        (output / name).write_text(contents)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--check", action="store_true")
    parser.add_argument("--output", type=Path, default=OUTPUT)
    args = parser.parse_args()
    expected = expected_schemas()
    if args.check:
        diagnostics = check(args.output, expected)
        if not diagnostics:
            return 0
        print("schema drift:\n" + "\n".join(diagnostics), file=sys.stderr)
        return 1
    # contracts/jsonschema is generated-only; normal mode owns its schema files.
    write(args.output, expected)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
