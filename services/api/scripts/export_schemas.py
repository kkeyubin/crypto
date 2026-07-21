import argparse
import json
import sys
from pathlib import Path

from crypto_research.contracts import (
    AIAssessment,
    DataManifest,
    MarketSnapshot,
    StrategySpec,
)

MODELS = [AIAssessment, DataManifest, MarketSnapshot, StrategySpec]
ROOT = Path(__file__).resolve().parents[3]
OUTPUT = ROOT / "contracts" / "jsonschema"


def render(model: type) -> str:
    schema = model.model_json_schema(mode="serialization")
    return json.dumps(schema, ensure_ascii=False, indent=2, sort_keys=True) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    OUTPUT.mkdir(parents=True, exist_ok=True)
    drift = []
    for model in MODELS:
        target = OUTPUT / f"{model.__name__}.schema.json"
        expected = render(model)
        if args.check:
            if not target.exists() or target.read_text() != expected:
                drift.append(str(target.relative_to(ROOT)))
        else:
            target.write_text(expected)
    if drift:
        print("schema drift: " + ", ".join(drift), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
