import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[4]
SKILL_ROOT = ROOT / "skills" / "crypto-trading-research"


def test_unified_skill_has_required_contract_and_boundaries() -> None:
    text = (SKILL_ROOT / "SKILL.md").read_text()

    assert re.search(r"^name: crypto-trading-research$", text, re.MULTILINE)
    for token in [
        "StrategySpec",
        "MarketSnapshot",
        "AIAssessment",
        "candidate",
        "rejected",
        "insufficient_evidence",
        "BB",
        "RB",
    ]:
        assert token in text
    assert "place real orders" not in text.lower()


def test_unified_skill_links_are_local_and_resolve() -> None:
    files = [SKILL_ROOT / "SKILL.md", *sorted((SKILL_ROOT / "references").glob("*.md"))]

    for source in files:
        for target in re.findall(r"\[[^]]+\]\(([^)]+)\)", source.read_text()):
            assert not target.startswith(("http://", "https://"))
            assert (source.parent / target).resolve().exists(), (
                f"broken link {source}: {target}"
            )
