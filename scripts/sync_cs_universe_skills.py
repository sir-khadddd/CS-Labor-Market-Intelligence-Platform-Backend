"""Sync config/cs_universe.yml skills: block from config/skill_keywords.yml."""

from __future__ import annotations

import re
import sys
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
UNIVERSE_PATH = ROOT / "config" / "cs_universe.yml"
KEYWORDS_PATH = ROOT / "config" / "skill_keywords.yml"


def build_skills_block(skills: list[dict]) -> str:
    lines = [
        "skills:",
        "  # Synced from config/skill_keywords.yml (scripts/sync_cs_universe_skills.py).",
        "  # skill_id = Revelio skill_k35000; used by DuckDB allowlist filtering.",
    ]
    for skill in skills:
        skill_id = str(skill["skill_id"]).replace('"', '\\"')
        skill_name = str(skill.get("skill_name") or skill["skill_id"]).replace('"', '\\"')
        lines.append(f'  - skill_id: "{skill_id}"')
        lines.append(f'    skill_name: "{skill_name}"')
    lines.append("")
    return "\n".join(lines)


def sync_skills() -> int:
    universe_text = UNIVERSE_PATH.read_text(encoding="utf-8")
    keywords_cfg = yaml.safe_load(KEYWORDS_PATH.read_text(encoding="utf-8"))
    skills = keywords_cfg.get("skills") or []
    if not skills:
        raise SystemExit(f"No skills found in {KEYWORDS_PATH}")

    skills_block = build_skills_block(skills)
    updated, count = re.subn(
        r"skills:\n.*?(?=^minimum_months)",
        skills_block,
        universe_text,
        count=1,
        flags=re.MULTILINE | re.DOTALL,
    )
    if count != 1:
        raise SystemExit("Could not locate skills: block in cs_universe.yml")

    UNIVERSE_PATH.write_text(updated, encoding="utf-8")
    return len(skills)


def main() -> None:
    n = sync_skills()
    print(f"Synced {n} skills into {UNIVERSE_PATH}")


if __name__ == "__main__":
    main()
