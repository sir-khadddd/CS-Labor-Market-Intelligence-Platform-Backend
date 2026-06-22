"""
Review inferred skill demand quality after build_duckdb.

Reads processed marts + skill_keywords.yml and writes a markdown report under
data/raw_cs_snapshot/metadata/.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
import sys

import pandas as pd
import yaml

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

PROCESSED_DIR = ROOT / "data" / "processed"
KEYWORDS_PATH = ROOT / "config" / "skill_keywords.yml"
REPORT_DIR = ROOT / "data" / "raw_cs_snapshot" / "metadata"

SHORT_KEYWORDS = {"r", "c", "go", "js", "ml", "dl", "sql", "ai", "it"}


def _load_keywords() -> pd.DataFrame:
    cfg = yaml.safe_load(KEYWORDS_PATH.read_text(encoding="utf-8"))
    rows = []
    for skill in cfg.get("skills") or []:
        skill_id = str(skill.get("skill_id", "")).strip()
        if not skill_id:
            continue
        keywords = [str(k).strip().lower() for k in (skill.get("keywords") or []) if str(k).strip()]
        rows.append(
            {
                "skill_id_config": skill_id,
                "skill_id_upper": skill_id.upper(),
                "skill_name": skill.get("skill_name") or skill_id,
                "skill_k15": skill.get("skill_k15"),
                "keywords": keywords,
                "short_keywords": [k for k in keywords if k in SHORT_KEYWORDS or len(k) <= 2],
            }
        )
    return pd.DataFrame(rows)


def main() -> None:
    skill_path = PROCESSED_DIR / "cs_skill_demand.csv"
    if not skill_path.exists():
        raise SystemExit(f"Missing {skill_path}; run scripts/build_duckdb.py first.")

    demand = pd.read_csv(
        skill_path,
        usecols=["month", "geo_id", "role_id", "skill_id", "skill_name", "skill_posting_count"],
    )
    keywords = _load_keywords()

    matched_ids = set(demand["skill_id"].astype(str).str.upper().unique())
    config_ids = set(keywords["skill_id_upper"])
    unmatched = sorted(config_ids - matched_ids)
    matched = sorted(config_ids & matched_ids)

    totals = (
        demand.groupby(["skill_id", "skill_name"], as_index=False)["skill_posting_count"]
        .sum()
        .sort_values("skill_posting_count", ascending=False)
    )
    by_role = (
        demand.groupby(["role_id", "skill_id", "skill_name"], as_index=False)["skill_posting_count"]
        .sum()
        .sort_values(["role_id", "skill_posting_count"], ascending=[True, False])
    )

    risky = keywords[keywords["short_keywords"].map(bool)].copy()
    run_id = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    report_path = REPORT_DIR / f"skill_quality_review_{run_id}.md"
    json_path = REPORT_DIR / f"skill_quality_review_{run_id}.json"

    lines = [
        "# Inferred skill demand quality review",
        "",
        f"Generated: {datetime.now(timezone.utc).isoformat()}",
        "",
        "## Summary",
        "",
        f"- Config skills: **{len(config_ids)}**",
        f"- Matched in marts: **{len(matched)}**",
        f"- Unmatched: **{len(unmatched)}**",
        f"- `cs_skill_demand` rows: **{len(demand):,}**",
        f"- Distinct skill ids in marts: **{demand['skill_id'].nunique()}**",
        f"- UNK rows: **{(demand['skill_id'] == 'UNK').sum():,}**",
        "",
        "## Top skills (total posting count)",
        "",
        "| skill_id | skill_name | skill_posting_count |",
        "|----------|------------|---------------------|",
    ]
    for row in totals.head(20).itertuples(index=False):
        lines.append(f"| {row.skill_id} | {row.skill_name} | {int(row.skill_posting_count):,} |")

    lines.extend(["", "## Top skills by role", ""])
    for role_id in sorted(by_role["role_id"].unique()):
        role_rows = by_role[by_role["role_id"] == role_id].head(10)
        lines.append(f"### {role_id}")
        lines.append("")
        lines.append("| skill_id | skill_name | skill_posting_count |")
        lines.append("|----------|------------|---------------------|")
        for row in role_rows.itertuples(index=False):
            lines.append(f"| {row.skill_id} | {row.skill_name} | {int(row.skill_posting_count):,} |")
        lines.append("")

    lines.extend(["## Unmatched config skills (no mart rows)", ""])
    if unmatched:
        for skill_id in unmatched:
            row = keywords[keywords["skill_id_upper"] == skill_id].iloc[0]
            lines.append(f"- {row['skill_id_config']} ({row['skill_k15']})")
    else:
        lines.append("- None")

    lines.extend(["", "## Short / risky keywords in config", ""])
    if len(risky):
        for row in risky.itertuples(index=False):
            lines.append(f"- **{row.skill_id_config}**: {', '.join(row.short_keywords)}")
    else:
        lines.append("- None flagged")

    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    report_path.write_text("\n".join(lines) + "\n", encoding="utf-8")

    payload = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "config_skill_count": len(config_ids),
        "matched_skill_count": len(matched),
        "unmatched_skill_ids": [
            keywords[keywords["skill_id_upper"] == sid].iloc[0]["skill_id_config"] for sid in unmatched
        ],
        "top_skills": totals.head(20).to_dict(orient="records"),
        "risky_short_keywords": risky[["skill_id_config", "short_keywords"]].to_dict(orient="records"),
    }
    json_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    print(f"Wrote {report_path}")
    print(f"Wrote {json_path}")
    print(f"Matched {len(matched)}/{len(config_ids)} config skills; UNK rows: {(demand['skill_id'] == 'UNK').sum()}")


if __name__ == "__main__":
    main()
