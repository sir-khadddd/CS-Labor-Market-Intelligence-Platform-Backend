"""Append WRDS role discovery candidates to config/cs_universe.yml."""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

import pandas as pd
import yaml

ROOT = Path(__file__).resolve().parents[1]
UNIVERSE_PATH = ROOT / "config" / "cs_universe.yml"
REPORT_DIR = ROOT / "data" / "raw_cs_snapshot" / "metadata" / "taxonomy_reports"


def _load_roles(text: str) -> list[dict]:
    cfg = yaml.safe_load(text) or {}
    return list(cfg.get("roles") or [])


def apply_candidates(csv_path: Path, *, min_score: int = 2, limit: int = 10) -> int:
    universe_text = UNIVERSE_PATH.read_text(encoding="utf-8")
    roles = _load_roles(universe_text)
    existing = {str(r["role_id"]).strip() for r in roles if r.get("role_id")}

    df = pd.read_csv(csv_path)
    picks = df[(~df["in_allowlist"]) & (df["cs_hint_score"] >= min_score)].copy()
    picks = picks.sort_values(["cs_hint_score", "posting_count"], ascending=[False, False])
    added = 0
    for row in picks.itertuples(index=False):
        if added >= limit:
            break
        role_id = str(row.role_k17000_v3).strip()
        if role_id in existing:
            continue
        role_name = str(row.top_role_k1500).strip() or role_id.lower()
        roles.append({"role_id": role_id, "role_name": role_name})
        existing.add(role_id)
        added += 1

    if added == 0:
        return 0

    roles_block_lines = ["roles:"]
    for role in roles:
        rid = str(role["role_id"]).replace('"', '\\"')
        rname = str(role.get("role_name") or role["role_id"]).replace('"', '\\"')
        roles_block_lines.append(f'  - role_id: "{rid}"')
        roles_block_lines.append(f'    role_name: "{rname}"')
    roles_block_lines.append("")
    roles_block = "\n".join(roles_block_lines)

    updated, count = re.subn(
        r"roles:\r?\n.*?(?=^skills:)",
        roles_block,
        universe_text,
        count=1,
        flags=re.MULTILINE | re.DOTALL,
    )
    if count != 1:
        raise SystemExit("Could not locate roles: block in cs_universe.yml")

    UNIVERSE_PATH.write_text(updated, encoding="utf-8")
    return added


def main() -> None:
    parser = argparse.ArgumentParser(description="Apply WRDS role discovery CSV to cs_universe.yml")
    parser.add_argument("--csv", default=None, help="Discovery CSV path (default: latest in taxonomy_reports/)")
    parser.add_argument("--min-score", type=int, default=2)
    parser.add_argument("--limit", type=int, default=10)
    args = parser.parse_args()

    if args.csv:
        csv_path = Path(args.csv)
    else:
        files = sorted(REPORT_DIR.glob("role_frequency_discovery_*.csv"))
        if not files:
            raise SystemExit("No role_frequency_discovery_*.csv found; run discover_role_candidates.py first.")
        csv_path = files[-1]

    added = apply_candidates(csv_path, min_score=args.min_score, limit=args.limit)
    print(f"Added {added} role(s) from {csv_path}")
    if added:
        print("Next: re-extract postings, rebuild DuckDB, reload Postgres for trajectory panel growth:")
        print("  python scripts/extract_wrds_cs_snapshot.py")
        print("  python scripts/extract_wrds_cs_posting_text.py --resume")
        print("  python scripts/build_duckdb.py")
        print("  python scripts/load_postgres.py")


if __name__ == "__main__":
    main()
