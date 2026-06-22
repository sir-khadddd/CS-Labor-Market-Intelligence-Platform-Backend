"""Discover top role_k17000_v3 values from WRDS for cs_universe expansion."""

from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import wrds
import yaml

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from config.backend_env import load_backend_env

load_backend_env()

CONFIG_PATH = ROOT / "config" / "wrds_extract.yml"
UNIVERSE_PATH = ROOT / "config" / "cs_universe.yml"
REPORT_DIR = ROOT / "data" / "raw_cs_snapshot" / "metadata" / "taxonomy_reports"

# Substrings used to rank CS-relevant role_k17000_v3 buckets (case-insensitive).
CS_ROLE_HINTS = (
    "software",
    "data",
    "machine learning",
    "cyber",
    "security",
    "cloud",
    "devops",
    "network",
    "database",
    "web",
    "mobile",
    "embedded",
    "it ",
    "information technology",
    "artificial intelligence",
    "analytics",
    "engineering",
)


def _load_existing_roles() -> set[str]:
    cfg = yaml.safe_load(UNIVERSE_PATH.read_text(encoding="utf-8")) or {}
    return {str(r["role_id"]).strip() for r in (cfg.get("roles") or []) if r.get("role_id")}


def _cs_relevance(role_name: str) -> int:
    lower = role_name.lower()
    return sum(1 for hint in CS_ROLE_HINTS if hint in lower)


def discover_roles(months: int = 1, limit: int = 50) -> pd.DataFrame:
    cfg = yaml.safe_load(CONFIG_PATH.read_text(encoding="utf-8"))
    username = os.getenv(cfg["wrds"]["username_env"], "")
    password = os.getenv(cfg["wrds"]["password_env"])
    if not username:
        raise SystemExit("WRDS username env var not set")

    table_cfg = cfg["tables"]["postings_cosmos"]
    schema = table_cfg["schema"]
    table = table_cfg["table"]
    date_col = table_cfg["date_column"]
    end_date = cfg["extract"]["end_date"]

    sql = f"""
        SELECT role_k17000_v3, role_k1500_v2, COUNT(*) AS posting_count
        FROM {schema}.{table}
        WHERE {date_col} >= DATE '{end_date}' - INTERVAL '{months} months'
          AND role_k17000_v3 IS NOT NULL
        GROUP BY 1, 2
        ORDER BY posting_count DESC
        LIMIT 5000
    """
    db = wrds.Connection(wrds_username=username, wrds_password=password)
    try:
        detail = db.raw_sql(sql)
    finally:
        db.close()

    if detail.empty:
        return detail

    summary = (
        detail.groupby("role_k17000_v3", as_index=False)
        .agg(posting_count=("posting_count", "sum"), top_role_k1500=("role_k1500_v2", "first"))
        .sort_values("posting_count", ascending=False)
    )
    summary["cs_hint_score"] = summary["role_k17000_v3"].map(_cs_relevance)
    existing = _load_existing_roles()
    summary["in_allowlist"] = summary["role_k17000_v3"].isin(existing)
    summary = summary.sort_values(
        ["in_allowlist", "cs_hint_score", "posting_count"],
        ascending=[True, False, False],
    )
    return summary.head(limit)


def main() -> None:
    parser = argparse.ArgumentParser(description="Discover WRDS role_k17000_v3 candidates for cs_universe.yml")
    parser.add_argument("--months", type=int, default=1, help="Lookback window from extract end_date (default 1).")
    parser.add_argument("--limit", type=int, default=50)
    parser.add_argument("--write-candidates", action="store_true", help="Write CSV under metadata/taxonomy_reports/")
    args = parser.parse_args()

    summary = discover_roles(months=args.months, limit=args.limit)
    if summary.empty:
        print("No roles returned from WRDS.")
        return

    print(summary.head(25).to_string(index=False))
    candidates = summary[(~summary["in_allowlist"]) & (summary["cs_hint_score"] > 0)].head(15)
    print("\nSuggested new CS role candidates:")
    for row in candidates.itertuples(index=False):
        print(f"  - role_id: \"{row.role_k17000_v3}\"  # ~{int(row.posting_count):,} postings; e.g. {row.top_role_k1500}")

    if args.write_candidates:
        REPORT_DIR.mkdir(parents=True, exist_ok=True)
        run_id = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        out = REPORT_DIR / f"role_frequency_discovery_{run_id}.csv"
        summary.to_csv(out, index=False)
        print(f"\nWrote {out}")


if __name__ == "__main__":
    main()
