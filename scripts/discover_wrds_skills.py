"""
Discover Revelio skill tables/columns on WRDS and write monthly taxonomy reports.

April 2026+ taxonomy: use skill_k35000 on individual_user_skills (skill_mapped deprecated).
Broader buckets: join individual_user_skill_lookup on skill_k35000.

Avoids full-table COUNT on individual_user_skills (very slow on WRDS). Default path
reads the small lookup table; optional --profile-counts uses TABLESAMPLE.
"""

from __future__ import annotations

import argparse
import json
import os
from datetime import datetime, timezone
from pathlib import Path
import sys
import uuid

import pandas as pd
import wrds
import yaml

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from config.backend_env import load_backend_env
from config.revelio_sources import get_source_root

load_backend_env()

CONFIG_PATH = ROOT / "config" / "wrds_extract.yml"
DEFAULT_SKILL_TABLE = "individual_user_skills"
DEFAULT_LOOKUP_TABLE = "individual_user_skill_lookup"
DEFAULT_SKILL_ID_COL = "skill_k35000"
DEFAULT_POSTINGS_LIBRARY = "revelio_job_postings"
DEFAULT_POSTINGS_TABLE = "postings_cosmos"

# Libraries to try when resolving skill tables (first match wins).
SKILL_LIBRARY_CANDIDATES = (
    "revelio",
    "revelio_individual",
    "revelio_labs",
    "revelio_job_postings",
)


def _log(msg: str) -> None:
    print(msg, flush=True)


def _load_skill_allowlist() -> list[str]:
    path = ROOT / "config" / "cs_universe.yml"
    cfg = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    skills = cfg.get("skills") or []
    return [str(s.get("skill_id", "")).strip() for s in skills if str(s.get("skill_id", "")).strip()]


def _safe_query(db: wrds.Connection, sql: str, label: str) -> pd.DataFrame:
    _log(f"  SQL ({label})...")
    try:
        out = db.raw_sql(sql)
        _log(f"  done ({label}): {len(out)} rows")
        return out
    except Exception as exc:
        _log(f"  failed ({label}): {exc}")
        return pd.DataFrame({"error": [str(exc)], "query_label": [label]})


def _list_revelio_libraries(db: wrds.Connection) -> list[str]:
    try:
        libs = db.list_libraries()
        if isinstance(libs, pd.DataFrame) and "name" in libs.columns:
            names = libs["name"].astype(str).tolist()
        elif isinstance(libs, (list, tuple)):
            names = [str(x) for x in libs]
        else:
            names = [str(x) for x in libs]
        return [n for n in names if "revelio" in n.lower()]
    except Exception as exc:
        _log(f"  list_libraries failed: {exc}")
        return list(SKILL_LIBRARY_CANDIDATES)


def _inventory_skill_tables(db: wrds.Connection) -> pd.DataFrame:
    rows: list[dict[str, str]] = []
    for lib in _list_revelio_libraries(db):
        try:
            tables = db.list_tables(lib)
            if isinstance(tables, pd.DataFrame):
                table_names = tables.iloc[:, 0].astype(str).tolist()
            else:
                table_names = [str(t) for t in tables]
        except Exception as exc:
            rows.append({"library": lib, "table_name": "", "note": f"list_tables error: {exc}"})
            continue
        for name in table_names:
            if "skill" in name.lower():
                rows.append({"library": lib, "table_name": name, "note": ""})
    if not rows:
        return pd.DataFrame(columns=["library", "table_name", "note"])
    return pd.DataFrame(rows)


def _resolve_table(
    inventory: pd.DataFrame,
    table_name: str,
    cli_library: str | None,
) -> tuple[str, str] | None:
    if cli_library:
        return cli_library, table_name
    if inventory.empty:
        for lib in SKILL_LIBRARY_CANDIDATES:
            return lib, table_name
        return None
    hits = inventory[inventory["table_name"].str.lower() == table_name.lower()]
    if not hits.empty:
        row = hits.iloc[0]
        return str(row["library"]), table_name
    return None


def _describe_columns(db: wrds.Connection, library: str, table: str) -> pd.DataFrame:
    try:
        desc = db.describe_table(library, table)
        if isinstance(desc, pd.DataFrame):
            return desc
    except Exception:
        pass
    return _safe_query(
        db,
        f"""
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = '{library}'
          AND table_name = '{table}'
        ORDER BY ordinal_position
        """,
        f"columns {library}.{table}",
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Discover WRDS Revelio skill taxonomy (Apr 2026+).")
    parser.add_argument("--config", default=str(CONFIG_PATH), help="Path to wrds_extract.yml for credentials.")
    parser.add_argument("--skill-library", default=None, help="WRDS library/schema for skill tables.")
    parser.add_argument("--skill-table", default=DEFAULT_SKILL_TABLE)
    parser.add_argument("--lookup-table", default=DEFAULT_LOOKUP_TABLE)
    parser.add_argument("--skill-id-col", default=DEFAULT_SKILL_ID_COL)
    parser.add_argument("--frequency-limit", type=int, default=500)
    parser.add_argument(
        "--profile-counts",
        action="store_true",
        help=(
            "Run COUNT(*) GROUP BY on individual_user_skills via TABLESAMPLE SYSTEM (1). "
            "Can still take several minutes."
        ),
    )
    parser.add_argument(
        "--sample-percent",
        type=float,
        default=1.0,
        help="TABLESAMPLE SYSTEM (n) percent when --profile-counts is set (default 1).",
    )
    args = parser.parse_args()

    cfg = yaml.safe_load(Path(args.config).read_text(encoding="utf-8"))
    username = os.getenv(cfg["wrds"]["username_env"], "")
    password = os.getenv(cfg["wrds"].get("password_env", "WRDS_PASSWORD"))
    if not username:
        raise ValueError(f"Missing WRDS username env var: {cfg['wrds']['username_env']}")

    run_id = f"skills_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:6]}"
    root = get_source_root()
    report_dir = root / "metadata" / "taxonomy_reports"
    report_dir.mkdir(parents=True, exist_ok=True)
    allowlist = _load_skill_allowlist()

    _log("Connecting to WRDS...")
    db = wrds.Connection(wrds_username=username, wrds_password=password)
    try:
        _log("Step 1/5: inventory skill-related tables (WRDS list_tables)...")
        inventory = _inventory_skill_tables(db)
        inventory_path = report_dir / f"skill_table_inventory_{run_id}.csv"
        inventory.to_csv(inventory_path, index=False)
        _log(f"Wrote {inventory_path} ({len(inventory)} rows)")

        resolved = _resolve_table(inventory, args.skill_table, args.skill_library)
        if not resolved:
            _log("Could not resolve skill library; pass --skill-library explicitly.")
            return
        skill_lib, skill_table = resolved
        lookup_lib = args.skill_library or skill_lib
        _log(f"Using skill table: {skill_lib}.{skill_table}")

        _log(f"Step 2/5: postings skill columns ({DEFAULT_POSTINGS_LIBRARY}.{DEFAULT_POSTINGS_TABLE})...")
        postings_cols = _describe_columns(db, DEFAULT_POSTINGS_LIBRARY, DEFAULT_POSTINGS_TABLE)
        if not postings_cols.empty and "column_name" in postings_cols.columns:
            postings_cols = postings_cols[
                postings_cols["column_name"].astype(str).str.contains("skill", case=False, na=False)
            ]
        postings_path = report_dir / f"postings_skill_columns_{run_id}.csv"
        postings_cols.to_csv(postings_path, index=False)
        _log(f"Wrote {postings_path} ({len(postings_cols)} rows)")

        skill_col = args.skill_id_col
        _log(f"Step 3/5: skill taxonomy from lookup ({lookup_lib}.{args.lookup_table})...")
        lookup_cols = _describe_columns(db, lookup_lib, args.lookup_table)
        lookup_cols_path = report_dir / f"skill_lookup_columns_{run_id}.csv"
        lookup_cols.to_csv(lookup_cols_path, index=False)
        _log(f"Wrote {lookup_cols_path}")

        # Fast allowlist report: entire lookup is ~35k rows, no scan of user_skills.
        lookup_sql = f"""
            SELECT *
            FROM {lookup_lib}.{args.lookup_table}
            ORDER BY {skill_col}
            LIMIT {int(args.frequency_limit)}
        """
        lookup_sample = _safe_query(db, lookup_sql, "lookup sample")
        lookup_sample_path = report_dir / f"skill_lookup_sample_{run_id}.csv"
        lookup_sample.to_csv(lookup_sample_path, index=False)
        _log(f"Wrote {lookup_sample_path}")

        freq_path = report_dir / f"skill_k35000_frequency_{run_id}.csv"
        if args.profile_counts:
            _log(
                f"Step 4/5: profile skill counts (TABLESAMPLE {args.sample_percent}%% on "
                f"{skill_lib}.{skill_table}) — may take minutes..."
            )
            pct = max(0.01, min(float(args.sample_percent), 100.0))
            freq_sql = f"""
                SELECT {skill_col} AS skill_id, COUNT(*) AS row_count
                FROM {skill_lib}.{skill_table} TABLESAMPLE SYSTEM ({pct})
                WHERE {skill_col} IS NOT NULL
                GROUP BY 1
                ORDER BY 2 DESC
                LIMIT {int(args.frequency_limit)}
            """
            freq = _safe_query(db, freq_sql, "profile counts")
        else:
            _log("Step 4/5: skip profile counts (default). Use --profile-counts for sampled popularity.")
            if skill_col in lookup_sample.columns:
                freq = (
                    lookup_sample[[skill_col]]
                    .drop_duplicates()
                    .rename(columns={skill_col: "skill_id"})
                )
                freq["row_count"] = pd.NA
                freq["source"] = "lookup_table_only"
            else:
                freq = pd.DataFrame(
                    columns=["skill_id", "row_count", "source"],
                    data=[],
                )

        if "skill_id" in freq.columns:
            allow_set = set(allowlist)
            freq["in_allowlist"] = freq["skill_id"].astype(str).isin(allow_set)
            if "row_count" in freq.columns and freq["row_count"].notna().any():
                freq = freq.sort_values(["in_allowlist", "row_count"], ascending=[True, False])
        freq.to_csv(freq_path, index=False)
        _log(f"Wrote {freq_path} ({len(freq)} rows)")

        _log("Step 5/5: manifest...")
        manifest = {
            "run_id": run_id,
            "run_timestamp_utc": datetime.now(timezone.utc).isoformat(),
            "taxonomy": "revelio_skills_april_2026",
            "skill_id_column": skill_col,
            "skill_table": f"{skill_lib}.{skill_table}",
            "lookup_table": f"{lookup_lib}.{args.lookup_table}",
            "profile_counts_ran": bool(args.profile_counts),
            "reports": {
                "inventory": str(inventory_path),
                "postings_skill_columns": str(postings_path),
                "skill_lookup_columns": str(lookup_cols_path),
                "skill_lookup_sample": str(lookup_sample_path),
                "skill_frequency": str(freq_path),
            },
            "cs_skill_allowlist_size": len(allowlist),
            "notes": (
                "Default run avoids full COUNT on individual_user_skills. "
                "skill_mapped deprecated; use skill_k35000."
            ),
        }
        manifest_path = root / "metadata" / f"{run_id}.json"
        manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
        _log(f"Discovery complete. run_id={run_id}")
        _log(f"Manifest: {manifest_path}")
    finally:
        db.close()


if __name__ == "__main__":
    main()
