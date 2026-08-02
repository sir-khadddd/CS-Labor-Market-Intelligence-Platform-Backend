"""
Find posting-level skill tables on WRDS (revelio_job_postings and related libraries).

Writes reports under data/raw_cs_snapshot/metadata/taxonomy_reports/.
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
POSTINGS_LIB = "revelio_job_postings"
POSTINGS_TABLE = "postings_cosmos"
JOB_KEY_CANDIDATES = ("job_id", "posting_id", "id")


def _log(msg: str) -> None:
    print(msg, flush=True)


def _list_tables(db: wrds.Connection, library: str) -> list[str]:
    try:
        tables = db.list_tables(library)
        if isinstance(tables, pd.DataFrame):
            return tables.iloc[:, 0].astype(str).tolist()
        return [str(t) for t in tables]
    except Exception as exc:
        _log(f"  list_tables({library}) failed: {exc}")
        return []


def _describe(db: wrds.Connection, library: str, table: str) -> pd.DataFrame:
    try:
        return db.describe_table(library, table)
    except Exception as exc:
        return pd.DataFrame({"error": [str(exc)]})


def _column_names(desc: pd.DataFrame) -> list[str]:
    if desc.empty:
        return []
    for col in ("name", "column_name"):
        if col in desc.columns:
            return desc[col].astype(str).str.lower().tolist()
    return []


def _score_posting_skill_table(
    library: str, table: str, columns: list[str]
) -> dict:
    cols = set(columns)
    has_job = any(c in cols for c in JOB_KEY_CANDIDATES)
    skill_cols = [c for c in columns if "skill" in c]
    has_k35000 = any("skill_k35000" in c or c == "skill_k35000" for c in cols)
    has_mapped = "skill_mapped" in cols
    score = 0
    if has_job:
        score += 3
    if skill_cols:
        score += 2
    if has_k35000:
        score += 5
    if has_mapped:
        score += 1
    if "posting" in table.lower() or "cosmos" in table.lower() or "job" in table.lower():
        score += 1
    return {
        "library": library,
        "table_name": table,
        "score": score,
        "has_job_key": has_job,
        "skill_column_count": len(skill_cols),
        "skill_columns": ", ".join(skill_cols[:20]),
        "has_skill_k35000": has_k35000,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Discover posting-level skill tables on WRDS.")
    parser.add_argument("--config", default=str(CONFIG_PATH))
    parser.add_argument("--sample-limit", type=int, default=5)
    args = parser.parse_args()

    cfg = yaml.safe_load(Path(args.config).read_text(encoding="utf-8"))
    username = os.getenv(cfg["wrds"]["username_env"], "")
    password = os.getenv(cfg["wrds"].get("password_env", "WRDS_PASSWORD"))
    if not username:
        raise ValueError(f"Missing WRDS username env var: {cfg['wrds']['username_env']}")

    run_id = f"posting_skills_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:6]}"
    root = get_source_root()
    report_dir = root / "metadata" / "taxonomy_reports"
    report_dir.mkdir(parents=True, exist_ok=True)

    _log("Connecting to WRDS...")
    db = wrds.Connection(wrds_username=username, wrds_password=password)
    try:
        libs = db.list_libraries()
        if isinstance(libs, pd.DataFrame) and "name" in libs.columns:
            lib_names = libs["name"].astype(str).tolist()
        else:
            lib_names = [str(x) for x in libs]
        revelio_libs = sorted({n for n in lib_names if "revelio" in n.lower()})
        _log(f"Revelio libraries ({len(revelio_libs)}): {', '.join(revelio_libs)}")

        all_tables: list[dict] = []
        candidates: list[dict] = []

        for lib in revelio_libs:
            _log(f"Listing tables in {lib}...")
            for table in _list_tables(db, lib):
                all_tables.append({"library": lib, "table_name": table})
                desc = _describe(db, lib, table)
                cols = _column_names(desc)
                if not cols:
                    continue
                skillish = [c for c in cols if "skill" in c]
                jobish = [c for c in cols if c in JOB_KEY_CANDIDATES or "job_id" in c]
                if skillish or (lib == POSTINGS_LIB and jobish):
                    row = _score_posting_skill_table(lib, table, cols)
                    row["job_columns"] = ", ".join(jobish[:5])
                    candidates.append(row)

        tables_path = report_dir / f"revelio_all_tables_{run_id}.csv"
        pd.DataFrame(all_tables).to_csv(tables_path, index=False)
        _log(f"Wrote {tables_path} ({len(all_tables)} tables)")

        cand_df = pd.DataFrame(candidates)
        if not cand_df.empty:
            cand_df = cand_df.sort_values("score", ascending=False)
        cand_path = report_dir / f"posting_skill_table_candidates_{run_id}.csv"
        cand_df.to_csv(cand_path, index=False)
        _log(f"Wrote {cand_path} ({len(cand_df)} candidates)")

        _log(f"Describing {POSTINGS_LIB}.{POSTINGS_TABLE}...")
        postings_desc = _describe(db, POSTINGS_LIB, POSTINGS_TABLE)
        postings_cols = _column_names(postings_desc)
        postings_skill_cols = [c for c in postings_cols if "skill" in c]
        _log(f"  postings_cosmos skill columns: {postings_skill_cols or '(none)'}")

        samples: list[dict] = []
        top = cand_df.head(8) if not cand_df.empty else pd.DataFrame()
        for _, row in top.iterrows():
            lib, table = row["library"], row["table_name"]
            if int(row.get("score", 0)) < 3:
                continue
            cols = row.get("skill_columns", "")
            if not cols and "skill" not in str(row):
                continue
            skill_col = "skill_k35000" if "skill_k35000" in cols else (
                cols.split(",")[0].strip() if cols else "skill_k35000"
            )
            job_col = "job_id"
            for jc in ("job_id", "posting_id"):
                if jc in str(row.get("job_columns", "")):
                    job_col = jc
                    break
            sql = f"""
                SELECT {job_col}, {skill_col}
                FROM {lib}.{table}
                WHERE {skill_col} IS NOT NULL
                LIMIT {int(args.sample_limit)}
            """
            _log(f"  Sample {lib}.{table}...")
            try:
                sample = db.raw_sql(sql)
                sample.insert(0, "source_table", f"{lib}.{table}")
                out = report_dir / f"posting_skill_sample_{lib}_{table}_{run_id}.csv"
                sample.to_csv(out, index=False)
                _log(f"    Wrote {out} ({len(sample)} rows)")
                samples.append({"table": f"{lib}.{table}", "path": str(out), "rows": len(sample)})
            except Exception as exc:
                _log(f"    Sample failed: {exc}")
                samples.append({"table": f"{lib}.{table}", "error": str(exc)})

        manifest = {
            "run_id": run_id,
            "postings_cosmos_skill_columns": postings_skill_cols,
            "revelio_libraries": revelio_libs,
            "top_candidates": cand_df.head(15).to_dict(orient="records") if not cand_df.empty else [],
            "samples": samples,
            "reports": {
                "all_tables": str(tables_path),
                "candidates": str(cand_path),
            },
        }
        manifest_path = root / "metadata" / f"{run_id}.json"
        manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
        _log(f"Done. Manifest: {manifest_path}")
        if not cand_df.empty:
            _log("Top candidates:")
            for _, r in cand_df.head(5).iterrows():
                _log(
                    f"  {r['library']}.{r['table_name']} score={r['score']} "
                    f"skills={r.get('skill_columns', '')[:80]}"
                )
    finally:
        db.close()


if __name__ == "__main__":
    main()
