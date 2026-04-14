from __future__ import annotations

from pathlib import Path
import duckdb
import yaml


ROOT = Path(__file__).resolve().parents[1]
CONFIG = ROOT / "config" / "cs_universe.yml"
PROCESSED_DIR = ROOT / "data" / "processed"
DEV_DIR = ROOT / "data" / "dev_processed"


def main() -> None:
    cfg = yaml.safe_load(CONFIG.read_text(encoding="utf-8"))
    min_row_threshold = int(cfg.get("min_row_threshold", 5))

    DEV_DIR.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    try:
        tables = [
            "cs_job_demand",
            "cs_skill_demand",
            "role_skill_associations",
            "salary_distribution",
            "trajectory_features",
            "trajectory_labels",
        ]
        for table in tables:
            src = PROCESSED_DIR / f"{table}.parquet"
            if not src.exists():
                continue
            con.execute(f"CREATE OR REPLACE TABLE tmp_{table} AS SELECT * FROM read_parquet('{src}');")

        # Thresholded aggregated-only exports for sharing in GitHub.
        export_specs = [
            (
                "cs_job_demand",
                f"SELECT * FROM tmp_cs_job_demand WHERE posting_count >= {min_row_threshold} ORDER BY month DESC, posting_count DESC LIMIT 25000",
            ),
            (
                "cs_skill_demand",
                f"SELECT * FROM tmp_cs_skill_demand WHERE skill_posting_count >= {min_row_threshold} ORDER BY month DESC, skill_posting_count DESC LIMIT 25000",
            ),
            (
                "role_skill_associations",
                f"SELECT * FROM tmp_role_skill_associations WHERE co_occurrence_count >= {min_row_threshold} ORDER BY month DESC, co_occurrence_count DESC LIMIT 25000",
            ),
            (
                "salary_distribution",
                "SELECT * FROM tmp_salary_distribution ORDER BY month DESC LIMIT 25000",
            ),
            (
                "trajectory_features",
                "SELECT * FROM tmp_trajectory_features ORDER BY month DESC LIMIT 25000",
            ),
            (
                "trajectory_labels",
                "SELECT * FROM tmp_trajectory_labels ORDER BY month DESC LIMIT 25000",
            ),
        ]

        for table, query in export_specs:
            row_count = con.execute(f"SELECT COUNT(*) FROM ({query}) q").fetchone()[0]
            target = DEV_DIR / f"{table}.csv"
            if row_count == 0 and target.exists():
                continue
            con.execute(f"COPY ({query}) TO '{target}' (HEADER, DELIMITER ',');")

        (DEV_DIR / "README.txt").write_text(
            "Aggregated, thresholded development snapshot for reproducible local setup.\n",
            encoding="utf-8",
        )
        print(f"Dev snapshot written to: {DEV_DIR}")
    finally:
        con.close()


if __name__ == "__main__":
    main()
