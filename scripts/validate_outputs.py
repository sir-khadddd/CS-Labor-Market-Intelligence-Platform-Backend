from __future__ import annotations

from pathlib import Path
import duckdb


ROOT = Path(__file__).resolve().parents[1]
PROCESSED_DIR = ROOT / "data" / "processed"


def main() -> None:
    con = duckdb.connect()
    checks = {
        "cs_job_demand": "SELECT COUNT(*) AS c FROM read_parquet(?)",
        "cs_skill_demand": "SELECT COUNT(*) AS c FROM read_parquet(?)",
        "role_skill_associations": "SELECT COUNT(*) AS c FROM read_parquet(?)",
        "salary_distribution": "SELECT COUNT(*) AS c FROM read_parquet(?)",
        "trajectory_features": "SELECT COUNT(*) AS c FROM read_parquet(?)",
        "trajectory_labels": "SELECT COUNT(*) AS c FROM read_parquet(?)",
    }
    try:
        for table, query in checks.items():
            path = PROCESSED_DIR / f"{table}.parquet"
            if not path.exists():
                raise FileNotFoundError(f"Missing file: {path}")
            row_count = con.execute(query, [str(path)]).fetchone()[0]
            print(f"{table}: {row_count} rows")

        negatives = con.execute(
            "SELECT COUNT(*) FROM read_parquet(?) WHERE posting_count < 0",
            [str(PROCESSED_DIR / "cs_job_demand.parquet")],
        ).fetchone()[0]
        if negatives > 0:
            raise ValueError("posting_count contains negative values")

        print("Validation checks passed.")
    finally:
        con.close()


if __name__ == "__main__":
    main()
