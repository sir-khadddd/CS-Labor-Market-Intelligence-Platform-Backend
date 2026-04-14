from __future__ import annotations

import os
from pathlib import Path
import sys
import uuid

import pandas as pd
import psycopg

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from config.backend_env import load_backend_env

load_backend_env()
SQL_SCHEMA = ROOT / "sql" / "postgres" / "schema.sql"
SQL_INDEXES = ROOT / "sql" / "postgres" / "indexes.sql"
SQL_METADATA = ROOT / "sql" / "postgres" / "metadata_contracts.sql"
PROCESSED_DIR = ROOT / "data" / "processed"


def _exec_file(cur: psycopg.Cursor, path: Path) -> None:
    cur.execute(path.read_text(encoding="utf-8"))


def _copy_csv(cur: psycopg.Cursor, table_name: str, csv_path: Path) -> None:
    cur.execute(f"TRUNCATE TABLE analytics.{table_name};")
    with csv_path.open("r", encoding="utf-8") as handle:
        with cur.copy(f"COPY analytics.{table_name} FROM STDIN WITH (FORMAT CSV, HEADER TRUE)") as copy:
            for line in handle:
                copy.write(line)


def main() -> None:
    dsn = os.getenv(
        "CS_LMI_POSTGRES_DSN",
        "postgresql://postgres:postgres@localhost:5432/cs_lmi",
    )
    run_id = f"run_{uuid.uuid4().hex[:12]}"

    table_names = [
        "cs_job_demand",
        "cs_skill_demand",
        "role_skill_associations",
        "salary_distribution",
        "trajectory_features",
        "trajectory_labels",
    ]

    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            _exec_file(cur, SQL_SCHEMA)
            _exec_file(cur, SQL_METADATA)
            for table in table_names:
                csv_path = PROCESSED_DIR / f"{table}.csv"
                if not csv_path.exists():
                    raise FileNotFoundError(f"Missing processed CSV: {csv_path}")
                _copy_csv(cur, table, csv_path)
            _exec_file(cur, SQL_INDEXES)

            cur.execute(
                """
                INSERT INTO metadata.pipeline_runs(
                    run_id, run_timestamp, feature_version, label_version, method_version,
                    cs_allowlist_version, notes
                ) VALUES (
                    %s, NOW(), %s, %s, %s, %s, %s
                )
                ON CONFLICT (run_id) DO NOTHING;
                """,
                (run_id, "phase1-v1", "phase1-v1", "rules-v1", "2026.04", "local load"),
            )
        conn.commit()

    summary = pd.DataFrame({"table": table_names})
    print(f"Loaded analytics tables into Postgres ({dsn})")
    print(summary.to_string(index=False))
    print(f"metadata.pipeline_runs.run_id={run_id}")


if __name__ == "__main__":
    main()
