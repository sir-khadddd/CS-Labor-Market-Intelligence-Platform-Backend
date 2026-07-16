from __future__ import annotations

import os
from pathlib import Path
import sys
import uuid
import io
import csv
import re

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

# Mapping of table names to integer count columns that may be exported with .0
# These columns should be cleaned from "6963.0" to "6963" before COPY
INTEGER_COUNT_COLUMNS = {
    "cs_job_demand": ["posting_count"],
    "cs_skill_demand": ["skill_posting_count", "role_posting_count"],
    "role_skill_associations": ["co_occurrence_count"],
    "salary_distribution": [],
    "trajectory_features": ["posting_count"],
    "trajectory_labels": [],
}

INTEGER_AS_FLOAT_PATTERN = re.compile(r"^([+-]?\d+)\.0+$")


def _clean_integer_value(value: str) -> str:
    """Convert integer-like float strings to valid integer strings."""
    stripped = value.strip()

    if not stripped or stripped.lower() in {"nan", "none", "null"}:
        return ""

    match = INTEGER_AS_FLOAT_PATTERN.fullmatch(stripped)
    if match:
        return match.group(1)

    return stripped


def _exec_file(cur: psycopg.Cursor, path: Path) -> None:
    cur.execute(path.read_text(encoding="utf-8"))


def _clean_integer_columns(csv_path: Path, table_name: str) -> io.StringIO:
    """
    Read CSV and clean integer columns that are exported with decimal notation.

    Some data sources export integer counts with '.0' suffix (e.g., "6963.0").
    Postgres BIGINT columns reject these as invalid. This function converts
    them back to integer strings (e.g., "6963") while preserving null values.

    Args:
        csv_path: Path to the CSV file
        table_name: Name of the table (used to lookup which columns to clean)

    Returns:
        A StringIO object containing the cleaned CSV data
    """
    columns_to_clean = INTEGER_COUNT_COLUMNS.get(table_name, [])

    if not columns_to_clean:
        # No cleaning needed for this table
        return io.StringIO(csv_path.read_text(encoding="utf-8"))

    # Read CSV with pandas to preserve column order and types
    df = pd.read_csv(csv_path)

    # Clean each integer count column
    for col in columns_to_clean:
        if col in df.columns:
            # Convert to string to handle .0 patterns
            df[col] = df[col].astype(str)

            def clean_float_string(val):
                """Convert '6963.0' to '6963', preserve nulls as empty strings"""
                if pd.isna(val) or val == "" or val == "NaN":
                    return ""
                val_str = str(val).strip()
                if val_str == "" or val_str == "NaN":
                    return ""
                # If ends with .0, remove it
                if val_str.endswith(".0"):
                    return val_str[:-2]
                # If it's a float that equals its int, convert to int string
                try:
                    float_val = float(val_str)
                    if float_val == int(float_val):
                        return str(int(float_val))
                except (ValueError, OverflowError):
                    pass
                return val_str

            df[col] = df[col].apply(clean_float_string)

    # Write cleaned data to StringIO buffer
    output = io.StringIO()
    df.to_csv(output, index=False, quoting=csv.QUOTE_MINIMAL)
    output.seek(0)
    return output


def _copy_csv(cur: psycopg.Cursor, table_name: str, csv_path: Path) -> None:
    cur.execute(f"TRUNCATE TABLE analytics.{table_name};")
    # Clean CSV data before loading (handles .0 in integer columns)
    cleaned_csv = _clean_integer_columns(csv_path, table_name)
    with cur.copy(f"COPY analytics.{table_name} FROM STDIN WITH (FORMAT CSV, HEADER TRUE)") as copy:
        for line in cleaned_csv:
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
