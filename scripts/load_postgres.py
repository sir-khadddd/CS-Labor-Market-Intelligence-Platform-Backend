from __future__ import annotations

import argparse
import csv
import io
import logging
import re
import sys
import time
import uuid
from pathlib import Path

import pandas as pd
import psycopg

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from config.postgres import (
    ANALYTICS_TABLES,
    check_postgres_health,
    get_postgres_dsn,
    redact_postgres_dsn,
)
from ml.constants import (
    CS_ALLOWLIST_VERSION,
    FEATURE_VERSION,
    LABEL_VERSION,
    METHOD,
    RULES_METHOD_VERSION,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

SQL_SCHEMA = ROOT / "sql" / "postgres" / "schema.sql"
SQL_METADATA = ROOT / "sql" / "postgres" / "metadata_contracts.sql"
SQL_INDEXES = ROOT / "sql" / "postgres" / "indexes.sql"
PROCESSED_DIR = ROOT / "data" / "processed"

LOCK_TIMEOUT = "30s"
STATEMENT_TIMEOUT = "0"

COPY_PROGRESS_EVERY_ROWS = 500_000

# DuckDB CSV export writes integer counts as floats ("6963.0"); Postgres BIGINT rejects those.
INTEGER_COUNT_COLUMNS: dict[str, list[str]] = {
    "cs_job_demand": ["posting_count"],
    "cs_skill_demand": ["skill_posting_count", "role_posting_count"],
    "role_skill_associations": ["co_occurrence_count"],
    "salary_distribution": [],
    "trajectory_features": ["posting_count"],
    "trajectory_labels": [],
}
INTEGER_AS_FLOAT_PATTERN = re.compile(r"^([+-]?\d+)\.0+$")

TABLE_PRIMARY_KEYS: dict[str, list[str]] = {
    "cs_job_demand": ["month", "geo_id", "industry_id", "role_id"],
    "cs_skill_demand": ["month", "geo_id", "role_id", "skill_id"],
    "role_skill_associations": ["month", "role_id", "skill_id"],
    "salary_distribution": ["month", "geo_id", "industry_id", "role_id"],
    "trajectory_features": ["entity_type", "entity_id", "month", "feature_version"],
    "trajectory_labels": ["entity_type", "entity_id", "month", "label_version", "method_version"],
}

INDEX_STMT_PATTERN = re.compile(
    r"(CREATE INDEX IF NOT EXISTS\s+\S+\s+ON analytics\.(\w+)\s+\([^;]+\);)",
    re.IGNORECASE | re.DOTALL,
)

TABLE_DEF_PATTERN = re.compile(
    r"CREATE TABLE IF NOT EXISTS analytics\.(\w+)\s*\((.*?)\n\);",
    re.IGNORECASE | re.DOTALL,
)
TEXT_COLUMN_PATTERN = re.compile(r"^\s*(\w+)\s+TEXT\b", re.IGNORECASE)


def _load_table_indexes() -> dict[str, list[str]]:
    """Parse sql/postgres/indexes.sql (source of truth for analytics indexes)."""
    text = SQL_INDEXES.read_text(encoding="utf-8")
    indexes: dict[str, list[str]] = {}
    for match in INDEX_STMT_PATTERN.finditer(text):
        stmt = re.sub(r"\s+", " ", match.group(1).strip())
        table_name = match.group(2)
        indexes.setdefault(table_name, []).append(stmt)
    return indexes


def _load_table_text_columns() -> dict[str, list[str]]:
    """Parse TEXT columns per analytics table from sql/postgres/schema.sql.

    COPY ... FORMAT CSV maps an unquoted empty field to NULL, and the cleaning
    pass below cannot tell an empty string apart from a NULL once the CSV is
    parsed. Listing these columns in FORCE_NOT_NULL keeps empty text as '',
    while numeric columns keep the empty-means-NULL behaviour they need.
    """
    text = SQL_SCHEMA.read_text(encoding="utf-8")
    columns: dict[str, list[str]] = {}
    for table_name, body in TABLE_DEF_PATTERN.findall(text):
        names = [
            match.group(1)
            for match in (TEXT_COLUMN_PATTERN.match(line) for line in body.splitlines())
            if match
        ]
        if names:
            columns[table_name] = names
    return columns


TABLE_INDEXES = _load_table_indexes()
TABLE_TEXT_COLUMNS = _load_table_text_columns()


def _configure_session(cur: psycopg.Cursor) -> None:
    cur.execute(f"SET lock_timeout = '{LOCK_TIMEOUT}'")
    cur.execute(f"SET statement_timeout = '{STATEMENT_TIMEOUT}'")


def _check_csv_primary_keys(table_name: str, csv_path: Path) -> dict[str, int]:
    pk_cols = TABLE_PRIMARY_KEYS[table_name]
    df = pd.read_csv(csv_path, usecols=pk_cols)
    total_rows = len(df)
    duplicate_rows = int(df.duplicated(subset=pk_cols).sum())
    distinct_keys = int(df.drop_duplicates(subset=pk_cols).shape[0])
    stats = {
        "total_rows": total_rows,
        "duplicate_rows": duplicate_rows,
        "distinct_keys": distinct_keys,
    }
    if duplicate_rows:
        logger.error(
            "%s CSV has %s duplicate primary-key rows (%s total, %s distinct keys)",
            table_name,
            f"{duplicate_rows:,}",
            f"{total_rows:,}",
            f"{distinct_keys:,}",
        )
        raise ValueError(
            f"{table_name} CSV contains duplicate primary keys "
            f"({duplicate_rows:,} duplicates). Rebuild processed outputs with build_duckdb.py."
        )
    return stats


def _exec_file(cur: psycopg.Cursor, path: Path, label: str) -> None:
    started = time.perf_counter()
    logger.info("Applying SQL file: %s", path.name)
    cur.execute(path.read_text(encoding="utf-8"))
    logger.info("Finished %s in %.1fs", label, time.perf_counter() - started)


def _clean_integer_cell(value: str) -> str:
    stripped = value.strip()
    if not stripped or stripped.lower() in {"nan", "none", "null"}:
        return ""
    match = INTEGER_AS_FLOAT_PATTERN.fullmatch(stripped)
    if match:
        return match.group(1)
    return stripped


def _copy_sql(table_name: str) -> str:
    """Build the COPY statement, forcing empty text cells to stay empty strings."""
    options = ["FORMAT CSV", "HEADER TRUE"]
    text_columns = TABLE_TEXT_COLUMNS.get(table_name, [])
    if text_columns:
        options.append(f"FORCE_NOT_NULL ({', '.join(text_columns)})")
    return f"COPY analytics.{table_name} FROM STDIN WITH ({', '.join(options)})"


def _iter_cleaned_csv_lines(csv_path: Path, table_name: str):
    """Yield CSV text lines, normalizing integer-count float strings for Postgres.

    Quoting stays minimal so an empty numeric cell is emitted unquoted and COPY
    reads it as NULL; empty text cells are protected by FORCE_NOT_NULL instead
    (see _copy_sql).
    """
    columns_to_clean = INTEGER_COUNT_COLUMNS.get(table_name, [])
    with csv_path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.reader(handle)
        try:
            header = next(reader)
        except StopIteration:
            return

        out = io.StringIO()
        writer = csv.writer(out, lineterminator="\n")
        writer.writerow(header)
        yield out.getvalue()

        if not columns_to_clean:
            for row in reader:
                out.seek(0)
                out.truncate(0)
                writer.writerow(row)
                yield out.getvalue()
            return

        indexes = [header.index(col) for col in columns_to_clean if col in header]
        for row in reader:
            for idx in indexes:
                if idx < len(row):
                    row[idx] = _clean_integer_cell(row[idx])
            out.seek(0)
            out.truncate(0)
            writer.writerow(row)
            yield out.getvalue()


def _copy_csv(cur: psycopg.Cursor, table_name: str, csv_path: Path) -> int:
    size_mb = csv_path.stat().st_size / (1024 * 1024)
    logger.info("Loading %s from %s (%.1f MB)", table_name, csv_path.name, size_mb)

    started = time.perf_counter()
    if table_name == "trajectory_labels":
        logger.warning(
            "TRUNCATE of analytics.trajectory_labels removes method=%s rows, which are not "
            "in the processed CSV. Re-run scripts/predict_trajectory_model.py after this load.",
            METHOD,
        )
    cur.execute(f"TRUNCATE TABLE analytics.{table_name};")

    row_count = 0
    with cur.copy(_copy_sql(table_name)) as copy:
        for line in _iter_cleaned_csv_lines(csv_path, table_name):
            copy.write(line)
            row_count += 1
            if row_count % COPY_PROGRESS_EVERY_ROWS == 0:
                elapsed = time.perf_counter() - started
                logger.info(
                    "  %s: %s rows copied (%.1fs elapsed)",
                    table_name,
                    f"{row_count:,}",
                    elapsed,
                )

    cur.execute(f"SELECT COUNT(*) FROM analytics.{table_name}")
    loaded_rows = int(cur.fetchone()[0])
    logger.info(
        "Loaded %s: %s rows in %.1fs",
        table_name,
        f"{loaded_rows:,}",
        time.perf_counter() - started,
    )
    return loaded_rows


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Load processed analytics CSVs into Postgres."
    )
    parser.add_argument(
        "--tables",
        default=None,
        help=(
            "Comma-separated analytics tables to load "
            f"(default: all {len(ANALYTICS_TABLES)} configured tables)"
        ),
    )
    return parser.parse_args()


def _resolve_tables(tables_arg: str | None) -> list[str]:
    if not tables_arg:
        return list(ANALYTICS_TABLES)

    selected = [name.strip() for name in tables_arg.split(",") if name.strip()]
    if not selected:
        raise ValueError("--tables was provided but no table names were given")

    unknown = sorted(set(selected) - set(ANALYTICS_TABLES))
    if unknown:
        raise ValueError(
            f"Unknown table(s): {', '.join(unknown)}. "
            f"Valid choices: {', '.join(ANALYTICS_TABLES)}"
        )
    return selected


def _drop_analytics_tables(cur: psycopg.Cursor, tables: list[str]) -> None:
    for table in tables:
        logger.info("Dropping analytics.%s for schema refresh", table)
        cur.execute(f"DROP TABLE IF EXISTS analytics.{table};")


def _setup_database(
    conn: psycopg.Connection,
    *,
    full_rebuild: bool,
    selected_tables: list[str],
) -> None:
    tables_to_drop = list(ANALYTICS_TABLES) if full_rebuild else selected_tables

    with conn.cursor() as cur:
        _configure_session(cur)

        if tables_to_drop:
            _drop_analytics_tables(cur, tables_to_drop)

        _exec_file(cur, SQL_SCHEMA, "schema")
        _exec_file(cur, SQL_METADATA, "metadata contracts")

    logger.info("Committing schema setup...")
    conn.commit()


def _apply_table_indexes(cur: psycopg.Cursor, table_name: str) -> None:
    for stmt in TABLE_INDEXES.get(table_name, []):
        cur.execute(stmt)


def _load_one_table(conn: psycopg.Connection, table_name: str, csv_path: Path) -> int:
    pk_stats = _check_csv_primary_keys(table_name, csv_path)
    logger.info(
        "Validated %s primary keys: %s rows, %s distinct keys",
        table_name,
        f"{pk_stats['total_rows']:,}",
        f"{pk_stats['distinct_keys']:,}",
    )

    with conn.cursor() as cur:
        row_count = _copy_csv(cur, table_name, csv_path)
        _apply_table_indexes(cur, table_name)

    logger.info("Committing %s...", table_name)
    conn.commit()
    return row_count


def main() -> None:
    args = parse_args()
    tables = _resolve_tables(args.tables)
    full_rebuild = args.tables is None

    dsn = get_postgres_dsn()
    safe_dsn = redact_postgres_dsn(dsn)
    run_id = f"run_{uuid.uuid4().hex[:12]}"
    load_started = time.perf_counter()

    logger.info("Starting Postgres load (run_id=%s, dsn=%s)", run_id, safe_dsn)
    logger.info("Processed CSV directory: %s", PROCESSED_DIR)
    if full_rebuild:
        logger.info("Loading all configured analytics tables (full rebuild)")
    else:
        logger.info(
            "Loading selected analytics tables (schema refresh before load): %s",
            ", ".join(tables),
        )

    logger.info("Checking Postgres connectivity...")
    health = check_postgres_health()
    if health["status"] != "up":
        logger.error("Postgres is unavailable: %s", health.get("error", "unknown error"))
        raise RuntimeError(f"Postgres health check failed: {health}")

    logger.info(
        "Connected to Postgres database=%s latency_ms=%s version=%s",
        health["database"],
        health["latency_ms"],
        health["postgres_version"],
    )

    row_counts: dict[str, int] = {}

    with psycopg.connect(dsn, connect_timeout=30) as conn:
        _setup_database(conn, full_rebuild=full_rebuild, selected_tables=tables)

        for table in tables:
            csv_path = PROCESSED_DIR / f"{table}.csv"
            if not csv_path.exists():
                raise FileNotFoundError(f"Missing processed CSV: {csv_path}")
            row_counts[table] = _load_one_table(conn, table, csv_path)

        with conn.cursor() as cur:
            logger.info("Recording pipeline run metadata (run_id=%s)", run_id)
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
                (
                    run_id,
                    FEATURE_VERSION,
                    LABEL_VERSION,
                    RULES_METHOD_VERSION,
                    CS_ALLOWLIST_VERSION,
                    "local load",
                ),
            )
        logger.info("Committing pipeline run metadata...")
        conn.commit()

    total_rows = sum(row_counts.values())
    elapsed = time.perf_counter() - load_started
    logger.info(
        "Postgres load complete: %s tables, %s total rows in %.1fs",
        len(row_counts),
        f"{total_rows:,}",
        elapsed,
    )

    summary = pd.DataFrame(
        {"table": list(row_counts.keys()), "rows": list(row_counts.values())}
    )
    print(f"Loaded analytics tables into Postgres ({safe_dsn})")
    print(summary.to_string(index=False))
    print(f"metadata.pipeline_runs.run_id={run_id}")


if __name__ == "__main__":
    main()
