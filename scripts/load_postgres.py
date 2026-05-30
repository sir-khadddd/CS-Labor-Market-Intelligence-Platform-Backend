from __future__ import annotations

import logging
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

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

SQL_SCHEMA = ROOT / "sql" / "postgres" / "schema.sql"
SQL_INDEXES = ROOT / "sql" / "postgres" / "indexes.sql"
SQL_METADATA = ROOT / "sql" / "postgres" / "metadata_contracts.sql"
PROCESSED_DIR = ROOT / "data" / "processed"

COPY_PROGRESS_EVERY_ROWS = 500_000

TABLE_PRIMARY_KEYS: dict[str, list[str]] = {
    "cs_job_demand": ["month", "geo_id", "industry_id", "role_id"],
    "cs_skill_demand": ["month", "geo_id", "role_id", "skill_id"],
    "role_skill_associations": ["month", "role_id", "skill_id"],
    "salary_distribution": ["month", "geo_id", "industry_id", "role_id"],
    "trajectory_features": ["entity_type", "entity_id", "month", "feature_version"],
    "trajectory_labels": ["entity_type", "entity_id", "month", "label_version", "method_version"],
}

DEBUG_LOG_PATH = ROOT / "debug-cb38af.log"


def _debug_log(hypothesis_id: str, location: str, message: str, data: dict) -> None:
    # #region agent log
    import json

    payload = {
        "sessionId": "cb38af",
        "hypothesisId": hypothesis_id,
        "location": location,
        "message": message,
        "data": data,
        "timestamp": int(time.time() * 1000),
        "runId": "pre-fix",
    }
    with DEBUG_LOG_PATH.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload) + "\n")
    # #endregion


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
    _debug_log(
        "H1",
        "load_postgres.py:_check_csv_primary_keys",
        "csv primary key scan",
        {"table": table_name, **stats},
    )
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


def _copy_csv(cur: psycopg.Cursor, table_name: str, csv_path: Path) -> int:
    size_mb = csv_path.stat().st_size / (1024 * 1024)
    logger.info("Loading %s from %s (%.1f MB)", table_name, csv_path.name, size_mb)

    started = time.perf_counter()
    cur.execute(f"TRUNCATE TABLE analytics.{table_name};")

    row_count = 0
    with csv_path.open("r", encoding="utf-8") as handle:
        with cur.copy(
            f"COPY analytics.{table_name} FROM STDIN WITH (FORMAT CSV, HEADER TRUE)"
        ) as copy:
            for line in handle:
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


def main() -> None:
    dsn = get_postgres_dsn()
    safe_dsn = redact_postgres_dsn(dsn)
    run_id = f"run_{uuid.uuid4().hex[:12]}"
    load_started = time.perf_counter()

    logger.info("Starting Postgres load (run_id=%s, dsn=%s)", run_id, safe_dsn)
    logger.info("Processed CSV directory: %s", PROCESSED_DIR)

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
        with conn.cursor() as cur:
            _exec_file(cur, SQL_SCHEMA, "schema")
            _exec_file(cur, SQL_METADATA, "metadata contracts")

            for table in ANALYTICS_TABLES:
                csv_path = PROCESSED_DIR / f"{table}.csv"
                if not csv_path.exists():
                    raise FileNotFoundError(f"Missing processed CSV: {csv_path}")
                pk_stats = _check_csv_primary_keys(table, csv_path)
                logger.info(
                    "Validated %s primary keys: %s rows, %s distinct keys",
                    table,
                    f"{pk_stats['total_rows']:,}",
                    f"{pk_stats['distinct_keys']:,}",
                )
                row_counts[table] = _copy_csv(cur, table, csv_path)

            _exec_file(cur, SQL_INDEXES, "indexes")

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
                (run_id, "phase1-v1", "phase1-v1", "rules-v1", "2026.04", "local load"),
            )

        logger.info("Committing transaction...")
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
