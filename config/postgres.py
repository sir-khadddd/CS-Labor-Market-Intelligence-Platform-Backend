"""Postgres connection configuration and health checks."""

from __future__ import annotations

import logging
import os
import time
from typing import Any
from urllib.parse import urlparse, urlunparse

import psycopg

from .backend_env import load_backend_env

load_backend_env()

logger = logging.getLogger(__name__)

DEFAULT_POSTGRES_DSN = "postgresql://postgres:postgres@localhost:5432/cs_lmi"

ANALYTICS_TABLES = (
    "cs_job_demand",
    "cs_skill_demand",
    "role_skill_associations",
    "salary_distribution",
    "trajectory_features",
    "trajectory_labels",
)


def get_postgres_dsn() -> str:
    dsn = os.getenv("CS_LMI_POSTGRES_DSN")
    if dsn:
        return dsn
    dsn = os.getenv("DATABASE_URL")
    if dsn:
        return dsn
    return DEFAULT_POSTGRES_DSN


def redact_postgres_dsn(dsn: str) -> str:
    parsed = urlparse(dsn)
    if not parsed.password:
        return dsn
    netloc = parsed.hostname or ""
    if parsed.port:
        netloc = f"{netloc}:{parsed.port}"
    if parsed.username:
        netloc = f"{parsed.username}:***@{netloc}"
    return urlunparse(parsed._replace(netloc=netloc))


def check_postgres_health(
    conn: psycopg.Connection | None = None,
    *,
    include_table_counts: bool = False,
) -> dict[str, Any]:
    """Return Postgres connectivity status and optional analytics table counts."""
    started = time.perf_counter()
    own_connection = conn is None
    if own_connection:
        try:
            conn = psycopg.connect(get_postgres_dsn(), connect_timeout=5)
        except psycopg.Error as exc:
            return {
                "status": "down",
                "latency_ms": round((time.perf_counter() - started) * 1000, 1),
                "error": exc.__class__.__name__,
            }

    assert conn is not None
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT version(), current_database()")
            version, database = cur.fetchone()

            payload: dict[str, Any] = {
                "status": "up",
                "database": database,
                "latency_ms": round((time.perf_counter() - started) * 1000, 1),
                "postgres_version": version.split(",")[0],
            }

            if include_table_counts:
                counts: dict[str, int | None] = {}
                for table in ANALYTICS_TABLES:
                    try:
                        cur.execute(f"SELECT COUNT(*) FROM analytics.{table}")
                        counts[table] = int(cur.fetchone()[0])
                    except psycopg.Error:
                        counts[table] = None
                payload["table_counts"] = counts

            return payload
    except psycopg.Error as exc:
        return {
            "status": "down",
            "latency_ms": round((time.perf_counter() - started) * 1000, 1),
            "error": exc.__class__.__name__,
        }
    finally:
        if own_connection:
            conn.close()
