"""Database dependencies and connection management."""

import logging
import os
from typing import Optional, Generator
import duckdb
import psycopg
import psycopg_pool

from config.postgres import check_postgres_health, get_postgres_dsn, redact_postgres_dsn

logger = logging.getLogger(__name__)

# Database connection instances
_duckdb_conn: Optional[duckdb.DuckDBPyConnection] = None
_postgres_pool: Optional[psycopg_pool.ConnectionPool] = None


def get_duckdb_connection() -> duckdb.DuckDBPyConnection:
    """Get or create DuckDB connection."""
    global _duckdb_conn
    if _duckdb_conn is None:
        db_path = os.getenv("DUCKDB_PATH", "analytics.duckdb")
        _duckdb_conn = duckdb.connect(db_path, read_only=True)
    return _duckdb_conn


def get_postgres_connection() -> Generator[psycopg.Connection, None, None]:
    """FastAPI dependency that yields a PostgreSQL connection from the pool.
    
    Use with `Depends(get_postgres_connection)` in route handler parameters.
    Each request gets its own connection from the pool, preventing concurrent access issues.
    """
    global _postgres_pool
    if _postgres_pool is None:
        connstr = get_postgres_dsn()
        logger.info("Initializing Postgres connection pool (dsn=%s)", redact_postgres_dsn(connstr))
        _postgres_pool = psycopg_pool.ConnectionPool(
            connstr,
            min_size=1,
            max_size=10,
        )
        health = check_postgres_health()
        if health["status"] == "up":
            logger.info(
                "Postgres pool ready database=%s latency_ms=%s",
                health.get("database"),
                health.get("latency_ms"),
            )
        else:
            logger.warning(
                "Postgres pool created but health check failed: %s",
                health.get("error", "unknown error"),
            )

    # Provide a connection from the pool for the duration of the request
    with _postgres_pool.connection() as conn:
        yield conn


def close_connections():
    """Close all database connections and pools."""
    global _duckdb_conn, _postgres_pool
    if _duckdb_conn is not None:
        logger.info("Closing DuckDB connection")
        _duckdb_conn.close()
        _duckdb_conn = None
    if _postgres_pool is not None:
        logger.info("Closing Postgres connection pool")
        _postgres_pool.close()
        _postgres_pool = None


def get_postgres_health(include_table_counts: bool = False) -> dict:
    """Return Postgres health for API endpoints."""
    if _postgres_pool is not None:
        with _postgres_pool.connection() as conn:
            return check_postgres_health(conn, include_table_counts=include_table_counts)
    return check_postgres_health(include_table_counts=include_table_counts)
