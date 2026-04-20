"""Database dependencies and connection management."""

import os
from typing import Optional
import duckdb
import psycopg

# Database connection instances
_duckdb_conn: Optional[duckdb.DuckDBPyConnection] = None
_postgres_conn: Optional[psycopg.Connection] = None


def get_duckdb_connection() -> duckdb.DuckDBPyConnection:
    """Get or create DuckDB connection."""
    global _duckdb_conn
    if _duckdb_conn is None:
        db_path = os.getenv("DUCKDB_PATH", "analytics.duckdb")
        _duckdb_conn = duckdb.connect(db_path, read_only=True)
    return _duckdb_conn


def get_postgres_connection() -> psycopg.Connection:
    """Get or create PostgreSQL connection."""
    global _postgres_conn
    if _postgres_conn is None:
        connstr = os.getenv(
            "DATABASE_URL",
            "postgresql://localhost/analytics"
        )
        _postgres_conn = psycopg.connect(connstr)
    return _postgres_conn


def close_connections():
    """Close all database connections."""
    global _duckdb_conn, _postgres_conn
    if _duckdb_conn is not None:
        _duckdb_conn.close()
        _duckdb_conn = None
    if _postgres_conn is not None:
        _postgres_conn.close()
        _postgres_conn = None
