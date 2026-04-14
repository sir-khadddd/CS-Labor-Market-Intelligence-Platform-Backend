from __future__ import annotations

import glob
from pathlib import Path
import uuid
import duckdb
import yaml
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

SQL_DIR = ROOT / "sql" / "duckdb"
CONFIG_PATH = ROOT / "config" / "cs_universe.yml"

from config.revelio_sources import get_duckdb_path, get_processed_dir, get_source_root


def _read_sql(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _load_allowlists(con: duckdb.DuckDBPyConnection, cfg: dict) -> None:
    con.execute("CREATE SCHEMA IF NOT EXISTS stage;")
    con.execute("DROP TABLE IF EXISTS stage.allowlist_roles;")
    con.execute("DROP TABLE IF EXISTS stage.allowlist_skills;")
    con.execute("CREATE TABLE stage.allowlist_roles(role_id VARCHAR, role_name VARCHAR);")
    con.execute("CREATE TABLE stage.allowlist_skills(skill_id VARCHAR, skill_name VARCHAR);")
    roles = cfg.get("roles") or []
    skills = cfg.get("skills") or []
    role_rows = [(r["role_id"].upper(), r["role_name"]) for r in roles if r.get("role_id") and r.get("role_name")]
    skill_rows = [(s["skill_id"].upper(), s["skill_name"]) for s in skills if s.get("skill_id") and s.get("skill_name")]
    if role_rows:
        con.executemany("INSERT INTO stage.allowlist_roles VALUES (?, ?)", role_rows)
    if skill_rows:
        con.executemany("INSERT INTO stage.allowlist_skills VALUES (?, ?)", skill_rows)


def _create_raw_postings(con: duckdb.DuckDBPyConnection, source_root: Path) -> None:
    pattern = str(
        source_root
        / "revelio_job_postings"
        / "postings_cosmos"
        / "year=*"
        / "month=*"
        / "*.parquet"
    )
    files = glob.glob(pattern)
    con.execute("DROP TABLE IF EXISTS stage.raw_postings;")
    con.execute("DROP TABLE IF EXISTS stage.postings_cosmos_raw;")
    con.execute("DROP TABLE IF EXISTS stage.company_mapping;")
    con.execute("DROP TABLE IF EXISTS stage.regions;")
    if files:
        con.execute(
            f"""
            CREATE TABLE stage.postings_cosmos_raw AS
            SELECT * FROM read_parquet('{pattern}', hive_partitioning=1);
            """
        )
        company_mapping_path = source_root / "revelio_common" / "company_mapping" / "company_mapping.parquet"
        if company_mapping_path.exists():
            con.execute(
                f"""
                CREATE TABLE stage.company_mapping AS
                SELECT *
                FROM read_parquet('{company_mapping_path}');
                """
            )
        else:
            con.execute(
                """
                CREATE TABLE stage.company_mapping (
                    rcid BIGINT,
                    company VARCHAR,
                    rics_k50 VARCHAR,
                    rics_k200 VARCHAR,
                    rics_k400 VARCHAR,
                    naics_code VARCHAR,
                    ultimate_parent_rcid BIGINT
                );
                """
            )

        regions_path = source_root / "revelio_common" / "regions" / "regions.parquet"
        if regions_path.exists():
            con.execute(
                f"""
                CREATE TABLE stage.regions AS
                SELECT *
                FROM read_parquet('{regions_path}');
                """
            )
        else:
            con.execute(
                """
                CREATE TABLE stage.regions (
                    country VARCHAR,
                    region VARCHAR
                );
                """
            )

        con.execute(
            """
            CREATE TABLE stage.raw_postings AS
            SELECT
                CAST(p.job_id AS BIGINT) AS job_id,
                CAST(p.post_date AS DATE) AS post_date,
                CAST(p.remove_date AS DATE) AS remove_date,
                CAST(p.rcid AS BIGINT) AS rcid,
                COALESCE(CAST(p.country AS VARCHAR), 'UNK') AS country,
                COALESCE(CAST(p.state AS VARCHAR), 'UNK') AS state,
                COALESCE(CAST(p.metro_area AS VARCHAR), 'UNK') AS metro_area,
                COALESCE(CAST(p.role_k17000_v3 AS VARCHAR), 'UNK') AS role_id,
                COALESCE(CAST(p.role_k1500_v2 AS VARCHAR), 'Unknown') AS role_name,
                NULL::VARCHAR AS skill_id,
                NULL::VARCHAR AS skill_name,
                TRY_CAST(p.salary AS DOUBLE) AS salary_usd,
                COALESCE(CAST(cm.rics_k200 AS VARCHAR), 'UNK') AS industry_id,
                COALESCE(CAST(cm.rics_k50 AS VARCHAR), 'Unknown') AS industry_name,
                COALESCE(CAST(p.metro_area AS VARCHAR), COALESCE(CAST(p.state AS VARCHAR), CAST(p.country AS VARCHAR))) AS geo_id,
                COALESCE(CAST(p.metro_area AS VARCHAR), COALESCE(CAST(p.state AS VARCHAR), CAST(p.country AS VARCHAR))) AS geo_name
            FROM stage.postings_cosmos_raw p
            LEFT JOIN stage.company_mapping cm
              ON CAST(p.rcid AS BIGINT) = CAST(cm.rcid AS BIGINT);
            """
        )
        return

    con.execute(
        """
        CREATE TABLE stage.raw_postings (
            job_id BIGINT,
            post_date DATE,
            remove_date DATE,
            rcid BIGINT,
            country VARCHAR,
            state VARCHAR,
            metro_area VARCHAR,
            role_id VARCHAR,
            role_name VARCHAR,
            skill_id VARCHAR,
            skill_name VARCHAR,
            salary_usd DOUBLE,
            industry_id VARCHAR,
            industry_name VARCHAR,
            geo_id VARCHAR,
            geo_name VARCHAR
        );
        """
    )


def _export_processed_tables(con: duckdb.DuckDBPyConnection, processed_dir: Path) -> None:
    processed_dir.mkdir(parents=True, exist_ok=True)
    table_names = [
        "cs_job_demand",
        "cs_skill_demand",
        "role_skill_associations",
        "salary_distribution",
        "trajectory_features",
        "trajectory_labels",
    ]
    for table in table_names:
        parquet_path = processed_dir / f"{table}.parquet"
        csv_path = processed_dir / f"{table}.csv"
        con.execute(f"COPY marts.{table} TO '{parquet_path}' (FORMAT PARQUET);")
        con.execute(f"COPY marts.{table} TO '{csv_path}' (HEADER, DELIMITER ',');")


def main() -> None:
    run_id = f"run_{uuid.uuid4().hex[:12]}"
    source_root = get_source_root()
    duckdb_path = get_duckdb_path()
    processed_dir = get_processed_dir()

    duckdb_path.parent.mkdir(parents=True, exist_ok=True)

    cfg = yaml.safe_load(CONFIG_PATH.read_text(encoding="utf-8"))
    con = duckdb.connect(str(duckdb_path))
    try:
        _load_allowlists(con, cfg)
        _create_raw_postings(con, source_root)

        sql_order = [
            "01_stage_revelio.sql",
            "10_cs_job_demand.sql",
            "20_cs_skill_demand.sql",
            "30_role_skill_associations.sql",
            "40_salary.sql",
            "50_trajectory_features.sql",
            "60_trajectory_labels.sql",
        ]
        for sql_name in sql_order:
            con.execute(_read_sql(SQL_DIR / sql_name))

        _export_processed_tables(con, processed_dir)
        print(f"Build complete. run_id={run_id}")
        print(f"DuckDB: {duckdb_path}")
        print(f"Processed outputs: {processed_dir}")
    finally:
        con.close()


if __name__ == "__main__":
    main()
