from __future__ import annotations

import glob
import json
import re
import shutil
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
SKILL_KEYWORDS_PATH = ROOT / "config" / "skill_keywords.yml"

from config.revelio_sources import get_duckdb_path, get_processed_dir, get_source_root


def _read_sql(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _configure_duckdb(con: duckdb.DuckDBPyConnection) -> None:
    temp_dir = ROOT / "data" / "local" / "duckdb_temp"
    temp_dir.mkdir(parents=True, exist_ok=True)
    con.execute(f"SET temp_directory='{temp_dir.as_posix()}'")
    con.execute("SET threads=2")
    con.execute("SET preserve_insertion_order=false")
    con.execute("SET memory_limit='4GB'")
    con.execute("SET max_temp_directory_size='12GiB'")


def _cleanup_build_artifacts(*, keep_duckdb: bool = True) -> None:
    """Remove large intermediates after a build."""
    hits_dir = ROOT / "data" / "local" / "posting_skill_hits_staging"
    if hits_dir.exists():
        shutil.rmtree(hits_dir, ignore_errors=True)
    temp_dir = ROOT / "data" / "local" / "duckdb_temp"
    if temp_dir.exists():
        shutil.rmtree(temp_dir, ignore_errors=True)
    if not keep_duckdb:
        duckdb_path = get_duckdb_path()
        duckdb_path.unlink(missing_ok=True)
        Path(str(duckdb_path) + ".wal").unlink(missing_ok=True)


def _keyword_match_mode(keyword: str) -> str:
    keyword = keyword.strip().lower()
    if not keyword:
        return "contains"
    if " " in keyword or "-" in keyword or len(keyword) >= 4:
        return "contains"
    if re.search(r"[^a-z0-9]", keyword):
        return "contains"
    return "word_boundary"


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


def _load_skill_keyword_terms(con: duckdb.DuckDBPyConnection, path: Path) -> int:
    cfg = yaml.safe_load(path.read_text(encoding="utf-8"))
    rows: list[tuple[str, str, str, str]] = []
    for skill in cfg.get("skills") or []:
        skill_id = skill.get("skill_id")
        skill_name = skill.get("skill_name") or skill_id
        if not skill_id:
            continue
        for keyword in skill.get("keywords") or []:
            kw = str(keyword).strip().lower()
            if not kw:
                continue
            rows.append((skill_id.upper(), skill_name, kw, _keyword_match_mode(kw)))

    con.execute("DROP TABLE IF EXISTS stage.skill_keyword_terms;")
    con.execute(
        """
        CREATE TABLE stage.skill_keyword_terms (
            skill_id VARCHAR,
            skill_name VARCHAR,
            keyword VARCHAR,
            match_mode VARCHAR
        );
        """
    )
    if rows:
        con.executemany(
            "INSERT INTO stage.skill_keyword_terms VALUES (?, ?, ?, ?)",
            rows,
        )
    return len(rows)


def _load_posting_text_partitions(con: duckdb.DuckDBPyConnection, source_root: Path) -> list[str]:
    pattern = str(
        source_root
        / "revelio_job_postings"
        / "postings_cosmos_raw"
        / "year=*"
        / "month=*"
        / "posting_text.parquet"
    )
    files = sorted(glob.glob(pattern))
    con.execute("DROP TABLE IF EXISTS stage.posting_text;")
    if not files:
        con.execute(
            """
            CREATE TABLE stage.posting_text (
                job_id BIGINT,
                title_raw VARCHAR,
                description VARCHAR
            );
            """
        )
        return []
    return files


def _build_posting_skill_hits(con: duckdb.DuckDBPyConnection, partition_files: list[str]) -> int:
    """Match keywords per month partition; insert into DuckDB (no staging parquet)."""
    con.execute("DROP TABLE IF EXISTS stage.posting_skill_hits;")
    con.execute(
        """
        CREATE TABLE stage.posting_skill_hits (
            job_id BIGINT,
            skill_id VARCHAR,
            skill_name VARCHAR
        );
        """
    )
    if not partition_files:
        return 0

    terms = con.execute(
        "SELECT skill_id, skill_name, keyword, match_mode FROM stage.skill_keyword_terms ORDER BY 1, 3"
    ).fetchall()
    if not terms:
        return 0

    skill_terms: dict[tuple[str, str], list[tuple[str, str]]] = {}
    for skill_id, skill_name, keyword, match_mode in terms:
        skill_terms.setdefault((skill_id, skill_name), []).append((keyword, match_mode))

    def _match_predicate(keyword: str, match_mode: str, text_col: str = "text") -> str:
        escaped = keyword.replace("'", "''")
        if match_mode == "word_boundary":
            return f"regexp_matches({text_col}, '(^|[^a-z0-9]){escaped}([^a-z0-9]|$)')"
        return f"CONTAINS({text_col}, '{escaped}')"

    total_parts = len(partition_files)
    for part_idx, path in enumerate(partition_files, start=1):
        posix = Path(path).as_posix()
        part_label = Path(path).parent.name
        print(f"  keyword match partition {part_idx}/{total_parts}: {part_label}", flush=True)

        skill_selects: list[str] = []
        for skill_id, skill_name in skill_terms:
            keyword_rows = skill_terms[(skill_id, skill_name)]
            predicates = " OR ".join(
                f"({ _match_predicate(keyword, match_mode) })"
                for keyword, match_mode in keyword_rows
            )
            escaped_skill_id = str(skill_id).replace("'", "''")
            escaped_skill_name = str(skill_name).replace("'", "''")
            skill_selects.append(
                f"""
                SELECT DISTINCT
                    job_id,
                    '{escaped_skill_id}' AS skill_id,
                    '{escaped_skill_name}' AS skill_name
                FROM posting_search
                WHERE {predicates}
                """
            )

        union_sql = "\n                UNION ALL\n".join(skill_selects)
        con.execute(
            f"""
            CREATE OR REPLACE TEMP TABLE posting_search AS
            SELECT
                CAST(p.job_id AS BIGINT) AS job_id,
                LOWER(CONCAT(COALESCE(p.title_raw, ''), ' ', COALESCE(p.description, ''))) AS text
            FROM read_parquet('{posix}') AS p
            INNER JOIN stage.raw_postings_jobs AS j
              ON CAST(p.job_id AS BIGINT) = j.job_id
            WHERE p.job_id IS NOT NULL
            """
        )
        part_rows = con.execute("SELECT COUNT(*) FROM posting_search").fetchone()[0]
        print(f"    searchable postings: {part_rows:,}", flush=True)

        con.execute(
            f"""
            INSERT INTO stage.posting_skill_hits
            {union_sql}
            """
        )
        con.execute("DROP TABLE IF EXISTS posting_search")

    con.execute(
        """
        CREATE OR REPLACE TABLE stage.posting_skill_hits AS
        SELECT DISTINCT job_id, skill_id, skill_name
        FROM stage.posting_skill_hits
        """
    )
    hit_count = int(con.execute("SELECT COUNT(*) FROM stage.posting_skill_hits").fetchone()[0])
    if partition_files and hit_count == 0:
        raise SystemExit(
            "posting_skill_hits is empty after keyword matching. "
            "Check posting_text.parquet overlap with postings_cosmos job_ids."
        )
    return hit_count


def _create_raw_postings_jobs(con: duckdb.DuckDBPyConnection, source_root: Path) -> None:
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
    con.execute("DROP TABLE IF EXISTS stage.raw_postings_jobs;")
    con.execute("DROP TABLE IF EXISTS stage.postings_cosmos_raw;")
    con.execute("DROP TABLE IF EXISTS stage.company_mapping;")
    con.execute("DROP TABLE IF EXISTS stage.regions;")
    if files:
        posix_pattern = pattern.replace("\\", "/")
        company_mapping_path = source_root / "revelio_common" / "company_mapping" / "company_mapping.parquet"
        if company_mapping_path.exists():
            cm_from = f"read_parquet('{company_mapping_path.as_posix()}')"
        else:
            cm_from = """
                (SELECT
                    CAST(NULL AS BIGINT) AS rcid,
                    CAST(NULL AS VARCHAR) AS company,
                    CAST(NULL AS VARCHAR) AS rics_k50,
                    CAST(NULL AS VARCHAR) AS rics_k200,
                    CAST(NULL AS VARCHAR) AS rics_k400,
                    CAST(NULL AS VARCHAR) AS naics_code,
                    CAST(NULL AS BIGINT) AS ultimate_parent_rcid
                 WHERE 1 = 0)
            """

        con.execute(
            f"""
            CREATE TABLE stage.raw_postings_jobs AS
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
                TRY_CAST(p.salary AS DOUBLE) AS salary_usd,
                COALESCE(CAST(cm.rics_k200 AS VARCHAR), 'UNK') AS industry_id,
                COALESCE(CAST(cm.rics_k50 AS VARCHAR), 'Unknown') AS industry_name,
                COALESCE(CAST(p.metro_area AS VARCHAR), COALESCE(CAST(p.state AS VARCHAR), CAST(p.country AS VARCHAR))) AS geo_id,
                COALESCE(CAST(p.metro_area AS VARCHAR), COALESCE(CAST(p.state AS VARCHAR), CAST(p.country AS VARCHAR))) AS geo_name
            FROM read_parquet('{posix_pattern}', hive_partitioning=1) AS p
            LEFT JOIN {cm_from} AS cm
              ON CAST(p.rcid AS BIGINT) = CAST(cm.rcid AS BIGINT);
            """
        )
        return

    con.execute(
        """
        CREATE TABLE stage.raw_postings_jobs (
            job_id BIGINT,
            post_date DATE,
            remove_date DATE,
            rcid BIGINT,
            country VARCHAR,
            state VARCHAR,
            metro_area VARCHAR,
            role_id VARCHAR,
            role_name VARCHAR,
            salary_usd DOUBLE,
            industry_id VARCHAR,
            industry_name VARCHAR,
            geo_id VARCHAR,
            geo_name VARCHAR
        );
        """
    )


def _export_processed_tables(
    con: duckdb.DuckDBPyConnection,
    processed_dir: Path,
    *,
    export_csv: bool = False,
) -> None:
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
        con.execute(
            f"COPY marts.{table} TO '{parquet_path.as_posix()}' (FORMAT PARQUET, COMPRESSION ZSTD);"
        )
        if export_csv:
            csv_path = processed_dir / f"{table}.csv"
            con.execute(f"COPY marts.{table} TO '{csv_path.as_posix()}' (HEADER, DELIMITER ',');")
        else:
            csv_path = processed_dir / f"{table}.csv"
            if csv_path.exists():
                csv_path.unlink()


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(description="Build DuckDB marts and export processed tables.")
    parser.add_argument(
        "--export-csv",
        action="store_true",
        help="Also write data/processed/*.csv (doubles disk use; parquet is enough for load_postgres).",
    )
    parser.add_argument(
        "--keep-duckdb",
        action="store_true",
        help="Keep data/local/cs_lmi.duckdb after build (default: delete after export).",
    )
    args = parser.parse_args()

    run_id = f"run_{uuid.uuid4().hex[:12]}"
    source_root = get_source_root()
    duckdb_path = get_duckdb_path()
    processed_dir = get_processed_dir()

    duckdb_path.parent.mkdir(parents=True, exist_ok=True)
    if duckdb_path.exists():
        duckdb_path.unlink()

    cfg = yaml.safe_load(CONFIG_PATH.read_text(encoding="utf-8"))
    con = duckdb.connect(str(duckdb_path))
    try:
        _configure_duckdb(con)
        _load_allowlists(con, cfg)
        _create_raw_postings_jobs(con, source_root)

        print("Building posting skill hits from keyword config ...", flush=True)
        keyword_terms = _load_skill_keyword_terms(con, SKILL_KEYWORDS_PATH)
        print(f"  loaded {keyword_terms:,} keyword terms", flush=True)
        partition_files = _load_posting_text_partitions(con, source_root)
        print(f"  found {len(partition_files):,} posting_text partition(s)", flush=True)
        hit_rows = _build_posting_skill_hits(con, partition_files)
        distinct_jobs = con.execute(
            "SELECT COUNT(DISTINCT job_id) FROM stage.posting_skill_hits"
        ).fetchone()[0]
        print(f"  posting_skill_hits: {hit_rows:,} rows ({distinct_jobs:,} distinct job_id)", flush=True)

        sql_order = [
            "05_posting_skill_hits.sql",
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

        skill_rows = con.execute(
            "SELECT COUNT(*) FROM marts.cs_skill_demand WHERE skill_id <> 'UNK'"
        ).fetchone()[0]
        unk_rows = con.execute(
            "SELECT COUNT(*) FROM marts.cs_skill_demand WHERE skill_id = 'UNK'"
        ).fetchone()[0]

        _export_processed_tables(con, processed_dir, export_csv=args.export_csv)
        print(f"Build complete. run_id={run_id}")
        print(f"DuckDB: {duckdb_path}")
        print(f"Processed outputs: {processed_dir}")
        print(f"cs_skill_demand non-UNK rows: {skill_rows:,} (UNK rows: {unk_rows:,})")
    finally:
        con.close()
        _cleanup_build_artifacts(keep_duckdb=args.keep_duckdb)


if __name__ == "__main__":
    main()
