from __future__ import annotations

import argparse
import calendar
from datetime import date, datetime, timedelta
import json
import os
from pathlib import Path
import sys
import uuid

import duckdb
import pandas as pd
import wrds
import yaml

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from config.revelio_sources import get_source_root


CONFIG_PATH = ROOT / "config" / "wrds_extract.yml"
CS_UNIVERSE_PATH = ROOT / "config" / "cs_universe.yml"


def _quote_sql_literal(value: str) -> str:
    return value.replace("'", "''")


def _load_role_allowlist() -> list[str]:
    cfg = yaml.safe_load(CS_UNIVERSE_PATH.read_text(encoding="utf-8")) or {}
    roles = cfg.get("roles") or []
    return [str(r.get("role_id", "")).strip() for r in roles if str(r.get("role_id", "")).strip()]


def _parse_iso_date(s: str) -> date:
    return datetime.strptime(s.strip(), "%Y-%m-%d").date()


def _day_after_inclusive(iso_inclusive: str) -> str:
    """Exclusive end for half-open [start, end) filtering (correct for timestamp columns)."""
    return (_parse_iso_date(iso_inclusive) + timedelta(days=1)).isoformat()


def _month_periods_inclusive(extract_start: str, extract_end: str) -> list[tuple[str, date, date]]:
    """Calendar months overlapping [extract_start, extract_end], inclusive dates."""
    start = _parse_iso_date(extract_start)
    end = _parse_iso_date(extract_end)
    if start > end:
        return []
    periods: list[tuple[str, date, date]] = []
    y, m = start.year, start.month
    while True:
        first = date(y, m, 1)
        last = date(y, m, calendar.monthrange(y, m)[1])
        span_start = max(first, start)
        span_end = min(last, end)
        if span_start <= span_end:
            label = f"{y:04d}-{m:02d}"
            periods.append((label, span_start, span_end))
        if (y, m) >= (end.year, end.month):
            break
        if m == 12:
            y += 1
            m = 1
        else:
            m += 1
    return periods


def _build_postings_sql(
    cfg: dict,
    role_allowlist: list[str],
    limit_rows: int | None,
    *,
    period_start: str | None = None,
    period_end: str | None = None,
) -> str:
    table_cfg = cfg["tables"]["postings_cosmos"]
    columns = ", ".join(table_cfg["select_columns"])
    schema = table_cfg["schema"]
    table = table_cfg["table"]
    date_col = table_cfg["date_column"]
    if period_start is not None and period_end is not None:
        start_inclusive = period_start
        end_exclusive = _day_after_inclusive(period_end)
    else:
        start_inclusive = cfg["extract"]["start_date"]
        end_exclusive = _day_after_inclusive(cfg["extract"]["end_date"])

    where_parts = [
        f"{date_col} >= DATE '{start_inclusive}'",
        f"{date_col} < DATE '{end_exclusive}'",
    ]

    if role_allowlist:
        sql_values = ", ".join(f"'{_quote_sql_literal(v)}'" for v in role_allowlist)
        where_parts.append(f"role_k17000_v3 IN ({sql_values})")

    query = f"""
        SELECT {columns}
        FROM {schema}.{table}
        WHERE {" AND ".join(where_parts)}
    """
    if limit_rows is not None:
        query += f"\nLIMIT {int(limit_rows)}"
    return query


def _build_simple_sql(table_cfg: dict) -> str:
    columns = ", ".join(table_cfg["select_columns"])
    return f"SELECT {columns} FROM {table_cfg['schema']}.{table_cfg['table']}"


def _distinct_rcids_from_postings(postings_df: pd.DataFrame) -> list[int]:
    if postings_df.empty or "rcid" not in postings_df.columns:
        return []
    v = pd.to_numeric(postings_df["rcid"], errors="coerce").dropna()
    return sorted({int(x) for x in v.unique()})


def _build_company_mapping_sql_in_rcids(table_cfg: dict, rcids: list[int]) -> str:
    columns = ", ".join(table_cfg["select_columns"])
    schema = table_cfg["schema"]
    table = table_cfg["table"]
    in_list = ", ".join(str(int(r)) for r in rcids)
    return f"SELECT {columns} FROM {schema}.{table} WHERE rcid IN ({in_list})"


def _fetch_company_mapping_for_postings(
    db: wrds.Connection,
    cfg: dict,
    postings_df: pd.DataFrame,
    *,
    chunk_size: int = 5000,
) -> pd.DataFrame:
    """Pull company_mapping rows only for rcids present in postings (matches staging join on rcid)."""
    table_cfg = cfg["tables"]["company_mapping"]
    rcids = _distinct_rcids_from_postings(postings_df)
    if not rcids:
        print("company_mapping: no distinct rcid in postings; writing empty extract.", flush=True)
        return pd.DataFrame(columns=list(table_cfg["select_columns"]))

    chunks_sql = [rcids[i : i + chunk_size] for i in range(0, len(rcids), chunk_size)]
    print(
        f"company_mapping: fetching {len(rcids):,} distinct rcids in {len(chunks_sql)} batch(es)...",
        flush=True,
    )
    parts: list[pd.DataFrame] = []
    for bi, part in enumerate(chunks_sql, start=1):
        sql = _build_company_mapping_sql_in_rcids(table_cfg, part)
        print(f"  company_mapping batch {bi}/{len(chunks_sql)} ({len(part):,} rcids)...", flush=True)
        parts.append(db.raw_sql(sql))
    out = pd.concat(parts, ignore_index=True)
    if "rcid" in out.columns:
        out = out.drop_duplicates(subset=["rcid"], keep="first")
    return out


def _write_partitioned_parquet(df: pd.DataFrame, out_dir: Path, partition_cols: list[str]) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    try:
        con.register("tmp_df", df)
        partitions = ", ".join(partition_cols)
        con.execute(
            f"COPY tmp_df TO '{out_dir}' (FORMAT PARQUET, PARTITION_BY ({partitions}), OVERWRITE_OR_IGNORE TRUE);"
        )
    finally:
        con.close()


def _write_single_parquet(df: pd.DataFrame, out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    try:
        con.register("tmp_df", df)
        con.execute(f"COPY tmp_df TO '{out_path}' (FORMAT PARQUET);")
    finally:
        con.close()


def _write_role_frequency_report(
    postings_df: pd.DataFrame, role_allowlist: list[str], run_id: str, out_dir: Path
) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    if "role_k17000_v3" not in postings_df.columns:
        report = pd.DataFrame(columns=["role_k17000_v3", "posting_count", "in_allowlist"])
    else:
        report = (
            postings_df.groupby("role_k17000_v3", dropna=False)["job_id"]
            .count()
            .reset_index(name="posting_count")
            .rename(columns={"role_k17000_v3": "role_k17000_v3"})
        )
        report["role_k17000_v3"] = report["role_k17000_v3"].fillna("NULL")
        allow_set = set(role_allowlist)
        report["in_allowlist"] = report["role_k17000_v3"].isin(allow_set)
        report = report.sort_values(["in_allowlist", "posting_count"], ascending=[True, False])
    report_path = out_dir / f"role_frequency_{run_id}.csv"
    report.to_csv(report_path, index=False)
    return report_path


def _fetch_postings_by_month(
    db: wrds.Connection,
    cfg: dict,
    role_allowlist: list[str],
    limit_rows: int | None,
    *,
    first_months: int | None = None,
    last_months: int | None = None,
) -> pd.DataFrame:
    """Fetch postings in calendar-month chunks and print progress after each month."""
    periods = _month_periods_inclusive(cfg["extract"]["start_date"], cfg["extract"]["end_date"])
    if not periods:
        print("No date range to fetch (check extract start_date / end_date).", flush=True)
        return pd.DataFrame()

    if first_months is not None:
        periods = periods[: max(0, int(first_months))]
    elif last_months is not None:
        periods = periods[-max(0, int(last_months)) :]

    total_months = len(periods)
    remaining = limit_rows
    chunks: list[pd.DataFrame] = []
    running = 0

    for i, (label, span_start, span_end) in enumerate(periods, start=1):
        if remaining is not None and remaining <= 0:
            break
        lim = remaining
        sql = _build_postings_sql(
            cfg,
            role_allowlist,
            lim,
            period_start=span_start.isoformat(),
            period_end=span_end.isoformat(),
        )
        print(f"Fetching postings_cosmos {label} ({i}/{total_months})...", flush=True)
        chunk = db.raw_sql(sql)
        n = len(chunk)
        running += n
        if remaining is not None:
            remaining -= n
        print(f"  {label}: {n:,} rows (total so far: {running:,})", flush=True)
        if n:
            chunks.append(chunk)

    if not chunks:
        return pd.DataFrame()
    return pd.concat(chunks, ignore_index=True)


def main() -> None:
    parser = argparse.ArgumentParser(description="Extract WRDS Revelio CS snapshot.")
    parser.add_argument("--config", default=str(CONFIG_PATH), help="Path to wrds extract config YAML.")
    parser.add_argument(
        "--limit-rows",
        type=int,
        default=None,
        help="Optional row limit for postings extraction.",
    )
    parser.add_argument(
        "--first-months",
        type=int,
        default=None,
        metavar="N",
        help="Only fetch the first N calendar months of the configured date range.",
    )
    parser.add_argument(
        "--last-months",
        type=int,
        default=None,
        metavar="N",
        help="Only fetch the last N calendar months of the configured date range (good for taxonomy sampling).",
    )
    parser.add_argument(
        "--no-role-filter",
        action="store_true",
        help="Ignore roles in cs_universe.yml (no role_k17000_v3 IN clause). Use to discover real role keys.",
    )
    parser.add_argument(
        "--full-company-mapping",
        action="store_true",
        help="Download full revelio_common.company_mapping (large). Default: only rcids present in extracted postings.",
    )
    args = parser.parse_args()

    if args.first_months is not None and args.last_months is not None:
        raise SystemExit("Use only one of --first-months or --last-months.")

    cfg = yaml.safe_load(Path(args.config).read_text(encoding="utf-8"))
    role_allowlist = [] if args.no_role_filter else _load_role_allowlist()
    if role_allowlist:
        print(
            f"Role allowlist: {len(role_allowlist)} ids from cs_universe.yml "
            "(must match role_k17000_v3 exactly; use --no-role-filter if you get zero rows).",
            flush=True,
        )

    username = os.getenv(cfg["wrds"]["username_env"], "")
    password = os.getenv(cfg["wrds"]["password_env"])
    if not username:
        raise ValueError(f"Missing WRDS username env var: {cfg['wrds']['username_env']}")

    run_id = f"wrds_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:6]}"
    root = get_source_root()
    metadata_dir = root / "metadata"
    metadata_dir.mkdir(parents=True, exist_ok=True)

    db = wrds.Connection(wrds_username=username, wrds_password=password)
    try:
        limit = args.limit_rows if args.limit_rows is not None else cfg["extract"]["limit_rows"]
        postings_df = _fetch_postings_by_month(
            db,
            cfg,
            role_allowlist,
            limit,
            first_months=args.first_months,
            last_months=args.last_months,
        )
        postings_out = root / "revelio_job_postings" / "postings_cosmos"
        if postings_df.empty:
            print("No posting rows returned; check date range, role allowlist, and WRDS access.", flush=True)
        else:
            postings_df["year"] = pd.to_datetime(postings_df["post_date"], errors="coerce").dt.year
            postings_df["month"] = pd.to_datetime(postings_df["post_date"], errors="coerce").dt.month
            postings_df = postings_df.dropna(subset=["year", "month"])
            postings_df["year"] = postings_df["year"].astype(int)
            postings_df["month"] = postings_df["month"].astype(int)
            _write_partitioned_parquet(postings_df, postings_out, ["year", "month"])

        company_rows = 0
        regions_rows = 0

        if cfg["extract"].get("include_company_mapping", True):
            if args.full_company_mapping:
                print("company_mapping: full table (no rcid filter)...", flush=True)
                company_sql = _build_simple_sql(cfg["tables"]["company_mapping"])
                company_df = db.raw_sql(company_sql)
            else:
                company_df = _fetch_company_mapping_for_postings(db, cfg, postings_df)
            _write_single_parquet(
                company_df, root / "revelio_common" / "company_mapping" / "company_mapping.parquet"
            )
            company_rows = len(company_df)

        if cfg["extract"].get("include_regions", True):
            regions_sql = _build_simple_sql(cfg["tables"]["regions"])
            regions_df = db.raw_sql(regions_sql)
            _write_single_parquet(regions_df, root / "revelio_common" / "regions" / "regions.parquet")
            regions_rows = len(regions_df)

        report_path = _write_role_frequency_report(
            postings_df, role_allowlist, run_id, root / "metadata" / "taxonomy_reports"
        )

        manifest = {
            "run_id": run_id,
            "run_timestamp_utc": datetime.utcnow().isoformat(),
            "source": "wrds",
            "tables": {
                "postings_cosmos": {
                    "rows": int(len(postings_df)),
                    "path": str(postings_out),
                    "date_range": {
                        "start": cfg["extract"]["start_date"],
                        "end": cfg["extract"]["end_date"],
                    },
                },
                "company_mapping": (
                    {
                        "rows": int(company_rows),
                        "scope": ("full" if args.full_company_mapping else "postings_rcid"),
                    }
                    if cfg["extract"].get("include_company_mapping", True)
                    else {"rows": 0}
                ),
                "regions": {"rows": int(regions_rows)},
            },
            "cs_allowlist_size": len(role_allowlist),
            "taxonomy_report": str(report_path),
        }
        (metadata_dir / f"{run_id}.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")

        print(f"WRDS extraction complete. run_id={run_id}")
        print(f"Postings rows: {len(postings_df)}")
        print(f"Output root: {root}")
        print(f"Taxonomy report: {report_path}")
    finally:
        db.close()


if __name__ == "__main__":
    main()
