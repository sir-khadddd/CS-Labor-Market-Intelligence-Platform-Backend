from __future__ import annotations

import argparse
import calendar
from datetime import date, datetime, timedelta
import json
import os
from pathlib import Path
import sys
import time
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


def _rcids_and_role_counts_from_postings_parquet(postings_out: Path) -> tuple[set[int], dict[str, int], int]:
    """Aggregate rcids / role counts / row count from already-written month partitions."""
    if not postings_out.exists() or not any(postings_out.rglob("*.parquet")):
        return set(), {}, 0
    glob_path = str(postings_out / "**" / "*.parquet").replace("\\", "/")
    con = duckdb.connect()
    try:
        row_count = int(con.execute(f"SELECT COUNT(*) FROM read_parquet('{glob_path}')").fetchone()[0])
        rcid_rows = con.execute(
            f"SELECT DISTINCT CAST(rcid AS BIGINT) AS rcid FROM read_parquet('{glob_path}') WHERE rcid IS NOT NULL"
        ).fetchall()
        role_rows = con.execute(
            f"""
            SELECT COALESCE(CAST(role_k17000_v3 AS VARCHAR), 'NULL') AS role_id, COUNT(*) AS c
            FROM read_parquet('{glob_path}')
            GROUP BY 1
            """
        ).fetchall()
    finally:
        con.close()
    rcids = {int(r[0]) for r in rcid_rows}
    role_counts = {str(r[0]): int(r[1]) for r in role_rows}
    return rcids, role_counts, row_count


def _open_wrds(cfg: dict) -> wrds.Connection:
    username = os.getenv(cfg["wrds"]["username_env"], "")
    password = os.getenv(cfg["wrds"]["password_env"])
    if not username:
        raise ValueError(f"Missing WRDS username env var: {cfg['wrds']['username_env']}")
    return wrds.Connection(wrds_username=username, wrds_password=password)


def _fetch_company_mapping_for_postings(
    db: wrds.Connection,
    cfg: dict,
    postings_df: pd.DataFrame,
    *,
    rcids: list[int] | set[int] | None = None,
    chunk_size: int = 2000,
    max_retries: int = 5,
) -> pd.DataFrame:
    """Pull company_mapping rows only for rcids present in postings (matches staging join on rcid)."""
    table_cfg = cfg["tables"]["company_mapping"]
    if rcids is None:
        rcid_list = _distinct_rcids_from_postings(postings_df)
    else:
        rcid_list = sorted({int(x) for x in rcids})
    if not rcid_list:
        print("company_mapping: no distinct rcid in postings; writing empty extract.", flush=True)
        return pd.DataFrame(columns=list(table_cfg["select_columns"]))

    chunks_sql = [rcid_list[i : i + chunk_size] for i in range(0, len(rcid_list), chunk_size)]
    print(
        f"company_mapping: fetching {len(rcid_list):,} distinct rcids in {len(chunks_sql)} batch(es) "
        f"(chunk_size={chunk_size}, retries={max_retries})...",
        flush=True,
    )
    parts: list[pd.DataFrame] = []
    for bi, part in enumerate(chunks_sql, start=1):
        sql = _build_company_mapping_sql_in_rcids(table_cfg, part)
        print(f"  company_mapping batch {bi}/{len(chunks_sql)} ({len(part):,} rcids)...", flush=True)
        last_err: Exception | None = None
        for attempt in range(1, max_retries + 1):
            try:
                parts.append(db.raw_sql(sql))
                last_err = None
                break
            except Exception as exc:  # noqa: BLE001 - WRDS drops connections unpredictably
                last_err = exc
                wait_s = min(60, 2 ** attempt)
                print(
                    f"    batch {bi} attempt {attempt}/{max_retries} failed: {exc.__class__.__name__}; "
                    f"reconnecting in {wait_s}s...",
                    flush=True,
                )
                try:
                    db.close()
                except Exception:  # noqa: BLE001
                    pass
                time.sleep(wait_s)
                db = _open_wrds(cfg)
        if last_err is not None:
            raise RuntimeError(f"company_mapping batch {bi}/{len(chunks_sql)} failed after retries") from last_err
    out = pd.concat(parts, ignore_index=True) if parts else pd.DataFrame(columns=list(table_cfg["select_columns"]))
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
    postings_df: pd.DataFrame,
    role_allowlist: list[str],
    run_id: str,
    out_dir: Path,
    role_counts: dict[str, int] | None = None,
) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    if role_counts is not None:
        report = pd.DataFrame(
            [{"role_k17000_v3": k, "posting_count": v} for k, v in role_counts.items()]
        )
    elif "role_k17000_v3" not in postings_df.columns:
        report = pd.DataFrame(columns=["role_k17000_v3", "posting_count", "in_allowlist"])
    else:
        report = (
            postings_df.groupby("role_k17000_v3", dropna=False)["job_id"]
            .count()
            .reset_index(name="posting_count")
        )
        report["role_k17000_v3"] = report["role_k17000_v3"].fillna("NULL")
    if not report.empty:
        allow_set = set(role_allowlist)
        report["in_allowlist"] = report["role_k17000_v3"].isin(allow_set)
        report = report.sort_values(["in_allowlist", "posting_count"], ascending=[True, False])
    report_path = out_dir / f"role_frequency_{run_id}.csv"
    report.to_csv(report_path, index=False)
    return report_path


def _partition_exists(out_dir: Path, year: int, month: int) -> bool:
    part_dir = out_dir / f"year={year}" / f"month={month}"
    if not part_dir.is_dir():
        return False
    return any(part_dir.glob("*.parquet"))


def _fetch_postings_by_month(
    db: wrds.Connection,
    cfg: dict,
    role_allowlist: list[str],
    limit_rows: int | None,
    *,
    first_months: int | None = None,
    last_months: int | None = None,
    out_dir: Path | None = None,
    resume: bool = True,
) -> tuple[pd.DataFrame, dict[str, int], set[int]]:
    """Fetch postings month-by-month.

    When out_dir is set, each month is written immediately as year/month parquet
    partitions (survives hangs) and full frames are not retained in memory.
    Returns (empty_or_legacy_df, role_counts, rcids).
    """
    periods = _month_periods_inclusive(cfg["extract"]["start_date"], cfg["extract"]["end_date"])
    if not periods:
        print("No date range to fetch (check extract start_date / end_date).", flush=True)
        return pd.DataFrame(), {}, set()

    if first_months is not None:
        periods = periods[: max(0, int(first_months))]
    elif last_months is not None:
        periods = periods[-max(0, int(last_months)) :]

    total_months = len(periods)
    remaining = limit_rows
    chunks: list[pd.DataFrame] = []
    running = 0
    role_counts: dict[str, int] = {}
    rcids: set[int] = set()
    flush = out_dir is not None
    if flush:
        out_dir.mkdir(parents=True, exist_ok=True)

    for i, (label, span_start, span_end) in enumerate(periods, start=1):
        if remaining is not None and remaining <= 0:
            break

        year, month = span_start.year, span_start.month
        if flush and resume and _partition_exists(out_dir, year, month):
            print(
                f"Skipping postings_cosmos {label} ({i}/{total_months}) — partition exists",
                flush=True,
            )
            continue

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
        if not n:
            continue

        if "role_k17000_v3" in chunk.columns:
            for role, cnt in chunk["role_k17000_v3"].fillna("NULL").value_counts().items():
                role_counts[str(role)] = role_counts.get(str(role), 0) + int(cnt)
        if "rcid" in chunk.columns:
            rcids.update(
                int(x) for x in pd.to_numeric(chunk["rcid"], errors="coerce").dropna().unique()
            )

        if flush:
            chunk = chunk.copy()
            chunk["year"] = year
            chunk["month"] = month
            _write_partitioned_parquet(chunk, out_dir, ["year", "month"])
            print(f"  wrote partition year={year}/month={month}", flush=True)
            del chunk
        else:
            chunks.append(chunk)

    if flush:
        return pd.DataFrame(), role_counts, rcids
    if not chunks:
        return pd.DataFrame(), role_counts, rcids
    return pd.concat(chunks, ignore_index=True), role_counts, rcids


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
    parser.add_argument(
        "--skip-postings",
        action="store_true",
        help="Skip postings fetch; reuse existing postings_cosmos parquet partitions for rcids/role counts, then fetch company_mapping + regions.",
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

    run_id = f"wrds_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:6]}"
    root = get_source_root()
    metadata_dir = root / "metadata"
    metadata_dir.mkdir(parents=True, exist_ok=True)
    postings_out = root / "revelio_job_postings" / "postings_cosmos"

    db = _open_wrds(cfg)
    try:
        limit = args.limit_rows if args.limit_rows is not None else cfg["extract"]["limit_rows"]
        postings_df = pd.DataFrame()
        role_counts: dict[str, int] = {}
        posting_rcids: set[int] = set()
        postings_rows = 0

        if args.skip_postings:
            print(f"Skipping postings fetch; reading rcids from {postings_out}...", flush=True)
            posting_rcids, role_counts, postings_rows = _rcids_and_role_counts_from_postings_parquet(
                postings_out
            )
            print(
                f"  found {postings_rows:,} posting rows, {len(posting_rcids):,} rcids, "
                f"{len(role_counts):,} roles",
                flush=True,
            )
            if postings_rows == 0:
                raise SystemExit("No postings parquet found; run a full extract first.")
        else:
            print(
                f"Flushing each month to {postings_out} (resume skips existing partitions)...",
                flush=True,
            )
            postings_df, role_counts, posting_rcids = _fetch_postings_by_month(
                db,
                cfg,
                role_allowlist,
                limit,
                first_months=args.first_months,
                last_months=args.last_months,
                out_dir=postings_out,
                resume=True,
            )
            postings_rows = int(sum(role_counts.values())) if role_counts else int(len(postings_df))
            if postings_rows == 0 and not any(postings_out.rglob("*.parquet")):
                print("No posting rows returned; check date range, role allowlist, and WRDS access.", flush=True)
            elif not postings_df.empty:
                postings_df["year"] = pd.to_datetime(postings_df["post_date"], errors="coerce").dt.year
                postings_df["month"] = pd.to_datetime(postings_df["post_date"], errors="coerce").dt.month
                postings_df = postings_df.dropna(subset=["year", "month"])
                postings_df["year"] = postings_df["year"].astype(int)
                postings_df["month"] = postings_df["month"].astype(int)
                _write_partitioned_parquet(postings_df, postings_out, ["year", "month"])
                postings_rows = int(len(postings_df))

        company_rows = 0
        regions_rows = 0

        if cfg["extract"].get("include_company_mapping", True):
            if args.full_company_mapping:
                print("company_mapping: full table (no rcid filter)...", flush=True)
                company_sql = _build_simple_sql(cfg["tables"]["company_mapping"])
                company_df = db.raw_sql(company_sql)
            else:
                company_df = _fetch_company_mapping_for_postings(
                    db, cfg, postings_df, rcids=posting_rcids or None
                )
            _write_single_parquet(
                company_df, root / "revelio_common" / "company_mapping" / "company_mapping.parquet"
            )
            company_rows = len(company_df)
            print(f"company_mapping rows written: {company_rows:,}", flush=True)

        if cfg["extract"].get("include_regions", True):
            print("Fetching regions...", flush=True)
            regions_sql = _build_simple_sql(cfg["tables"]["regions"])
            regions_df = db.raw_sql(regions_sql)
            _write_single_parquet(regions_df, root / "revelio_common" / "regions" / "regions.parquet")
            regions_rows = len(regions_df)
            print(f"regions rows written: {regions_rows:,}", flush=True)

        report_path = _write_role_frequency_report(
            postings_df,
            role_allowlist,
            run_id,
            root / "metadata" / "taxonomy_reports",
            role_counts=role_counts or None,
        )

        manifest = {
            "run_id": run_id,
            "run_timestamp_utc": datetime.utcnow().isoformat(),
            "source": "wrds",
            "tables": {
                "postings_cosmos": {
                    "rows": postings_rows,
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
            "skip_postings": bool(args.skip_postings),
        }
        (metadata_dir / f"{run_id}.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")

        print(f"WRDS extraction complete. run_id={run_id}", flush=True)
        print(f"Postings rows: {postings_rows}", flush=True)
        print(f"Output root: {root}", flush=True)
        print(f"Taxonomy report: {report_path}", flush=True)
    finally:
        db.close()


if __name__ == "__main__":
    main()
