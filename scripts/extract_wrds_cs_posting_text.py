"""
Extract posting text (title + description) for CS jobs already in local postings_cosmos.

Reads job_id (+ post_date) from your existing parquet extract, then fetches text from
WRDS postings_cosmos_raw in small job_id IN (...) batches. Never bulk-pulls the
~2.5B-row raw table.

Checkpoints each WRDS batch to disk so a connection drop can resume without re-fetching.
Reconnects and retries on transient WRDS/Postgres errors.

Output:
  data/raw_cs_snapshot/revelio_job_postings/postings_cosmos_raw/year=YYYY/month=MM/*.parquet

Checkpoints:
  data/raw_cs_snapshot/revelio_job_postings/postings_cosmos_raw/_checkpoints/batch_00001.parquet

Examples:
  python scripts/extract_wrds_cs_posting_text.py --last-months 1
  python scripts/extract_wrds_cs_posting_text.py --chunk-size 2000
  python scripts/extract_wrds_cs_posting_text.py --resume
  python scripts/extract_wrds_cs_posting_text.py --merge-only
"""

from __future__ import annotations

import argparse
import calendar
import glob
import json
import os
import shutil
import sys
import time
import uuid
from datetime import date, datetime, timedelta
from pathlib import Path

import duckdb
import pandas as pd
import wrds
import yaml
from sqlalchemy.exc import DBAPIError, OperationalError

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from config.backend_env import load_backend_env
from config.revelio_sources import get_source_root

load_backend_env()

CONFIG_PATH = ROOT / "config" / "wrds_extract.yml"
RAW_SCHEMA = "revelio_job_postings"
RAW_TABLE = "postings_cosmos_raw"
DEFAULT_CHUNK_SIZE = 2000
CHECKPOINT_DIRNAME = "_checkpoints"
PROGRESS_FILENAME = "progress.json"
DEFAULT_MAX_RETRIES = 4
DEFAULT_RETRY_SLEEP_SEC = 15


def _parse_iso_date(s: str) -> date:
    return datetime.strptime(s.strip(), "%Y-%m-%d").date()


def _month_periods_inclusive(extract_start: str, extract_end: str) -> list[tuple[str, date, date]]:
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
            periods.append((f"{y:04d}-{m:02d}", span_start, span_end))
        if (y, m) >= (end.year, end.month):
            break
        if m == 12:
            y += 1
            m = 1
        else:
            m += 1
    return periods


def _checkpoint_dir(out_dir: Path) -> Path:
    return out_dir / CHECKPOINT_DIRNAME


def _checkpoint_path(checkpoint_dir: Path, batch_no: int) -> Path:
    return checkpoint_dir / f"batch_{batch_no:05d}.parquet"


def _load_local_job_ids(source_root: Path, periods: list[tuple[str, date, date]]) -> pd.DataFrame:
    con = duckdb.connect()
    chunks: list[pd.DataFrame] = []
    try:
        for label, _, _ in periods:
            year_s, month_s = label.split("-")
            pattern = (
                source_root
                / "revelio_job_postings"
                / "postings_cosmos"
                / f"year={int(year_s)}"
                / f"month={int(month_s)}"
                / "*.parquet"
            )
            files = glob.glob(str(pattern))
            if not files:
                print(f"  local {label}: no parquet (skip)", flush=True)
                continue
            df = con.execute(
                f"""
                SELECT DISTINCT
                    CAST(job_id AS BIGINT) AS job_id,
                    CAST(post_date AS DATE) AS post_date
                FROM read_parquet('{pattern}')
                WHERE job_id IS NOT NULL
                """
            ).df()
            print(f"  local {label}: {len(df):,} distinct job_id", flush=True)
            if not df.empty:
                chunks.append(df)
    finally:
        con.close()

    if not chunks:
        return pd.DataFrame(columns=["job_id", "post_date"])
    out = pd.concat(chunks, ignore_index=True)
    return out.drop_duplicates(subset=["job_id"], keep="first")


def _build_raw_sql(job_ids: list[int]) -> str:
    in_list = ", ".join(str(int(j)) for j in job_ids)
    return f"""
        SELECT
            CAST(job_id AS BIGINT) AS job_id,
            CAST(title_raw AS VARCHAR) AS title_raw,
            CAST(description AS VARCHAR) AS description
        FROM {RAW_SCHEMA}.{RAW_TABLE}
        WHERE job_id IN ({in_list})
    """


def _is_transient_db_error(exc: BaseException) -> bool:
    msg = str(exc).lower()
    needles = (
        "server closed the connection",
        "connection unexpectedly",
        "connection reset",
        "connection timed out",
        "timeout expired",
        "could not connect",
        "connection already closed",
        "ssl syscall error",
        "broken pipe",
    )
    return any(n in msg for n in needles)


def _connect_wrds(username: str, password: str | None) -> wrds.Connection:
    return wrds.Connection(wrds_username=username, wrds_password=password)


def _fetch_batch_with_retry(
    username: str,
    password: str | None,
    job_ids: list[int],
    *,
    max_retries: int,
    retry_sleep_sec: int,
) -> pd.DataFrame:
    last_exc: BaseException | None = None
    for attempt in range(1, max_retries + 1):
        db = None
        try:
            db = _connect_wrds(username, password)
            return db.raw_sql(_build_raw_sql(job_ids))
        except (OperationalError, DBAPIError, Exception) as exc:
            last_exc = exc
            if not _is_transient_db_error(exc) or attempt >= max_retries:
                raise
            wait = retry_sleep_sec * attempt
            print(
                f"    transient WRDS error (attempt {attempt}/{max_retries}): {exc}",
                flush=True,
            )
            print(f"    retrying in {wait}s with a fresh connection...", flush=True)
            time.sleep(wait)
        finally:
            if db is not None:
                try:
                    db.close()
                except Exception:
                    pass
    if last_exc is not None:
        raise last_exc
    return pd.DataFrame(columns=["job_id", "title_raw", "description"])


def _write_checkpoint(path: Path, df: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    try:
        con.register("tmp_df", df)
        con.execute(f"COPY tmp_df TO '{path}' (FORMAT PARQUET);")
    finally:
        con.close()


def _load_progress(checkpoint_dir: Path) -> dict | None:
    path = checkpoint_dir / PROGRESS_FILENAME
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def _save_progress(checkpoint_dir: Path, progress: dict) -> None:
    checkpoint_dir.mkdir(parents=True, exist_ok=True)
    path = checkpoint_dir / PROGRESS_FILENAME
    path.write_text(json.dumps(progress, indent=2), encoding="utf-8")


def _completed_batch_numbers(checkpoint_dir: Path) -> set[int]:
    done: set[int] = set()
    for path in checkpoint_dir.glob("batch_*.parquet"):
        stem = path.stem  # batch_00001
        try:
            done.add(int(stem.split("_", 1)[1]))
        except ValueError:
            continue
    return done


def _fetch_text_batches_checkpointed(
    username: str,
    password: str | None,
    job_ids: list[int],
    chunk_size: int,
    checkpoint_dir: Path,
    *,
    start_batch: int,
    resume: bool,
    force_refetch: bool,
    max_retries: int,
    retry_sleep_sec: int,
    run_id: str,
    months: list[str],
) -> int:
    batches = [job_ids[i : i + chunk_size] for i in range(0, len(job_ids), chunk_size)]
    total_batches = len(batches)
    completed_before = _completed_batch_numbers(checkpoint_dir) if resume else set()

    if resume and completed_before:
        print(
            f"  resume: found {len(completed_before):,} completed checkpoint batches on disk",
            flush=True,
        )

    progress = _load_progress(checkpoint_dir) or {
        "run_id": run_id,
        "started_at_utc": datetime.utcnow().isoformat(),
        "chunk_size": chunk_size,
        "total_batches": total_batches,
        "total_job_ids": len(job_ids),
        "months": months,
        "completed_batches": sorted(completed_before),
    }
    progress["chunk_size"] = chunk_size
    progress["total_batches"] = total_batches
    progress["total_job_ids"] = len(job_ids)
    progress["months"] = months

    fetched_this_run = 0
    for batch_no, batch in enumerate(batches, start=1):
        if batch_no < start_batch:
            continue

        ckpt_path = _checkpoint_path(checkpoint_dir, batch_no)
        if resume and ckpt_path.exists() and not force_refetch:
            if batch_no % 50 == 0 or batch_no == total_batches:
                print(f"  WRDS raw batch {batch_no}/{total_batches}: checkpoint exists (skip)", flush=True)
            continue

        print(
            f"  WRDS raw batch {batch_no}/{total_batches} ({len(batch):,} job_ids)...",
            flush=True,
        )
        part = _fetch_batch_with_retry(
            username,
            password,
            batch,
            max_retries=max_retries,
            retry_sleep_sec=retry_sleep_sec,
        )
        _write_checkpoint(ckpt_path, part)
        fetched_this_run += 1

        done = _completed_batch_numbers(checkpoint_dir)
        progress["completed_batches"] = sorted(done)
        progress["last_completed_batch"] = max(done) if done else 0
        progress["updated_at_utc"] = datetime.utcnow().isoformat()
        _save_progress(checkpoint_dir, progress)

    return fetched_this_run


def _configure_merge_connection(con: duckdb.DuckDBPyConnection) -> None:
    temp_dir = ROOT / "data" / "local" / "duckdb_temp"
    temp_dir.mkdir(parents=True, exist_ok=True)
    con.execute(f"SET temp_directory='{temp_dir.as_posix()}'")
    con.execute("SET threads=2")
    con.execute("SET preserve_insertion_order=false")
    con.execute("SET memory_limit='4GB'")
    con.execute("SET max_temp_directory_size='40GiB'")


def _expected_total_batches(checkpoint_dir: Path) -> int:
    progress = _load_progress(checkpoint_dir)
    if progress and progress.get("total_batches"):
        return int(progress["total_batches"])
    completed = _completed_batch_numbers(checkpoint_dir)
    if completed:
        return max(completed)
    consolidated = list((checkpoint_dir / "_staging").glob("consolidated_*.parquet"))
    if consolidated:
        return len(consolidated) * 150
    raise SystemExit(f"No checkpoint files found under {checkpoint_dir}")


def _checkpoint_batch_paths(
    checkpoint_dir: Path, batch_lo: int, batch_hi: int
) -> list[Path]:
    return [
        _checkpoint_path(checkpoint_dir, batch_no)
        for batch_no in range(batch_lo, batch_hi + 1)
        if _checkpoint_path(checkpoint_dir, batch_no).exists()
    ]


def _unlink_checkpoint_batches(paths: list[Path]) -> int:
    removed = 0
    for path in paths:
        if path.exists():
            path.unlink()
            removed += 1
    if removed:
        print(f"    removed {removed} checkpoint batch file(s)", flush=True)
    return removed


def _cleanup_merge_artifacts(checkpoint_dir: Path, staging_dir: Path) -> None:
    for path in checkpoint_dir.glob("batch_*.parquet"):
        path.unlink(missing_ok=True)
    if staging_dir.exists():
        shutil.rmtree(staging_dir)
    print("  cleaned up checkpoint batches and staging files", flush=True)


def _consolidate_checkpoints(checkpoint_dir: Path, staging_dir: Path, *, group_size: int = 150) -> str:
    """Merge checkpoint batch files into fewer staging parquets to limit scan width."""
    total_batches = _expected_total_batches(checkpoint_dir)
    if not _checkpoint_batch_paths(checkpoint_dir, 1, total_batches) and not list(
        staging_dir.glob("consolidated_*.parquet")
    ):
        raise SystemExit(f"No checkpoint files found under {checkpoint_dir}")

    staging_dir.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    try:
        _configure_merge_connection(con)
        total_groups = (total_batches + group_size - 1) // group_size
        for gi in range(total_groups):
            out_path = staging_dir / f"consolidated_{gi + 1:04d}.parquet"
            batch_lo = gi * group_size + 1
            batch_hi = min((gi + 1) * group_size, total_batches)
            source_batches = _checkpoint_batch_paths(checkpoint_dir, batch_lo, batch_hi)
            if out_path.exists():
                _unlink_checkpoint_batches(source_batches)
                continue
            if not source_batches:
                raise SystemExit(
                    f"Missing checkpoint batches {batch_lo}-{batch_hi} and no "
                    f"consolidated file at {out_path}"
                )
            files_sql = ", ".join(f"'{path.as_posix()}'" for path in source_batches)
            print(
                f"  staging group {gi + 1}/{total_groups} ({len(source_batches)} checkpoint files)...",
                flush=True,
            )
            con.execute(
                f"""
                COPY (
                    SELECT DISTINCT
                        CAST(job_id AS BIGINT) AS job_id,
                        CAST(title_raw AS VARCHAR) AS title_raw,
                        CAST(description AS VARCHAR) AS description
                    FROM read_parquet([{files_sql}])
                    WHERE job_id IS NOT NULL
                ) TO '{out_path.as_posix()}' (FORMAT PARQUET)
                """
            )
            _unlink_checkpoint_batches(source_batches)
    finally:
        con.close()

    consolidated = sorted(staging_dir.glob("consolidated_*.parquet"))
    if len(consolidated) != total_groups:
        raise SystemExit(
            f"Staging incomplete: {len(consolidated)}/{total_groups} consolidated files "
            f"under {staging_dir}"
        )
    print(f"  staging: {len(consolidated)} consolidated parquet file(s) ready", flush=True)

    return str(staging_dir / "consolidated_*.parquet")


def _existing_text_job_ids(out_dir: Path) -> set[int]:
    pattern = str(out_dir / "year=*" / "month=*" / "posting_text.parquet")
    import glob

    files = glob.glob(pattern)
    if not files:
        return set()
    con = duckdb.connect()
    try:
        rows = con.execute(
            f"""
            SELECT DISTINCT CAST(job_id AS BIGINT) AS job_id
            FROM read_parquet('{pattern.replace(chr(92), "/")}', hive_partitioning=1)
            WHERE job_id IS NOT NULL
            """
        ).fetchall()
    finally:
        con.close()
    return {int(r[0]) for r in rows}


def _merge_checkpoints_to_output(
    jobs: pd.DataFrame,
    checkpoint_dir: Path,
    out_dir: Path,
    *,
    incremental: bool = False,
) -> int:
    staging_dir = checkpoint_dir / "_staging"
    try:
        _expected_total_batches(checkpoint_dir)
    except SystemExit:
        if not list(staging_dir.glob("consolidated_*.parquet")):
            raise

    text_pattern = _consolidate_checkpoints(checkpoint_dir, staging_dir)

    con = duckdb.connect()
    try:
        _configure_merge_connection(con)
        con.register("jobs_df", jobs)

        months = con.execute(
            """
            SELECT DISTINCT
                EXTRACT(year FROM CAST(post_date AS DATE))::INTEGER AS year,
                EXTRACT(month FROM CAST(post_date AS DATE))::INTEGER AS month
            FROM jobs_df
            ORDER BY 1, 2
            """
        ).fetchall()

        out_dir.mkdir(parents=True, exist_ok=True)
        total_rows = 0

        for year, month in months:
            part_dir = out_dir / f"year={year}" / f"month={month}"
            part_dir.mkdir(parents=True, exist_ok=True)
            out_file = part_dir / "posting_text.parquet"
            new_rows_sql = f"""
                SELECT DISTINCT
                    CAST(j.job_id AS BIGINT) AS job_id,
                    CAST(c.title_raw AS VARCHAR) AS title_raw,
                    CAST(c.description AS VARCHAR) AS description,
                    CAST(j.post_date AS DATE) AS post_date,
                    {int(year)} AS year,
                    {int(month)} AS month
                FROM jobs_df AS j
                INNER JOIN read_parquet('{text_pattern}') AS c
                  ON CAST(j.job_id AS BIGINT) = CAST(c.job_id AS BIGINT)
                WHERE EXTRACT(year FROM CAST(j.post_date AS DATE)) = {int(year)}
                  AND EXTRACT(month FROM CAST(j.post_date AS DATE)) = {int(month)}
            """
            if out_file.exists() and not incremental:
                n = con.execute(
                    f"SELECT COUNT(*) FROM read_parquet('{out_file.as_posix()}')"
                ).fetchone()[0]
                total_rows += int(n)
                print(f"  merge partition {year}-{month:02d}: {n:,} rows (existing)", flush=True)
                continue
            if out_file.exists() and incremental:
                print(f"  merge partition {year}-{month:02d} (incremental union) ...", flush=True)
                con.execute(
                    f"""
                    COPY (
                        SELECT DISTINCT job_id, title_raw, description, post_date, year, month
                        FROM (
                            SELECT * FROM read_parquet('{out_file.as_posix()}')
                            UNION ALL
                            {new_rows_sql}
                        )
                    ) TO '{out_file.as_posix()}.tmp' (FORMAT PARQUET)
                    """
                )
                Path(f"{out_file.as_posix()}.tmp").replace(out_file)
            else:
                print(f"  merge partition {year}-{month:02d} ...", flush=True)
                con.execute(
                    f"""
                    COPY (
                        {new_rows_sql}
                    ) TO '{out_file.as_posix()}' (FORMAT PARQUET)
                    """
                )
            n = con.execute(
                f"SELECT COUNT(*) FROM read_parquet('{out_file.as_posix()}')"
            ).fetchone()[0]
            total_rows += int(n)
            print(f"    {year}-{month:02d}: {n:,} rows", flush=True)
    finally:
        con.close()

    _cleanup_merge_artifacts(checkpoint_dir, staging_dir)
    return total_rows


def _write_partitioned_parquet(df: pd.DataFrame, out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    work = df.copy()
    work["year"] = pd.to_datetime(work["post_date"], errors="coerce").dt.year
    work["month"] = pd.to_datetime(work["post_date"], errors="coerce").dt.month
    work = work.dropna(subset=["year", "month", "job_id"])
    work["year"] = work["year"].astype(int)
    work["month"] = work["month"].astype(int)
    work = work.drop_duplicates(subset=["job_id"], keep="first")
    con = duckdb.connect()
    try:
        con.register("tmp_df", work)
        con.execute(
            f"COPY tmp_df TO '{out_dir}' (FORMAT PARQUET, PARTITION_BY (year, month), OVERWRITE_OR_IGNORE TRUE);"
        )
    finally:
        con.close()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Extract postings_cosmos_raw text for CS job_ids in local postings_cosmos."
    )
    parser.add_argument("--config", default=str(CONFIG_PATH))
    parser.add_argument("--first-months", type=int, default=None, metavar="N")
    parser.add_argument("--last-months", type=int, default=None, metavar="N")
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=DEFAULT_CHUNK_SIZE,
        help=f"job_ids per WRDS IN (...) query (default {DEFAULT_CHUNK_SIZE}).",
    )
    parser.add_argument("--limit-job-ids", type=int, default=None)
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Skip batches that already have checkpoint parquet files.",
    )
    parser.add_argument(
        "--start-batch",
        type=int,
        default=1,
        metavar="N",
        help="1-based batch number to start from (default 1).",
    )
    parser.add_argument(
        "--force-refetch",
        action="store_true",
        help="Re-fetch WRDS rows even when a checkpoint file already exists.",
    )
    parser.add_argument(
        "--max-retries",
        type=int,
        default=DEFAULT_MAX_RETRIES,
        help=f"WRDS retries per batch on transient connection errors (default {DEFAULT_MAX_RETRIES}).",
    )
    parser.add_argument(
        "--retry-sleep-sec",
        type=int,
        default=DEFAULT_RETRY_SLEEP_SEC,
        help=f"Base seconds between retries (multiplied by attempt number).",
    )
    parser.add_argument(
        "--merge-only",
        action="store_true",
        help="Skip WRDS fetch; merge existing checkpoints into partitioned output parquet.",
    )
    parser.add_argument(
        "--fetch-all",
        action="store_true",
        help="Fetch all local job_ids (default: only job_ids missing from posting_text output).",
    )
    args = parser.parse_args()
    missing_only = not args.fetch_all

    if args.first_months is not None and args.last_months is not None:
        raise SystemExit("Use only one of --first-months or --last-months.")
    if args.start_batch < 1:
        raise SystemExit("--start-batch must be >= 1.")

    cfg = yaml.safe_load(Path(args.config).read_text(encoding="utf-8"))
    periods = _month_periods_inclusive(cfg["extract"]["start_date"], cfg["extract"]["end_date"])
    if args.first_months is not None:
        periods = periods[: max(0, int(args.first_months))]
    elif args.last_months is not None:
        periods = periods[-max(0, int(args.last_months)) :]
    if not periods:
        raise SystemExit("No calendar months in configured extract date range.")

    username = os.getenv(cfg["wrds"]["username_env"], "")
    password = os.getenv(cfg["wrds"]["password_env"])
    if not username and not args.merge_only:
        raise ValueError(f"Missing WRDS username env var: {cfg['wrds']['username_env']}")

    run_id = f"posting_text_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:6]}"
    source_root = get_source_root()
    out_dir = source_root / "revelio_job_postings" / "postings_cosmos_raw"
    checkpoint_dir = _checkpoint_dir(out_dir)
    metadata_dir = source_root / "metadata"
    metadata_dir.mkdir(parents=True, exist_ok=True)

    print("Step 1/3: load job_id list from local postings_cosmos ...", flush=True)
    jobs = _load_local_job_ids(source_root, periods)
    if jobs.empty:
        raise SystemExit(
            "No local postings_cosmos parquet for selected months. "
            "Run: python scripts/extract_wrds_cs_snapshot.py"
        )

    job_ids = jobs["job_id"].astype(int).tolist()
    incremental_merge = False
    if missing_only and not args.merge_only:
        existing_ids = _existing_text_job_ids(out_dir)
        if existing_ids:
            before = len(job_ids)
            job_ids = [jid for jid in job_ids if jid not in existing_ids]
            jobs = jobs[jobs["job_id"].isin(job_ids)]
            incremental_merge = True
            print(
                f"  missing-only: {before:,} local job_ids -> {len(job_ids):,} without text "
                f"({len(existing_ids):,} already in output)",
                flush=True,
            )
            if not job_ids:
                print("  all local job_ids already have posting text; nothing to fetch.", flush=True)
                args.merge_only = True
    if args.limit_job_ids is not None:
        job_ids = job_ids[: max(0, int(args.limit_job_ids))]
        jobs = jobs[jobs["job_id"].isin(job_ids)]

    n_batches = (len(job_ids) + args.chunk_size - 1) // args.chunk_size
    months = [p[0] for p in periods]

    if args.merge_only:
        print("Step 2/3: skipped (--merge-only)", flush=True)
    else:
        print(
            f"Step 2/3: fetch text from WRDS {RAW_SCHEMA}.{RAW_TABLE} "
            f"({len(job_ids):,} job_ids, {n_batches} batches, chunk={args.chunk_size})...",
            flush=True,
        )
        print(f"  checkpoints: {checkpoint_dir}", flush=True)
        fetched = _fetch_text_batches_checkpointed(
            username,
            password,
            job_ids,
            args.chunk_size,
            checkpoint_dir,
            start_batch=args.start_batch,
            resume=args.resume,
            force_refetch=args.force_refetch,
            max_retries=args.max_retries,
            retry_sleep_sec=args.retry_sleep_sec,
            run_id=run_id,
            months=months,
        )
        print(f"  fetched {fetched:,} new batches this run", flush=True)

    print("Step 3/3: merge checkpoints and write partitioned parquet ...", flush=True)
    rows_with_text = _merge_checkpoints_to_output(
        jobs, checkpoint_dir, out_dir, incremental=incremental_merge
    )
    missing = len(jobs) - rows_with_text
    if missing:
        print(f"  warning: {missing:,} local job_ids had no raw text row on WRDS", flush=True)

    manifest = {
        "run_id": run_id,
        "run_timestamp_utc": datetime.utcnow().isoformat(),
        "strategy": "local_job_ids_then_wrds_raw_in_checkpointed_batches",
        "local_job_ids": len(jobs),
        "rows_with_text": int(rows_with_text),
        "wrds_batches": n_batches,
        "chunk_size": args.chunk_size,
        "months": months,
        "checkpoint_dir": str(checkpoint_dir),
        "output_path": str(out_dir),
        "columns": ["job_id", "title_raw", "description", "post_date"],
        "resume": args.resume,
        "start_batch": args.start_batch,
    }
    manifest_path = metadata_dir / f"{run_id}.json"
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")

    print(f"Posting text extract complete. run_id={run_id}")
    print(f"Rows with text: {rows_with_text:,}")
    print(f"Output: {out_dir}")
    print(f"Manifest: {manifest_path}")


if __name__ == "__main__":
    main()
