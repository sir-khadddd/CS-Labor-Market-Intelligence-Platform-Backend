"""Remove large intermediate pipeline artifacts to free disk space."""

from __future__ import annotations

import argparse
import shutil
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from config.revelio_sources import get_duckdb_path, get_processed_dir, get_source_root

DEFAULT_TARGETS = {
    "duckdb_temp": ROOT / "data" / "local" / "duckdb_temp",
    "skill_hits_staging": ROOT / "data" / "local" / "posting_skill_hits_staging",
    "text_checkpoints": get_source_root()
    / "revelio_job_postings"
    / "postings_cosmos_raw"
    / "_checkpoints",
    "processed_csv": get_processed_dir(),
    "duckdb_file": get_duckdb_path(),
}


def _dir_size(path: Path) -> int:
    if not path.exists():
        return 0
    if path.is_file():
        return path.stat().st_size
    return sum(p.stat().st_size for p in path.rglob("*") if p.is_file())


def _remove_path(path: Path) -> int:
    if not path.exists():
        return 0
    size = _dir_size(path)
    if path.is_dir():
        shutil.rmtree(path)
    else:
        path.unlink()
    return size


def cleanup(
    *,
    duckdb_temp: bool = True,
    skill_hits_staging: bool = True,
    text_checkpoints: bool = False,
    processed_csv: bool = False,
    duckdb_file: bool = False,
) -> int:
    freed = 0
    if duckdb_temp:
        freed += _remove_path(DEFAULT_TARGETS["duckdb_temp"])
    if skill_hits_staging:
        freed += _remove_path(DEFAULT_TARGETS["skill_hits_staging"])
    if text_checkpoints:
        freed += _remove_path(DEFAULT_TARGETS["text_checkpoints"])
    if duckdb_file:
        duck = DEFAULT_TARGETS["duckdb_file"]
        freed += _remove_path(duck)
        freed += _remove_path(Path(str(duck) + ".wal"))
    if processed_csv:
        processed = DEFAULT_TARGETS["processed_csv"]
        if processed.exists():
            for path in processed.glob("*.csv"):
                freed += _remove_path(path)
    return freed


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Delete large intermediate pipeline files (staging, temp, optional CSV/checkpoints)."
    )
    parser.add_argument(
        "--include-checkpoints",
        action="store_true",
        help="Also delete postings_cosmos_raw/_checkpoints (only if merge already completed).",
    )
    parser.add_argument(
        "--include-csv",
        action="store_true",
        help="Delete data/processed/*.csv (keep parquet; load_postgres reads parquet).",
    )
    parser.add_argument(
        "--include-duckdb",
        action="store_true",
        help="Delete data/local/cs_lmi.duckdb (+ WAL).",
    )
    parser.add_argument(
        "--all-safe",
        action="store_true",
        help="Clean temp, staging, CSV exports, and DuckDB file (not checkpoints).",
    )
    args = parser.parse_args()

    all_safe = args.all_safe
    freed = cleanup(
        duckdb_temp=True,
        skill_hits_staging=True,
        text_checkpoints=args.include_checkpoints,
        processed_csv=args.include_csv or all_safe,
        duckdb_file=args.include_duckdb or all_safe,
    )
    print(f"Freed approximately {freed / (1024 ** 3):.2f} GiB")


if __name__ == "__main__":
    main()
