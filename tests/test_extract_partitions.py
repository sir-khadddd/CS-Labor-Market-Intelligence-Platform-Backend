"""Unit tests for WRDS extract partition resume/reset semantics."""

from pathlib import Path

import pytest

pytest.importorskip("wrds")

from scripts.extract_wrds_cs_snapshot import (  # noqa: E402
    _complete_parquet_paths,
    _partition_dir,
    _partition_exists,
    _reset_partition,
)


def _make_partition(out_dir: Path, year: int, month: int, *, parquet: bool, success: bool) -> Path:
    part_dir = _partition_dir(out_dir, year, month)
    part_dir.mkdir(parents=True, exist_ok=True)
    if parquet:
        (part_dir / "data_0.parquet").write_bytes(b"PAR1")
    if success:
        (part_dir / "_SUCCESS").write_text("", encoding="utf-8")
    return part_dir


def test_partition_exists_with_success_sentinel(tmp_path: Path):
    _make_partition(tmp_path, 2024, 1, parquet=True, success=True)
    assert _partition_exists(tmp_path, 2024, 1) is True


def test_partition_exists_for_legacy_parquet_without_sentinel(tmp_path: Path):
    _make_partition(tmp_path, 2024, 2, parquet=True, success=False)
    assert _partition_exists(tmp_path, 2024, 2) is True


def test_partition_missing_when_dir_absent_or_empty(tmp_path: Path):
    assert _partition_exists(tmp_path, 2024, 3) is False
    _make_partition(tmp_path, 2024, 4, parquet=False, success=False)
    assert _partition_exists(tmp_path, 2024, 4) is False


def test_reset_partition_removes_directory(tmp_path: Path):
    part_dir = _make_partition(tmp_path, 2024, 5, parquet=True, success=True)
    _reset_partition(tmp_path, 2024, 5)
    assert not part_dir.exists()


def test_complete_parquet_paths_includes_legacy_partitions(tmp_path: Path):
    _make_partition(tmp_path, 2024, 6, parquet=True, success=True)
    _make_partition(tmp_path, 2024, 7, parquet=True, success=False)
    _make_partition(tmp_path, 2024, 8, parquet=False, success=False)

    found = {p.parent.name for p in _complete_parquet_paths(tmp_path)}
    assert found == {"month=6", "month=7"}
