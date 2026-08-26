"""Unit tests for load_postgres --input-dir path resolution."""

import sys
from pathlib import Path

import pytest

from scripts.load_postgres import PROCESSED_DIR, ROOT, _resolve_input_dir, parse_args


def test_parse_args_default_input_dir():
    sys.argv = ["load_postgres.py"]
    args = parse_args()
    assert args.input_dir is None
    assert _resolve_input_dir(args.input_dir) == PROCESSED_DIR


def test_parse_args_input_dir_dev_processed():
    sys.argv = ["load_postgres.py", "--input-dir", "data/dev_processed"]
    args = parse_args()
    assert args.input_dir == Path("data/dev_processed")
    resolved = _resolve_input_dir(args.input_dir)
    assert resolved == (ROOT / "data" / "dev_processed").resolve()


def test_resolve_input_dir_missing_raises(tmp_path: Path):
    missing = tmp_path / "does_not_exist"
    with pytest.raises(FileNotFoundError, match="Input directory does not exist"):
        _resolve_input_dir(missing)


def test_resolve_input_dir_absolute_existing(tmp_path: Path):
    resolved = _resolve_input_dir(tmp_path)
    assert resolved == tmp_path.resolve()
