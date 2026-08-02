"""Unit tests for load_postgres CSV cleaning before COPY."""

import csv
import io
from pathlib import Path

from scripts.load_postgres import _clean_integer_cell, _iter_cleaned_csv_lines


def test_clean_integer_cell_strips_float_suffix():
    assert _clean_integer_cell("6963.0") == "6963"
    assert _clean_integer_cell(" 42.00 ") == "42"


def test_clean_integer_cell_empty_and_null_tokens_become_blank():
    assert _clean_integer_cell("") == ""
    assert _clean_integer_cell("null") == ""
    assert _clean_integer_cell("NaN") == ""


def test_iter_cleaned_csv_preserves_quoted_empty_text(tmp_path: Path):
    csv_path = tmp_path / "trajectory_labels.csv"
    csv_path.write_text(
        "entity_type,entity_id,month,trajectory_class,method\n"
        'role,R1,2024-01-01,stable_growth,phase1_rules\n'
        'role,R2,2024-02-01,,phase1_rules\n',
        encoding="utf-8",
    )

    lines = list(_iter_cleaned_csv_lines(csv_path, "trajectory_labels"))
    assert len(lines) == 3

    reader = csv.reader(io.StringIO("".join(lines[1:])))
    rows = list(reader)
    assert rows[1][3] == ""

    rewritten = lines[2]
    assert '""' in rewritten


def test_iter_cleaned_csv_normalizes_integer_floats(tmp_path: Path):
    csv_path = tmp_path / "trajectory_features.csv"
    csv_path.write_text(
        "entity_type,entity_id,month,posting_count\n"
        "role,R1,2024-01-01,6963.0\n",
        encoding="utf-8",
    )

    lines = list(_iter_cleaned_csv_lines(csv_path, "trajectory_features"))
    reader = csv.reader(io.StringIO("".join(lines[1:])))
    row = next(reader)
    assert row[3] == "6963"
