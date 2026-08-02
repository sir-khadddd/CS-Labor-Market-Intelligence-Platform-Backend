"""Unit tests for load_postgres CSV cleaning before COPY."""

import csv
import io
from pathlib import Path

from scripts.load_postgres import (
    TABLE_TEXT_COLUMNS,
    _clean_integer_cell,
    _copy_sql,
    _iter_cleaned_csv_lines,
)


def test_clean_integer_cell_strips_float_suffix():
    assert _clean_integer_cell("6963.0") == "6963"
    assert _clean_integer_cell(" 42.00 ") == "42"


def test_clean_integer_cell_empty_and_null_tokens_become_blank():
    assert _clean_integer_cell("") == ""
    assert _clean_integer_cell("null") == ""
    assert _clean_integer_cell("NaN") == ""


def test_iter_cleaned_csv_keeps_empty_text_cell_value(tmp_path: Path):
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


def test_iter_cleaned_csv_leaves_null_numeric_unquoted(tmp_path: Path):
    csv_path = tmp_path / "trajectory_features.csv"
    csv_path.write_text(
        "entity_type,entity_id,month,posting_count,yoy_growth\n"
        'role,R1,2024-01-01,10,\n',
        encoding="utf-8",
    )

    lines = list(_iter_cleaned_csv_lines(csv_path, "trajectory_features"))
    data_line = lines[1].rstrip("\n")
    assert data_line == "role,R1,2024-01-01,10,"
    assert '""' not in data_line


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


def test_iter_cleaned_csv_blanks_null_token_in_integer_column(tmp_path: Path):
    csv_path = tmp_path / "trajectory_features.csv"
    csv_path.write_text(
        "entity_type,entity_id,month,posting_count\n"
        "role,R1,2024-01-01,NaN\n",
        encoding="utf-8",
    )

    lines = list(_iter_cleaned_csv_lines(csv_path, "trajectory_features"))
    assert lines[1].rstrip("\n") == "role,R1,2024-01-01,"


def test_text_columns_parsed_from_schema():
    assert TABLE_TEXT_COLUMNS["trajectory_labels"] == [
        "entity_type",
        "entity_id",
        "trajectory_class",
        "method",
        "label_version",
        "method_version",
    ]
    assert "posting_count" not in TABLE_TEXT_COLUMNS["trajectory_features"]


def test_copy_sql_forces_not_null_on_text_columns_only():
    sql = _copy_sql("trajectory_features")
    assert "FORMAT CSV, HEADER TRUE" in sql
    assert "FORCE_NOT_NULL (entity_type, entity_id, feature_version, cs_allowlist_version)" in sql
    assert "posting_count" not in sql
