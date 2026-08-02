"""Leakage guard: labels must be forward-looking, not contemporaneous with features."""

import re
from pathlib import Path

from ml.constants import FEATURE_COLUMNS, LABEL_HORIZON_MONTHS

ROOT = Path(__file__).resolve().parents[1]
LABELS_SQL = ROOT / "sql" / "duckdb" / "60_trajectory_labels.sql"


def test_label_horizon_is_positive():
    assert LABEL_HORIZON_MONTHS >= 1


def test_labels_sql_uses_forward_join_interval():
    text = LABELS_SQL.read_text(encoding="utf-8")
    assert "JOIN marts.trajectory_features fwd" in text
    assert "INTERVAL" in text and "MONTH" in text
    assert f"tf.month + INTERVAL {LABEL_HORIZON_MONTHS} MONTH" in text


def test_feature_columns_not_used_as_contemporaneous_label_inputs():
    text = LABELS_SQL.read_text(encoding="utf-8")
    outer_select = text.split(")\nSELECT", 1)[1]
    case_section = outer_select.split("FROM forward f", 1)[0]
    case_match = re.search(
        r"CASE\s+(.*?)\s+END AS trajectory_class",
        case_section,
        re.DOTALL,
    )
    assert case_match is not None
    case_block = case_match.group(1)

    for col in FEATURE_COLUMNS:
        assert f"tf.{col}" not in case_block, (
            f"Feature column {col!r} appears as a contemporaneous label input"
        )
        assert f"f.{col}" not in case_block, (
            f"Feature column {col!r} appears without forward prefix in label CASE"
        )
