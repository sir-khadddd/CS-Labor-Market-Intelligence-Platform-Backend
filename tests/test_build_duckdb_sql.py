"""Unit tests for DuckDB SQL templating from cs_universe.yml."""

import pytest

from scripts.build_duckdb import _render_sql


def test_render_sql_substitutes_trajectory_split_months():
    cfg = {
        "trajectory_ml": {
            "train_start_month": "2023-01-01",
            "validation_start_month": "2025-01-01",
        }
    }
    sql = (
        "SELECT '{{TRAIN_START_MONTH}}'::DATE AS train_start_month, "
        "'{{VALIDATION_START_MONTH}}'::DATE AS validation_start_month"
    )
    rendered = _render_sql(sql, cfg)
    assert "'2023-01-01'::DATE AS train_start_month" in rendered
    assert "'2025-01-01'::DATE AS validation_start_month" in rendered


@pytest.mark.parametrize(
    "value",
    ["2023-13-01", "not-a-date", "2023/01/01", "2023-01-01' OR '1'='1"],
)
def test_render_sql_rejects_non_iso_split_month(value):
    cfg = {"trajectory_ml": {"train_start_month": value}}
    with pytest.raises(ValueError, match="must be an ISO date"):
        _render_sql("SELECT '{{TRAIN_START_MONTH}}'::DATE", cfg)


def test_render_sql_rejects_non_iso_validation_month():
    cfg = {"trajectory_ml": {"validation_start_month": "2025-01"}}
    with pytest.raises(ValueError, match="validation_start_month"):
        _render_sql("SELECT '{{VALIDATION_START_MONTH}}'::DATE", cfg)
