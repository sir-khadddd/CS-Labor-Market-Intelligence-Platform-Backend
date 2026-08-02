"""Unit tests for DuckDB SQL templating from cs_universe.yml."""

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
