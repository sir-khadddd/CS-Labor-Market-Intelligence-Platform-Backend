"""Version strings in ml.constants must match DuckDB stage SQL."""

from pathlib import Path

from ml.constants import FEATURE_VERSION, LABEL_VERSION, RULES_METHOD_VERSION

ROOT = Path(__file__).resolve().parents[1]
STAGE_SQL = ROOT / "sql" / "duckdb" / "01_stage_revelio.sql"


def test_run_context_versions_match_constants():
    text = STAGE_SQL.read_text(encoding="utf-8")
    assert f"'{FEATURE_VERSION}'::VARCHAR AS feature_version" in text
    assert f"'{LABEL_VERSION}'::VARCHAR AS label_version" in text
    assert f"'{RULES_METHOD_VERSION}'::VARCHAR AS method_version" in text
