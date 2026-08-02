"""Shared constants for trajectory ML."""

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DEV_PROCESSED_DIR = ROOT / "data" / "dev_processed"
DEFAULT_ARTIFACTS_DIR = ROOT / "ml" / "artifacts"

# Leakage guard: every column here must be observable at month t. The label for month t is
# derived only from the entity's state at t + LABEL_HORIZON_MONTHS (see
# sql/duckdb/60_trajectory_labels.sql), so no contemporaneous label input appears below.
# Before adding a column, confirm the label rule does not read it for the same row.
FEATURE_COLUMNS = [
    "posting_count",
    "yoy_growth",
    "rolling_3m_growth",
    "acceleration",
    "volatility_12m",
    "demand_concentration_index",
    "momentum_score",
]

TRAJECTORY_CLASSES = [
    "declining",
    "emerging",
    "plateau",
    "stable_growth",
    "uncertain",
]

# Months ahead the label describes. Hardcoded in sql/duckdb/60_trajectory_labels.sql too;
# changing one requires changing the other and a DuckDB rebuild.
LABEL_HORIZON_MONTHS = 3

METHOD = "ml_classifier"
# ml-v2 predicts the forward-looking phase1-v3 target; ml-v1 predicted the circular v2 target.
METHOD_VERSION = "ml-v2"
# Rules-derived target label_version that ML predictions describe (not the ML method lineage).
LABEL_VERSION = "phase1-v3"
FEATURE_VERSION = "phase1-v1"
# DuckDB pipeline / trajectory label rules (sql/duckdb/01_stage_revelio.sql).
RULES_METHOD_VERSION = "rules-v3"
CS_ALLOWLIST_VERSION = "2026.04"

DEFAULT_MODEL_FILENAME = "trajectory_role_classifier.joblib"
