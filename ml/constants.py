"""Shared constants for trajectory ML."""

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DEV_PROCESSED_DIR = ROOT / "data" / "dev_processed"
DEFAULT_ARTIFACTS_DIR = ROOT / "ml" / "artifacts"

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

METHOD = "ml_classifier"
METHOD_VERSION = "ml-v1"
LABEL_VERSION = "phase1-v2"
FEATURE_VERSION = "phase1-v1"

DEFAULT_MODEL_FILENAME = "trajectory_role_classifier.joblib"
