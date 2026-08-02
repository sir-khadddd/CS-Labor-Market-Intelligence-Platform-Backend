"""Unit tests for sklearn version metadata and compatibility checks."""

import pandas as pd
import sklearn

from ml.model import sklearn_version_mismatch, train_trajectory_classifier


def _minimal_dataset():
    """Months straddle the split configured in config/cs_universe.yml."""
    rows = []
    for month in ("2024-11-01", "2024-12-01", "2025-01-01", "2025-02-01"):
        rows.append(
            {
                "entity_type": "role",
                "entity_id": "R1",
                "month": pd.Timestamp(month),
                "posting_count": 10,
                "yoy_growth": 1.0,
                "rolling_3m_growth": 0.5,
                "acceleration": 0.1,
                "volatility_12m": 5.0,
                "demand_concentration_index": 0.2,
                "momentum_score": 0.3,
                "trajectory_class": "stable_growth",
                "validation_start_month": pd.Timestamp("2025-01-01"),
                "train_start_month": pd.Timestamp("2023-01-01"),
            }
        )
    return pd.DataFrame(rows)


def test_train_trajectory_classifier_records_sklearn_version():
    _, metrics = train_trajectory_classifier(_minimal_dataset())
    assert metrics["sklearn_version"] == sklearn.__version__


def test_sklearn_major_version_mismatch_detected():
    runtime_major = sklearn.__version__.split(".", 1)[0]
    other_major = "99" if runtime_major != "99" else "98"
    metrics = {"sklearn_version": f"{other_major}.0.0"}
    message = sklearn_version_mismatch(metrics)
    assert message is not None
    assert "version mismatch" in message


def test_sklearn_minor_version_mismatch_detected():
    major, minor = sklearn.__version__.split(".")[:2]
    other_minor = int(minor) + 1
    message = sklearn_version_mismatch({"sklearn_version": f"{major}.{other_minor}.0"})
    assert message is not None
    assert "version mismatch" in message


def test_sklearn_patch_version_difference_is_accepted():
    major, minor = sklearn.__version__.split(".")[:2]
    assert sklearn_version_mismatch({"sklearn_version": f"{major}.{minor}.99"}) is None


def test_sklearn_version_mismatch_absent_when_metadata_missing():
    assert sklearn_version_mismatch({}) is None
