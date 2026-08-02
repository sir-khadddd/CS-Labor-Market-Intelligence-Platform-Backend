"""Unit tests for ml.data dedupe, temporal split, and join guards."""

from datetime import datetime

import pandas as pd
import pytest

from ml.constants import FEATURE_VERSION, LABEL_VERSION
from ml.data import dedupe_entity_month, join_trajectory_dataset, temporal_split


def _feature_row(**overrides):
    base = {
        "entity_type": "role",
        "entity_id": "R1",
        "month": pd.Timestamp("2024-01-01"),
        "posting_count": 10,
        "yoy_growth": 1.0,
        "rolling_3m_growth": 0.5,
        "acceleration": 0.1,
        "volatility_12m": 5.0,
        "demand_concentration_index": 0.2,
        "momentum_score": 0.3,
        "feature_version": FEATURE_VERSION,
        "validation_start_month": pd.Timestamp("2024-06-01"),
    }
    base.update(overrides)
    return base


def _label_row(**overrides):
    base = {
        "entity_type": "role",
        "entity_id": "R1",
        "month": pd.Timestamp("2024-01-01"),
        "trajectory_class": "stable_growth",
        "trajectory_score": 0.5,
        "confidence": 0.9,
        "method": "phase1_rules",
        "label_version": LABEL_VERSION,
        "method_version": "rules-v3",
    }
    base.update(overrides)
    return base


def test_dedupe_entity_month_empty_returns_empty():
    empty = pd.DataFrame(columns=["entity_type", "entity_id", "month", "posting_count"])
    result = dedupe_entity_month(empty)
    assert result.empty


def test_dedupe_entity_month_keeps_highest_posting_count():
    frame = pd.DataFrame(
        [
            _feature_row(posting_count=5),
            _feature_row(posting_count=20),
            _feature_row(entity_id="R2", posting_count=7),
        ]
    )
    result = dedupe_entity_month(frame)
    assert len(result) == 2
    r1 = result[result["entity_id"] == "R1"].iloc[0]
    assert r1["posting_count"] == 20


def test_dedupe_entity_month_respects_sort_by():
    frame = pd.DataFrame(
        [
            _label_row(confidence=0.5),
            _label_row(confidence=0.95),
        ]
    )
    result = dedupe_entity_month(frame, sort_by="confidence")
    assert len(result) == 1
    assert result.iloc[0]["confidence"] == 0.95


def test_join_trajectory_dataset_empty_features():
    labels = pd.DataFrame([_label_row()])
    features = pd.DataFrame(columns=list(_feature_row().keys()))
    result = join_trajectory_dataset(features, labels)
    assert result.empty


def test_join_trajectory_dataset_empty_labels():
    features = pd.DataFrame([_feature_row()])
    labels = pd.DataFrame(columns=list(_label_row().keys()))
    result = join_trajectory_dataset(features, labels)
    assert result.empty


def test_join_trajectory_dataset_inner_join_on_entity_month():
    features = pd.DataFrame([_feature_row(entity_id="R1"), _feature_row(entity_id="R2")])
    labels = pd.DataFrame([_label_row(entity_id="R1")])
    result = join_trajectory_dataset(features, labels)
    assert len(result) == 1
    assert result.iloc[0]["entity_id"] == "R1"
    assert result.iloc[0]["trajectory_class"] == "stable_growth"


def test_join_trajectory_dataset_keeps_rules_label_over_higher_confidence_ml():
    features = pd.DataFrame([_feature_row(entity_id="R1")])
    labels = pd.DataFrame(
        [
            _label_row(
                entity_id="R1",
                trajectory_class="stable_growth",
                confidence=0.4,
                method="phase1_rules",
                method_version="rules-v3",
            ),
            _label_row(
                entity_id="R1",
                trajectory_class="declining",
                confidence=0.99,
                method="ml_classifier",
                method_version="ml-v2",
            ),
        ]
    )
    result = join_trajectory_dataset(features, labels, method="phase1_rules")
    assert len(result) == 1
    assert result.iloc[0]["method"] == "phase1_rules"
    assert result.iloc[0]["trajectory_class"] == "stable_growth"


def test_join_trajectory_dataset_without_method_filter_keeps_highest_confidence():
    features = pd.DataFrame([_feature_row(entity_id="R1")])
    labels = pd.DataFrame(
        [
            _label_row(entity_id="R1", confidence=0.4, method="phase1_rules"),
            _label_row(
                entity_id="R1",
                trajectory_class="declining",
                confidence=0.99,
                method="ml_classifier",
                method_version="ml-v2",
            ),
        ]
    )
    result = join_trajectory_dataset(features, labels, method=None)
    assert len(result) == 1
    assert result.iloc[0]["method"] == "ml_classifier"


def test_temporal_split_empty_raises():
    empty = pd.DataFrame()
    with pytest.raises(ValueError, match="empty trajectory dataset"):
        temporal_split(empty)


def test_temporal_split_by_validation_start_month():
    dataset = pd.DataFrame(
        [
            _feature_row(month=pd.Timestamp("2024-03-01")),
            _feature_row(month=pd.Timestamp("2024-06-01")),
            _feature_row(month=pd.Timestamp("2024-07-01")),
        ]
    )
    dataset["trajectory_class"] = "stable_growth"
    train, validation = temporal_split(dataset)
    assert all(train["month"] < datetime(2024, 6, 1))
    assert all(validation["month"] >= datetime(2024, 6, 1))
    assert len(train) == 1
    assert len(validation) == 2
