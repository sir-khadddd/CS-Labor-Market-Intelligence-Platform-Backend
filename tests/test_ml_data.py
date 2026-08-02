"""Unit tests for ml.data dedupe, temporal split, and join guards."""

from datetime import datetime

import pandas as pd
import pytest

from ml import data as ml_data
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


def _patch_split_config(monkeypatch, *, train_start=None, validation_start=None):
    monkeypatch.setattr(
        ml_data,
        "_load_trajectory_split_config",
        lambda: {
            "train_start_month": pd.to_datetime(train_start) if train_start else None,
            "validation_start_month": (
                pd.to_datetime(validation_start) if validation_start else None
            ),
        },
    )


def test_temporal_split_by_validation_start_month(monkeypatch):
    _patch_split_config(monkeypatch, validation_start="2024-06-01")
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


def test_temporal_split_prefers_config_over_embedded_month(monkeypatch):
    _patch_split_config(monkeypatch, validation_start="2024-06-01")
    dataset = pd.DataFrame(
        [
            _feature_row(
                month=pd.Timestamp(month),
                validation_start_month=pd.Timestamp("2024-03-01"),
            )
            for month in ("2024-03-01", "2024-05-01", "2024-06-01")
        ]
    )
    dataset["trajectory_class"] = "stable_growth"
    train, validation = temporal_split(dataset)
    assert len(train) == 2
    assert len(validation) == 1
    assert validation.iloc[0]["month"] == pd.Timestamp("2024-06-01")


def test_temporal_split_applies_config_train_start_lower_bound(monkeypatch):
    _patch_split_config(monkeypatch, train_start="2024-04-01", validation_start="2024-06-01")
    dataset = pd.DataFrame(
        [
            _feature_row(month=pd.Timestamp(month))
            for month in ("2024-02-01", "2024-04-01", "2024-05-01", "2024-07-01")
        ]
    )
    dataset["trajectory_class"] = "stable_growth"
    train, validation = temporal_split(dataset)
    assert list(train["month"]) == [pd.Timestamp("2024-04-01"), pd.Timestamp("2024-05-01")]
    assert len(validation) == 1


def test_temporal_split_falls_back_to_embedded_months(monkeypatch):
    _patch_split_config(monkeypatch)
    dataset = pd.DataFrame(
        [
            _feature_row(
                month=pd.Timestamp(month),
                train_start_month=pd.Timestamp("2024-04-01"),
                validation_start_month=pd.Timestamp("2024-06-01"),
            )
            for month in ("2024-02-01", "2024-04-01", "2024-07-01")
        ]
    )
    dataset["trajectory_class"] = "stable_growth"
    train, validation = temporal_split(dataset)
    assert list(train["month"]) == [pd.Timestamp("2024-04-01")]
    assert len(validation) == 1


def test_temporal_split_without_any_split_month_raises(monkeypatch):
    _patch_split_config(monkeypatch)
    dataset = pd.DataFrame([_feature_row(month=pd.Timestamp("2024-02-01"))])
    dataset = dataset.drop(columns=["validation_start_month"])
    dataset["trajectory_class"] = "stable_growth"
    with pytest.raises(ValueError, match="No validation_start_month"):
        temporal_split(dataset)


def test_temporal_split_warns_on_config_mismatch(monkeypatch, caplog):
    _patch_split_config(monkeypatch, train_start="2024-01-01", validation_start="2024-06-01")
    dataset = pd.DataFrame(
        [
            _feature_row(
                month=pd.Timestamp("2024-03-01"),
                validation_start_month=pd.Timestamp("2024-05-01"),
                train_start_month=pd.Timestamp("2098-01-01"),
            ),
            _feature_row(
                month=pd.Timestamp("2024-07-01"),
                validation_start_month=pd.Timestamp("2024-05-01"),
                train_start_month=pd.Timestamp("2098-01-01"),
            ),
        ]
    )
    dataset["trajectory_class"] = "stable_growth"
    with caplog.at_level("WARNING"):
        train, validation = temporal_split(dataset)
    assert len(train) == 1
    assert len(validation) == 1
    assert any("validation_start_month in dataset" in record.message for record in caplog.records)
    assert any("train_start_month in dataset" in record.message for record in caplog.records)


def test_load_trajectory_split_config_reads_repo_config():
    config = ml_data._load_trajectory_split_config()
    assert config["train_start_month"] == pd.Timestamp("2023-01-01")
    assert config["validation_start_month"] == pd.Timestamp("2025-01-01")
