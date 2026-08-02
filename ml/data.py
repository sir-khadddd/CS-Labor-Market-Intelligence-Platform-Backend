"""Load and split trajectory feature/label datasets."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Literal

import pandas as pd
import psycopg
import yaml

from config.postgres import get_postgres_dsn
from ml.constants import DEV_PROCESSED_DIR, FEATURE_COLUMNS, FEATURE_VERSION, LABEL_VERSION

logger = logging.getLogger(__name__)
CONFIG_PATH = Path(__file__).resolve().parents[1] / "config" / "cs_universe.yml"


def _load_csv_tables(data_dir: Path) -> tuple[pd.DataFrame, pd.DataFrame]:
    features = pd.read_csv(data_dir / "trajectory_features.csv", parse_dates=["month"])
    labels = pd.read_csv(data_dir / "trajectory_labels.csv", parse_dates=["month"])
    return features, labels


def _load_postgres_tables(conn: psycopg.Connection) -> tuple[pd.DataFrame, pd.DataFrame]:
    features = pd.read_sql(
        """
        SELECT *
        FROM analytics.trajectory_features
        WHERE feature_version = %s
        """,
        conn,
        params=(FEATURE_VERSION,),
    )
    labels = pd.read_sql(
        """
        SELECT *
        FROM analytics.trajectory_labels
        WHERE label_version = %s
        """,
        conn,
        params=(LABEL_VERSION,),
    )
    for frame in (features, labels):
        frame["month"] = pd.to_datetime(frame["month"])
    return features, labels


def dedupe_entity_month(
    frame: pd.DataFrame,
    *,
    sort_by: str = "posting_count",
) -> pd.DataFrame:
    """Keep one row per entity-month, preferring the strongest demand signal."""
    if frame.empty:
        return frame
    ordered = frame.sort_values(sort_by, ascending=False, na_position="last")
    return ordered.drop_duplicates(
        subset=["entity_type", "entity_id", "month"],
        keep="first",
    ).reset_index(drop=True)


def join_trajectory_dataset(
    features: pd.DataFrame,
    labels: pd.DataFrame,
    *,
    entity_type: str = "role",
    method: str | None = "phase1_rules",
) -> pd.DataFrame:
    """Join features and labels on entity-month keys.

    The join is inner by design: labels are forward-looking over
    LABEL_HORIZON_MONTHS, so the last months of the panel have features but no
    outcome yet and must be dropped from supervised training.

    The method filter is applied before deduping so that high-confidence rows
    from another method (e.g. ml_classifier predictions written back into
    analytics.trajectory_labels) cannot displace the requested method's row for
    the same entity-month and silently shrink or poison the training set.
    """
    features = features[features["feature_version"] == FEATURE_VERSION].copy()
    labels = labels[labels["label_version"] == LABEL_VERSION].copy()
    if method is not None:
        labels = labels[labels["method"] == method].copy()

    label_cols = [
        "entity_type",
        "entity_id",
        "month",
        "trajectory_class",
        "trajectory_score",
        "confidence",
        "method",
        "label_version",
        "method_version",
    ]
    features = dedupe_entity_month(features)
    labels = dedupe_entity_month(labels, sort_by="confidence")

    merged = features.merge(
        labels[label_cols],
        on=["entity_type", "entity_id", "month"],
        how="inner",
    )
    merged = merged[merged["entity_type"] == entity_type].copy()
    return merged.reset_index(drop=True)


def load_trajectory_dataset(
    *,
    source: Literal["dev", "postgres"] = "dev",
    data_dir: Path | None = None,
    entity_type: str = "role",
    method: str | None = "phase1_rules",
) -> pd.DataFrame:
    """Load joined trajectory dataset from dev CSVs or Postgres."""
    if source == "dev":
        features, labels = _load_csv_tables(data_dir or DEV_PROCESSED_DIR)
    elif source == "postgres":
        with psycopg.connect(get_postgres_dsn()) as conn:
            features, labels = _load_postgres_tables(conn)
    else:
        raise ValueError(f"Unsupported source: {source}")

    return join_trajectory_dataset(
        features,
        labels,
        entity_type=entity_type,
        method=method,
    )


def _load_trajectory_split_config() -> dict[str, pd.Timestamp]:
    """Read train/validation split months from cs_universe.yml."""
    cfg = yaml.safe_load(CONFIG_PATH.read_text(encoding="utf-8"))
    traj = cfg.get("trajectory_ml") or {}
    return {
        "train_start_month": pd.to_datetime(traj.get("train_start_month", "2023-01-01")),
        "validation_start_month": pd.to_datetime(
            traj.get("validation_start_month", "2025-01-01")
        ),
    }


def _warn_split_config_mismatch(dataset: pd.DataFrame) -> None:
    """Log when embedded split months disagree with cs_universe.yml."""
    expected = _load_trajectory_split_config()
    for column in ("train_start_month", "validation_start_month"):
        if column not in dataset.columns:
            continue
        values = pd.to_datetime(dataset[column].dropna().unique())
        if len(values) > 1:
            logger.warning("Multiple %s values in dataset: %s", column, values.tolist())
        if len(values) == 0:
            continue
        actual = values[0]
        expected_value = expected[column]
        if actual != expected_value:
            logger.warning(
                "%s in dataset (%s) differs from config/cs_universe.yml (%s)",
                column,
                actual.date(),
                expected_value.date(),
            )


def temporal_split(
    dataset: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Split dataset using validation_start_month embedded in features."""
    if dataset.empty:
        raise ValueError("Cannot split an empty trajectory dataset")

    _warn_split_config_mismatch(dataset)

    validation_start = pd.to_datetime(dataset["validation_start_month"].iloc[0])
    train = dataset[dataset["month"] < validation_start].copy()
    validation = dataset[dataset["month"] >= validation_start].copy()
    return train.reset_index(drop=True), validation.reset_index(drop=True)


def prepare_xy(
    frame: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.Series]:
    """Extract model features and target labels."""
    x = frame[FEATURE_COLUMNS].copy()
    y = frame["trajectory_class"].copy()
    return x, y
