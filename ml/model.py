"""Trajectory classifier training and persistence."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import joblib
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.impute import SimpleImputer
from sklearn.pipeline import Pipeline

from ml.constants import (
    DEFAULT_ARTIFACTS_DIR,
    DEFAULT_MODEL_FILENAME,
    FEATURE_COLUMNS,
    METHOD,
    METHOD_VERSION,
)
from ml.data import prepare_xy, temporal_split
from ml.evaluate import classification_report_dict


def build_pipeline() -> Pipeline:
    """Create sklearn pipeline for trajectory classification."""
    return Pipeline(
        steps=[
            ("imputer", SimpleImputer(strategy="median")),
            (
                "classifier",
                RandomForestClassifier(
                    n_estimators=200,
                    random_state=42,
                    class_weight="balanced_subsample",
                    n_jobs=-1,
                ),
            ),
        ]
    )


def train_trajectory_classifier(
    dataset: pd.DataFrame,
) -> tuple[Pipeline, dict[str, Any]]:
    """Train classifier with temporal train/validation split."""
    train_df, validation_df = temporal_split(dataset)
    if train_df.empty or validation_df.empty:
        raise ValueError(
            "Temporal split produced empty train or validation set. "
            "Regenerate processed outputs and dev snapshot, or train with --source postgres."
        )

    x_train, y_train = prepare_xy(train_df)
    x_val, y_val = prepare_xy(validation_df)

    pipeline = build_pipeline()
    pipeline.fit(x_train, y_train)

    val_predictions = pipeline.predict(x_val)
    metrics = classification_report_dict(y_val, val_predictions)
    metrics.update(
        {
            "method": METHOD,
            "method_version": METHOD_VERSION,
            "feature_columns": FEATURE_COLUMNS,
            "train_rows": len(train_df),
            "validation_rows": len(validation_df),
            "train_month_min": str(train_df["month"].min().date()),
            "train_month_max": str(train_df["month"].max().date()),
            "validation_month_min": str(validation_df["month"].min().date()),
            "validation_month_max": str(validation_df["month"].max().date()),
        }
    )
    return pipeline, metrics


def save_model(
    pipeline: Pipeline,
    metrics: dict[str, Any],
    *,
    output_dir: Path | None = None,
    model_filename: str = DEFAULT_MODEL_FILENAME,
) -> Path:
    """Persist trained pipeline and metrics metadata."""
    artifacts_dir = output_dir or DEFAULT_ARTIFACTS_DIR
    artifacts_dir.mkdir(parents=True, exist_ok=True)

    model_path = artifacts_dir / model_filename
    metadata_path = artifacts_dir / f"{model_path.stem}_metrics.json"

    joblib.dump(pipeline, model_path)
    pd.Series(metrics).to_json(metadata_path, indent=2)
    return model_path


def load_model(model_path: Path) -> Pipeline:
    """Load a persisted trajectory classifier."""
    return joblib.load(model_path)
