"""Trajectory classifier training and persistence."""

from __future__ import annotations

import logging
import uuid
from pathlib import Path
from typing import Any

import joblib
import pandas as pd
import psycopg
from sklearn.ensemble import RandomForestClassifier
from sklearn.impute import SimpleImputer
from sklearn.pipeline import Pipeline

from config.postgres import get_postgres_dsn
from ml.constants import (
    DEFAULT_ARTIFACTS_DIR,
    DEFAULT_MODEL_FILENAME,
    FEATURE_COLUMNS,
    FEATURE_VERSION,
    LABEL_VERSION,
    METHOD,
    METHOD_VERSION,
)
from ml.data import prepare_xy, temporal_split
from ml.evaluate import classification_report_dict

logger = logging.getLogger(__name__)


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
    if "cs_allowlist_version" in dataset.columns and not dataset["cs_allowlist_version"].empty:
        metrics["cs_allowlist_version"] = str(dataset["cs_allowlist_version"].iloc[0])
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


def persist_eval_split(
    metrics: dict[str, Any],
    *,
    entity_type: str,
    dsn: str | None = None,
) -> str | None:
    """Record the train/validation split used for this training run in Postgres.

    Inserts a backing `metadata.pipeline_runs` row (if needed) and a
    `metadata.model_eval_splits` row describing the temporal split. This is
    best-effort: if Postgres is unavailable, a warning is logged and `None`
    is returned so dev CSV training never hard-fails.
    """
    run_id = f"run_{uuid.uuid4().hex[:12]}"
    split_id = f"split_{uuid.uuid4().hex[:12]}"

    try:
        with psycopg.connect(dsn or get_postgres_dsn(), connect_timeout=5) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO metadata.pipeline_runs(
                        run_id, run_timestamp, feature_version, label_version,
                        method_version, cs_allowlist_version, notes
                    ) VALUES (%s, NOW(), %s, %s, %s, %s, %s)
                    ON CONFLICT (run_id) DO NOTHING;
                    """,
                    (
                        run_id,
                        FEATURE_VERSION,
                        LABEL_VERSION,
                        METHOD_VERSION,
                        metrics.get("cs_allowlist_version", "unknown"),
                        f"trajectory classifier training run (entity_type={entity_type})",
                    ),
                )
                cur.execute(
                    """
                    INSERT INTO metadata.model_eval_splits(
                        split_id, run_id, entity_type, train_start_month, train_end_month,
                        validation_start_month, validation_end_month
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (split_id) DO NOTHING;
                    """,
                    (
                        split_id,
                        run_id,
                        entity_type,
                        metrics["train_month_min"],
                        metrics["train_month_max"],
                        metrics["validation_month_min"],
                        metrics["validation_month_max"],
                    ),
                )
            conn.commit()
    except psycopg.Error as exc:
        logger.warning(
            "Skipping eval split persistence: Postgres unavailable (%s: %s)",
            exc.__class__.__name__,
            exc,
        )
        return None

    logger.info("Persisted eval split split_id=%s run_id=%s", split_id, run_id)
    return split_id
