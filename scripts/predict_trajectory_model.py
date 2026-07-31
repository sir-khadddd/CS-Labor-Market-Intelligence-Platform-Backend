"""Generate ML trajectory label predictions from a trained classifier.

Loads the persisted classifier and the latest trajectory features (from
Postgres by default, or dev CSVs with --source dev), predicts
trajectory_class per entity-month, and writes results into
analytics.trajectory_labels tagged with method=ml_classifier. Writes are
idempotent: existing rows for the same (entity_type, method_version) are
replaced via delete-then-insert with an ON CONFLICT upsert as a safety net.
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import psycopg

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from config.postgres import get_postgres_dsn
from ml.constants import (
    DEFAULT_ARTIFACTS_DIR,
    DEFAULT_MODEL_FILENAME,
    DEV_PROCESSED_DIR,
    FEATURE_COLUMNS,
    FEATURE_VERSION,
    LABEL_VERSION,
    METHOD,
    METHOD_VERSION,
)
from ml.data import dedupe_entity_month
from ml.model import load_model

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

DEFAULT_CSV_OUTPUT = DEFAULT_ARTIFACTS_DIR / "trajectory_labels_ml.csv"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--model-path",
        type=Path,
        default=DEFAULT_ARTIFACTS_DIR / DEFAULT_MODEL_FILENAME,
        help="Path to trained model joblib artifact",
    )
    parser.add_argument(
        "--source",
        choices=("postgres", "dev"),
        default="postgres",
        help="Feature source (default: postgres)",
    )
    parser.add_argument(
        "--entity-type",
        default="role",
        help="Entity type to predict for (default: role)",
    )
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=DEV_PROCESSED_DIR,
        help="Dev CSV directory (used when --source dev)",
    )
    parser.add_argument(
        "--write-postgres",
        action="store_true",
        help="Force writing predictions to Postgres even when --source dev",
    )
    parser.add_argument(
        "--output-csv",
        type=Path,
        default=None,
        help="Optional CSV path for predictions. Defaults to "
        f"{DEFAULT_CSV_OUTPUT} when Postgres is not written to.",
    )
    return parser.parse_args()


def _load_features_dev(data_dir: Path, entity_type: str) -> pd.DataFrame:
    features = pd.read_csv(data_dir / "trajectory_features.csv", parse_dates=["month"])
    features = features[features["feature_version"] == FEATURE_VERSION]
    features = features[features["entity_type"] == entity_type]
    return dedupe_entity_month(features).reset_index(drop=True)


def _load_features_postgres(dsn: str, entity_type: str) -> pd.DataFrame:
    with psycopg.connect(dsn, connect_timeout=10) as conn:
        features = pd.read_sql(
            """
            SELECT * FROM analytics.trajectory_features
            WHERE feature_version = %s AND entity_type = %s
            """,
            conn,
            params=(FEATURE_VERSION, entity_type),
        )
    if features.empty:
        return features
    features["month"] = pd.to_datetime(features["month"])
    return dedupe_entity_month(features).reset_index(drop=True)


def predict(pipeline, features: pd.DataFrame) -> pd.DataFrame:
    """Run the classifier over feature rows and assemble a labels frame."""
    x = features[FEATURE_COLUMNS]
    predicted_class = pipeline.predict(x)

    if hasattr(pipeline, "predict_proba"):
        confidence = pipeline.predict_proba(x).max(axis=1)
    else:
        confidence = np.full(len(features), np.nan)

    result = features[["entity_type", "entity_id", "month"]].copy()
    result["trajectory_class"] = predicted_class
    result["trajectory_score"] = np.nan
    result["confidence"] = confidence
    result["method"] = METHOD
    result["label_version"] = LABEL_VERSION
    result["method_version"] = METHOD_VERSION
    return result


def _upsert_postgres(predictions: pd.DataFrame, entity_type: str) -> bool:
    """Delete-then-insert ML predictions for this entity_type/method_version.

    Returns True on success, False if Postgres is unavailable.
    """
    dsn = get_postgres_dsn()
    try:
        with psycopg.connect(dsn, connect_timeout=10) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "DELETE FROM analytics.trajectory_labels "
                    "WHERE method_version = %s AND entity_type = %s",
                    (METHOD_VERSION, entity_type),
                )
                rows = [
                    (
                        row.entity_type,
                        row.entity_id,
                        row.month.date(),
                        row.trajectory_class,
                        None if pd.isna(row.trajectory_score) else float(row.trajectory_score),
                        None if pd.isna(row.confidence) else float(row.confidence),
                        row.method,
                        row.label_version,
                        row.method_version,
                    )
                    for row in predictions.itertuples()
                ]
                cur.executemany(
                    """
                    INSERT INTO analytics.trajectory_labels(
                        entity_type, entity_id, month, trajectory_class, trajectory_score,
                        confidence, method, label_version, method_version, run_timestamp
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                    ON CONFLICT (entity_type, entity_id, month, label_version, method_version)
                    DO UPDATE SET
                        trajectory_class = EXCLUDED.trajectory_class,
                        trajectory_score = EXCLUDED.trajectory_score,
                        confidence = EXCLUDED.confidence,
                        method = EXCLUDED.method,
                        run_timestamp = EXCLUDED.run_timestamp;
                    """,
                    rows,
                )
            conn.commit()
    except psycopg.Error as exc:
        logger.warning(
            "Could not write predictions to Postgres (%s: %s); falling back to CSV",
            exc.__class__.__name__,
            exc,
        )
        return False

    logger.info("Upserted %s ML trajectory labels into Postgres", len(predictions))
    return True


def main() -> None:
    args = parse_args()

    if not args.model_path.exists():
        raise FileNotFoundError(f"Model artifact not found: {args.model_path}")

    logger.info("Loading model from %s", args.model_path)
    pipeline = load_model(args.model_path)

    logger.info("Loading features source=%s entity_type=%s", args.source, args.entity_type)
    if args.source == "postgres":
        features = _load_features_postgres(get_postgres_dsn(), args.entity_type)
    else:
        features = _load_features_dev(args.data_dir, args.entity_type)

    if features.empty:
        logger.warning("No features found for entity_type=%s; nothing to predict", args.entity_type)
        print("No features found; nothing to predict.")
        return

    logger.info("Predicting trajectory_class for %s rows", len(features))
    predictions = predict(pipeline, features)

    wrote_postgres = False
    if args.source == "postgres" or args.write_postgres:
        wrote_postgres = _upsert_postgres(predictions, args.entity_type)

    output_csv = args.output_csv
    if output_csv is None and not wrote_postgres:
        output_csv = DEFAULT_CSV_OUTPUT

    if output_csv is not None:
        output_csv.parent.mkdir(parents=True, exist_ok=True)
        predictions.to_csv(output_csv, index=False)
        logger.info("Wrote predictions CSV to %s", output_csv)
        print(f"Predictions CSV: {output_csv}")

    print(f"Predicted {len(predictions)} rows using model {args.model_path}")
    print(f"Postgres write: {'ok' if wrote_postgres else 'skipped'}")


if __name__ == "__main__":
    main()
