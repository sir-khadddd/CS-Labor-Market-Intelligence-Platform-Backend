"""Trajectory feature and label endpoints.

NOTE ON ML HONESTY: the ML classifier (method="ml_classifier",
method_version from ml/constants.py METHOD_VERSION, currently ml-v2) is an
experimental baseline trained on limited history. Do not treat it as the
primary trajectory signal in downstream products or dashboards until it
clears an explicit accuracy/F1 metrics gate and is promoted via a
method_version bump. Prefer method="phase1_rules" for anything user-facing
until then.

The ML model artifact is cached in-process and reloaded automatically when
its file mtime changes (e.g. after retraining). Set ML_MODEL_PATH to override
the default artifact location.
"""

import logging
import os
from datetime import date
from pathlib import Path
from typing import Optional

import pandas as pd
from fastapi import APIRouter, Depends, HTTPException, Query
from psycopg import Connection

from api.dependencies import get_postgres_connection
from api.schemas import (
    TrajectoryFeatureRecord,
    TrajectoryFeatureResponse,
    TrajectoryLabelRecord,
    TrajectoryLabelResponse,
    TrajectoryPredictionResponse,
)
from ml.constants import (
    DEFAULT_ARTIFACTS_DIR,
    DEFAULT_MODEL_FILENAME,
    FEATURE_COLUMNS,
    FEATURE_VERSION,
    METHOD,
    METHOD_VERSION,
)
from ml.model import load_model, load_model_metrics, sklearn_major_version_mismatch

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/trajectory", tags=["trajectory"])

# cache_key -> (model, artifact_mtime)
_model_cache: dict[str, tuple[object, float]] = {}


def _get_model_path() -> Path:
    """Resolve the ML model artifact path, honoring ML_MODEL_PATH override."""
    override = os.getenv("ML_MODEL_PATH")
    if override:
        return Path(override)
    return DEFAULT_ARTIFACTS_DIR / DEFAULT_MODEL_FILENAME


def _load_cached_model():
    """Load and cache the trajectory classifier, or None if missing.

    Reloads when the artifact file mtime changes so retrained models are picked
    up without restarting the API process. Returns None when the artifact is
    missing or sklearn major version is incompatible with training metadata.
    """
    model_path = _get_model_path()
    cache_key = str(model_path)
    if not model_path.exists():
        return None

    metrics = load_model_metrics(model_path)
    if metrics is not None:
        mismatch = sklearn_major_version_mismatch(metrics)
        if mismatch is not None:
            logger.warning("Refusing to load ML model: %s", mismatch)
            return None

    mtime = model_path.stat().st_mtime
    cached = _model_cache.get(cache_key)
    if cached is not None and cached[1] == mtime:
        return cached[0]

    model = load_model(model_path)
    _model_cache.clear()
    _model_cache[cache_key] = (model, mtime)
    return model


@router.get("/features", response_model=TrajectoryFeatureResponse)
def get_trajectory_features(
    entity_type: Optional[str] = Query(None, description="Entity type (e.g. role)"),
    entity_id: Optional[str] = Query(None, description="Entity identifier"),
    month: Optional[date] = Query(None, description="Filter by month (YYYY-MM-01)"),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    conn: Connection = Depends(get_postgres_connection),
):
    """Get trajectory features with optional filters."""
    query = "SELECT * FROM analytics.trajectory_features WHERE 1=1"
    params: list = []

    if entity_type:
        query += " AND entity_type = %s"
        params.append(entity_type)
    if entity_id:
        query += " AND entity_id = %s"
        params.append(entity_id)
    if month:
        query += " AND month = %s"
        params.append(month)

    count_query = f"SELECT COUNT(*) FROM ({query}) AS subq"
    with conn.cursor() as cur:
        cur.execute(count_query, params)
        total = cur.fetchone()[0]

        query += " ORDER BY month DESC, entity_id LIMIT %s OFFSET %s"
        cur.execute(query, params + [limit, offset])
        rows = cur.fetchall()
        columns = [desc[0] for desc in cur.description]

    data = [
        TrajectoryFeatureRecord(**dict(zip(columns, row)))
        for row in rows
    ]

    return TrajectoryFeatureResponse(
        data=data,
        total=total,
        limit=limit,
        offset=offset,
    )


@router.get("/labels", response_model=TrajectoryLabelResponse)
def get_trajectory_labels(
    entity_type: Optional[str] = Query(None, description="Entity type (e.g. role)"),
    entity_id: Optional[str] = Query(None, description="Entity identifier"),
    month: Optional[date] = Query(None, description="Filter by month (YYYY-MM-01)"),
    method: Optional[str] = Query(None, description="Label method (e.g. phase1_rules)"),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    conn: Connection = Depends(get_postgres_connection),
):
    """Get trajectory labels with optional filters."""
    query = "SELECT * FROM analytics.trajectory_labels WHERE 1=1"
    params: list = []

    if entity_type:
        query += " AND entity_type = %s"
        params.append(entity_type)
    if entity_id:
        query += " AND entity_id = %s"
        params.append(entity_id)
    if month:
        query += " AND month = %s"
        params.append(month)
    if method:
        query += " AND method = %s"
        params.append(method)

    count_query = f"SELECT COUNT(*) FROM ({query}) AS subq"
    with conn.cursor() as cur:
        cur.execute(count_query, params)
        total = cur.fetchone()[0]

        query += " ORDER BY month DESC, entity_id LIMIT %s OFFSET %s"
        cur.execute(query, params + [limit, offset])
        rows = cur.fetchall()
        columns = [desc[0] for desc in cur.description]

    data = [
        TrajectoryLabelRecord(**dict(zip(columns, row)))
        for row in rows
    ]

    return TrajectoryLabelResponse(
        data=data,
        total=total,
        limit=limit,
        offset=offset,
    )


@router.get("/predict", response_model=TrajectoryPredictionResponse)
def predict_trajectory(
    entity_type: str = Query("role", description="Entity type (e.g. role)"),
    entity_id: str = Query(..., description="Entity identifier"),
    month: date = Query(..., description="Month (YYYY-MM-01)"),
    conn: Connection = Depends(get_postgres_connection),
):
    """Predict trajectory_class for an entity-month using the ML classifier.

    Experimental baseline (see module docstring): not the primary
    trajectory signal until the accuracy/F1 metrics gate is met.
    """
    model = _load_cached_model()
    if model is None:
        model_path = _get_model_path()
        metrics = load_model_metrics(model_path)
        if metrics is not None:
            mismatch = sklearn_major_version_mismatch(metrics)
            if mismatch is not None:
                logger.warning("ML model unavailable due to sklearn mismatch: %s", mismatch)
                raise HTTPException(
                    status_code=503,
                    detail=mismatch,
                )
        logger.warning("ML model artifact not found at %s", model_path)
        raise HTTPException(
            status_code=503,
            detail="ML model artifact not available",
        )

    query = """
        SELECT * FROM analytics.trajectory_features
        WHERE entity_type = %s AND entity_id = %s AND month = %s AND feature_version = %s
        ORDER BY posting_count DESC
        LIMIT 1
    """
    with conn.cursor() as cur:
        cur.execute(query, (entity_type, entity_id, month, FEATURE_VERSION))
        row = cur.fetchone()
        if row is None:
            raise HTTPException(
                status_code=404,
                detail=(
                    f"No trajectory features for entity_type={entity_type} "
                    f"entity_id={entity_id} month={month}"
                ),
            )
        columns = [desc[0] for desc in cur.description]

    feature_row = dict(zip(columns, row))
    x = pd.DataFrame([{col: feature_row.get(col) for col in FEATURE_COLUMNS}])

    trajectory_class = str(model.predict(x)[0])
    confidence: Optional[float] = None
    if hasattr(model, "predict_proba"):
        confidence = float(model.predict_proba(x)[0].max())

    return TrajectoryPredictionResponse(
        entity_type=entity_type,
        entity_id=entity_id,
        month=month,
        trajectory_class=trajectory_class,
        confidence=confidence,
        method=METHOD,
        method_version=METHOD_VERSION,
    )
