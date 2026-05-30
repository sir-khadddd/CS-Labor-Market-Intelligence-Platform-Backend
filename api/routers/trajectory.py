"""Trajectory feature and label endpoints."""

from datetime import date
from typing import Optional

from fastapi import APIRouter, Depends, Query
from psycopg import Connection

from api.dependencies import get_postgres_connection
from api.schemas import (
    TrajectoryFeatureRecord,
    TrajectoryFeatureResponse,
    TrajectoryLabelRecord,
    TrajectoryLabelResponse,
)

router = APIRouter(prefix="/api/v1/trajectory", tags=["trajectory"])


@router.get("/features", response_model=TrajectoryFeatureResponse)
async def get_trajectory_features(
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
async def get_trajectory_labels(
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
