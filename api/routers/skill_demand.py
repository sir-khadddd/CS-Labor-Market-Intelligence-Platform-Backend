"""Skill demand endpoints."""

from datetime import date
from typing import Optional

from fastapi import APIRouter, Depends, Query
from psycopg import Connection

from api.dependencies import get_postgres_connection
from api.schemas import SkillDemandResponse, SkillDemandRecord
from api.skill_config import skill_filter_clause

router = APIRouter(prefix="/api/v1/skill-demand", tags=["skill-demand"])


def _fetch_skill_demand(
    conn: Connection,
    *,
    base_query: str,
    params: list,
    sort_by: str,
    limit: int,
    offset: int,
) -> SkillDemandResponse:
    count_query = f"SELECT COUNT(*) FROM ({base_query}) AS subq"
    with conn.cursor() as cur:
        cur.execute(count_query, params)
        total = cur.fetchone()[0]

        cur.execute(base_query + f" ORDER BY {sort_by} DESC LIMIT %s OFFSET %s", params + [limit, offset])
        rows = cur.fetchall()
        columns = [desc[0] for desc in cur.description]

    data = [SkillDemandRecord(**dict(zip(columns, row))) for row in rows]
    return SkillDemandResponse(data=data, total=total, limit=limit, offset=offset)


@router.get("", response_model=SkillDemandResponse)
async def get_skill_demand(
    month: Optional[date] = Query(None),
    geo_id: Optional[str] = Query(None),
    role_id: Optional[str] = Query(None),
    skill_id: Optional[str] = Query(None),
    include_unknown: bool = Query(False, description="Include placeholder UNK/Unknown skill ids"),
    sort_by: Optional[str] = Query("share_within_role", pattern="^(share_within_role|yoy_growth|skill_posting_count)$"),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    conn: Connection = Depends(get_postgres_connection),
):
    """Get inferred skill demand with optional filters."""
    query = f"SELECT * FROM analytics.cs_skill_demand WHERE {skill_filter_clause(include_unknown=include_unknown)}"
    params: list = []

    if month:
        query += " AND month = %s"
        params.append(month)
    if geo_id:
        query += " AND geo_id = %s"
        params.append(geo_id)
    if role_id:
        query += " AND role_id = %s"
        params.append(role_id)
    if skill_id:
        query += " AND skill_id = %s"
        params.append(skill_id)

    return _fetch_skill_demand(
        conn,
        base_query=query,
        params=params,
        sort_by=sort_by,
        limit=limit,
        offset=offset,
    )


@router.get("/by-role/{role_id}", response_model=SkillDemandResponse)
async def get_skills_for_role(
    role_id: str,
    month: Optional[date] = Query(None),
    geo_id: Optional[str] = Query(None),
    include_unknown: bool = Query(False),
    sort_by: Optional[str] = Query("share_within_role", pattern="^(share_within_role|yoy_growth|skill_posting_count)$"),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    conn: Connection = Depends(get_postgres_connection),
):
    """Get top inferred skills for a specific role."""
    query = (
        f"SELECT * FROM analytics.cs_skill_demand WHERE role_id = %s "
        f"AND {skill_filter_clause(include_unknown=include_unknown)}"
    )
    params: list = [role_id]

    if month:
        query += " AND month = %s"
        params.append(month)
    if geo_id:
        query += " AND geo_id = %s"
        params.append(geo_id)

    return _fetch_skill_demand(
        conn,
        base_query=query,
        params=params,
        sort_by=sort_by,
        limit=limit,
        offset=offset,
    )


@router.get("/trending", response_model=SkillDemandResponse)
async def get_trending_skills(
    month: date = Query(...),
    geo_id: Optional[str] = Query(None),
    role_id: Optional[str] = Query(None),
    include_unknown: bool = Query(False),
    min_yoy_growth: float = Query(0, description="Minimum YoY growth percentage"),
    limit: int = Query(50, ge=1, le=1000),
    conn: Connection = Depends(get_postgres_connection),
):
    """Get trending inferred skills by YoY growth."""
    query = (
        f"SELECT * FROM analytics.cs_skill_demand WHERE month = %s "
        f"AND yoy_growth IS NOT NULL AND yoy_growth > %s "
        f"AND {skill_filter_clause(include_unknown=include_unknown)}"
    )
    params: list = [month, min_yoy_growth]

    if geo_id:
        query += " AND geo_id = %s"
        params.append(geo_id)
    if role_id:
        query += " AND role_id = %s"
        params.append(role_id)

    return _fetch_skill_demand(
        conn,
        base_query=query,
        params=params,
        sort_by="yoy_growth",
        limit=limit,
        offset=0,
    )
