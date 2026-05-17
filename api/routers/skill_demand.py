"""Skill demand endpoints."""

from datetime import date
from typing import Optional
from fastapi import APIRouter, Query
from api.schemas import SkillDemandResponse, SkillDemandRecord
from api.dependencies import get_postgres_connection

router = APIRouter(prefix="/api/v1/skill-demand", tags=["skill-demand"])


@router.get("", response_model=SkillDemandResponse)
async def get_skill_demand(
    month: Optional[date] = Query(None),
    geo_id: Optional[str] = Query(None),
    role_id: Optional[str] = Query(None),
    skill_id: Optional[str] = Query(None),
    sort_by: Optional[str] = Query("share_within_role", regex="^(share_within_role|yoy_growth|skill_posting_count)$"),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
):
    """Get skill demand data with optional filters."""
    conn = get_postgres_connection()
    
    query = "SELECT * FROM analytics.cs_skill_demand WHERE 1=1"
    params = []
    
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
    
    # Get total count
    count_query = f"SELECT COUNT(*) FROM ({query}) AS subq"
    with conn.cursor() as cur:
        cur.execute(count_query, params)
        total = cur.fetchone()[0]
    
    # Get paginated results
    query += f" ORDER BY {sort_by} DESC LIMIT %s OFFSET %s"
    params.extend([limit, offset])
    
    with conn.cursor() as cur:
        cur.execute(query, params)
        rows = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
    
    data = [
        SkillDemandRecord(**dict(zip(columns, row))) for row in rows
    ]
    
    return SkillDemandResponse(
        data=data,
        total=total,
        limit=limit,
        offset=offset
    )


@router.get("/by-role/{role_id}", response_model=SkillDemandResponse)
async def get_skills_for_role(
    role_id: str,
    month: Optional[date] = Query(None),
    geo_id: Optional[str] = Query(None),
    sort_by: Optional[str] = Query("share_within_role", regex="^(share_within_role|yoy_growth|skill_posting_count)$"),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
):
    """Get top skills for a specific role."""
    conn = get_postgres_connection()
    
    query = "SELECT * FROM analytics.cs_skill_demand WHERE role_id = %s"
    params = [role_id]
    
    if month:
        query += " AND month = %s"
        params.append(month)
    if geo_id:
        query += " AND geo_id = %s"
        params.append(geo_id)
    
    # Get total count
    count_query = f"SELECT COUNT(*) FROM ({query}) AS subq"
    with conn.cursor() as cur:
        cur.execute(count_query, params)
        total = cur.fetchone()[0]
    
    # Get paginated results
    query += f" ORDER BY {sort_by} DESC LIMIT %s OFFSET %s"
    params.extend([limit, offset])
    
    with conn.cursor() as cur:
        cur.execute(query, params)
        rows = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
    
    data = [
        SkillDemandRecord(**dict(zip(columns, row))) for row in rows
    ]
    
    return SkillDemandResponse(
        data=data,
        total=total,
        limit=limit,
        offset=offset
    )


@router.get("/trending", response_model=SkillDemandResponse)
async def get_trending_skills(
    month: date = Query(...),
    geo_id: Optional[str] = Query(None),
    role_id: Optional[str] = Query(None),
    min_yoy_growth: float = Query(0, description="Minimum YoY growth percentage"),
    limit: int = Query(50, ge=1, le=1000),
):
    """Get trending skills by YoY growth."""
    conn = get_postgres_connection()
    
    query = "SELECT * FROM analytics.cs_skill_demand WHERE month = %s AND yoy_growth IS NOT NULL AND yoy_growth > %s"
    params = [month, min_yoy_growth]
    
    if geo_id:
        query += " AND geo_id = %s"
        params.append(geo_id)
    if role_id:
        query += " AND role_id = %s"
        params.append(role_id)
    
    with conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM ({query}) AS subq", params)
        total = cur.fetchone()[0]
        
        cur.execute(query + " ORDER BY yoy_growth DESC LIMIT %s", params + [limit])
        rows = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
    
    data = [
        SkillDemandRecord(**dict(zip(columns, row))) for row in rows
    ]
    
    return SkillDemandResponse(
        data=data,
        total=total,
        limit=limit,
        offset=0
    )
