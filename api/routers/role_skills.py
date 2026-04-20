"""Role-skill association endpoints."""

from datetime import date
from typing import Optional
from fastapi import APIRouter, Query
from api.schemas import RoleSkillAssociationResponse, RoleSkillAssociationRecord
from api.dependencies import get_postgres_connection

router = APIRouter(prefix="/api/v1/role-skills", tags=["role-skills"])


@router.get("", response_model=RoleSkillAssociationResponse)
async def get_role_skill_associations(
    month: Optional[date] = Query(None),
    role_id: Optional[str] = Query(None),
    skill_id: Optional[str] = Query(None),
    sort_by: Optional[str] = Query("lift", regex="^(lift|co_occurrence_count|p_skill_given_role)$"),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
):
    """Get role-skill associations with optional filters."""
    conn = get_postgres_connection()
    
    query = "SELECT * FROM analytics.role_skill_associations WHERE 1=1"
    params = []
    
    if month:
        query += " AND month = %s"
        params.append(month)
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
        RoleSkillAssociationRecord(**dict(zip(columns, row))) for row in rows
    ]
    
    return RoleSkillAssociationResponse(
        data=data,
        total=total,
        limit=limit,
        offset=offset
    )


@router.get("/{role_id}", response_model=RoleSkillAssociationResponse)
async def get_skills_for_role(
    role_id: str,
    month: Optional[date] = Query(None),
    min_lift: float = Query(0, description="Minimum lift threshold"),
    sort_by: Optional[str] = Query("lift", regex="^(lift|co_occurrence_count|p_skill_given_role)$"),
    limit: int = Query(50, ge=1, le=1000),
):
    """Get top skills for a specific role by association strength."""
    conn = get_postgres_connection()
    
    query = "SELECT * FROM analytics.role_skill_associations WHERE role_id = %s"
    params = [role_id]
    
    if month:
        query += " AND month = %s"
        params.append(month)
    
    query += " AND lift > %s"
    params.append(min_lift)
    
    with conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM ({query}) AS subq", params)
        total = cur.fetchone()[0]
        
        cur.execute(query + f" ORDER BY {sort_by} DESC LIMIT %s", params + [limit])
        rows = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
    
    data = [
        RoleSkillAssociationRecord(**dict(zip(columns, row))) for row in rows
    ]
    
    return RoleSkillAssociationResponse(
        data=data,
        total=total,
        limit=limit,
        offset=0
    )


@router.get("/strong-associations", response_model=RoleSkillAssociationResponse)
async def get_strong_associations(
    month: date = Query(...),
    min_lift: float = Query(1.5, description="Minimum lift threshold"),
    limit: int = Query(100, ge=1, le=1000),
):
    """Get strongest role-skill associations above lift threshold."""
    conn = get_postgres_connection()
    
    query = "SELECT * FROM analytics.role_skill_associations WHERE month = %s AND lift > %s"
    params = [month, min_lift]
    
    with conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM ({query}) AS subq", params)
        total = cur.fetchone()[0]
        
        cur.execute(query + " ORDER BY lift DESC LIMIT %s", params + [limit])
        rows = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
    
    data = [
        RoleSkillAssociationRecord(**dict(zip(columns, row))) for row in rows
    ]
    
    return RoleSkillAssociationResponse(
        data=data,
        total=total,
        limit=limit,
        offset=0
    )
