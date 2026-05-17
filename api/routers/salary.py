"""Salary endpoints."""

from datetime import date
from typing import Optional
from fastapi import APIRouter, Query
from api.schemas import SalaryResponse, SalaryRecord
from api.dependencies import get_postgres_connection

router = APIRouter(prefix="/api/v1/salaries", tags=["salary"])


@router.get("", response_model=SalaryResponse)
async def get_salaries(
    month: Optional[date] = Query(None),
    geo_id: Optional[str] = Query(None),
    role_id: Optional[str] = Query(None),
    industry_id: Optional[str] = Query(None),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
):
    """Get salary distribution data with optional filters."""
    conn = get_postgres_connection()
    
    query = "SELECT * FROM analytics.salary_distribution WHERE 1=1"
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
    if industry_id:
        query += " AND industry_id = %s"
        params.append(industry_id)
    
    # Get total count
    count_query = f"SELECT COUNT(*) FROM ({query}) AS subq"
    with conn.cursor() as cur:
        cur.execute(count_query, params)
        total = cur.fetchone()[0]
    
    # Get paginated results
    query += " ORDER BY month DESC, geo_id, role_id LIMIT %s OFFSET %s"
    params.extend([limit, offset])
    
    with conn.cursor() as cur:
        cur.execute(query, params)
        rows = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
    
    data = [
        SalaryRecord(**dict(zip(columns, row))) for row in rows
    ]
    
    return SalaryResponse(
        data=data,
        total=total,
        limit=limit,
        offset=offset
    )


@router.get("/by-role/{role_id}", response_model=SalaryResponse)
async def get_salary_by_role(
    role_id: str,
    month: Optional[date] = Query(None),
    geo_id: Optional[str] = Query(None),
    limit: int = Query(100, ge=1, le=1000),
):
    """Get salary info for a specific role."""
    conn = get_postgres_connection()
    
    query = "SELECT * FROM analytics.salary_distribution WHERE role_id = %s"
    params = [role_id]
    
    if month:
        query += " AND month = %s"
        params.append(month)
    if geo_id:
        query += " AND geo_id = %s"
        params.append(geo_id)
    
    with conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM ({query}) AS subq", params)
        total = cur.fetchone()[0]
        
        cur.execute(query + " ORDER BY month DESC LIMIT %s", params + [limit])
        rows = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
    
    data = [
        SalaryRecord(**dict(zip(columns, row))) for row in rows
    ]
    
    return SalaryResponse(
        data=data,
        total=total,
        limit=limit,
        offset=0
    )


@router.get("/by-geo/{geo_id}", response_model=SalaryResponse)
async def get_salary_by_geo(
    geo_id: str,
    month: Optional[date] = Query(None),
    role_id: Optional[str] = Query(None),
    limit: int = Query(100, ge=1, le=1000),
):
    """Get salary info for a specific geography."""
    conn = get_postgres_connection()
    
    query = "SELECT * FROM analytics.salary_distribution WHERE geo_id = %s"
    params = [geo_id]
    
    if month:
        query += " AND month = %s"
        params.append(month)
    if role_id:
        query += " AND role_id = %s"
        params.append(role_id)
    
    with conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM ({query}) AS subq", params)
        total = cur.fetchone()[0]
        
        cur.execute(query + " ORDER BY month DESC, role_id LIMIT %s", params + [limit])
        rows = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
    
    data = [
        SalaryRecord(**dict(zip(columns, row))) for row in rows
    ]
    
    return SalaryResponse(
        data=data,
        total=total,
        limit=limit,
        offset=0
    )
