"""Job demand endpoints."""

from datetime import date
from typing import Optional
from fastapi import APIRouter, Query
from api.schemas import JobDemandResponse, JobDemandRecord
from api.dependencies import get_postgres_connection

router = APIRouter(prefix="/api/v1/job-demand", tags=["job-demand"])


@router.get("", response_model=JobDemandResponse)
async def get_job_demand(
    month: Optional[date] = Query(None, description="Filter by month (YYYY-MM-01)"),
    geo_id: Optional[str] = Query(None, description="Geographic ID"),
    industry_id: Optional[str] = Query(None, description="Industry ID"),
    role_id: Optional[str] = Query(None, description="Role ID"),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
):
    """Get job demand data with optional filters."""
    conn = get_postgres_connection()
    
    query = "SELECT * FROM analytics.cs_job_demand WHERE 1=1"
    params = []
    
    if month:
        query += " AND month = %s"
        params.append(month)
    if geo_id:
        query += " AND geo_id = %s"
        params.append(geo_id)
    if industry_id:
        query += " AND industry_id = %s"
        params.append(industry_id)
    if role_id:
        query += " AND role_id = %s"
        params.append(role_id)
    
    # Get total count
    count_query = f"SELECT COUNT(*) FROM ({query}) AS subq"
    with conn.cursor() as cur:
        cur.execute(count_query, params)
        total = cur.fetchone()[0]
    
    # Get paginated results
    query += f" ORDER BY month DESC, geo_id, industry_id, role_id LIMIT %s OFFSET %s"
    params.extend([limit, offset])
    
    with conn.cursor() as cur:
        cur.execute(query, params)
        rows = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
    
    data = [
        JobDemandRecord(
            **dict(zip(columns, row))
        ) for row in rows
    ]
    
    return JobDemandResponse(
        data=data,
        total=total,
        limit=limit,
        offset=offset
    )


@router.get("/by-geo", response_model=JobDemandResponse)
async def get_job_demand_by_geo(
    month: date = Query(..., description="Required month"),
    limit: int = Query(100, ge=1, le=1000),
):
    """Get top job demands by geography for a specific month."""
    conn = get_postgres_connection()
    
    query = """
    SELECT * FROM analytics.cs_job_demand
    WHERE month = %s
    ORDER BY posting_count DESC
    LIMIT %s
    """
    
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM analytics.cs_job_demand WHERE month = %s", (month,))
        total = cur.fetchone()[0]
        
        cur.execute(query, (month, limit))
        rows = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
    
    data = [
        JobDemandRecord(**dict(zip(columns, row))) for row in rows
    ]
    
    return JobDemandResponse(
        data=data,
        total=total,
        limit=limit,
        offset=0
    )


@router.get("/by-role", response_model=JobDemandResponse)
async def get_job_demand_by_role(
    role_id: str = Query(..., description="Role ID"),
    month: Optional[date] = Query(None),
    geo_id: Optional[str] = Query(None),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
):
    """Get job demand for a specific role."""
    conn = get_postgres_connection()
    
    query = "SELECT * FROM analytics.cs_job_demand WHERE role_id = %s"
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
    query += " ORDER BY month DESC LIMIT %s OFFSET %s"
    params.extend([limit, offset])
    
    with conn.cursor() as cur:
        cur.execute(query, params)
        rows = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
    
    data = [
        JobDemandRecord(**dict(zip(columns, row))) for row in rows
    ]
    
    return JobDemandResponse(
        data=data,
        total=total,
        limit=limit,
        offset=offset
    )
