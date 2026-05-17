"""Pydantic schemas for API responses."""

from datetime import date
from typing import Optional, List
from pydantic import BaseModel, Field


class PaginationParams(BaseModel):
    """Pagination parameters."""
    limit: int = Field(default=100, ge=1, le=1000)
    offset: int = Field(default=0, ge=0)


class JobDemandRecord(BaseModel):
    """Job demand data record."""
    month: date
    geo_id: str
    geo_name: str
    industry_id: str
    industry_name: str
    role_id: str
    role_name: str
    posting_count: int
    yoy_growth: Optional[float] = None
    rolling_3m_growth: Optional[float] = None
    acceleration: Optional[float] = None


class JobDemandResponse(BaseModel):
    """Job demand API response."""
    data: List[JobDemandRecord]
    total: int
    limit: int
    offset: int


class SkillDemandRecord(BaseModel):
    """Skill demand data record."""
    month: date
    geo_id: str
    role_id: str
    skill_id: str
    skill_name: str
    skill_posting_count: int
    role_posting_count: int
    share_within_role: float
    yoy_growth: Optional[float] = None


class SkillDemandResponse(BaseModel):
    """Skill demand API response."""
    data: List[SkillDemandRecord]
    total: int
    limit: int
    offset: int


class RoleSkillAssociationRecord(BaseModel):
    """Role-skill association record."""
    month: date
    role_id: str
    skill_id: str
    skill_name: Optional[str] = None
    co_occurrence_count: int
    p_skill_given_role: float
    lift: float


class RoleSkillAssociationResponse(BaseModel):
    """Role-skill association API response."""
    data: List[RoleSkillAssociationRecord]
    total: int
    limit: int
    offset: int


class SalaryRecord(BaseModel):
    """Salary distribution record."""
    month: date
    geo_id: str
    geo_name: str
    industry_id: str
    industry_name: str
    role_id: str
    role_name: str
    salary_p25: Optional[float] = None
    salary_p50: Optional[float] = None
    salary_p75: Optional[float] = None


class SalaryResponse(BaseModel):
    """Salary API response."""
    data: List[SalaryRecord]
    total: int
    limit: int
    offset: int


class TrajectoryFeatureRecord(BaseModel):
    """Trajectory feature record."""
    entity_type: str
    entity_id: str
    month: date
    posting_count: int
    yoy_growth: Optional[float] = None
    rolling_3m_growth: Optional[float] = None
    acceleration: Optional[float] = None
    volatility_12m: Optional[float] = None
    demand_concentration_index: Optional[float] = None
    momentum_score: Optional[float] = None


class TrajectoryFeatureResponse(BaseModel):
    """Trajectory features API response."""
    data: List[TrajectoryFeatureRecord]
    total: int
    limit: int
    offset: int


class TrajectoryLabelRecord(BaseModel):
    """Trajectory label record."""
    entity_type: str
    entity_id: str
    month: date
    trajectory_class: str
    trajectory_score: Optional[float] = None
    confidence: Optional[float] = None
    method: str
    label_version: str
    method_version: str


class TrajectoryLabelResponse(BaseModel):
    """Trajectory labels API response."""
    data: List[TrajectoryLabelRecord]
    total: int
    limit: int
    offset: int


class TimeSeriesRecord(BaseModel):
    """Time series data point."""
    month: date
    posting_count: Optional[int] = None
    yoy_growth: Optional[float] = None
    rolling_3m_growth: Optional[float] = None
    salary_p50: Optional[float] = None


class TimeSeriesResponse(BaseModel):
    """Time series API response."""
    entity_type: str
    entity_id: str
    metrics: List[str]
    data: List[TimeSeriesRecord]


class MarketSummaryRecord(BaseModel):
    """Market summary item."""
    role_name: str
    posting_count: int
    yoy_growth: Optional[float] = None


class SkillSummaryRecord(BaseModel):
    """Skill summary item."""
    skill_name: str
    share: float
    yoy_growth: Optional[float] = None


class RoleSkillPairRecord(BaseModel):
    """Role-skill pair record."""
    role_id: str
    role_name: str
    skill_id: str
    skill_name: str
    lift: float


class MarketSummaryResponse(BaseModel):
    """Market summary API response."""
    month: date
    geo_id: str
    geo_name: str
    top_roles: List[MarketSummaryRecord]
    top_skills: List[SkillSummaryRecord]
    hottest_role_skill_pairs: List[RoleSkillPairRecord]


class GeoComparisonRecord(BaseModel):
    """Geographic comparison record."""
    geo_id: str
    geo_name: str
    posting_count: int
    yoy_growth: Optional[float] = None
    salary_p50: Optional[float] = None


class GeoComparisonResponse(BaseModel):
    """Geographic comparison API response."""
    role_id: str
    role_name: str
    month: date
    data: List[GeoComparisonRecord]
