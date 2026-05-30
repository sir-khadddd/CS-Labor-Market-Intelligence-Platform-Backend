"""FastAPI application factory and route registration."""

import logging

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from api.dependencies import close_connections, get_postgres_health

# Import routers
from api.routers.job_demand import router as job_demand_router
from api.routers.skill_demand import router as skill_demand_router
from api.routers.salary import router as salary_router
from api.routers.role_skills import router as role_skills_router
from api.routers.trajectory import router as trajectory_router

logger = logging.getLogger(__name__)


def create_app() -> FastAPI:
    """Create and configure FastAPI application."""
    app = FastAPI(
        title="CS Labor Market Intelligence API",
        description="API for accessing computer science labor market data",
        version="1.0.0",
    )

    # Add CORS middleware
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],  # Configure as needed for production
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # Register routers
    app.include_router(job_demand_router)
    app.include_router(skill_demand_router)
    app.include_router(salary_router)
    app.include_router(role_skills_router)
    app.include_router(trajectory_router)

    # Health check endpoint
    @app.get("/health", tags=["health"])
    async def health_check():
        """Health check endpoint with Postgres status."""
        postgres = get_postgres_health()
        status = "healthy" if postgres.get("status") == "up" else "degraded"
        return {"status": status, "postgres": postgres}

    @app.get("/health/db", tags=["health"])
    async def database_health_check():
        """Detailed Postgres health including analytics table row counts."""
        postgres = get_postgres_health(include_table_counts=True)
        status = "healthy" if postgres.get("status") == "up" else "degraded"
        return {"status": status, "postgres": postgres}

    # API info endpoint
    @app.get("/api/v1/info", tags=["info"])
    async def api_info():
        """API information endpoint."""
        return {
            "name": "CS Labor Market Intelligence API",
            "version": "1.0.0",
            "endpoints": {
                "job_demand": "/api/v1/job-demand",
                "skill_demand": "/api/v1/skill-demand",
                "salary": "/api/v1/salaries",
                "role_skills": "/api/v1/role-skills",
                "trajectory": "/api/v1/trajectory",
            }
        }

    # Startup event
    @app.on_event("startup")
    async def startup_event():
        """Log Postgres health when the API starts."""
        postgres = get_postgres_health()
        if postgres.get("status") == "up":
            logger.info(
                "API startup Postgres check passed database=%s latency_ms=%s",
                postgres.get("database"),
                postgres.get("latency_ms"),
            )
        else:
            logger.warning(
                "API startup Postgres check failed: %s",
                postgres.get("error", "unknown error"),
            )

    # Shutdown event
    @app.on_event("shutdown")
    async def shutdown_event():
        """Close database connections on shutdown."""
        close_connections()

    return app


# Create the app instance
app = create_app()


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
