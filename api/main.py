"""FastAPI application factory and route registration."""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from api.routers import job_demand, skill_demand, salary, role_skills
from api.dependencies import close_connections

# Import routers
from api.routers.job_demand import router as job_demand_router
from api.routers.skill_demand import router as skill_demand_router
from api.routers.salary import router as salary_router
from api.routers.role_skills import router as role_skills_router


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

    # Health check endpoint
    @app.get("/health", tags=["health"])
    async def health_check():
        """Health check endpoint."""
        return {"status": "healthy"}

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
            }
        }

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
