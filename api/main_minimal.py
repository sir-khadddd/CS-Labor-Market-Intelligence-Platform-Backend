"""Minimal FastAPI app for testing router registration."""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

# Import test router
from api.routers.test import router as test_router

app = FastAPI(
    title="CS Labor Market Intelligence API",
    description="API for accessing computer science labor market data",
    version="1.0.0",
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Register the test router
app.include_router(test_router)

# Health check endpoint
@app.get("/health", tags=["health"])
async def health_check():
    """Health check endpoint."""
    return {"status": "healthy"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
