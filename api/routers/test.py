"""Test router for minimal example."""

from fastapi import APIRouter

router = APIRouter(prefix="/api/v1/test", tags=["test"])


@router.get("/hello")
async def hello():
    """Simple hello endpoint."""
    return {"message": "Hello from test router!"}


@router.get("/items/{item_id}")
async def get_item(item_id: int):
    """Get an item by ID."""
    return {"item_id": item_id, "name": f"Item {item_id}"}
