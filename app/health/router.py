from fastapi import APIRouter

from app.socket.registry import registry

router = APIRouter(tags=["health"])


@router.get("/health")
async def health():
    return {"status": "ok", "active_connections": registry.active_count}


@router.get("/ready")
async def ready():
    return {"status": "ready"}
