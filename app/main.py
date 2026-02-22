import asyncio
import json
import os
import socketio
import urllib.request
from contextlib import asynccontextmanager

from fastapi import FastAPI
from loguru import logger

from app.health.router import router as health_router
from app.kafka.consumer import kafka_consumer
from app.kafka.producer import kafka_producer
from app.socket.server import sio
from app.socket import handlers  # noqa: F401 — registers event handlers on import

_SERVICE_NAME = "socketio-handler"
_SERVICE_PORT = int(os.getenv("SERVICE_PORT", "8002"))


async def _gateway_register() -> None:
    gateway_url = os.getenv("GATEWAY_URL", "")
    secret = os.getenv("GATEWAY_SECRET", "")
    host = os.getenv("SERVICE_HOST", "")
    if not gateway_url or not secret:
        return

    def _call() -> None:
        data = json.dumps({
            "name": _SERVICE_NAME,
            "address": host,
            "port": _SERVICE_PORT,
            "health_check": "/health",
        }).encode()
        req = urllib.request.Request(
            f"{gateway_url}/internal/register",
            data=data,
            headers={"Content-Type": "application/json", "Authorization": f"Bearer {secret}"},
            method="POST",
        )
        urllib.request.urlopen(req, timeout=5)

    try:
        await asyncio.get_event_loop().run_in_executor(None, _call)
        logger.info("Registered with api-gateway")
    except Exception as exc:
        logger.warning(f"Could not register with api-gateway: {exc}")


async def _gateway_deregister() -> None:
    gateway_url = os.getenv("GATEWAY_URL", "")
    secret = os.getenv("GATEWAY_SECRET", "")
    if not gateway_url or not secret:
        return

    def _call() -> None:
        req = urllib.request.Request(
            f"{gateway_url}/internal/deregister?name={_SERVICE_NAME}",
            headers={"Authorization": f"Bearer {secret}"},
            method="DELETE",
        )
        urllib.request.urlopen(req, timeout=5)

    try:
        await asyncio.get_event_loop().run_in_executor(None, _call)
        logger.info("Deregistered from api-gateway")
    except Exception as exc:
        logger.warning(f"Could not deregister from api-gateway: {exc}")


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting socketio-handler...")
    await kafka_producer.start()
    await kafka_consumer.start()
    await _gateway_register()
    yield
    logger.info("Shutting down socketio-handler...")
    await _gateway_deregister()
    await kafka_consumer.stop()
    await kafka_producer.stop()


fastapi_app = FastAPI(
    title="Companion SocketIO Handler",
    version="0.1.0",
    lifespan=lifespan,
    root_path=os.getenv("ROOT_PATH", ""),
)
fastapi_app.include_router(health_router)

# Socket.IO wraps FastAPI — this is the ASGI entry point
app = socketio.ASGIApp(sio, other_asgi_app=fastapi_app)
