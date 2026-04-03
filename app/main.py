import asyncio
import json
import os
import socketio
import urllib.request
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.openapi.docs import get_swagger_ui_html
from fastapi.responses import HTMLResponse, JSONResponse
from loguru import logger

from app.config import settings
from app.config_client import ConfigClient
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


_config_client: ConfigClient | None = None


def _apply_socketio_config(config: dict) -> None:
    if "rate_limit_location_per_second" in config:
        settings.RATE_LIMIT_LOCATION_PER_SECOND = config["rate_limit_location_per_second"]
    if "rate_limit_default_per_second" in config:
        settings.RATE_LIMIT_DEFAULT_PER_SECOND = config["rate_limit_default_per_second"]


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _config_client
    logger.info("Starting socketio-handler...")

    # Config-service integration
    if settings.CONFIG_SERVICE_URL and settings.CONFIG_SERVICE_API_KEY:
        _config_client = ConfigClient(
            settings.CONFIG_SERVICE_URL,
            settings.CONFIG_SERVICE_API_KEY,
            "socketio",
        )
        try:
            config, changed = await _config_client.fetch()
            if changed and config:
                _apply_socketio_config(config)
                logger.info("Loaded config from config-service")
        except Exception as exc:
            logger.warning(f"config-service unavailable at startup, using defaults: {exc}")

        await _config_client.start_polling(30, _apply_socketio_config)

    await kafka_producer.start()
    await kafka_consumer.start()
    await _gateway_register()
    yield
    logger.info("Shutting down socketio-handler...")
    await _gateway_deregister()
    await kafka_consumer.stop()
    await kafka_producer.stop()

    if _config_client:
        await _config_client.stop()


fastapi_app = FastAPI(
    title="Companion SocketIO Handler",
    version="0.1.0",
    lifespan=lifespan,
    root_path=os.getenv("ROOT_PATH", ""),
    redirect_slashes=False,
    docs_url=None,
    openapi_url=None,
)


@fastapi_app.get("/docs", include_in_schema=False, response_class=HTMLResponse)
async def swagger_ui():
    return get_swagger_ui_html(
        openapi_url="./openapi.json",
        title="Companion SocketIO Handler",
    )


@fastapi_app.get("/openapi.json", include_in_schema=False)
async def openapi(request: Request):
    prefix = request.headers.get("x-forwarded-prefix", "")
    spec = fastapi_app.openapi()
    if prefix:
        return JSONResponse({**spec, "servers": [{"url": prefix}]})
    return JSONResponse(spec)


fastapi_app.include_router(health_router)

# Socket.IO wraps FastAPI — this is the ASGI entry point
app = socketio.ASGIApp(sio, other_asgi_app=fastapi_app)
