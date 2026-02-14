import socketio
from contextlib import asynccontextmanager

from fastapi import FastAPI
from loguru import logger

from app.health.router import router as health_router
from app.kafka.consumer import kafka_consumer
from app.kafka.producer import kafka_producer
from app.socket.server import sio
from app.socket import handlers  # noqa: F401 — registers event handlers on import


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting socketio-handler...")
    await kafka_producer.start()
    await kafka_consumer.start()
    yield
    logger.info("Shutting down socketio-handler...")
    await kafka_consumer.stop()
    await kafka_producer.stop()


fastapi_app = FastAPI(
    title="Companion SocketIO Handler",
    version="0.1.0",
    lifespan=lifespan,
)
fastapi_app.include_router(health_router)

# Socket.IO wraps FastAPI — this is the ASGI entry point
app = socketio.ASGIApp(sio, other_asgi_app=fastapi_app)
