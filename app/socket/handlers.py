import uuid

import jwt
from loguru import logger
from pydantic import ValidationError

from app.auth.jwt import decode_access_token
from app.config import settings
from app.kafka.producer import kafka_producer
from app.schemas.events import LocationUpdate, RideCancel, RideRequest
from app.socket.middleware import rate_limiter
from app.socket.registry import registry
from app.socket.server import sio


@sio.event
async def connect(sid: str, environ: dict, auth: dict | None = None):
    token = None
    if auth and isinstance(auth, dict):
        token = auth.get("token")

    if not token:
        logger.warning("Connection rejected: no token (sid={})", sid)
        raise ConnectionRefusedError("Authentication required")

    try:
        payload = decode_access_token(token)
    except jwt.InvalidTokenError as e:
        logger.warning("Connection rejected: invalid token (sid={}, err={})", sid, e)
        raise ConnectionRefusedError("Invalid token")

    user_id = payload["sub"]
    role = payload["role"]

    await registry.register(user_id, sid, role)
    logger.info("Connected: user_id={}, role={}, sid={}", user_id, role, sid)
    return True


@sio.event
async def disconnect(sid: str):
    user_id = await registry.unregister(sid)
    rate_limiter.cleanup(sid)
    logger.info("Disconnected: user_id={}, sid={}", user_id, sid)


@sio.event
async def location_update(sid: str, data: dict):
    user_id = registry.get_user_id(sid)
    role = registry.get_role(sid)
    if not user_id or not role:
        return {"error": "Not authenticated"}

    if not rate_limiter.is_allowed(sid, settings.RATE_LIMIT_LOCATION_PER_SECOND):
        return {"error": "Rate limit exceeded"}

    try:
        payload = LocationUpdate(**data)
    except ValidationError as e:
        return {"error": e.errors()}

    topic = (
        settings.KAFKA_TOPIC_DRIVER_LOCATIONS
        if role == "driver"
        else settings.KAFKA_TOPIC_PASSENGER_LOCATIONS
    )

    message = {
        "user_id": user_id,
        "user_type": role,
        "latitude": payload.latitude,
        "longitude": payload.longitude,
        "timestamp": payload.timestamp.isoformat(),
        "accuracy": payload.accuracy,
    }

    await kafka_producer.send(topic, message, key=user_id)
    return {"status": "ok"}


@sio.event
async def ride_request(sid: str, data: dict):
    user_id = registry.get_user_id(sid)
    role = registry.get_role(sid)
    if not user_id:
        return {"error": "Not authenticated"}

    if role != "passenger":
        return {"error": "Only passengers can request rides"}

    if not rate_limiter.is_allowed(sid, settings.RATE_LIMIT_DEFAULT_PER_SECOND):
        return {"error": "Rate limit exceeded"}

    try:
        payload = RideRequest(**data)
    except ValidationError as e:
        return {"error": e.errors()}

    message = {
        "request_id": str(uuid.uuid4()),
        "passenger_id": user_id,
        "pickup": {"latitude": payload.pickup_lat, "longitude": payload.pickup_lon},
        "dropoff": {"latitude": payload.dropoff_lat, "longitude": payload.dropoff_lon},
        "price": payload.price,
        "preferences": payload.preferences,
        "baggage": payload.baggage,
        "comment": payload.comment,
        "action": "request",
    }

    await kafka_producer.send(settings.KAFKA_TOPIC_RIDE_REQUESTS, message, key=user_id)
    logger.info("Ride request from passenger={}", user_id)
    return {"status": "ok", "request_id": message["request_id"]}


@sio.event
async def ride_cancel(sid: str, data: dict):
    user_id = registry.get_user_id(sid)
    if not user_id:
        return {"error": "Not authenticated"}

    if not rate_limiter.is_allowed(sid, settings.RATE_LIMIT_DEFAULT_PER_SECOND):
        return {"error": "Rate limit exceeded"}

    try:
        payload = RideCancel(**data)
    except ValidationError as e:
        return {"error": e.errors()}

    message = {
        "ride_id": str(payload.ride_id),
        "user_id": user_id,
        "reason": payload.reason,
        "action": "cancel",
    }

    await kafka_producer.send(settings.KAFKA_TOPIC_RIDE_REQUESTS, message, key=user_id)
    logger.info("Ride cancel from user={}, ride={}", user_id, payload.ride_id)
    return {"status": "ok"}


@sio.event
async def ride_event(sid: str, data: dict):
    user_id = registry.get_user_id(sid)
    if not user_id:
        return {"error": "Not authenticated"}

    if not rate_limiter.is_allowed(sid, settings.RATE_LIMIT_DEFAULT_PER_SECOND):
        return {"error": "Rate limit exceeded"}

    if not isinstance(data, dict) or "action" not in data:
        return {"error": "Missing action field"}

    data["user_id"] = user_id
    await kafka_producer.send(settings.KAFKA_TOPIC_RIDE_REQUESTS, data, key=user_id)
    return {"status": "ok"}
