import uuid

import jwt
from loguru import logger
from pydantic import ValidationError

from app.auth.jwt import decode_access_token
from app.config import settings
import httpx

from app.kafka.producer import kafka_producer
from app.schemas.events import ChatHistory, ChatMessage, LocationUpdate, RideCancel, RideRequest
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

    # Auto-join user to their active chat rooms
    try:
        async with httpx.AsyncClient() as client:
            resp = await client.get(
                f"{settings.CHAT_SERVICE_URL}/chat/users/{user_id}/rooms",
                headers={"Authorization": f"Bearer {_get_internal_token()}"},
                timeout=5.0,
            )
            if resp.status_code == 200:
                rooms = resp.json()
                for room in rooms:
                    room_id = room.get("room_id", "")
                    if room_id:
                        sio.enter_room(sid, f"chat_{room_id}")
    except Exception:
        pass  # Non-critical: user can still receive messages via direct emit

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
    logger.info("location_update from user={}, role={}, lat={}, lon={}", user_id, role, payload.latitude, payload.longitude)
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


@sio.event
async def chat_message(sid: str, data: dict):
    """Handle incoming chat message from client."""
    user_id = registry.get_user_id(sid)
    if not user_id:
        await sio.emit("chat_error", {"error": "not authenticated", "code": "auth_error"}, to=sid)
        return

    if not rate_limiter.is_allowed(sid, settings.RATE_LIMIT_DEFAULT_PER_SECOND):
        await sio.emit("chat_error", {"error": "rate limited", "code": "rate_limited"}, to=sid)
        return

    try:
        msg = ChatMessage(**data)
    except Exception as e:
        await sio.emit("chat_error", {"error": str(e), "code": "validation_error", "room_id": data.get("room_id", "")}, to=sid)
        return

    await kafka_producer.send(
        topic=settings.KAFKA_TOPIC_CHAT_INBOUND,
        value={
            "room_id": msg.room_id,
            "sender_id": user_id,
            "content": msg.content,
        },
        key=msg.room_id,
    )


@sio.event
async def chat_history(sid: str, data: dict):
    """Fetch chat history via chat-service REST API."""
    user_id = registry.get_user_id(sid)
    if not user_id:
        await sio.emit("chat_error", {"error": "not authenticated", "code": "auth_error"}, to=sid)
        return

    try:
        req = ChatHistory(**data)
    except Exception as e:
        await sio.emit("chat_error", {"error": str(e), "code": "validation_error"}, to=sid)
        return

    try:
        async with httpx.AsyncClient() as client:
            params = {"limit": req.limit}
            if req.cursor:
                params["cursor"] = req.cursor
            resp = await client.get(
                f"{settings.CHAT_SERVICE_URL}/chat/rooms/{req.room_id}/messages",
                params=params,
                headers={"Authorization": f"Bearer {_get_internal_token()}"},
                timeout=10.0,
            )
            if resp.status_code == 200:
                result = resp.json()
                result["room_id"] = req.room_id
                await sio.emit("chat_history_response", result, to=sid)
            else:
                await sio.emit("chat_error", {
                    "error": "failed to fetch history",
                    "code": "server_error",
                    "room_id": req.room_id,
                }, to=sid)
    except Exception as e:
        await sio.emit("chat_error", {
            "error": str(e),
            "code": "server_error",
            "room_id": req.room_id,
        }, to=sid)


def _get_internal_token() -> str:
    """Generate a minimal internal JWT for service-to-service calls."""
    import time as _time

    import jwt as pyjwt

    payload = {
        "sub": "socketio-handler",
        "role": "admin",
        "type": "access",
        "iat": int(_time.time()),
        "exp": int(_time.time()) + 300,
    }
    return pyjwt.encode(payload, settings.JWT_SECRET_KEY, algorithm="HS256")
