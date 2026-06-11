import socketio
from loguru import logger

sio = socketio.AsyncServer(
    async_mode="asgi",
    cors_allowed_origins="*",
    logger=False,
    engineio_logger=False,
)


def _redact(data):
    if isinstance(data, dict):
        return {k: ("<redacted>" if k in ("token", "access_token", "refresh_token") else _redact(v)) for k, v in data.items()}
    if isinstance(data, list):
        return [_redact(v) for v in data]
    return data


def _summarize_args(event, args):
    if event == "connect":
        sid = args[0] if len(args) > 0 else None
        auth = args[2] if len(args) > 2 else None
        return {"sid": sid, "auth": _redact(auth)}
    sid = args[0] if len(args) > 0 else None
    payload = args[1] if len(args) > 1 else None
    return {"sid": sid, "data": _redact(payload)}


_orig_trigger = sio._trigger_event


async def _logging_trigger(event, namespace, *args):
    logger.info("socket event in: event={} ns={} {}", event, namespace, _summarize_args(event, args))
    return await _orig_trigger(event, namespace, *args)


sio._trigger_event = _logging_trigger
