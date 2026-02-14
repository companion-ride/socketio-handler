import time
from collections import defaultdict


class SocketRateLimiter:
    def __init__(self) -> None:
        self._timestamps: dict[str, list[float]] = defaultdict(list)

    def is_allowed(self, sid: str, max_per_second: int, window: float = 1.0) -> bool:
        now = time.monotonic()
        timestamps = self._timestamps[sid]
        self._timestamps[sid] = [t for t in timestamps if now - t < window]
        if len(self._timestamps[sid]) >= max_per_second:
            return False
        self._timestamps[sid].append(now)
        return True

    def cleanup(self, sid: str) -> None:
        self._timestamps.pop(sid, None)


rate_limiter = SocketRateLimiter()
