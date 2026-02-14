import asyncio


class ConnectionRegistry:
    def __init__(self) -> None:
        self._user_to_sid: dict[str, str] = {}
        self._sid_to_user: dict[str, str] = {}
        self._sid_to_role: dict[str, str] = {}
        self._lock = asyncio.Lock()

    async def register(self, user_id: str, sid: str, role: str) -> None:
        async with self._lock:
            # One connection per user — disconnect previous
            old_sid = self._user_to_sid.pop(user_id, None)
            if old_sid:
                self._sid_to_user.pop(old_sid, None)
                self._sid_to_role.pop(old_sid, None)
            self._user_to_sid[user_id] = sid
            self._sid_to_user[sid] = user_id
            self._sid_to_role[sid] = role

    async def unregister(self, sid: str) -> str | None:
        async with self._lock:
            user_id = self._sid_to_user.pop(sid, None)
            self._sid_to_role.pop(sid, None)
            if user_id:
                self._user_to_sid.pop(user_id, None)
            return user_id

    def get_sid(self, user_id: str) -> str | None:
        return self._user_to_sid.get(user_id)

    def get_user_id(self, sid: str) -> str | None:
        return self._sid_to_user.get(sid)

    def get_role(self, sid: str) -> str | None:
        return self._sid_to_role.get(sid)

    @property
    def active_count(self) -> int:
        return len(self._sid_to_user)


registry = ConnectionRegistry()
