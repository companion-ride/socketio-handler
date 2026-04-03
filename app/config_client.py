import asyncio
import logging
from typing import Any, Callable

import httpx

logger = logging.getLogger(__name__)


class ConfigClient:
    def __init__(self, base_url: str, api_key: str, service_name: str) -> None:
        self._base_url = base_url.rstrip("/")
        self._api_key = api_key
        self._service_name = service_name
        self._last_etag: str = ""
        self._err_count: int = 0
        self._client = httpx.AsyncClient(timeout=10.0)
        self._task: asyncio.Task | None = None

    async def fetch(self) -> tuple[dict[str, Any] | None, bool]:
        """GET /service/:name with If-None-Match.

        Returns (config_dict, changed).
        On 304 returns (None, False).
        Raises on error.
        """
        url = f"{self._base_url}/service/{self._service_name}"
        headers: dict[str, str] = {"Authorization": f"Bearer {self._api_key}"}
        if self._last_etag:
            headers["If-None-Match"] = self._last_etag

        resp = await self._client.get(url, headers=headers)

        if resp.status_code == 304:
            return None, False

        resp.raise_for_status()

        data = resp.json()
        etag = resp.headers.get("etag", "")
        if etag:
            self._last_etag = etag

        return data["config"], True

    async def start_polling(
        self,
        interval: int,
        on_update: Callable[[dict[str, Any]], None],
    ) -> None:
        """Start background polling task. Call stop() to cancel."""
        self._task = asyncio.create_task(self._poll_loop(interval, on_update))

    async def _poll_loop(
        self,
        interval: int,
        on_update: Callable[[dict[str, Any]], None],
    ) -> None:
        while True:
            await asyncio.sleep(interval)
            try:
                config, changed = await self.fetch()
                if self._err_count > 0:
                    self._err_count = 0
                if changed and config is not None:
                    logger.info(
                        "New config applied for %s (etag=%s)",
                        self._service_name,
                        self._last_etag,
                    )
                    on_update(config)
            except asyncio.CancelledError:
                raise
            except Exception:
                self._err_count += 1
                if self._err_count <= 5 or self._err_count % 2 == 0:
                    logger.error(
                        "Config polling failed for %s (consecutive=%d)",
                        self._service_name,
                        self._err_count,
                        exc_info=True,
                    )

    async def stop(self) -> None:
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        await self._client.aclose()
