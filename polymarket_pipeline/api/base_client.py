"""
api/base_client.py — Async HTTP client nền tảng với Retry + Rate-limit.

Tất cả API client đều kế thừa class này. Thiết kế theo pattern này giúp:
- Dễ swap sang WebSocket client sau này
- Tập trung xử lý lỗi tại một nơi duy nhất
- Dễ mock trong unit tests
"""

import asyncio
import logging
from typing import Any, Optional

import aiohttp

from polymarket_pipeline.config import APIConfig

logger = logging.getLogger(__name__)


class BaseAsyncClient:
    """
    Async HTTP client với:
    - Connection pooling qua aiohttp.ClientSession
    - Exponential backoff retry cho 429 / 5xx
    - Semaphore để giới hạn concurrent requests
    """

    def __init__(self, base_url: str, config: APIConfig):
        self.base_url = base_url.rstrip("/")
        self.cfg = config
        self._session: Optional[aiohttp.ClientSession] = None
        # Semaphore giới hạn số request chạy đồng thời
        self._semaphore = asyncio.Semaphore(config.MAX_CONCURRENT_REQUESTS)

    # ── Context manager để đảm bảo session luôn được đóng ─────────────────────
    async def __aenter__(self):
        connector = aiohttp.TCPConnector(
            limit=self.cfg.MAX_CONCURRENT_REQUESTS,
            ttl_dns_cache=300,      # cache DNS 5 phút
            enable_cleanup_closed=True,
        )
        timeout = aiohttp.ClientTimeout(total=self.cfg.REQUEST_TIMEOUT)
        self._session = aiohttp.ClientSession(
            connector=connector,
            timeout=timeout,
            headers={"Accept": "application/json", "User-Agent": "PolymarketPipeline/1.0"},
        )
        return self

    async def __aexit__(self, *args):
        if self._session:
            await self._session.close()

    # ── Core GET với retry logic ───────────────────────────────────────────────
    async def _get(
        self,
        endpoint: str,
        params: Optional[dict] = None,
    ) -> Optional[Any]:
        """
        Thực hiện GET request với exponential backoff retry.
        Trả về parsed JSON hoặc None nếu thất bại sau tất cả retry.
        """
        url = f"{self.base_url}{endpoint}"
        delay = self.cfg.RETRY_BASE_DELAY

        async with self._semaphore:
            for attempt in range(1, self.cfg.RETRY_ATTEMPTS + 1):
                try:
                    async with self._session.get(url, params=params) as resp:
                        # ── Rate limit: chờ và retry ───────────────────────
                        if resp.status == 429:
                            retry_after = float(resp.headers.get("Retry-After", delay))
                            logger.warning(
                                "Rate limited on %s. Retrying in %.1fs (attempt %d/%d)",
                                url, retry_after, attempt, self.cfg.RETRY_ATTEMPTS,
                            )
                            await asyncio.sleep(retry_after)
                            delay *= self.cfg.RETRY_BACKOFF
                            continue

                        # ── Server error: retry với backoff ───────────────
                        if resp.status >= 500:
                            logger.warning(
                                "Server error %d on %s. Retrying in %.1fs (attempt %d/%d)",
                                resp.status, url, delay, attempt, self.cfg.RETRY_ATTEMPTS,
                            )
                            await asyncio.sleep(delay)
                            delay *= self.cfg.RETRY_BACKOFF
                            continue

                        # ── Client error (4xx trừ 429): không retry ────────
                        if resp.status >= 400:
                            logger.error(
                                "Client error %d on %s with params %s",
                                resp.status, url, params,
                            )
                            return None

                        # ── Success ────────────────────────────────────────
                        return await resp.json(content_type=None)

                except asyncio.TimeoutError:
                    logger.warning(
                        "Timeout on %s (attempt %d/%d). Retrying in %.1fs",
                        url, attempt, self.cfg.RETRY_ATTEMPTS, delay,
                    )
                    await asyncio.sleep(delay)
                    delay *= self.cfg.RETRY_BACKOFF

                except aiohttp.ClientError as exc:
                    logger.warning(
                        "Connection error on %s: %s (attempt %d/%d)",
                        url, exc, attempt, self.cfg.RETRY_ATTEMPTS,
                    )
                    await asyncio.sleep(delay)
                    delay *= self.cfg.RETRY_BACKOFF

        logger.error("All %d retry attempts exhausted for %s", self.cfg.RETRY_ATTEMPTS, url)
        return None
