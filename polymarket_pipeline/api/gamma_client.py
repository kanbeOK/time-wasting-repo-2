"""
api/gamma_client.py — Polymarket Gamma API Client.

Gamma API cung cấp metadata của toàn bộ events và markets:
- Tên sự kiện / câu hỏi
- Liquidity, Volume
- Token IDs (dùng để query CLOB API)
- Trạng thái (active/closed/resolved)
"""

import asyncio
import logging
from typing import AsyncIterator, Optional

from polymarket_pipeline.api.base_client import BaseAsyncClient
from polymarket_pipeline.config import APIConfig

logger = logging.getLogger(__name__)


class GammaClient(BaseAsyncClient):
    """
    Client cho Polymarket Gamma API.
    Sử dụng async generator để stream pages → tiết kiệm memory khi có hàng nghìn markets.
    """

    def __init__(self, config: APIConfig):
        super().__init__(config.GAMMA_BASE_URL, config)

    # ── Fetch toàn bộ events (có phân trang) ──────────────────────────────────
    async def iter_events(
        self,
        active_only: bool = True,
    ) -> AsyncIterator[dict]:
        """
        Async generator: yield từng event dict từ Gamma API.
        Tự động phân trang cho đến khi hết data hoặc đạt MAX_PAGES.
        """
        offset = 0
        page = 0
        params_base: dict = {"limit": self.cfg.GAMMA_PAGE_SIZE}
        if active_only:
            params_base["active"] = "true"
            params_base["closed"] = "false"

        while True:
            # Kiểm tra giới hạn số trang (nếu có)
            if self.cfg.MAX_PAGES and page >= self.cfg.MAX_PAGES:
                logger.info("Reached MAX_PAGES=%d, stopping pagination.", self.cfg.MAX_PAGES)
                break

            params = {**params_base, "offset": offset}
            logger.debug("Fetching events page %d (offset=%d)", page, offset)

            data = await self._get(self.cfg.GAMMA_EVENTS_ENDPOINT, params=params)

            # Gamma API trả về list trực tiếp hoặc dict có key "events"
            events = self._extract_list(data)

            if not events:
                logger.info("No more events at offset=%d. Total pages fetched: %d", offset, page)
                break

            for event in events:
                yield event

            offset += len(events)
            page += 1

            # Nhỏ delay để thân thiện với rate limit
            await asyncio.sleep(self.cfg.RATE_LIMIT_DELAY)

    # ── Fetch toàn bộ markets trực tiếp (alternative endpoint) ───────────────
    async def iter_markets(
        self,
        active_only: bool = True,
    ) -> AsyncIterator[dict]:
        """
        Fetch markets trực tiếp từ /markets endpoint.
        Mỗi market chứa đầy đủ token IDs để query CLOB.
        """
        offset = 0
        page = 0
        params_base: dict = {"limit": self.cfg.GAMMA_PAGE_SIZE}
        if active_only:
            params_base["active"] = "true"
            params_base["closed"] = "false"

        while True:
            if self.cfg.MAX_PAGES and page >= self.cfg.MAX_PAGES:
                break

            params = {**params_base, "offset": offset}
            data = await self._get(self.cfg.GAMMA_MARKETS_ENDPOINT, params=params)
            markets = self._extract_list(data)

            if not markets:
                logger.info("No more markets at offset=%d.", offset)
                break

            for market in markets:
                yield market

            offset += len(markets)
            page += 1
            await asyncio.sleep(self.cfg.RATE_LIMIT_DELAY)

    # ── Parse markets từ event ─────────────────────────────────────────────────
    @staticmethod
    def extract_markets_from_event(event: dict) -> list[dict]:
        """
        Mỗi event chứa một list markets.
        Inject event-level metadata (tên sự kiện, slug) vào từng market.
        """
        event_title = event.get("title", "")
        event_slug = event.get("slug", "")
        event_id = event.get("id", "")

        markets = event.get("markets", [])
        enriched = []
        for m in markets:
            m["event_title"] = event_title
            m["event_slug"] = event_slug
            m["event_id"] = event_id
            enriched.append(m)
        return enriched

    # ── Helper: normalize response format ─────────────────────────────────────
    @staticmethod
    def _extract_list(data: Optional[dict | list]) -> list:
        """
        Gamma API đôi khi trả về list, đôi khi trả về {"events": [...]} hoặc {"markets": [...]}.
        Normalize về list.
        """
        if data is None:
            return []
        if isinstance(data, list):
            return data
        # Thử các key phổ biến
        for key in ("events", "markets", "data", "results"):
            if key in data and isinstance(data[key], list):
                return data[key]
        logger.warning("Unexpected Gamma API response format: %s", type(data))
        return []

    # ── Parse token IDs (YES/NO) từ market ────────────────────────────────────
    @staticmethod
    def get_token_ids(market: dict) -> tuple[Optional[str], Optional[str]]:
        """
        Trả về (yes_token_id, no_token_id).
        clobTokenIds là list [yes_id, no_id] theo thứ tự chuẩn của Polymarket.
        """
        tokens = market.get("clobTokenIds")
        if not tokens:
            # Thử parse từ string JSON nếu cần
            import json
            raw = market.get("clob_token_ids", "[]")
            try:
                tokens = json.loads(raw) if isinstance(raw, str) else raw
            except (json.JSONDecodeError, TypeError):
                tokens = []

        yes_id = tokens[0] if len(tokens) > 0 else None
        no_id = tokens[1] if len(tokens) > 1 else None
        return yes_id, no_id
