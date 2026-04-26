"""
pipeline.py — Main Pipeline Orchestrator.

Đây là "bộ não" của hệ thống. Orchestrator điều phối:
1. Fetch toàn bộ events/markets từ Gamma API
2. Batch fetch orderbook từ CLOB API (concurrent)
3. (Tùy chọn) Fetch lịch sử giá
4. Transform data → pandas DataFrame
5. Lưu vào storage layer

Thiết kế: Pipeline không biết gì về CSV hay DB — nó chỉ
giao tiếp qua interface BaseStorage. Để chuyển sang DB,
chỉ cần inject một DatabaseHandler thay vì CSVHandler.
"""

import asyncio
import logging
import time
from datetime import datetime, timezone
from typing import Optional

import pandas as pd

from polymarket_pipeline.api.clob_client import ClobClient, OrderbookSnapshot
from polymarket_pipeline.api.gamma_client import GammaClient
from polymarket_pipeline.config import PipelineConfig
from polymarket_pipeline.storage.csv_handler import BaseStorage

logger = logging.getLogger(__name__)


class PolymarketPipeline:
    """
    Full ETL pipeline cho Polymarket data.

    Usage:
        async with PolymarketPipeline(config, storage) as pipeline:
            await pipeline.run_full()          # current + history
            await pipeline.run_current_only()  # chỉ snapshot hiện tại (nhanh hơn)
    """

    def __init__(self, config: PipelineConfig, storage: BaseStorage):
        self.cfg = config
        self.storage = storage
        self._gamma: Optional[GammaClient] = None
        self._clob: Optional[ClobClient] = None

    async def __aenter__(self):
        self._gamma = GammaClient(self.cfg.api)
        self._clob = ClobClient(self.cfg.api)
        # Khởi động cả 2 HTTP session
        await self._gamma.__aenter__()
        await self._clob.__aenter__()
        logger.info("Pipeline initialized. Sessions ready.")
        return self

    async def __aexit__(self, *args):
        if self._gamma:
            await self._gamma.__aexit__(*args)
        if self._clob:
            await self._clob.__aexit__(*args)
        logger.info("Pipeline shutdown. Sessions closed.")

    # ── Public entry points ───────────────────────────────────────────────────

    async def run_full(self) -> dict:
        """
        Chạy pipeline đầy đủ: current snapshot + historical prices.
        Trả về summary stats.
        """
        logger.info("=== Starting FULL pipeline run ===")
        t_start = time.monotonic()

        # Step 1: Collect markets metadata
        markets = await self._collect_markets()
        logger.info("Collected %d markets from Gamma API.", len(markets))

        if not markets:
            logger.warning("No markets found. Exiting.")
            return {"markets_count": 0, "duration_sec": 0}

        # Step 2: Fetch orderbooks (current state)
        current_df = await self._build_current_snapshot(markets)
        self.storage.save_current_snapshot(current_df)

        # Step 3: Fetch price history
        history_df = await self._build_history(markets)
        if not history_df.empty:
            self.storage.append_history(history_df)

        elapsed = time.monotonic() - t_start
        stats = {
            "markets_count": len(markets),
            "current_rows": len(current_df),
            "history_rows": len(history_df),
            "duration_sec": round(elapsed, 2),
        }
        logger.info("=== Pipeline run complete: %s ===", stats)
        return stats

    async def run_current_only(self) -> dict:
        """
        Chạy pipeline nhanh: chỉ lấy current snapshot (không lấy history).
        Phù hợp cho cronjob chạy mỗi 5 phút.
        """
        logger.info("=== Starting CURRENT-ONLY pipeline run ===")
        t_start = time.monotonic()

        markets = await self._collect_markets()
        logger.info("Collected %d markets.", len(markets))

        current_df = await self._build_current_snapshot(markets)
        self.storage.save_current_snapshot(current_df)

        elapsed = time.monotonic() - t_start
        stats = {
            "markets_count": len(markets),
            "current_rows": len(current_df),
            "duration_sec": round(elapsed, 2),
        }
        logger.info("=== Pipeline run complete: %s ===", stats)
        return stats

    # ── Private: Data collection ──────────────────────────────────────────────

    async def _collect_markets(self) -> list[dict]:
        """
        Collect toàn bộ markets từ Gamma API.
        Strategy: iter events → extract markets từ mỗi event.
        (Tốt hơn iter_markets trực tiếp vì mang theo event title)
        """
        markets: list[dict] = []

        async for event in self._gamma.iter_events(active_only=self.cfg.ONLY_ACTIVE_MARKETS):
            event_markets = self._gamma.extract_markets_from_event(event)

            for m in event_markets:
                # Filter theo liquidity/volume nếu cần
                liquidity = self._safe_float(m.get("liquidity") or m.get("liquidityNum"))
                volume = self._safe_float(m.get("volume") or m.get("volumeNum"))

                if liquidity is not None and liquidity < self.cfg.MIN_LIQUIDITY:
                    continue
                if volume is not None and volume < self.cfg.MIN_VOLUME:
                    continue

                markets.append(m)

        return markets

    # ── Private: Build current snapshot DataFrame ─────────────────────────────

    async def _build_current_snapshot(self, markets: list[dict]) -> pd.DataFrame:
        """
        Fetch orderbook cho YES token của tất cả markets.
        Xử lý theo batch để không overwhelm API.
        """
        # Tập hợp token IDs cần fetch
        token_to_market: dict[str, dict] = {}  # yes_token_id → market metadata
        for m in markets:
            yes_id, _ = self._gamma.get_token_ids(m)
            if yes_id:
                token_to_market[yes_id] = m

        if not token_to_market:
            logger.warning("No valid YES token IDs found.")
            return pd.DataFrame()

        token_ids = list(token_to_market.keys())
        logger.info("Fetching orderbooks for %d YES tokens...", len(token_ids))

        # Batch fetch (asyncio.gather bên trong, được giới hạn bởi semaphore)
        orderbooks = await self._clob.get_orderbooks_batch(token_ids)

        # Build rows
        rows = []
        fetched_at = datetime.now(timezone.utc).isoformat()

        for yes_id, market in token_to_market.items():
            ob: Optional[OrderbookSnapshot] = orderbooks.get(yes_id)
            row = self._build_current_row(market, ob, fetched_at)
            rows.append(row)

        df = pd.DataFrame(rows)
        logger.info("Built current snapshot DataFrame: %d rows.", len(df))
        return df

    # ── Private: Build historical DataFrame ───────────────────────────────────

    async def _build_history(self, markets: list[dict]) -> pd.DataFrame:
        """
        Fetch lịch sử giá cho tất cả markets.
        Sử dụng condition_id (không phải token_id) cho CLOB history endpoint.
        """
        # Map condition_id → market
        cid_to_market: dict[str, dict] = {}
        for m in markets:
            cid = m.get("conditionId") or m.get("condition_id")
            if cid:
                cid_to_market[cid] = m

        if not cid_to_market:
            logger.warning("No condition IDs found for history fetch.")
            return pd.DataFrame()

        condition_ids = list(cid_to_market.keys())
        logger.info("Fetching price history for %d markets...", len(condition_ids))

        histories = await self._clob.get_price_histories_batch(condition_ids)

        fetched_at = datetime.now(timezone.utc).isoformat()
        rows = []

        for cid, price_points in histories.items():
            market = cid_to_market.get(cid, {})
            for pt in price_points:
                rows.append({
                    "market_id": market.get("id", ""),
                    "condition_id": cid,
                    "event_title": market.get("event_title", ""),
                    "question": market.get("question", ""),
                    "price_timestamp": pt.timestamp,
                    "price": pt.price,
                    "implied_probability_pct": round(pt.price * 100, 2),
                    "fetched_at": fetched_at,
                })

        df = pd.DataFrame(rows)
        logger.info("Built history DataFrame: %d rows.", len(df))
        return df

    # ── Private: Row builders ─────────────────────────────────────────────────

    @staticmethod
    def _build_current_row(
        market: dict,
        ob: Optional[OrderbookSnapshot],
        fetched_at: str,
    ) -> dict:
        """
        Transform market metadata + orderbook snapshot → dict row.
        Handle gracefully khi ob=None (market chưa có thanh khoản).
        """
        # Implied probability: từ orderbook nếu có, ngược lại từ Gamma metadata
        implied_prob = None
        best_bid = best_ask = mid_price = spread = None

        if ob:
            best_bid = ob.best_bid
            best_ask = ob.best_ask
            mid_price = ob.mid_price
            spread = ob.spread
            implied_prob = ob.implied_probability
        else:
            # Fallback: dùng outcomePrices từ Gamma API
            outcome_prices = market.get("outcomePrices", [])
            if isinstance(outcome_prices, list) and outcome_prices:
                try:
                    implied_prob = float(outcome_prices[0])
                    mid_price = implied_prob
                except (ValueError, TypeError):
                    pass

        # Convert probability sang phần trăm (0-100)
        implied_pct = (
            round(implied_prob * 100, 2)
            if implied_prob is not None
            else None
        )

        return {
            "market_id": market.get("id", ""),
            "condition_id": market.get("conditionId") or market.get("condition_id", ""),
            "event_id": market.get("event_id", ""),
            "event_title": market.get("event_title", ""),
            "question": market.get("question", ""),
            "category": market.get("category", ""),
            "liquidity": PolymarketPipeline._safe_float(
                market.get("liquidity") or market.get("liquidityNum")
            ),
            "volume": PolymarketPipeline._safe_float(
                market.get("volume") or market.get("volumeNum")
            ),
            "best_bid_yes": best_bid,
            "best_ask_yes": best_ask,
            "mid_price_yes": mid_price,
            "implied_probability_pct": implied_pct,
            "spread": spread,
            "market_status": market.get("status", ""),
            "end_date": market.get("endDate") or market.get("end_date", ""),
            "fetched_at": fetched_at,
        }

    # ── Utilities ─────────────────────────────────────────────────────────────

    @staticmethod
    def _safe_float(value) -> Optional[float]:
        """Parse float an toàn, trả về None nếu không parse được."""
        if value is None:
            return None
        try:
            return float(value)
        except (ValueError, TypeError):
            return None
