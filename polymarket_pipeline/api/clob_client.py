"""
api/clob_client.py — Polymarket CLOB API Client.

CLOB (Central Limit Order Book) API cung cấp:
1. /book?token_id=<id>  → Best Bid / Best Ask realtime
2. /prices-history      → Chuỗi giá lịch sử theo time interval

Quy tắc quan trọng:
  - Token YES: giá (0-1) = implied probability (xác suất thắng)
  - Spread = Best Ask - Best Bid  (tight spread = thanh khoản tốt)
"""

import asyncio
import logging
import time
from dataclasses import dataclass
from typing import Optional

from polymarket_pipeline.api.base_client import BaseAsyncClient
from polymarket_pipeline.config import APIConfig

logger = logging.getLogger(__name__)


# ── Data containers (dễ mở rộng thành SQLAlchemy model sau này) ───────────────

@dataclass
class OrderbookSnapshot:
    """Snapshot của L2 orderbook tại một thời điểm."""
    token_id: str
    best_bid: Optional[float]       # giá mua tốt nhất (người mua trả)
    best_ask: Optional[float]       # giá bán tốt nhất (người bán muốn)
    mid_price: Optional[float]      # (bid + ask) / 2
    spread: Optional[float]         # ask - bid
    implied_probability: Optional[float]  # = mid_price (theo quy tắc Polymarket)
    timestamp: float                # unix timestamp


@dataclass
class PricePoint:
    """Một điểm dữ liệu giá lịch sử."""
    market_id: str          # condition_id của market
    timestamp: int          # unix timestamp
    price: float            # giá token YES tại thời điểm đó


class ClobClient(BaseAsyncClient):
    """Client cho Polymarket CLOB API."""

    def __init__(self, config: APIConfig):
        super().__init__(config.CLOB_BASE_URL, config)

    # ── 1. Lấy orderbook snapshot cho một token ────────────────────────────────
    async def get_orderbook(self, token_id: str) -> Optional[OrderbookSnapshot]:
        """
        Fetch Level-2 orderbook cho token_id, trích xuất best bid/ask.
        token_id ở đây là YES token của market.
        """
        data = await self._get(self.cfg.CLOB_BOOK_ENDPOINT, params={"token_id": token_id})
        if not data:
            return None
        return self._parse_orderbook(token_id, data)

    # ── 2. Batch fetch orderbook cho nhiều tokens (concurrent) ────────────────
    async def get_orderbooks_batch(
        self,
        token_ids: list[str],
    ) -> dict[str, Optional[OrderbookSnapshot]]:
        """
        Fetch orderbook đồng thời cho list token IDs.
        Trả về dict {token_id: OrderbookSnapshot}.
        Semaphore trong BaseAsyncClient tự động giới hạn concurrency.
        """
        tasks = {tid: self.get_orderbook(tid) for tid in token_ids}
        results = await asyncio.gather(*tasks.values(), return_exceptions=True)

        output = {}
        for token_id, result in zip(tasks.keys(), results):
            if isinstance(result, Exception):
                logger.warning("Error fetching orderbook for %s: %s", token_id, result)
                output[token_id] = None
            else:
                output[token_id] = result
        return output

    # ── 3. Lấy lịch sử giá của một market ────────────────────────────────────
    async def get_price_history(
        self,
        condition_id: str,
        start_ts: Optional[int] = None,
        end_ts: Optional[int] = None,
        fidelity: Optional[int] = None,
    ) -> list[PricePoint]:
        """
        Fetch chuỗi giá lịch sử (OHLC-style) cho một market.

        Args:
            condition_id: Market condition ID (từ Gamma API field "conditionId")
            start_ts: Unix timestamp bắt đầu (default: 7 ngày trước)
            end_ts: Unix timestamp kết thúc (default: hiện tại)
            fidelity: Độ phân giải tính bằng phút (default từ config)
        """
        now = int(time.time())
        params = {
            "market": condition_id,
            "startTs": start_ts or (now - self.cfg.HISTORY_DAYS_BACK * 86400),
            "endTs": end_ts or now,
            "fidelity": fidelity or self.cfg.HISTORY_FIDELITY,
        }

        data = await self._get(self.cfg.CLOB_HISTORY_ENDPOINT, params=params)
        if not data:
            return []

        return self._parse_price_history(condition_id, data)

    # ── 4. Batch fetch lịch sử cho nhiều markets ──────────────────────────────
    async def get_price_histories_batch(
        self,
        condition_ids: list[str],
        **kwargs,
    ) -> dict[str, list[PricePoint]]:
        """Fetch lịch sử giá đồng thời cho nhiều markets."""
        tasks = {
            cid: self.get_price_history(cid, **kwargs)
            for cid in condition_ids
        }
        results = await asyncio.gather(*tasks.values(), return_exceptions=True)

        output = {}
        for cid, result in zip(tasks.keys(), results):
            if isinstance(result, Exception):
                logger.warning("Error fetching history for %s: %s", cid, result)
                output[cid] = []
            else:
                output[cid] = result
        return output

    # ── Parsers nội bộ ────────────────────────────────────────────────────────

    @staticmethod
    def _parse_orderbook(token_id: str, data: dict) -> Optional[OrderbookSnapshot]:
        """
        Parse response từ /book endpoint.
        Response format:
        {
          "bids": [{"price": "0.62", "size": "100"}, ...],
          "asks": [{"price": "0.64", "size": "50"}, ...]
        }
        """
        try:
            bids: list[dict] = data.get("bids", [])
            asks: list[dict] = data.get("asks", [])

            # Best bid = giá cao nhất trong bids (người mua trả nhiều nhất)
            # Best ask = giá thấp nhất trong asks (người bán muốn thấp nhất)
            best_bid: Optional[float] = None
            best_ask: Optional[float] = None

            if bids:
                # Bids thường được sort DESC, lấy phần tử đầu tiên
                bid_prices = [float(b["price"]) for b in bids if "price" in b]
                best_bid = max(bid_prices) if bid_prices else None

            if asks:
                # Asks thường được sort ASC, lấy phần tử đầu tiên
                ask_prices = [float(a["price"]) for a in asks if "price" in a]
                best_ask = min(ask_prices) if ask_prices else None

            # Mid price và spread
            mid_price = None
            spread = None
            if best_bid is not None and best_ask is not None:
                mid_price = (best_bid + best_ask) / 2
                spread = best_ask - best_bid

            # Implied probability = mid price (quy tắc Polymarket: giá YES = xác suất)
            implied_prob = mid_price

            # Nếu không có cả 2 phía, thử dùng giá có sẵn
            if implied_prob is None:
                implied_prob = best_bid or best_ask

            return OrderbookSnapshot(
                token_id=token_id,
                best_bid=best_bid,
                best_ask=best_ask,
                mid_price=mid_price,
                spread=spread,
                implied_probability=implied_prob,
                timestamp=time.time(),
            )

        except (KeyError, ValueError, TypeError) as exc:
            logger.warning("Failed to parse orderbook for token %s: %s", token_id, exc)
            return None

    @staticmethod
    def _parse_price_history(condition_id: str, data: dict | list) -> list[PricePoint]:
        """
        Parse response từ /prices-history endpoint.
        Response format:
        {
          "history": [{"t": 1700000000, "p": "0.65"}, ...]
        }
        """
        try:
            # Normalize: API có thể trả về list hoặc dict với key "history"
            history: list = []
            if isinstance(data, list):
                history = data
            elif isinstance(data, dict):
                for key in ("history", "prices", "data"):
                    if key in data:
                        history = data[key]
                        break

            points = []
            for entry in history:
                ts = entry.get("t") or entry.get("timestamp")
                price = entry.get("p") or entry.get("price")
                if ts is None or price is None:
                    continue
                try:
                    points.append(PricePoint(
                        market_id=condition_id,
                        timestamp=int(ts),
                        price=float(price),
                    ))
                except (ValueError, TypeError):
                    continue

            return points

        except Exception as exc:
            logger.warning("Failed to parse price history for %s: %s", condition_id, exc)
            return []
