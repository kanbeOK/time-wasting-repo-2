"""
config.py — Centralized configuration for the Polymarket pipeline.
Tách biệt config giúp dễ dàng chuyển sang env vars hoặc .env file sau này.
"""

import os
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class APIConfig:
    # ── Polymarket Gamma API (events / markets metadata) ──────────────────────
    GAMMA_BASE_URL: str = "https://gamma-api.polymarket.com"
    GAMMA_EVENTS_ENDPOINT: str = "/events"
    GAMMA_MARKETS_ENDPOINT: str = "/markets"

    # ── Polymarket CLOB API (orderbook + price history) ───────────────────────
    CLOB_BASE_URL: str = "https://clob.polymarket.com"
    CLOB_BOOK_ENDPOINT: str = "/book"           # GET ?token_id=
    CLOB_HISTORY_ENDPOINT: str = "/prices-history"  # GET ?market=&startTs=&endTs=&fidelity=

    # ── Pagination ─────────────────────────────────────────────────────────────
    GAMMA_PAGE_SIZE: int = 100        # max records per page từ Gamma API
    MAX_PAGES: Optional[int] = None   # None = không giới hạn (cào toàn bộ)

    # ── Concurrency & Rate-limit ───────────────────────────────────────────────
    MAX_CONCURRENT_REQUESTS: int = 20   # số coroutine chạy đồng thời
    RATE_LIMIT_DELAY: float = 0.05      # giây chờ giữa các batch request
    RETRY_ATTEMPTS: int = 3             # số lần retry khi gặp lỗi 429/5xx
    RETRY_BASE_DELAY: float = 1.0       # giây chờ trước lần retry đầu tiên
    RETRY_BACKOFF: float = 2.0          # hệ số exponential backoff
    REQUEST_TIMEOUT: int = 30           # timeout mỗi request (giây)

    # ── Historical data ────────────────────────────────────────────────────────
    HISTORY_FIDELITY: int = 60          # độ phân giải lịch sử: 60 phút
    HISTORY_DAYS_BACK: int = 7          # số ngày lấy lịch sử


@dataclass
class StorageConfig:
    OUTPUT_DIR: str = field(default_factory=lambda: os.getenv("OUTPUT_DIR", "data"))
    CURRENT_CSV: str = "polymarket_current.csv"      # snapshot hiện tại
    HISTORY_CSV: str = "polymarket_history.csv"      # dữ liệu lịch sử


@dataclass
class PipelineConfig:
    api: APIConfig = field(default_factory=APIConfig)
    storage: StorageConfig = field(default_factory=StorageConfig)

    # ── Filters (để reduce noise) ──────────────────────────────────────────────
    ONLY_ACTIVE_MARKETS: bool = True     # chỉ lấy market đang active
    MIN_LIQUIDITY: float = 0.0           # bỏ qua market có liquidity < threshold
    MIN_VOLUME: float = 0.0


# Singleton instance dùng xuyên suốt pipeline
CONFIG = PipelineConfig()
