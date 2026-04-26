"""
storage/csv_handler.py — CSV Storage Handler.

Thiết kế với interface trừu tượng (BaseStorage) để sau này
dễ swap sang PostgreSQL / ClickHouse / BigQuery chỉ bằng cách
implement một class mới mà không cần sửa pipeline logic.
"""

import logging
import os
from abc import ABC, abstractmethod
from pathlib import Path

import pandas as pd

from polymarket_pipeline.config import StorageConfig

logger = logging.getLogger(__name__)


# ── Abstract interface (dễ swap sang DB sau này) ──────────────────────────────

class BaseStorage(ABC):
    @abstractmethod
    def save_current_snapshot(self, df: pd.DataFrame) -> None: ...

    @abstractmethod
    def append_history(self, df: pd.DataFrame) -> None: ...

    @abstractmethod
    def load_current(self) -> pd.DataFrame: ...


# ── CSV Implementation ─────────────────────────────────────────────────────────

class CSVHandler(BaseStorage):
    """
    Lưu trữ data vào CSV file với logic:
    - save_current_snapshot: Ghi đè file snapshot (luôn là data mới nhất)
    - append_history: Append vào file lịch sử, tránh duplicate header
    """

    # Columns chuẩn cho snapshot
    CURRENT_COLUMNS = [
        "market_id",
        "condition_id",
        "event_id",
        "event_title",
        "question",
        "category",
        "liquidity",
        "volume",
        "best_bid_yes",
        "best_ask_yes",
        "mid_price_yes",
        "implied_probability_pct",   # 0-100 %
        "spread",
        "market_status",
        "end_date",
        "fetched_at",                # ISO timestamp lúc fetch
    ]

    # Columns chuẩn cho lịch sử
    HISTORY_COLUMNS = [
        "market_id",
        "condition_id",
        "event_title",
        "question",
        "price_timestamp",       # unix timestamp của điểm giá
        "price",                 # giá token YES tại thời điểm đó
        "implied_probability_pct",
        "fetched_at",
    ]

    def __init__(self, config: StorageConfig):
        self.cfg = config
        self._output_dir = Path(config.OUTPUT_DIR)
        self._output_dir.mkdir(parents=True, exist_ok=True)
        self._current_path = self._output_dir / config.CURRENT_CSV
        self._history_path = self._output_dir / config.HISTORY_CSV

    # ── Public interface ───────────────────────────────────────────────────────

    def save_current_snapshot(self, df: pd.DataFrame) -> None:
        """
        Ghi đè file CSV snapshot hiện tại.
        Chuẩn hóa columns và kiểu dữ liệu trước khi ghi.
        """
        if df.empty:
            logger.warning("Empty DataFrame, skipping current snapshot save.")
            return

        clean_df = self._normalize_current(df)
        clean_df.to_csv(self._current_path, index=False, encoding="utf-8-sig")
        logger.info(
            "Saved current snapshot: %d rows → %s",
            len(clean_df), self._current_path,
        )

    def append_history(self, df: pd.DataFrame) -> None:
        """
        Append vào file CSV lịch sử.
        Tránh duplicate header bằng cách kiểm tra file đã tồn tại chưa.
        De-duplicate dựa trên (condition_id, price_timestamp).
        """
        if df.empty:
            logger.warning("Empty DataFrame, skipping history append.")
            return

        clean_df = self._normalize_history(df)

        file_exists = self._history_path.exists() and self._history_path.stat().st_size > 0
        if file_exists:
            clean_df = self._deduplicate_against_existing(clean_df)
            if clean_df.empty:
                logger.info("No new history rows to append (all duplicates).")
                return

        # Append mode: write_header=True chỉ khi file chưa tồn tại
        clean_df.to_csv(
            self._history_path,
            mode="a",
            header=not file_exists,
            index=False,
            encoding="utf-8-sig",
        )
        logger.info(
            "Appended %d new history rows → %s",
            len(clean_df), self._history_path,
        )

    def load_current(self) -> pd.DataFrame:
        """Load file CSV hiện tại (nếu tồn tại)."""
        if not self._current_path.exists():
            return pd.DataFrame(columns=self.CURRENT_COLUMNS)
        try:
            return pd.read_csv(self._current_path)
        except Exception as exc:
            logger.error("Failed to load current CSV: %s", exc)
            return pd.DataFrame(columns=self.CURRENT_COLUMNS)

    def load_history(self) -> pd.DataFrame:
        """Load toàn bộ history CSV."""
        if not self._history_path.exists():
            return pd.DataFrame(columns=self.HISTORY_COLUMNS)
        try:
            return pd.read_csv(self._history_path)
        except Exception as exc:
            logger.error("Failed to load history CSV: %s", exc)
            return pd.DataFrame(columns=self.HISTORY_COLUMNS)

    # ── Normalization helpers ──────────────────────────────────────────────────

    def _normalize_current(self, df: pd.DataFrame) -> pd.DataFrame:
        """Chuẩn hóa kiểu dữ liệu và chọn đúng columns."""
        # Chỉ giữ columns đã định nghĩa (bỏ qua extra columns)
        available = [c for c in self.CURRENT_COLUMNS if c in df.columns]
        missing = [c for c in self.CURRENT_COLUMNS if c not in df.columns]
        if missing:
            logger.debug("Missing columns in current data: %s", missing)

        out = df[available].copy()

        # Numeric conversions
        for col in ["liquidity", "volume", "best_bid_yes", "best_ask_yes",
                    "mid_price_yes", "implied_probability_pct", "spread"]:
            if col in out.columns:
                out[col] = pd.to_numeric(out[col], errors="coerce")

        # Round xác suất về 2 chữ số thập phân
        if "implied_probability_pct" in out.columns:
            out["implied_probability_pct"] = out["implied_probability_pct"].round(2)

        return out.sort_values("implied_probability_pct", ascending=False, na_position="last")

    def _normalize_history(self, df: pd.DataFrame) -> pd.DataFrame:
        """Chuẩn hóa history DataFrame."""
        available = [c for c in self.HISTORY_COLUMNS if c in df.columns]
        out = df[available].copy()

        if "price" in out.columns:
            out["price"] = pd.to_numeric(out["price"], errors="coerce")
        if "implied_probability_pct" in out.columns:
            out["implied_probability_pct"] = pd.to_numeric(
                out["implied_probability_pct"], errors="coerce"
            ).round(2)

        return out

    def _deduplicate_against_existing(self, new_df: pd.DataFrame) -> pd.DataFrame:
        """
        Loại bỏ rows đã tồn tại trong history CSV.
        Key de-dup: (condition_id, price_timestamp)
        """
        try:
            existing = pd.read_csv(
                self._history_path,
                usecols=["condition_id", "price_timestamp"],
                dtype=str,
            )
            existing_keys = set(
                zip(existing["condition_id"], existing["price_timestamp"].astype(str))
            )
            mask = ~new_df.apply(
                lambda r: (str(r.get("condition_id", "")), str(r.get("price_timestamp", "")))
                in existing_keys,
                axis=1,
            )
            return new_df[mask].copy()
        except Exception as exc:
            logger.warning("Dedup check failed (%s), appending all rows.", exc)
            return new_df

    # ── Utility ───────────────────────────────────────────────────────────────

    @property
    def current_path(self) -> Path:
        return self._current_path

    @property
    def history_path(self) -> Path:
        return self._history_path

    def get_stats(self) -> dict:
        """Trả về thống kê nhanh về files hiện tại."""
        stats = {}
        for name, path in [("current", self._current_path), ("history", self._history_path)]:
            if path.exists():
                size_kb = path.stat().st_size / 1024
                try:
                    df = pd.read_csv(path, nrows=0)
                    stats[name] = {"path": str(path), "size_kb": round(size_kb, 1)}
                except Exception:
                    stats[name] = {"path": str(path), "size_kb": round(size_kb, 1)}
            else:
                stats[name] = {"path": str(path), "exists": False}
        return stats
