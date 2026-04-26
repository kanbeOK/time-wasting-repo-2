"""
main.py — Entry point của Polymarket Data Pipeline.

Usage:
    python -m polymarket_pipeline                   # full run (current + history)
    python -m polymarket_pipeline --mode current    # chỉ snapshot hiện tại
    python -m polymarket_pipeline --mode history    # chỉ history
    python -m polymarket_pipeline --max-pages 5     # giới hạn số trang (dev/test)
    python -m polymarket_pipeline --output-dir /tmp/data
"""

import argparse
import asyncio
import logging
import sys
from pathlib import Path

from polymarket_pipeline.config import CONFIG, PipelineConfig
from polymarket_pipeline.pipeline import PolymarketPipeline
from polymarket_pipeline.storage.csv_handler import CSVHandler


def setup_logging(level: str = "INFO") -> None:
    """Cấu hình logging với format chuẩn, output ra stdout."""
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[
            logging.StreamHandler(sys.stdout),
        ],
    )
    # Giảm noise từ thư viện bên thứ 3
    logging.getLogger("aiohttp").setLevel(logging.WARNING)
    logging.getLogger("asyncio").setLevel(logging.WARNING)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Polymarket Data Pipeline — Fetch markets, orderbooks & history.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python -m polymarket_pipeline
  python -m polymarket_pipeline --mode current --output-dir ./output
  python -m polymarket_pipeline --max-pages 3 --log-level DEBUG
        """,
    )
    parser.add_argument(
        "--mode",
        choices=["full", "current", "history"],
        default="full",
        help="Pipeline mode: 'full' = current + history, 'current' = snapshot only (default: full)",
    )
    parser.add_argument(
        "--output-dir",
        default=None,
        help="Thư mục output CSV (default: ./data)",
    )
    parser.add_argument(
        "--max-pages",
        type=int,
        default=None,
        help="Giới hạn số pages từ Gamma API (dùng khi test, default: không giới hạn)",
    )
    parser.add_argument(
        "--min-liquidity",
        type=float,
        default=0.0,
        help="Chỉ lấy markets có liquidity >= ngưỡng này (default: 0)",
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=None,
        help="Số requests đồng thời tối đa (default: 20)",
    )
    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default="INFO",
        help="Logging level (default: INFO)",
    )
    return parser.parse_args()


async def run_pipeline(config: PipelineConfig, mode: str) -> dict:
    """Khởi chạy pipeline với config đã cho."""
    storage = CSVHandler(config.storage)
    async with PolymarketPipeline(config, storage) as pipeline:
        if mode == "current":
            return await pipeline.run_current_only()
        elif mode == "history":
            # Nếu chỉ muốn chạy history, vẫn cần collect markets trước
            markets = await pipeline._collect_markets()
            history_df = await pipeline._build_history(markets)
            pipeline.storage.append_history(history_df)
            return {"history_rows": len(history_df)}
        else:
            return await pipeline.run_full()


def main() -> None:
    args = parse_args()
    setup_logging(args.log_level)
    logger = logging.getLogger(__name__)

    # Override config từ CLI arguments
    config = CONFIG

    if args.output_dir:
        config.storage.OUTPUT_DIR = args.output_dir

    if args.max_pages is not None:
        config.api.MAX_PAGES = args.max_pages
        logger.info("Running in limited mode: max %d pages.", args.max_pages)

    if args.min_liquidity > 0:
        config.MIN_LIQUIDITY = args.min_liquidity

    if args.concurrency:
        config.api.MAX_CONCURRENT_REQUESTS = args.concurrency

    logger.info("Starting Polymarket Pipeline | mode=%s | output=%s",
                args.mode, config.storage.OUTPUT_DIR)

    # Chạy event loop
    try:
        stats = asyncio.run(run_pipeline(config, args.mode))
        logger.info("Pipeline completed successfully: %s", stats)
        sys.exit(0)
    except KeyboardInterrupt:
        logger.info("Pipeline interrupted by user.")
        sys.exit(0)
    except Exception as exc:
        logger.exception("Pipeline failed with unhandled exception: %s", exc)
        sys.exit(1)


if __name__ == "__main__":
    main()
