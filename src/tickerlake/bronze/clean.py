"""Clean bronze layer by deleting all Parquet files."""

from pathlib import Path

from tickerlake.logging_config import get_logger, setup_logging
from tickerlake.storage import get_table_path

setup_logging()
logger = get_logger(__name__)


def main() -> None:
    """Delete all bronze layer Parquet files."""
    logger.info("🧹 Cleaning bronze layer...")
    logger.warning("⚠️  This will DELETE all bronze layer files: stocks, tickers, splits")

    # Delete each bronze table
    tables = ["stocks", "tickers", "splits"]
    deleted_count = 0

    for table_name in tables:
        table_path = get_table_path("bronze", table_name)
        if Path(table_path).exists():
            logger.info(f"🗑️  Deleting {table_path}")
            Path(table_path).unlink()
            deleted_count += 1
        else:
            logger.info(f"⏭️  Skipping {table_name} (doesn't exist)")

    if deleted_count > 0:
        logger.info(f"✅ Bronze layer cleaned! {deleted_count} file(s) deleted.")
    else:
        logger.info("✅ Bronze layer was already clean (no files found)")

    logger.info("💡 Run 'uv run bronze' to reload data")


if __name__ == "__main__":
    main()
