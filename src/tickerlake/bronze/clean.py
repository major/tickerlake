"""Clean bronze layer by dropping all Postgres tables."""

from tickerlake.bronze import postgres
from tickerlake.bronze.models import metadata
from tickerlake.logging_config import get_logger, setup_logging

setup_logging()
logger = get_logger(__name__)


def main() -> None:
    """Drop all bronze layer tables from Postgres database."""
    logger.info("🧹 Cleaning bronze layer...")
    logger.warning("⚠️  This will DROP all bronze layer tables: stocks, tickers, splits")

    engine = postgres.get_engine()

    # Drop all tables defined in metadata
    logger.info("🗑️  Dropping tables...")
    metadata.drop_all(engine)

    logger.info("✅ Bronze layer cleaned! All tables dropped.")
    logger.info("💡 Run 'uv run bronze' to recreate schema and reload data")


if __name__ == "__main__":
    main()
