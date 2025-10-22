"""Gold layer main entry point for analytics and DuckDB views."""

from pathlib import Path

import duckdb

from tickerlake.config import settings
from tickerlake.gold.views import create_all_views, list_views
from tickerlake.logging_config import get_logger, setup_logging

setup_logging()
logger = get_logger(__name__)


def get_duckdb_path() -> str:
    """Get path to DuckDB database file.

    Returns:
        Path to gold.duckdb file.
    """
    db_path = Path(settings.silver_storage_path).parent / "gold" / "gold.duckdb"
    db_path.parent.mkdir(parents=True, exist_ok=True)
    return str(db_path)


def initialize_gold_layer(in_memory: bool = False) -> duckdb.DuckDBPyConnection:
    """Initialize gold layer and create DuckDB views.

    Args:
        in_memory: If True, create an in-memory database. Default False creates
                  a persistent database at data/gold/gold.duckdb.

    Returns:
        DuckDB connection with all views created.
    """
    logger.info("Initializing gold layer... 💎")

    # Create DuckDB connection
    if in_memory:
        logger.info("Creating in-memory DuckDB database")
        con = duckdb.connect(":memory:")
    else:
        db_path = get_duckdb_path()
        logger.info(f"Creating persistent DuckDB database at {db_path}")
        con = duckdb.connect(db_path)

    # Create all views
    create_all_views(con)

    # List all views
    logger.info("\n📊 Available views:")
    views_df = list_views(con)
    for view in views_df.iter_rows(named=True):
        logger.info(f"  - {view['table_name']}")

    logger.info("\n✅ Gold layer initialized successfully! 🎉")
    logger.info("\nYou can now query these views using:")
    logger.info("  import duckdb")
    logger.info(f"  con = duckdb.connect('{get_duckdb_path()}')")
    logger.info("  df = con.execute('SELECT * FROM daily_enriched LIMIT 10').pl()")

    return con


def main() -> None:
    """CLI entry point for gold layer."""
    connection = initialize_gold_layer()
    connection.close()


if __name__ == "__main__":  # pragma: no cover
    main()
