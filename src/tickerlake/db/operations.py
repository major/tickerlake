"""Generic database operations for bulk loading and querying. 💾"""

from typing import Any

import polars as pl
from sqlalchemy import Table

from tickerlake.db.engine import get_engine
from tickerlake.logging_config import get_logger

logger = get_logger(__name__)


def bulk_load(table: Table, df: pl.DataFrame) -> None:
    """Fast bulk load using SQLAlchemy bulk insert. ⚡

    This generic function replaces all the layer-specific bulk load functions
    by using SQLAlchemy's bulk insert operations.

    Args:
        table: SQLAlchemy Table object (contains schema info).
        df: Polars DataFrame with data to load (must match table columns).

    Example:
        >>> from tickerlake.bronze.models import stocks
        >>> bulk_load(stocks, stocks_df)
    """
    if len(df) == 0:
        logger.warning(f"⚠️  No data to load for {table.name}")
        return

    # Convert Polars DataFrame to list of dictionaries for SQLAlchemy
    # Using Polars' native method for best performance 🚀
    records = df.to_dicts()

    engine = get_engine()

    # Use SQLAlchemy's bulk insert with executemany (10k chunks for optimal performance)
    with engine.begin() as conn:
        # Process in chunks to avoid memory issues with large datasets
        chunk_size = 10000
        for i in range(0, len(records), chunk_size):
            chunk = records[i : i + chunk_size]
            conn.execute(table.insert(), chunk)


def execute_query(query: Any, scalar: bool = False) -> Any:
    """Execute a SQL query and return results.

    Args:
        query: SQLAlchemy query object (e.g., select(...)).
        scalar: If True, return single scalar value instead of ResultProxy.

    Returns:
        Query results (ResultProxy or scalar value).

    Example:
        >>> from sqlalchemy import select, func
        >>> from tickerlake.bronze.models import stocks
        >>> count = execute_query(select(func.count()).select_from(stocks), scalar=True)
    """
    engine = get_engine()

    with engine.connect() as conn:
        result = conn.execute(query)
        if scalar:
            return result.scalar()
        return result.fetchall()


def clear_table(table: Table) -> None:
    """Delete all rows from a table (fast TRUNCATE).

    Args:
        table: SQLAlchemy Table object to clear.

    Example:
        >>> from tickerlake.bronze.models import stocks
        >>> clear_table(stocks)
    """
    engine = get_engine()

    logger.warning(f"⚠️  Clearing table {table.name}...")

    with engine.begin() as conn:
        # Use TRUNCATE for better performance than DELETE
        conn.execute(table.delete())

    logger.info(f"✅ Cleared table {table.name}")
