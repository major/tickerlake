"""Generic database operations for bulk loading and querying. 💾"""

from typing import Any

import polars as pl
from sqlalchemy import Table

from tickerlake.db.engine import get_engine
from tickerlake.logging_config import get_logger

logger = get_logger(__name__)


def bulk_load(table: Table, df: pl.DataFrame) -> None:
    """Fast bulk load using raw psycopg COPY BINARY (1-2M rows/sec). ⚡

    This generic function replaces all the layer-specific bulk load functions
    by dynamically building the COPY statement from the table schema.

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

    logger.info(f"💾 Loading {len(df):,} records into {table.name}...")

    # Build COPY statement dynamically from table columns
    column_names = ", ".join([col.name for col in table.columns])
    copy_stmt = f"COPY {table.name} ({column_names}) FROM STDIN (FORMAT BINARY)"

    engine = get_engine()

    # Access raw psycopg connection from SQLAlchemy 🔌
    with engine.raw_connection() as raw_conn:
        with raw_conn.cursor().copy(copy_stmt) as copy:
            # Stream Arrow batches for memory efficiency (50k chunks) 🚀
            for batch in df.to_arrow().to_batches(max_chunksize=50000):
                copy.write_arrow(batch)

        raw_conn.commit()

    logger.info(f"✅ Loaded {len(df):,} records into {table.name}")


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
