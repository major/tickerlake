"""Parquet read/write operations for local filesystem storage."""

import logging
from pathlib import Path

import polars as pl

logger = logging.getLogger(__name__)


def write_table(
    table_path: str,
    df: pl.DataFrame,
    mode: str = "overwrite",
    partition_by: str | list[str] | None = None,
) -> None:
    """
    Write Polars DataFrame to Parquet file or partitioned dataset.

    Args:
        table_path: Local filesystem path (file for single, directory for partitioned)
        df: Polars DataFrame to write
        mode: Write mode (only 'overwrite' supported for Parquet)
        partition_by: Column(s) to partition by (creates Hive-style partitions)

    Example:
        >>> # Single file
        >>> df = pl.DataFrame({"ticker": ["AAPL"], "date": ["2025-10-29"]})
        >>> write_table("data/bronze/tickers.parquet", df)
        >>>
        >>> # Partitioned by date (creates date=YYYY-MM-DD/ subdirectories)
        >>> write_table("data/bronze/stocks", df, partition_by="date")
    """
    try:
        if mode != "overwrite":
            raise ValueError(
                f"Parquet writer only supports overwrite mode, received {mode!r}"
            )

        # Ensure parent directory exists
        Path(table_path).parent.mkdir(parents=True, exist_ok=True)

        if partition_by:
            # For partitioned datasets, table_path should be a directory
            Path(table_path).mkdir(parents=True, exist_ok=True)
            # Polars creates Hive-style partitions (column=value/)
            df.write_parquet(table_path, compression="zstd", partition_by=partition_by)
            logger.debug(f"✅ Wrote {len(df)} rows to partitioned dataset {table_path}")
        else:
            # Single Parquet file
            df.write_parquet(table_path, compression="zstd")
            logger.debug(f"✅ Wrote {len(df)} rows to {table_path}")
    except Exception as e:
        logger.error(f"❌ Failed to write to {table_path}: {e}")
        raise


def read_table(table_path: str) -> pl.DataFrame:
    """
    Read Parquet file or partitioned dataset into Polars DataFrame.

    Automatically handles both single files and Hive-style partitioned datasets.

    Args:
        table_path: Local filesystem path (file or directory for partitioned)

    Returns:
        Polars DataFrame with all table data

    Example:
        >>> # Single file
        >>> df = read_table("data/bronze/tickers.parquet")
        >>> len(df)
        10000
        >>>
        >>> # Partitioned dataset (reads all partitions)
        >>> df = read_table("data/bronze/stocks")
        >>> len(df)
        1000000
    """
    try:
        path = Path(table_path)

        if path.is_dir():
            # Partitioned dataset - read all parquet files recursively
            df = pl.read_parquet(f"{table_path}/**/*.parquet", hive_partitioning=True)
            logger.debug(f"📖 Read {len(df)} rows from partitioned dataset {table_path}")
        else:
            # Single file
            df = pl.read_parquet(table_path)
            logger.debug(f"📖 Read {len(df)} rows from {table_path}")

        return df
    except Exception as e:
        logger.error(f"❌ Failed to read from {table_path}: {e}")
        raise


def table_exists(table_path: str) -> bool:
    """
    Check if Parquet file or partitioned dataset exists.

    Args:
        table_path: Local filesystem path (file or directory)

    Returns:
        True if file or directory exists, False otherwise

    Example:
        >>> # Single file
        >>> table_exists("data/bronze/tickers.parquet")
        True
        >>> # Partitioned dataset
        >>> table_exists("data/bronze/stocks")
        True
    """
    path = Path(table_path)
    if path.is_dir():
        # For directories, check if any parquet files exist
        return any(path.glob("**/*.parquet"))
    return path.exists()


def init_table(table_path: str, schema: dict) -> None:
    """
    Initialize empty Parquet file if it doesn't exist.

    Args:
        table_path: Local filesystem path to Parquet file
        schema: Polars schema dictionary (column_name → dtype)

    Example:
        >>> init_table(
        ...     "data/bronze/stocks.parquet",
        ...     {"ticker": pl.String, "date": pl.Date, "close": pl.Float64}
        ... )
    """
    if not table_exists(table_path):
        try:
            empty_df = pl.DataFrame(schema=schema)
            write_table(table_path, empty_df)
            logger.info(f"✨ Created new table: {table_path}")
        except Exception as e:
            logger.error(f"❌ Failed to initialize {table_path}: {e}")
            raise
    else:
        logger.debug(f"ℹ️  Table already exists: {table_path}")


def get_max_date(table_path: str) -> str | None:
    """
    Get maximum date from Parquet file using pure Polars.

    Args:
        table_path: Local filesystem path to Parquet file

    Returns:
        Maximum date as ISO string, or None if table is empty or doesn't exist

    Example:
        >>> get_max_date("data/bronze/stocks.parquet")
        '2025-10-29'
    """
    try:
        if not table_exists(table_path):
            logger.debug(f"📅 Table doesn't exist: {table_path}")
            return None

        df = read_table(table_path)

        if len(df) == 0 or "date" not in df.columns:
            logger.debug(f"📅 No data in {table_path}")
            return None

        max_date = df["date"].max()
        if max_date is not None:
            return str(max_date)

        logger.debug(f"📅 No date values in {table_path}")
        return None

    except Exception as e:
        logger.warning(f"⚠️  Could not get max date from {table_path}: {e}")
        return None
