"""Utility functions for Delta Lake table operations."""

from datetime import date
from typing import Literal

import polars as pl
from deltalake import DeltaTable
from deltalake.exceptions import TableNotFoundError

from tickerlake.config import s3_storage_options, settings
from tickerlake.logging_config import get_logger

logger = get_logger(__name__)


def get_delta_table_path(table_name: str) -> str:
    """Construct S3 path for a Delta table.

    Args:
        table_name: Name of the Delta table (e.g., 'daily', 'tickers').

    Returns:
        str: Full S3 path to the Delta table.
    """
    return f"s3://{settings.s3_bucket_name}/silver/{table_name}"


def delta_table_exists(table_name: str) -> bool:
    """Check if a Delta table exists.

    Args:
        table_name: Name of the Delta table.

    Returns:
        bool: True if table exists, False otherwise.
    """
    try:
        path = get_delta_table_path(table_name)
        DeltaTable(path, storage_options=s3_storage_options)
        return True
    except TableNotFoundError:
        return False


def read_delta_table(table_name: str, version: int | None = None) -> pl.DataFrame:
    """Read a Delta table into a Polars DataFrame.

    Args:
        table_name: Name of the Delta table.
        version: Optional version number for time travel queries.

    Returns:
        pl.DataFrame: Data from the Delta table.
    """
    path = get_delta_table_path(table_name)
    version_str = f", version={version}" if version is not None else ""
    logger.info(f"Reading Delta table: {table_name} (path={path}{version_str})")

    if version is not None:
        return pl.read_delta(path, storage_options=s3_storage_options, version=version)
    return pl.read_delta(path, storage_options=s3_storage_options)


def scan_delta_table(table_name: str) -> pl.LazyFrame:
    """Scan a Delta table lazily for efficient query planning.

    Args:
        table_name: Name of the Delta table.

    Returns:
        pl.LazyFrame: Lazy frame for the Delta table.
    """
    path = get_delta_table_path(table_name)
    logger.info(f"Scanning Delta table: {table_name} (path={path})")
    return pl.scan_delta(path, storage_options=s3_storage_options)


def write_delta_table(
    df: pl.DataFrame,
    table_name: str,
    mode: Literal["error", "append", "overwrite", "ignore"] = "overwrite",
    partition_by: list[str] | None = None,
) -> None:
    """Write a Polars DataFrame to a Delta table.

    Args:
        df: DataFrame to write.
        table_name: Name of the Delta table.
        mode: Write mode - 'overwrite', 'append', or 'merge'.
        partition_by: Optional list of columns to partition by.
    """
    path = get_delta_table_path(table_name)
    partition_str = f", partition_by={partition_by}" if partition_by else ""
    logger.info(
        f"Writing Delta table: {table_name} (path={path}, mode={mode}, rows={df.shape[0]:,}{partition_str})"
    )

    # Convert categorical columns to strings to avoid Delta Lake compatibility issues
    # Polars categoricals use Utf8View which causes errors in delta-rs
    df_to_write = df.clone()
    for col in df_to_write.columns:
        if df_to_write[col].dtype == pl.Categorical:
            df_to_write = df_to_write.with_columns(pl.col(col).cast(pl.String))

    df_to_write.write_delta(
        path,
        mode=mode,
        storage_options=s3_storage_options,
        delta_write_options={
            "partition_by": partition_by if partition_by else None,
        },
    )


def append_to_delta_table(df: pl.DataFrame, table_name: str) -> None:
    """Append data to an existing Delta table.

    Args:
        df: DataFrame to append.
        table_name: Name of the Delta table.
    """
    write_delta_table(df, table_name, mode="append")


def merge_to_delta_table(
    df: pl.DataFrame,
    table_name: str,
    merge_keys: list[str],
) -> None:
    """Merge data into a Delta table (upsert operation).

    Args:
        df: DataFrame with new/updated data.
        table_name: Name of the Delta table.
        merge_keys: Columns to use for matching records.
    """
    path = get_delta_table_path(table_name)
    logger.info(
        f"Merging into Delta table: {table_name} (path={path}, rows={df.shape[0]:,}, merge_keys={merge_keys})"
    )

    # Convert categorical columns to strings to avoid Delta Lake compatibility issues
    df_to_merge = df.clone()
    for col in df_to_merge.columns:
        if df_to_merge[col].dtype == pl.Categorical:
            df_to_merge = df_to_merge.with_columns(pl.col(col).cast(pl.String))

    # Convert to PyArrow for DeltaTable operations
    table = df_to_merge.to_arrow()

    # Build merge predicate
    predicate = " AND ".join([f"target.{key} = source.{key}" for key in merge_keys])

    # Load existing Delta table
    dt = DeltaTable(path, storage_options=s3_storage_options)

    # Perform merge (upsert)
    (
        dt.merge(
            source=table,
            predicate=predicate,
            source_alias="source",
            target_alias="target",
        )
        .when_matched_update_all()
        .when_not_matched_insert_all()
        .execute()
    )


def get_max_date_from_delta(table_name: str, date_column: str = "date") -> date | None:
    """Get the maximum date from a Delta table.

    Args:
        table_name: Name of the Delta table.
        date_column: Name of the date column.

    Returns:
        date | None: Maximum date in the table, or None if table is empty or doesn't exist.
    """
    if not delta_table_exists(table_name):
        logger.info(f"Delta table {table_name} does not exist yet")
        return None

    try:
        max_date_df = (
            scan_delta_table(table_name).select(pl.col(date_column).max()).collect()
        )

        if max_date_df.height == 0 or max_date_df[date_column][0] is None:
            logger.info(f"Delta table {table_name} is empty")
            return None

        max_date_value = max_date_df[date_column][0]
        logger.info(f"Max date in {table_name}: {max_date_value}")
        return max_date_value
    except Exception as e:
        logger.warning(f"Error getting max date from {table_name}: {e}")
        return None


def optimize_delta_table(table_name: str) -> None:
    """Optimize a Delta table by compacting small files.

    Args:
        table_name: Name of the Delta table.
    """
    path = get_delta_table_path(table_name)
    logger.info(f"Optimizing Delta table: {table_name} (path={path})")

    dt = DeltaTable(path, storage_options=s3_storage_options)
    dt.optimize.compact()
    logger.info(f"Optimization complete for {table_name}")


def vacuum_delta_table(table_name: str, retention_hours: int = 168) -> None:
    """Remove old files from Delta table history.

    Args:
        table_name: Name of the Delta table.
        retention_hours: Number of hours to retain old files (default: 7 days).
    """
    path = get_delta_table_path(table_name)
    logger.info(
        f"Vacuuming Delta table: {table_name} (path={path}, retention_hours={retention_hours})"
    )

    dt = DeltaTable(path, storage_options=s3_storage_options)
    dt.vacuum(retention_hours=retention_hours)
    logger.info(f"Vacuum complete for {table_name}")
