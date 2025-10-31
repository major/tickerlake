"""Path management for Parquet files on local filesystem."""

from tickerlake.config import settings


def get_table_path(layer: str, table: str, partitioned: bool = False) -> str:
    """
    Returns local filesystem path to Parquet file or partitioned dataset directory.

    Args:
        layer: Data layer (bronze, silver, gold)
        table: Table name (without .parquet extension)
        partitioned: If True, returns directory path for partitioned dataset

    Returns:
        Full local path to Parquet file or directory

    Example:
        >>> get_table_path("bronze", "tickers")
        "data/bronze/tickers.parquet"
        >>> get_table_path("bronze", "stocks", partitioned=True)
        "data/bronze/stocks"
    """
    if partitioned:
        return f"{settings.base_path}/{layer}/{table}"
    return f"{settings.base_path}/{layer}/{table}.parquet"


def get_checkpoint_path() -> str:
    """
    Returns local filesystem path for checkpoint file.

    Returns:
        Full local path to checkpoints.json

    Example:
        >>> get_checkpoint_path()
        "data/checkpoints.json"
    """
    return settings.checkpoint_path
