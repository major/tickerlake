"""Timestamp utilities for adding metadata to DataFrames. ⏰

This module provides utilities for working with UTC timestamps, commonly
used to track when data was calculated or processed across all layers.
"""

from datetime import datetime, timezone

import polars as pl


def get_utc_timestamp() -> datetime:
    """Get current UTC timestamp. 🕐

    Returns:
        Current datetime in UTC timezone.

    Example:
        >>> ts = get_utc_timestamp()
        >>> print(ts.tzinfo)
        UTC
    """
    return datetime.now(timezone.utc)


def add_timestamp(
    df: pl.DataFrame, column: str = "calculated_at"
) -> pl.DataFrame:
    """Add UTC timestamp column to DataFrame. 📅

    Commonly used in gold layer analytics to track when calculations
    were performed. The timestamp is added as a literal value, so all
    rows get the same timestamp.

    Args:
        df: Input DataFrame.
        column: Name of timestamp column to add (default: 'calculated_at').

    Returns:
        DataFrame with timestamp column added.

    Example:
        >>> df = pl.DataFrame({"ticker": ["AAPL", "MSFT"], "value": [100, 200]})
        >>> df_with_ts = add_timestamp(df)
        >>> print(df_with_ts.columns)
        ['ticker', 'value', 'calculated_at']
    """
    return df.with_columns(pl.lit(get_utc_timestamp()).alias(column))
