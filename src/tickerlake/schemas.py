"""Polars schema definitions and validation functions for TickerLake."""

from typing import Any

import polars as pl


# Schema definitions for common data structures
DAILY_AGGREGATE_SCHEMA: dict[str, Any] = {
    "ticker": pl.String,
    "date": pl.Date,
    "open": pl.Float64,
    "high": pl.Float64,
    "low": pl.Float64,
    "close": pl.Float64,
    "volume": pl.UInt64,
    "transactions": pl.UInt64,
}

DAILY_AGGREGATE_WITH_TICKER_TYPE_SCHEMA: dict[str, Any] = {
    **DAILY_AGGREGATE_SCHEMA,
    "ticker_type": pl.String,
}

DAILY_AGGREGATE_WITH_VOLUME_RATIO_SCHEMA: dict[str, Any] = {
    **DAILY_AGGREGATE_WITH_TICKER_TYPE_SCHEMA,
    "volume_avg": pl.UInt64,
    "volume_avg_ratio": pl.Float64,
}

SPLIT_SCHEMA: dict[str, Any] = {
    "ticker": pl.String,
    "execution_date": pl.Date,
    "split_from": pl.Float32,
    "split_to": pl.Float32,
}

TICKER_DETAILS_SCHEMA: dict[str, Any] = {
    "ticker": pl.String,
    "name": pl.String,
    "ticker_type": pl.String,
}

TICKER_DETAILS_WITH_ETFS_SCHEMA: dict[str, Any] = {
    **TICKER_DETAILS_SCHEMA,
    "etfs": pl.List(pl.String),
}

ETF_HOLDING_SCHEMA: dict[str, Any] = {
    "ticker": pl.String,
    "etf": pl.String,
}

HIGH_VOLUME_CLOSE_SCHEMA: dict[str, Any] = {
    "date": pl.Date,
    "ticker": pl.String,
    "ticker_type": pl.String,
    "close": pl.Float64,
    "volume_avg_ratio": pl.Float64,
    "volume": pl.UInt64,
    "volume_avg": pl.UInt64,
}


# Validation functions
def validate_daily_aggregates(df: pl.DataFrame, include_volume_ratio: bool = False) -> pl.DataFrame:
    """Validate and cast daily aggregates to expected schema.

    Args:
        df: DataFrame containing daily aggregate data.
        include_volume_ratio: If True, validate volume_avg and volume_avg_ratio columns.

    Returns:
        DataFrame with validated schema.
    """
    if include_volume_ratio:
        return df.cast(DAILY_AGGREGATE_WITH_VOLUME_RATIO_SCHEMA, strict=False)  # type: ignore[arg-type]
    elif "ticker_type" in df.columns:
        return df.cast(DAILY_AGGREGATE_WITH_TICKER_TYPE_SCHEMA, strict=False)  # type: ignore[arg-type]
    else:
        return df.cast(DAILY_AGGREGATE_SCHEMA, strict=False)  # type: ignore[arg-type]


def validate_splits(df: pl.DataFrame) -> pl.DataFrame:
    """Validate and cast split details to expected schema.

    Args:
        df: DataFrame containing split details.

    Returns:
        DataFrame with validated schema.
    """
    return df.cast(SPLIT_SCHEMA, strict=False)  # type: ignore[arg-type]


def validate_ticker_details(df: pl.DataFrame, include_etfs: bool = False) -> pl.DataFrame:
    """Validate and cast ticker details to expected schema.

    Args:
        df: DataFrame containing ticker details.
        include_etfs: If True, validate etfs column.

    Returns:
        DataFrame with validated schema.
    """
    if include_etfs:
        return df.cast(TICKER_DETAILS_WITH_ETFS_SCHEMA, strict=False)  # type: ignore[arg-type]
    else:
        return df.cast(TICKER_DETAILS_SCHEMA, strict=False)  # type: ignore[arg-type]


def validate_etf_holdings(df: pl.DataFrame) -> pl.DataFrame:
    """Validate and cast ETF holdings to expected schema.

    Args:
        df: DataFrame containing ETF holdings.

    Returns:
        DataFrame with validated schema.
    """
    return df.cast(ETF_HOLDING_SCHEMA, strict=False)  # type: ignore[arg-type]


def validate_high_volume_closes(df: pl.DataFrame) -> pl.DataFrame:
    """Validate and cast high volume closes to expected schema.

    Args:
        df: DataFrame containing high volume close records.

    Returns:
        DataFrame with validated schema.
    """
    return df.cast(HIGH_VOLUME_CLOSE_SCHEMA, strict=False)  # type: ignore[arg-type]
