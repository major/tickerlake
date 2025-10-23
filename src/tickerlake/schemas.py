"""Polars schema definitions and validation functions for TickerLake."""

from typing import Any

import polars as pl


# =============================================================================
# Bronze Layer Schemas (Raw Polygon API data)
# =============================================================================

STOCKS_RAW_SCHEMA: dict[str, Any] = {
    "ticker": pl.Categorical,
    "volume": pl.UInt64,
    "open": pl.Float32,
    "close": pl.Float32,
    "high": pl.Float32,
    "low": pl.Float32,
    "window_start": pl.Int64,
    "transactions": pl.UInt32,
}

STOCKS_RAW_SCHEMA_MODIFIED: dict[str, Any] = {
    "ticker": pl.Categorical,
    "volume": pl.UInt64,
    "open": pl.Float32,
    "close": pl.Float32,
    "high": pl.Float32,
    "low": pl.Float32,
    "date": pl.Date,
    "transactions": pl.UInt32,
}

SPLITS_RAW_SCHEMA: dict[str, Any] = {
    "id": pl.Utf8,
    "execution_date": pl.Date,
    "split_from": pl.Float32,
    "split_to": pl.Float32,
    "ticker": pl.Categorical,
}

TICKERS_RAW_SCHEMA: dict[str, Any] = {
    "active": pl.Boolean,
    "base_currency_name": pl.Utf8,
    "base_currency_symbol": pl.Utf8,
    "cik": pl.Utf8,
    "composite_figi": pl.Utf8,
    "currency_name": pl.Categorical,
    "currency_symbol": pl.Categorical,
    "delisted_utc": pl.Utf8,
    "last_updated_utc": pl.Utf8,
    "locale": pl.Categorical,
    "market": pl.Categorical,
    "name": pl.Utf8,
    "primary_exchange": pl.Categorical,
    "share_class_figi": pl.Utf8,
    "ticker": pl.Categorical,
    "type": pl.Categorical,
}


# =============================================================================
# Silver/Gold Layer Schemas (Processed data)
# =============================================================================
DAILY_AGGREGATE_SCHEMA: dict[str, Any] = {
    "ticker": pl.Categorical,
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
    "ticker_type": pl.Categorical,
}

DAILY_AGGREGATE_WITH_VOLUME_RATIO_SCHEMA: dict[str, Any] = {
    **DAILY_AGGREGATE_WITH_TICKER_TYPE_SCHEMA,
    "volume_avg": pl.UInt64,
    "volume_avg_ratio": pl.Float64,
}

SPLIT_SCHEMA: dict[str, Any] = {
    "ticker": pl.Categorical,
    "execution_date": pl.Date,
    "split_from": pl.Float32,
    "split_to": pl.Float32,
}

TICKER_DETAILS_SCHEMA: dict[str, Any] = {
    "ticker": pl.Categorical,
    "name": pl.String,
    "ticker_type": pl.Categorical,
}

TICKER_DETAILS_WITH_ETFS_SCHEMA: dict[str, Any] = {
    **TICKER_DETAILS_SCHEMA,
    "etfs": pl.List(pl.String),
}

ETF_HOLDING_SCHEMA: dict[str, Any] = {
    "ticker": pl.Categorical,
    "etf": pl.Categorical,
}

HIGH_VOLUME_CLOSE_SCHEMA: dict[str, Any] = {
    "date": pl.Date,
    "ticker": pl.Categorical,
    "ticker_type": pl.Categorical,
    "close": pl.Float64,
    "volume_avg_ratio": pl.Float64,
    "volume": pl.UInt64,
    "volume_avg": pl.UInt64,
}

INDICATORS_SCHEMA: dict[str, Any] = {
    "ticker": pl.Categorical,
    "date": pl.Date,
    "sma_20": pl.Float64,
    "sma_50": pl.Float64,
    "sma_200": pl.Float64,
    "atr_14": pl.Float64,
    "volume_ma_20": pl.UInt64,
    "volume_ratio": pl.Float64,
    "is_hvc": pl.Boolean,
    # Weinstein Stage Analysis columns (weekly data only)
    "ma_30": pl.Float64,
    "price_vs_ma_pct": pl.Float64,
    "ma_slope_pct": pl.Float64,
    "raw_stage": pl.UInt8,
    "stage": pl.UInt8,
    "stage_changed": pl.Boolean,
    "weeks_in_stage": pl.UInt32,
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


def validate_indicators(df: pl.DataFrame, include_stages: bool = False) -> pl.DataFrame:
    """Validate and cast technical indicators to expected schema.

    Args:
        df: DataFrame containing technical indicator data.
        include_stages: If True, validate Weinstein stage columns (weekly data only).

    Returns:
        DataFrame with validated schema.
    """
    if include_stages:
        return df.cast(INDICATORS_SCHEMA, strict=False)  # type: ignore[arg-type]
    else:
        # Daily/monthly indicators don't have stage analysis columns
        basic_schema = {
            "ticker": pl.Categorical,
            "date": pl.Date,
            "sma_20": pl.Float64,
            "sma_50": pl.Float64,
            "sma_200": pl.Float64,
            "atr_14": pl.Float64,
            "volume_ma_20": pl.UInt64,
            "volume_ratio": pl.Float64,
            "is_hvc": pl.Boolean,
        }
        return df.cast(basic_schema, strict=False)  # type: ignore[arg-type]
