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
# Silver Layer Schemas (Processed data)
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

INDICATORS_SCHEMA: dict[str, Any] = {
    "ticker": pl.Categorical,
    "date": pl.Date,
    "sma_20": pl.Float64,
    "sma_50": pl.Float64,
    "sma_200": pl.Float64,
    "atr_14": pl.Float64,
    "volume_ma_20": pl.UInt64,
    "volume_ratio": pl.Float64,
}


# =============================================================================
# Gold Layer Schemas (VWAP Analytics)
# =============================================================================

VWAP_SIGNALS_SCHEMA: dict[str, Any] = {
    "ticker": pl.String,
    "date": pl.Date,
    "close": pl.Float64,
    "ytd_vwap": pl.Float64,
    "qtd_vwap": pl.Float64,
    "above_ytd_vwap": pl.Boolean,
    "above_qtd_vwap": pl.Boolean,
    "above_both": pl.Boolean,
    "calculated_at": pl.Datetime,
}


# =============================================================================
# Validation Functions
# =============================================================================
def validate_daily_aggregates(df: pl.DataFrame) -> pl.DataFrame:
    """Validate and cast daily aggregates to expected schema.

    Args:
        df: DataFrame containing daily aggregate data.

    Returns:
        DataFrame with validated schema.
    """
    return df.cast(DAILY_AGGREGATE_SCHEMA, strict=False)  # type: ignore[arg-type]


def validate_indicators(df: pl.DataFrame) -> pl.DataFrame:
    """Validate and cast technical indicators to expected schema.

    Args:
        df: DataFrame containing technical indicator data.

    Returns:
        DataFrame with validated schema.
    """
    return df.cast(INDICATORS_SCHEMA, strict=False)  # type: ignore[arg-type]
