"""Data transformation utilities for bronze layer. 🔄

This module provides utilities for transforming raw API responses into
clean Polars DataFrames ready for storage in Parquet files.
"""

import polars as pl


def convert_api_response_to_dicts(response) -> list[dict]:
    """Convert Polygon API response to list of dictionaries. 🔄

    Extracts OHLCV data and transaction counts from the Polygon API's
    grouped daily aggregates response format.

    Args:
        response: Polygon API response object (iterable of result objects).

    Returns:
        List of dictionaries with keys: ticker, volume, open, close, high,
        low, window_start, transactions.

    Example:
        >>> response = client.get_grouped_daily_aggs("2024-01-01")
        >>> data = convert_api_response_to_dicts(response)
        >>> print(len(data))  # Number of tickers with data for that date
    """
    return [
        {
            "ticker": r.ticker,  # type: ignore[attr-defined]
            "volume": r.volume,  # type: ignore[attr-defined]
            "open": r.open,  # type: ignore[attr-defined]
            "close": r.close,  # type: ignore[attr-defined]
            "high": r.high,  # type: ignore[attr-defined]
            "low": r.low,  # type: ignore[attr-defined]
            "window_start": r.timestamp,  # type: ignore[attr-defined]
            "transactions": r.transactions if r.transactions is not None else 0,  # type: ignore[attr-defined]
        }
        for r in response
    ]


def transform_stocks_dataframe(df: pl.DataFrame) -> pl.DataFrame:
    """Transform stocks DataFrame by converting timestamps and ticker types. ⚙️

    Applies bronze layer transformations:
    1. Convert window_start (Unix timestamp in ms) to date
    2. Drop window_start column (no longer needed)
    3. Convert ticker to categorical for memory efficiency

    Args:
        df: Raw DataFrame from API response with window_start column.

    Returns:
        Transformed DataFrame with date column and categorical ticker.

    Example:
        >>> raw_df = pl.DataFrame({
        ...     "ticker": ["AAPL", "MSFT"],
        ...     "window_start": [1704067200000, 1704067200000],  # Unix timestamp
        ...     "close": [150.0, 300.0]
        ... })
        >>> clean_df = transform_stocks_dataframe(raw_df)
        >>> print(clean_df.columns)
        ['ticker', 'close', 'date']
        >>> print(clean_df.dtypes)
        [Categorical, Float64, Date]
    """
    return (
        df.with_columns([
            # Convert timestamp (milliseconds since epoch) to date
            pl.col("window_start").cast(pl.Datetime("ms")).cast(pl.Date).alias("date"),
            # Convert ticker to categorical for memory efficiency (typical 70% reduction!)
            pl.col("ticker").cast(pl.Categorical),
        ])
        .drop("window_start")
    )


def is_api_limit_error(error: Exception) -> bool:
    """Check if error indicates API subscription limit reached. 🚫

    The Polygon API returns a 403 Forbidden error when the subscription
    plan's historical data limit is reached. This function detects that
    specific error condition.

    Args:
        error: Exception from API call.

    Returns:
        True if error is a 403/Forbidden (subscription limit), False otherwise.

    Example:
        >>> try:
        ...     response = client.get_grouped_daily_aggs("2010-01-01")
        ... except Exception as e:
        ...     if is_api_limit_error(e):
        ...         print("Reached subscription limit - oldest accessible date")
        ...     else:
        ...         raise  # Re-raise other errors
    """
    error_str = str(error)
    return "403" in error_str or "Forbidden" in error_str
