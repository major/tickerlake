"""Common DataFrame operations used across all layers. 🐻‍❄️

This module provides reusable Polars DataFrame operations that follow
consistent patterns across bronze, silver, and gold layers.
"""

import polars as pl


def filter_by_ticker(df: pl.DataFrame, ticker: str) -> pl.DataFrame:
    """Filter DataFrame to single ticker. 🎯

    Args:
        df: Input DataFrame with 'ticker' column.
        ticker: Stock ticker symbol to filter.

    Returns:
        Filtered DataFrame containing only rows for the specified ticker.

    Example:
        >>> stocks = pl.DataFrame({
        ...     "ticker": ["AAPL", "MSFT", "AAPL"],
        ...     "close": [150.0, 300.0, 155.0]
        ... })
        >>> aapl = filter_by_ticker(stocks, "AAPL")
        >>> print(len(aapl))
        2
    """
    return df.filter(pl.col("ticker") == ticker)


def filter_by_tickers(df: pl.DataFrame, tickers: list[str]) -> pl.DataFrame:
    """Filter DataFrame to multiple tickers. 🎯

    Args:
        df: Input DataFrame with 'ticker' column.
        tickers: List of stock ticker symbols to filter.

    Returns:
        Filtered DataFrame containing only rows for the specified tickers.

    Example:
        >>> stocks = pl.DataFrame({
        ...     "ticker": ["AAPL", "MSFT", "GOOGL"],
        ...     "close": [150.0, 300.0, 2800.0]
        ... })
        >>> filtered = filter_by_tickers(stocks, ["AAPL", "MSFT"])
        >>> print(len(filtered))
        2
    """
    return df.filter(pl.col("ticker").is_in(tickers))


def sort_by_ticker_and_date(df: pl.DataFrame) -> pl.DataFrame:
    """Standard sort for time series data. 📅

    Ensures consistent ordering across all layers: first by ticker
    (alphabetically), then by date (chronologically).

    Args:
        df: Input DataFrame with 'ticker' and 'date' columns.

    Returns:
        Sorted DataFrame.

    Example:
        >>> stocks = pl.DataFrame({
        ...     "ticker": ["MSFT", "AAPL", "AAPL"],
        ...     "date": ["2024-01-02", "2024-01-01", "2024-01-02"],
        ...     "close": [300.0, 150.0, 155.0]
        ... })
        >>> sorted_stocks = sort_by_ticker_and_date(stocks)
        >>> print(sorted_stocks["ticker"].to_list())
        ['AAPL', 'AAPL', 'MSFT']
    """
    return df.sort("ticker", "date")


def sort_by_date(df: pl.DataFrame, descending: bool = False) -> pl.DataFrame:
    """Sort DataFrame by date column. 📅

    Args:
        df: Input DataFrame with 'date' column.
        descending: If True, sort newest to oldest. If False, oldest to newest.

    Returns:
        Sorted DataFrame.

    Example:
        >>> stocks = pl.DataFrame({
        ...     "date": ["2024-01-03", "2024-01-01", "2024-01-02"],
        ...     "close": [155.0, 150.0, 152.0]
        ... })
        >>> sorted_stocks = sort_by_date(stocks)
        >>> print(sorted_stocks["date"].to_list())
        ['2024-01-01', '2024-01-02', '2024-01-03']
    """
    return df.sort("date", descending=descending)


def convert_to_categorical_ticker(df: pl.DataFrame) -> pl.DataFrame:
    """Convert ticker column to categorical for memory efficiency. 💾

    Categorical dtype reduces memory usage significantly when the same
    tickers appear many times (which is common in time series data).

    Args:
        df: Input DataFrame with 'ticker' column.

    Returns:
        DataFrame with ticker as categorical dtype.

    Example:
        >>> stocks = pl.DataFrame({"ticker": ["AAPL"] * 1000, "close": range(1000)})
        >>> # Before: ~8KB for ticker column
        >>> stocks = convert_to_categorical_ticker(stocks)
        >>> # After: ~2KB for ticker column (75% reduction!)
    """
    return df.with_columns(pl.col("ticker").cast(pl.Categorical))


def get_unique_tickers(df: pl.DataFrame) -> list[str]:
    """Get sorted list of unique tickers from DataFrame. 📋

    Args:
        df: Input DataFrame with 'ticker' column.

    Returns:
        Sorted list of unique ticker symbols.

    Example:
        >>> stocks = pl.DataFrame({
        ...     "ticker": ["MSFT", "AAPL", "MSFT", "GOOGL"],
        ...     "close": [300.0, 150.0, 305.0, 2800.0]
        ... })
        >>> tickers = get_unique_tickers(stocks)
        >>> print(tickers)
        ['AAPL', 'GOOGL', 'MSFT']
    """
    return sorted(df["ticker"].unique().to_list())


def ensure_sorted_by_date(df: pl.DataFrame) -> pl.DataFrame:
    """Ensure DataFrame is sorted by date (idempotent operation). 📅

    This is a safe way to ensure data is sorted before operations that
    require chronological ordering (like calculating indicators).

    Args:
        df: Input DataFrame with 'date' column.

    Returns:
        DataFrame sorted by date (ascending).

    Example:
        >>> df = ensure_sorted_by_date(df)  # Always safe to call
        >>> # Now we can safely calculate rolling windows, etc.
    """
    return df.sort("date")
