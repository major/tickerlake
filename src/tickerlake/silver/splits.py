"""Stock split adjustment logic for silver layer. 📊

This module applies split adjustments to historical prices, ensuring that
prices before a split are adjusted to match current split-adjusted values.

Example:
    If a stock has a 2:1 split on 2024-01-15 at $100:
    - Prices after 2024-01-15: remain unchanged ($50 post-split)
    - Prices before 2024-01-15: divided by 2 ($100 → $50)

This keeps all historical analysis consistent with current prices.
"""

import polars as pl


def apply_splits(
    stocks_df: pl.DataFrame,
    splits_df: pl.DataFrame,
) -> pl.DataFrame:
    """Apply split adjustments to stock data. 💰

    For each stock date, multiplies prices by the ratio (split_from/split_to) for
    all splits that occurred AFTER that date. This adjusts historical prices to
    match current split-adjusted prices.

    The adjustment is retroactive: prices BEFORE the split date are adjusted.
    Prices AFTER the split date remain unchanged.

    Args:
        stocks_df: DataFrame with stock data (must have columns: ticker, date,
                   open, high, low, close, volume, transactions).
        splits_df: DataFrame with splits data (must have columns: ticker,
                   execution_date, split_from, split_to).

    Returns:
        DataFrame with split-adjusted OHLCV data. Volume and transactions are
        also adjusted (divided by the adjustment factor to maintain liquidity
        consistency).

    Example:
        >>> stocks = pl.DataFrame({
        ...     "ticker": ["AAPL", "AAPL"],
        ...     "date": ["2024-01-01", "2024-01-20"],
        ...     "close": [100.0, 55.0]
        ... })
        >>> splits = pl.DataFrame({
        ...     "ticker": ["AAPL"],
        ...     "execution_date": ["2024-01-15"],
        ...     "split_from": [2.0],
        ...     "split_to": [1.0]
        ... })
        >>> adjusted = apply_splits(stocks, splits)
        >>> # 2024-01-01 close: 100 / 2 = 50 (adjusted for future split)
        >>> # 2024-01-20 close: 55 (unchanged, after split)
    """
    # Join stocks with all splits for that ticker
    # Then calculate adjustment factor: for each date, apply split_from/split_to
    # if the split occurred AFTER that date
    adjusted_df = (
        stocks_df.lazy()
        .join(
            splits_df.select(["ticker", "execution_date", "split_from", "split_to"]).lazy(),
            on="ticker",
            how="left",
        )
        # Calculate adjustment factor for each split
        .with_columns([
            pl.when(pl.col("date") < pl.col("execution_date"))
            .then(pl.col("split_from") / pl.col("split_to"))
            .otherwise(1.0)
            .alias("adjustment_factor")
        ])
        # Calculate total adjustment by multiplying all factors for this ticker+date
        .group_by(["ticker", "date"])
        .agg([
            pl.col("adjustment_factor").product().alias("total_adjustment"),
            pl.col("open").first(),
            pl.col("high").first(),
            pl.col("low").first(),
            pl.col("close").first(),
            pl.col("volume").first(),
            pl.col("transactions").first(),
        ])
        # Apply adjustments to prices and volume
        .with_columns([
            (pl.col("open") * pl.col("total_adjustment")).alias("open"),
            (pl.col("high") * pl.col("total_adjustment")).alias("high"),
            (pl.col("low") * pl.col("total_adjustment")).alias("low"),
            (pl.col("close") * pl.col("total_adjustment")).alias("close"),
            # Volume is inversely adjusted (divided) to maintain liquidity consistency
            (pl.col("volume") / pl.col("total_adjustment"))
            .cast(pl.UInt64)
            .alias("volume"),
            (pl.col("transactions") / pl.col("total_adjustment"))
            .cast(pl.UInt64)
            .alias("transactions"),
        ])
        .drop("total_adjustment")
        .select(["ticker", "date", "open", "high", "low", "close", "volume", "transactions"])
        .sort(["ticker", "date"])  # Ensure deterministic ordering
        .collect()
    )

    return adjusted_df
