import polars as pl


PRICE_COLUMNS = ("open", "high", "low", "close", "vwap")


def adjust_splits(bars: pl.DataFrame, splits: pl.DataFrame) -> pl.DataFrame:
    if splits.is_empty():
        return bars

    splits_shifted = splits.with_columns(
        (pl.col("execution_date") - pl.duration(days=1)).alias("execution_date")
    ).select(["ticker", "execution_date", "adjustment_factor"])

    factor = pl.col("adjustment_factor").fill_null(1.0)
    sorted_bars = bars.sort(["ticker", "date"])
    sorted_splits = splits_shifted.sort(["ticker", "execution_date"])
    joined = sorted_bars.join_asof(
        sorted_splits,
        left_on="date",
        right_on="execution_date",
        by="ticker",
        strategy="forward",
        check_sortedness=False,
    )

    adjusted = joined.with_columns(
        [
            (pl.col(column).cast(pl.Float64) * factor).cast(pl.Float32).alias(column)
            for column in PRICE_COLUMNS
        ]
        + [
            (pl.col("volume").cast(pl.Float64) / factor)
            .cast(pl.Float32)
            .alias("volume")
        ]
    )

    return adjusted.select(bars.columns)


def filter_tickers(bars: pl.DataFrame, tickers: pl.DataFrame) -> pl.DataFrame:
    return bars.join(tickers.select("ticker"), on="ticker", how="inner")


def compute_metrics(bars: pl.DataFrame) -> pl.DataFrame:
    sorted_bars = bars.sort(["ticker", "date"])
    return sorted_bars.select(
        [
            pl.col("date"),
            pl.col("ticker"),
            pl.col("close")
            .cast(pl.Float64)
            .rolling_mean(window_size=50)
            .over("ticker")
            .cast(pl.Float32)
            .alias("sma_50"),
            pl.col("close")
            .cast(pl.Float64)
            .rolling_mean(window_size=200)
            .over("ticker")
            .cast(pl.Float32)
            .alias("sma_200"),
        ]
    )
