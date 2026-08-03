import polars as pl

from tickerlake.extract import DAILY_AGGS_SCHEMA

PRICE_COLUMNS = ("open", "high", "low", "close", "vwap")
PIVOTS_SCHEMA = {
    "date": pl.Date,
    "ticker": pl.Utf8,
    "pivot_type": pl.Utf8,
    "price": pl.Float32,
    "confirmed_at": pl.Date,
}


def adjust_splits(bars: pl.DataFrame, splits: pl.DataFrame) -> pl.DataFrame:
    """Adjust bar prices and volumes for stock splits.

    The Massive API returns cumulative adjustment factors: each split's factor
    already accounts for all later splits on the same ticker. join_asof(forward)
    matches each bar to the nearest future split, whose factor is the correct
    cumulative multiplier for that bar's position in the split timeline.
    """
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


def _compute_atr(bars: pl.DataFrame, period: int = 14) -> pl.DataFrame:
    """Compute Average True Range (ATR) per ticker using simple rolling mean.

    True Range = max(high - low, |high - prev_close|, |low - prev_close|).
    ATR = rolling_mean(True Range, period). The first row per ticker has no
    prev_close, so max_horizontal returns high - low (non-null). This means
    ATR has period - 1 leading nulls per ticker.
    """
    sorted_bars = bars.sort(["ticker", "date"])
    prev_close = pl.col("close").shift(1).over("ticker")
    tr_hl = pl.col("high") - pl.col("low")
    tr_hc = (pl.col("high") - prev_close).abs()
    tr_lc = (pl.col("low") - prev_close).abs()
    true_range = pl.max_horizontal(tr_hl, tr_hc, tr_lc)
    return sorted_bars.select(
        [
            pl.col("date"),
            pl.col("ticker"),
            true_range.cast(pl.Float64)
            .rolling_mean(period)
            .over("ticker")
            .cast(pl.Float32)
            .alias("atr_14"),
        ]
    )


def _compute_adr_pct(bars: pl.DataFrame, period: int = 20) -> pl.DataFrame:
    """Compute Average Daily Range percent per ticker using simple rolling mean.

    ADR% = SMA(period) of ((high - low) / close). Measures the average daily
    price range as a fraction of closing price (e.g., 0.04 means 4% average
    daily range). Note: NOT expressed as a true percentage — 0.04 represents 4%.

    Warmup: the first (period - 1) rows per ticker are null (rolling_mean needs
    period values to compute the first non-null result).

    ADR% is NOT the same as ATR% — ADR% ignores gaps, measuring only the
    intraday high-low range. Use ATR% when you need gap-inclusive volatility.
    """
    sorted_bars = bars.sort(["ticker", "date"])
    daily_range_pct = (pl.col("high") - pl.col("low")) / pl.col("close")
    return sorted_bars.select(
        [
            pl.col("date"),
            pl.col("ticker"),
            daily_range_pct.cast(pl.Float64)
            .rolling_mean(period)
            .over("ticker")
            .cast(pl.Float32)
            .alias("adr_pct"),
        ]
    )


def compute_metrics(bars: pl.DataFrame) -> pl.DataFrame:
    """Compute per-ticker technical metrics.

    Computes: SMA-20, SMA-50, SMA-200, ATR-14, ATR%, ADR%, volume_sma_20.

    ATR% (atr_pct) = ATR-14 / close price (ATR as fraction of closing price).
    """
    sorted_bars = bars.sort(["ticker", "date"])

    atr_df = _compute_atr(sorted_bars)
    adr_df = _compute_adr_pct(sorted_bars)

    df = sorted_bars.join(
        atr_df.select(["date", "ticker", "atr_14"]),
        on=["date", "ticker"],
        how="left",
    ).join(
        adr_df.select(["date", "ticker", "adr_pct"]),
        on=["date", "ticker"],
        how="left",
    )

    # Compute derived metrics: sma_20, atr_pct
    df = df.with_columns(
        [
            pl.col("close")
            .cast(pl.Float64)
            .rolling_mean(window_size=20)
            .over("ticker")
            .cast(pl.Float32)
            .alias("sma_20"),
            (pl.col("atr_14") / pl.col("close")).cast(pl.Float32).alias("atr_pct"),
        ]
    )

    return df.select(
        [
            pl.col("date"),
            pl.col("ticker"),
            pl.col("sma_20"),
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
            pl.col("atr_14"),
            pl.col("atr_pct"),
            pl.col("adr_pct"),
            pl.col("volume")
            .cast(pl.Float64)
            .rolling_mean(window_size=20)
            .over("ticker")
            .cast(pl.Float32)
            .alias("volume_sma_20"),
        ]
    )


def _aggregate_to_period(bars: pl.DataFrame, every: str) -> pl.DataFrame:
    """Aggregate daily OHLCV bars into calendar periods per ticker."""
    if bars.is_empty():
        return pl.DataFrame(schema=DAILY_AGGS_SCHEMA)

    return (
        bars.sort(["ticker", "date"])
        .group_by_dynamic(
            "date",
            every=every,
            period=every,
            group_by="ticker",
            start_by="monday" if every == "1w" else "window",
        )
        .agg(
            [
                pl.col("open").sort_by("date").first().cast(pl.Float32).alias("open"),
                pl.col("high").max().cast(pl.Float32).alias("high"),
                pl.col("low").min().cast(pl.Float32).alias("low"),
                pl.col("close").sort_by("date").last().cast(pl.Float32).alias("close"),
                pl.col("volume").sum().cast(pl.Float32).alias("volume"),
                pl.when(pl.col("volume").sum() == 0)
                .then(None)
                .otherwise(
                    (pl.col("vwap") * pl.col("volume")).sum() / pl.col("volume").sum()
                )
                .cast(pl.Float32)
                .alias("vwap"),
                pl.col("transactions").sum().cast(pl.UInt32).alias("transactions"),
                pl.col("date").max().alias("period_date"),
            ]
        )
        .drop("date")
        .rename({"period_date": "date"})
        .sort(["ticker", "date"])
        .select(list(DAILY_AGGS_SCHEMA.keys()))
    )


def aggregate_to_weekly(bars: pl.DataFrame) -> pl.DataFrame:
    """Aggregate daily OHLCV bars into weekly bars per ticker.

    Weekly grouping is by calendar week. The output date is the actual last
    trading day present in that ticker-week.
    """
    return _aggregate_to_period(bars, "1w")


def aggregate_to_monthly(bars: pl.DataFrame) -> pl.DataFrame:
    """Aggregate daily OHLCV bars into monthly bars per ticker.

    Monthly grouping is by calendar month. The output date is the actual last
    trading day present in that ticker-month.
    """
    return _aggregate_to_period(bars, "1mo")


def bars_for_timeframe(bars: pl.DataFrame, timeframe: str) -> pl.DataFrame:
    if timeframe == "daily":
        return bars.sort(["ticker", "date"]).select(list(DAILY_AGGS_SCHEMA.keys()))
    if timeframe == "weekly":
        return aggregate_to_weekly(bars)
    if timeframe == "monthly":
        return aggregate_to_monthly(bars)
    msg = "timeframe must be one of: daily, weekly, monthly"
    raise ValueError(msg)


def _shifted_expressions(
    column: str, k: int, *, forward: bool = False
) -> list[pl.Expr]:
    multiplier = -1 if forward else 1
    return [
        pl.col(column).shift(multiplier * offset).over("ticker")
        for offset in range(1, k + 1)
    ]


def _complete_window_expression(expressions: list[pl.Expr]) -> pl.Expr:
    return pl.all_horizontal([expr.is_not_null() for expr in expressions])


def _pivot_high_expression(
    prior_highs: list[pl.Expr], next_highs: list[pl.Expr], complete_window: pl.Expr
) -> pl.Expr:
    return (
        complete_window
        & pl.all_horizontal([pl.col("high") > expr for expr in prior_highs])
        & pl.all_horizontal([pl.col("high") >= expr for expr in next_highs])
    )


def _pivot_low_expression(
    prior_lows: list[pl.Expr], next_lows: list[pl.Expr], complete_window: pl.Expr
) -> pl.Expr:
    return (
        complete_window
        & pl.all_horizontal([pl.col("low") < expr for expr in prior_lows])
        & pl.all_horizontal([pl.col("low") <= expr for expr in next_lows])
    )


def _select_pivots(
    classified: pl.DataFrame, flag_column: str, pivot_type: str, price_column: str
) -> pl.DataFrame:
    return classified.filter(pl.col(flag_column)).select(
        [
            pl.col("date"),
            pl.col("ticker"),
            pl.lit(pivot_type).alias("pivot_type"),
            pl.col(price_column).cast(pl.Float32).alias("price"),
            pl.col("confirmed_at"),
        ]
    )


def find_pivots(bars: pl.DataFrame, *, k: int = 4) -> pl.DataFrame:
    """Find confirmed fractal pivot highs/lows in sorted per-ticker bars.

    A pivot high is greater than each prior k high values and greater than or
    equal to each next k high values. A pivot low uses the inverse comparisons.
    The first k and last k rows per ticker are unknown and are not emitted.
    """
    if k < 1:
        msg = "k must be >= 1"
        raise ValueError(msg)
    if bars.is_empty():
        return pl.DataFrame(schema=PIVOTS_SCHEMA)

    sorted_bars = bars.sort(["ticker", "date"])
    prior_highs = _shifted_expressions("high", k)
    next_highs = _shifted_expressions("high", k, forward=True)
    prior_lows = _shifted_expressions("low", k)
    next_lows = _shifted_expressions("low", k, forward=True)
    confirmed_at = pl.col("date").shift(-k).over("ticker")
    confirmed = confirmed_at.is_not_null()
    complete_window = _complete_window_expression(
        prior_highs + next_highs + prior_lows + next_lows
    )

    classified = sorted_bars.with_columns(
        [
            _pivot_high_expression(
                prior_highs, next_highs, complete_window & confirmed
            ).alias("is_pivot_high"),
            _pivot_low_expression(
                prior_lows, next_lows, complete_window & confirmed
            ).alias("is_pivot_low"),
            confirmed_at.alias("confirmed_at"),
        ]
    )

    high_pivots = _select_pivots(classified, "is_pivot_high", "high", "high")
    low_pivots = _select_pivots(classified, "is_pivot_low", "low", "low")

    return pl.concat([high_pivots, low_pivots]).sort(["ticker", "date", "pivot_type"])
