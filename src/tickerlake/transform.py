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


def compute_metrics(bars: pl.DataFrame) -> pl.DataFrame:
    """Compute per-ticker technical metrics: SMA-50, SMA-200, ATR-14, ATR%, SMA50_ATR_Distance, RS, RS_SMA_20, VARS, VARS_SMA_20.

    RS measures cumulative return outperformance vs SPY over a 50-day rolling window:
    rolling_sum(stock_pct - spy_pct, 50). RS_SMA_20 is the 20-day rolling mean of RS.

    VARS (Volatility Adjusted Relative Strength) normalizes daily price changes by each
    ticker's ATR(14) before comparing to SPY: rolling_sum(stock_norm - spy_norm, 50)
    where norm = daily_change / ATR14. VARS_SMA_20 is the 20-day rolling mean of VARS.

    ATR% (atr_pct) = ATR-14 / close price (ATR as fraction of closing price).
    SMA50_ATR_Distance (sma50_atr_distance) = ((close - SMA-50) / SMA-50) / ATR% (ATR% multiple from 50-MA).

    When SPY is absent, rs, rs_sma_20, vars, and vars_sma_20 are null for all tickers.
    ATR-14 is always computed regardless of SPY presence.
    When ATR=0, the normalized change is null (not inf/NaN) via fill_nan(None).
    """
    sorted_bars = bars.sort(["ticker", "date"])
    spy_present = "SPY" in sorted_bars["ticker"]

    pct_change = (pl.col("close") - pl.col("close").shift(1).over("ticker")) / pl.col(
        "close"
    ).shift(1).over("ticker")

    atr_df = _compute_atr(sorted_bars)

    if spy_present:
        spy_pct = sorted_bars.filter(pl.col("ticker") == "SPY").select(
            [pl.col("date"), pct_change.alias("_spy_pct")]
        )
        df = sorted_bars.with_columns(pct_change.alias("_pct")).join(
            spy_pct, on="date", how="left"
        )
        rs_daily = pl.col("_pct") - pl.col("_spy_pct")
        df = df.with_columns(
            rs_daily.cast(pl.Float64)
            .rolling_sum(50)
            .over("ticker")
            .cast(pl.Float32)
            .alias("rs")
        ).with_columns(
            pl.col("rs")
            .cast(pl.Float64)
            .rolling_mean(20)
            .over("ticker")
            .cast(pl.Float32)
            .alias("rs_sma_20")
        )

        df = df.join(
            atr_df.select(["date", "ticker", "atr_14"]),
            on=["date", "ticker"],
            how="left",
        )

        spy_norm_df = (
            sorted_bars.filter(pl.col("ticker") == "SPY")
            .join(
                atr_df.filter(pl.col("ticker") == "SPY").select(["date", "atr_14"]),
                on="date",
                how="left",
            )
            .with_columns(
                (pl.col("close") - pl.col("close").shift(1)).alias("_spy_daily_change")
            )
            .select(
                [
                    pl.col("date"),
                    (pl.col("_spy_daily_change") / pl.col("atr_14"))
                    .fill_nan(None)
                    .alias("_spy_norm"),
                ]
            )
        )
        df = df.join(spy_norm_df, on="date", how="left")

        daily_change = pl.col("close") - pl.col("close").shift(1).over("ticker")
        df = df.with_columns(
            (daily_change / pl.col("atr_14")).fill_nan(None).alias("_stock_norm")
        )

        vars_daily = pl.col("_stock_norm") - pl.col("_spy_norm")
        df = df.with_columns(
            vars_daily.cast(pl.Float64)
            .rolling_sum(50)
            .over("ticker")
            .cast(pl.Float32)
            .alias("vars")
        ).with_columns(
            pl.col("vars")
            .cast(pl.Float64)
            .rolling_mean(20)
            .over("ticker")
            .cast(pl.Float32)
            .alias("vars_sma_20")
        )
    else:
        df = sorted_bars.join(
            atr_df.select(["date", "ticker", "atr_14"]),
            on=["date", "ticker"],
            how="left",
        ).with_columns(
            [
                pl.lit(None).cast(pl.Float32).alias("rs"),
                pl.lit(None).cast(pl.Float32).alias("rs_sma_20"),
                pl.lit(None).cast(pl.Float32).alias("vars"),
                pl.lit(None).cast(pl.Float32).alias("vars_sma_20"),
            ]
        )

    # Compute derived metrics: sma_50, atr_pct, sma50_atr_distance
    df = df.with_columns(
        [
            pl.col("close")
            .cast(pl.Float64)
            .rolling_mean(window_size=50)
            .over("ticker")
            .cast(pl.Float32)
            .alias("sma_50"),
            (pl.col("atr_14") / pl.col("close")).cast(pl.Float32).alias("atr_pct"),
        ]
    ).with_columns(
        [
            (
                ((pl.col("close") - pl.col("sma_50")) / pl.col("sma_50"))
                / (pl.col("atr_14") / pl.col("close"))
            )
            .fill_nan(None)
            .cast(pl.Float32)
            .alias("sma50_atr_distance"),
        ]
    )

    return df.select(
        [
            pl.col("date"),
            pl.col("ticker"),
            pl.col("sma_50"),
            pl.col("close")
            .cast(pl.Float64)
            .rolling_mean(window_size=200)
            .over("ticker")
            .cast(pl.Float32)
            .alias("sma_200"),
            pl.col("atr_14"),
            pl.col("atr_pct"),
            pl.col("sma50_atr_distance"),
            pl.col("rs"),
            pl.col("rs_sma_20"),
            pl.col("vars"),
            pl.col("vars_sma_20"),
        ]
    )
