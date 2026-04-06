import polars as pl

from tickerlake.extract import DAILY_AGGS_SCHEMA


PRICE_COLUMNS = ("open", "high", "low", "close", "vwap")


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
    """Compute per-ticker technical metrics: SMA-50, SMA-200, ATR-14, ATR%, SMA50_ATR_Distance, RS, RS_SMA_20, VARS, VARS_SMA_20, volume_sma_20.

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
    adr_df = _compute_adr_pct(sorted_bars)

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

        df = df.join(
            adr_df.select(["date", "ticker", "adr_pct"]),
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

        df = df.join(
            adr_df.select(["date", "ticker", "adr_pct"]),
            on=["date", "ticker"],
            how="left",
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
            pl.col("adr_pct"),
            pl.col("sma50_atr_distance"),
            pl.col("rs"),
            pl.col("rs_sma_20"),
            pl.col("vars"),
            pl.col("vars_sma_20"),
            pl.col("volume")
            .cast(pl.Float64)
            .rolling_mean(window_size=20)
            .over("ticker")
            .cast(pl.Float32)
            .alias("volume_sma_20"),
        ]
    )


def aggregate_to_weekly(bars: pl.DataFrame) -> pl.DataFrame:
    """Aggregate daily OHLCV bars into weekly bars per ticker.

    Weekly grouping is based on each row's calendar Friday for the ISO week.
    The output date is the actual last trading day present in that ticker-week.
    """
    if bars.is_empty():
        return pl.DataFrame(schema=DAILY_AGGS_SCHEMA)

    sorted_bars = bars.sort(["ticker", "date"])
    bars_with_week_end = sorted_bars.with_columns(
        (pl.col("date") + pl.duration(days=(4 - pl.col("date").dt.weekday()))).alias(
            "week_end"
        )
    )

    weekly = (
        bars_with_week_end.group_by(["ticker", "week_end"])
        .agg(
            [
                pl.col("open").sort_by("date").first().cast(pl.Float32).alias("open"),
                pl.col("high").max().cast(pl.Float32).alias("high"),
                pl.col("low").min().cast(pl.Float32).alias("low"),
                pl.col("close").sort_by("date").last().cast(pl.Float32).alias("close"),
                pl.col("volume").sum().cast(pl.Float32).alias("volume"),
                pl.col("vwap").sort_by("date").last().cast(pl.Float32).alias("vwap"),
                pl.col("transactions").sum().cast(pl.UInt32).alias("transactions"),
                pl.col("date").max().alias("date"),
            ]
        )
        .drop("week_end")
        .sort(["ticker", "date"])
        .select(list(DAILY_AGGS_SCHEMA.keys()))
    )

    return weekly


HVC_SCHEMA = {
    "ticker": pl.Utf8,
    "date": pl.Date,
    "open": pl.Float32,
    "high": pl.Float32,
    "low": pl.Float32,
    "close": pl.Float32,
    "prev_close": pl.Float32,
    "volume": pl.Float32,
    "volume_sma_20": pl.Float32,
    "volume_multiplier": pl.Float32,
    "total_move_pct": pl.Float32,
    "gap_pct": pl.Float32,
    "intraday_move_pct": pl.Float32,
    "bar_range_pct": pl.Float32,
    "adr_pct": pl.Float32,
    "atr_pct": pl.Float32,
    "close_position_in_range": pl.Float32,
    "is_up_day": pl.Boolean,
    "price_vs_sma50_pct": pl.Float32,
    "price_vs_sma200_pct": pl.Float32,
    "rs": pl.Float32,
}


def detect_hvcs(bars: pl.DataFrame, metrics: pl.DataFrame) -> pl.DataFrame:
    """Detect High Volume Catalyst days and compute pre-calculated derived fields.

    An HVC occurs when volume >= 3x the 20-day average volume (volume_sma_20 from
    metrics), close >= $5.00, volume_sma_20 is non-null (past warmup period), and
    prev_close is non-null (not the first bar per ticker).

    Derived fields are computed on the FULL joined dataset before filtering to
    ensure shift(1).over("ticker") produces correct prev_close values. Filtering
    after computation avoids incorrect shift windows on sparse HVC-only data.

    Float64 intermediate computation with Float32 storage for all numeric fields.
    Division-by-zero safety: fill_nan(None) on close_position_in_range (doji candles
    where high==low) and volume_multiplier (zero volume_sma_20 edge case).

    is_up_day uses close-to-close comparison: close > prev_close.

    Returns a DataFrame with exactly 21 columns per HVC_SCHEMA. Returns an empty
    DataFrame with correct schema when bars is empty.
    """
    if bars.is_empty():
        return pl.DataFrame(schema=HVC_SCHEMA)

    joined = bars.join(metrics, on=["ticker", "date"], how="inner").sort(
        [
            "ticker",
            "date",
        ]
    )

    joined = joined.with_columns(
        pl.col("close").shift(1).over("ticker").cast(pl.Float32).alias("prev_close")
    )

    joined = joined.with_columns(
        [
            (
                pl.col("volume").cast(pl.Float64)
                / pl.col("volume_sma_20").cast(pl.Float64)
            )
            .fill_nan(None)
            .cast(pl.Float32)
            .alias("volume_multiplier"),
            (
                (
                    pl.col("close").cast(pl.Float64)
                    - pl.col("prev_close").cast(pl.Float64)
                )
                / pl.col("prev_close").cast(pl.Float64)
                * 100
            )
            .cast(pl.Float32)
            .alias("total_move_pct"),
            (
                (
                    pl.col("open").cast(pl.Float64)
                    - pl.col("prev_close").cast(pl.Float64)
                )
                / pl.col("prev_close").cast(pl.Float64)
                * 100
            )
            .cast(pl.Float32)
            .alias("gap_pct"),
            (
                (pl.col("close").cast(pl.Float64) - pl.col("open").cast(pl.Float64))
                / pl.col("open").cast(pl.Float64)
                * 100
            )
            .cast(pl.Float32)
            .alias("intraday_move_pct"),
            (
                (pl.col("high").cast(pl.Float64) - pl.col("low").cast(pl.Float64))
                / pl.col("prev_close").cast(pl.Float64)
                * 100
            )
            .cast(pl.Float32)
            .alias("bar_range_pct"),
            (
                (pl.col("close").cast(pl.Float64) - pl.col("low").cast(pl.Float64))
                / (pl.col("high").cast(pl.Float64) - pl.col("low").cast(pl.Float64))
            )
            .fill_nan(None)
            .cast(pl.Float32)
            .alias("close_position_in_range"),
            (pl.col("close") > pl.col("prev_close")).alias("is_up_day"),
            (
                (pl.col("close").cast(pl.Float64) - pl.col("sma_50").cast(pl.Float64))
                / pl.col("sma_50").cast(pl.Float64)
                * 100
            )
            .cast(pl.Float32)
            .alias("price_vs_sma50_pct"),
            (
                (pl.col("close").cast(pl.Float64) - pl.col("sma_200").cast(pl.Float64))
                / pl.col("sma_200").cast(pl.Float64)
                * 100
            )
            .cast(pl.Float32)
            .alias("price_vs_sma200_pct"),
        ]
    )

    filtered = joined.filter(
        (pl.col("volume") >= 3.0 * pl.col("volume_sma_20"))
        & (pl.col("close") >= 5.0)
        & pl.col("volume_sma_20").is_not_null()
        & pl.col("prev_close").is_not_null()
    )

    return filtered.select(
        [
            "ticker",
            "date",
            "open",
            "high",
            "low",
            "close",
            "prev_close",
            "volume",
            "volume_sma_20",
            "volume_multiplier",
            "total_move_pct",
            "gap_pct",
            "intraday_move_pct",
            "bar_range_pct",
            "adr_pct",
            "atr_pct",
            "close_position_in_range",
            "is_up_day",
            "price_vs_sma50_pct",
            "price_vs_sma200_pct",
            "rs",
        ]
    )


HVC_VWAP_SCHEMA = {
    "ticker": pl.Utf8,
    "date": pl.Date,
    "anchor_date": pl.Date,
    "vwap_value": pl.Float32,
}


def compute_hvc_vwap_anchors(
    bars: pl.DataFrame,
    hvcs: pl.DataFrame,
    volume_floor: float = 1_000_000.0,
) -> pl.DataFrame:
    """Compute anchored VWAP lines from qualifying High Volume Catalyst events.

    For each HVC where volume_sma_20 >= volume_floor, computes a running VWAP
    (cumulative typical_price * volume / cumulative volume) from the HVC date
    forward through all subsequent bars. The volume floor ensures VWAPs are only
    anchored to HVCs in liquid stocks where the volume signal is meaningful.

    Returns a normalized DataFrame with one row per (ticker, date, anchor_date)
    combination. Multiple rows per (ticker, date) indicate multiple active VWAP
    lines from different HVC anchors.

    Typical price = (high + low + close) / 3. Float64 intermediate computation
    with Float32 storage. Zero-volume bars produce null vwap_value via
    fill_nan(None).

    Returns an empty DataFrame with correct schema when no qualifying HVCs exist.
    """
    if hvcs.is_empty():
        return pl.DataFrame(schema=HVC_VWAP_SCHEMA)

    anchors = hvcs.filter(pl.col("volume_sma_20") >= volume_floor).select(
        [pl.col("ticker"), pl.col("date").alias("anchor_date")]
    )

    if anchors.is_empty():
        return pl.DataFrame(schema=HVC_VWAP_SCHEMA)

    # Join each anchor with all bars for the same ticker at or after the anchor
    # date, then compute cumulative VWAP per anchor group.
    expanded = (
        anchors.join(
            bars.select(["ticker", "date", "high", "low", "close", "volume"]),
            on="ticker",
            how="inner",
        )
        .filter(pl.col("date") >= pl.col("anchor_date"))
        .sort(["ticker", "anchor_date", "date"])
    )

    if expanded.is_empty():
        return pl.DataFrame(schema=HVC_VWAP_SCHEMA)

    typical_price = (
        pl.col("high").cast(pl.Float64)
        + pl.col("low").cast(pl.Float64)
        + pl.col("close").cast(pl.Float64)
    ) / 3

    return (
        expanded.with_columns(
            (typical_price * pl.col("volume").cast(pl.Float64)).alias("_pv"),
        )
        .with_columns(
            pl.col("_pv").cum_sum().over(["ticker", "anchor_date"]).alias("_cum_pv"),
            pl.col("volume")
            .cast(pl.Float64)
            .cum_sum()
            .over(["ticker", "anchor_date"])
            .alias("_cum_vol"),
        )
        .with_columns(
            (pl.col("_cum_pv") / pl.col("_cum_vol"))
            .fill_nan(None)
            .cast(pl.Float32)
            .alias("vwap_value"),
        )
        .select(list(HVC_VWAP_SCHEMA.keys()))
    )
