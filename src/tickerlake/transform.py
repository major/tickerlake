"""Transform market data into adjusted bars and technical metrics."""

import datetime
from typing import Literal

import polars as pl

from tickerlake.calendar import period_session_bounds


def adjust_splits(bars: pl.DataFrame, splits: pl.DataFrame) -> pl.DataFrame:
    """Adjust bar prices and volumes for stock splits.

    The Massive API returns cumulative adjustment factors: each split's factor
    already accounts for all later splits on the same ticker. join_asof(forward)
    matches each bar to the nearest future split, whose factor is the correct
    cumulative multiplier for that bar's position in the split timeline.
    """
    if splits.is_empty():
        return bars.with_columns(pl.col("volume").cast(pl.Float64))

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
            for column in ("open", "high", "low", "close")
        ]
        + [(pl.col("volume").cast(pl.Float64) / factor).alias("volume")]
    )
    if adjusted.filter(pl.col("volume").is_not_null() & ~pl.col("volume").is_finite()).height:
        raise ValueError

    return adjusted.select(bars.columns)


def filter_tickers(bars: pl.DataFrame, tickers: pl.DataFrame) -> pl.DataFrame:
    """Keep bars whose ticker appears in the ticker metadata DataFrame."""
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
            true_range.cast(pl.Float64).rolling_mean(period).over("ticker").cast(pl.Float32).alias("atr_14"),
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
            daily_range_pct.cast(pl.Float64).rolling_mean(period).over("ticker").cast(pl.Float32).alias("adr_pct"),
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

    metrics = df.select(
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
            pl.col("volume").cast(pl.Float64).rolling_mean(window_size=20).over("ticker").alias("volume_sma_20"),
        ]
    )
    if metrics.filter(pl.col("volume_sma_20").is_not_null() & ~pl.col("volume_sma_20").is_finite()).height:
        raise ValueError
    return metrics


PERIOD_AGGS_SCHEMA = {
    "date": pl.Date,
    "ticker": pl.Utf8,
    "open": pl.Float32,
    "high": pl.Float32,
    "low": pl.Float32,
    "close": pl.Float32,
    "volume": pl.Float64,
    "left_truncated": pl.Boolean,
    "calendar_closed": pl.Boolean,
}


def _aggregate_to_period(
    bars: pl.DataFrame,
    every: str,
    *,
    collection_start: datetime.date,
    target: datetime.date,
) -> pl.DataFrame:
    """Aggregate daily OHLCV bars into calendar periods per ticker.

    Weekly bars are labeled with the Monday that starts their week (the
    week-start convention used by most charting platforms); monthly bars
    with the last observed bar date in the ticker-month.
    """
    if collection_start > target:
        raise ValueError
    if bars.is_empty():
        return pl.DataFrame(schema=PERIOD_AGGS_SCHEMA)

    is_weekly = every == "1w"
    period: Literal["week", "month"] = "week" if is_weekly else "month"
    dates = bars.get_column("date").unique().to_list()
    period_keys = {
        date - datetime.timedelta(days=date.weekday()) if is_weekly else date.replace(day=1) for date in dates
    }
    bounds_by_period = {key: period_session_bounds(key, period) for key in period_keys}
    boundary_rows = pl.DataFrame(
        {
            "period_key": list(bounds_by_period),
            "first_session": [value[0] for value in bounds_by_period.values()],
            "last_session": [value[1] for value in bounds_by_period.values()],
        },
        schema={"period_key": pl.Date, "first_session": pl.Date, "last_session": pl.Date},
    )
    aggregated = (
        bars.sort(["ticker", "date"])
        .group_by_dynamic(
            "date",
            every=every,
            period=every,
            group_by="ticker",
            start_by="monday" if is_weekly else "window",
        )
        .agg(
            [
                pl.col("open").sort_by("date").first().cast(pl.Float32).alias("open"),
                pl.col("high").max().cast(pl.Float32).alias("high"),
                pl.col("low").min().cast(pl.Float32).alias("low"),
                pl.col("close").sort_by("date").last().cast(pl.Float32).alias("close"),
                pl.col("volume").cast(pl.Float64).sum().alias("volume"),
                pl.col("date").max().alias("period_date"),
            ]
        )
    )
    if is_weekly:
        result = aggregated.drop("period_date").sort(["ticker", "date"])
    else:
        result = aggregated.drop("date").rename({"period_date": "date"}).sort(["ticker", "date"])
    boundary_rows = boundary_rows.with_columns(
        (collection_start > pl.col("first_session")).alias("left_truncated"),
        (pl.col("last_session") <= target).alias("calendar_closed"),
    )
    date_key = pl.col("date").dt.truncate("1w") if is_weekly else pl.col("date").dt.truncate("1mo")
    result = result.with_columns(date_key.alias("period_key")).join(boundary_rows, on="period_key", how="left")
    result = result.drop("period_key", "first_session", "last_session").select(list(PERIOD_AGGS_SCHEMA))
    if result.filter(pl.col("volume").is_not_null() & ~pl.col("volume").is_finite()).height:
        raise ValueError
    return result


def aggregate_to_weekly(bars: pl.DataFrame, *, collection_start: datetime.date, target: datetime.date) -> pl.DataFrame:
    """Aggregate daily OHLCV bars into weekly bars per ticker.

    Weekly grouping is by calendar week (Monday-start). The output date is
    the Monday that starts the ticker-week. ``target`` determines whether a
    period is calendar-closed; it does not filter retained future bars.
    """
    return _aggregate_to_period(bars, "1w", collection_start=collection_start, target=target)


def aggregate_to_monthly(bars: pl.DataFrame, *, collection_start: datetime.date, target: datetime.date) -> pl.DataFrame:
    """Aggregate daily OHLCV bars into monthly bars per ticker.

    Monthly grouping is by calendar month. The output date is the last
    observed bar date in that ticker-month. ``target`` determines whether a
    period is calendar-closed; it does not filter retained future bars.
    """
    return _aggregate_to_period(bars, "1mo", collection_start=collection_start, target=target)
