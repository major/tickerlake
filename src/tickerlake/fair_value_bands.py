"""Native Fair Value Bands and higher-timeframe overlay alignment.

The indicator uses a 33-period SMA of OHLC4 and expanding median deviations.
Native calculation always operates on the bars supplied by the caller.
Higher-timeframe overlays are aligned separately with a backward as-of join.

For each ticker, a bar contributes upper and lower deviations when its high
and low straddle the fair value.
The expanding medians of those deviations define the bands available on that
bar, so a later observation never changes an earlier result.
"""

from __future__ import annotations

import polars as pl

from tickerlake.transform import VALID_TIMEFRAMES

# Public schema shared by all fair-value-band timeframe tables.
FAIR_VALUE_BANDS_SCHEMA: dict = {
    "ticker": pl.Utf8,
    "as_of_date": pl.Date,
    "fair_value": pl.Float32,
    "upper_band": pl.Float32,
    "lower_band": pl.Float32,
    "current_close": pl.Float32,
    "upper_dev": pl.Float32,
    "lower_dev": pl.Float32,
    "n_straddling_bars": pl.UInt32,
    "zone": pl.Utf8,
    "bar_count": pl.UInt32,
}

# Indicator parameter: the tested setting from the original indicator.
SMA_PERIOD: int = 33

# Zone labels.
ZONE_BELOW_LOWER = "below_lower"
ZONE_IN_BAND = "in_band"
ZONE_ABOVE_UPPER = "above_upper"


def _validate_timeframe(timeframe: str) -> None:
    if timeframe not in VALID_TIMEFRAMES:
        msg = f"timeframe must be one of: {', '.join(sorted(VALID_TIMEFRAMES))}"
        raise ValueError(msg)


def _empty_df() -> pl.DataFrame:
    return pl.DataFrame(schema=FAIR_VALUE_BANDS_SCHEMA)


def _compute_ohlc4(bars: pl.DataFrame) -> pl.DataFrame:
    """Add OHLC4 = (open + high + low + close) / 4 to timeframe bars."""
    sum_ohlc = pl.col("open") + pl.col("high") + pl.col("low") + pl.col("close")
    return bars.with_columns(
        (sum_ohlc.cast(pl.Float64) / pl.lit(4.0)).cast(pl.Float32).alias("ohlc4")
    )


def _compute_fair_value(bars: pl.DataFrame, period: int = SMA_PERIOD) -> pl.DataFrame:
    """Compute the period-SMA of OHLC4 per ticker."""
    sorted_bars = bars.sort(["ticker", "date"])
    return sorted_bars.with_columns(
        pl.col("ohlc4")
        .cast(pl.Float64)
        .rolling_mean(period)
        .over("ticker")
        .cast(pl.Float32)
        .alias("fair_value")
    )


def _per_ticker_band_widths(bars_with_fv: pl.DataFrame) -> pl.DataFrame:
    """Compute expanding median deviations and cumulative straddling counts."""
    if bars_with_fv.is_empty():
        return pl.DataFrame(
            schema={
                "ticker": pl.Utf8,
                "date": pl.Date,
                "upper_dev": pl.Float32,
                "lower_dev": pl.Float32,
                "n_straddling_bars": pl.UInt32,
            }
        )

    bars = bars_with_fv.sort(["ticker", "date"])
    fv64 = pl.col("fair_value").cast(pl.Float64)
    high64 = pl.col("high").cast(pl.Float64)
    low64 = pl.col("low").cast(pl.Float64)
    straddles = (
        pl.col("fair_value").is_not_null()
        & (pl.col("fair_value") > 0)
        & (pl.col("low") < pl.col("fair_value"))
        & (pl.col("high") > pl.col("fair_value"))
    )
    deviations = bars.with_columns(
        [
            pl.when(straddles)
            .then((high64 - fv64) / fv64)
            .otherwise(None)
            .cast(pl.Float32)
            .alias("upper_dev"),
            pl.when(straddles)
            .then((fv64 - low64) / fv64)
            .otherwise(None)
            .cast(pl.Float32)
            .alias("lower_dev"),
        ]
    )
    # A full-input rolling window becomes an expanding window within each
    # ticker, while null deviations are ignored by rolling_median.
    window_size = deviations.height
    return deviations.with_columns(
        [
            pl.col("upper_dev")
            .rolling_median(window_size=window_size, min_samples=1)
            .over("ticker")
            .cast(pl.Float32)
            .alias("upper_dev"),
            pl.col("lower_dev")
            .rolling_median(window_size=window_size, min_samples=1)
            .over("ticker")
            .cast(pl.Float32)
            .alias("lower_dev"),
            pl.col("upper_dev")
            .is_not_null()
            .cast(pl.UInt32)
            .cum_sum()
            .over("ticker")
            .alias("n_straddling_bars"),
        ]
    ).select(
        [
            "ticker",
            "date",
            "upper_dev",
            "lower_dev",
            "n_straddling_bars",
        ]
    )


def _zone_expression() -> pl.Expr:
    """Classify closes relative to the exclusive band boundaries."""
    return (
        pl.when(pl.col("current_close") < pl.col("lower_band"))
        .then(pl.lit(ZONE_BELOW_LOWER))
        .when(pl.col("current_close") > pl.col("upper_band"))
        .then(pl.lit(ZONE_ABOVE_UPPER))
        .otherwise(pl.lit(ZONE_IN_BAND))
    )


def compute_native_fair_value_bands(bars: pl.DataFrame) -> pl.DataFrame:
    """Compute native Fair Value Bands for already-aggregated bars."""
    if bars is None or bars.is_empty():
        return _empty_df()

    with_ohlc4 = _compute_ohlc4(bars)
    with_fv = _compute_fair_value(with_ohlc4)
    deviations = _per_ticker_band_widths(with_fv)
    joined = (
        with_fv.with_columns(
            pl.int_range(1, pl.len() + 1)
            .over("ticker")
            .cast(pl.UInt32)
            .alias("bar_count")
        )
        .join(deviations, on=["ticker", "date"], how="inner")
        .filter(pl.col("upper_dev").is_not_null())
        .with_columns(
            [
                pl.col("date").alias("as_of_date"),
                pl.col("close").cast(pl.Float32).alias("current_close"),
            ]
        )
    )
    if joined.is_empty():
        return _empty_df()

    fv64 = pl.col("fair_value").cast(pl.Float64)
    upper_expr = (fv64 * (pl.lit(1.0) + pl.col("upper_dev").cast(pl.Float64))).cast(
        pl.Float32
    )
    lower_expr = (fv64 * (pl.lit(1.0) - pl.col("lower_dev").cast(pl.Float64))).cast(
        pl.Float32
    )
    return (
        joined.with_columns(
            [upper_expr.alias("upper_band"), lower_expr.alias("lower_band")]
        )
        .with_columns(_zone_expression().alias("zone"))
        .select(list(FAIR_VALUE_BANDS_SCHEMA))
        .sort(["ticker", "as_of_date"])
    )


def compute_fair_value_bands(
    bars: pl.DataFrame, timeframe: str = "monthly"
) -> pl.DataFrame:
    """Compute native Fair Value Bands for a valid source timeframe."""
    _validate_timeframe(timeframe)
    return compute_native_fair_value_bands(bars)


def _validate_source_timeframe(source_timeframe: str) -> None:
    if source_timeframe not in {"weekly", "monthly"}:
        msg = "source_timeframe must be one of: monthly, weekly"
        raise ValueError(msg)


def _source_availability_expression(source_timeframe: str) -> pl.Expr:
    if source_timeframe == "weekly":
        return pl.col("source_date") + pl.duration(weeks=1)
    return pl.col("source_date").dt.month_start().dt.offset_by("1mo")


def align_fair_value_bands(
    source_bands: pl.DataFrame,
    display_bars: pl.DataFrame,
    source_timeframe: str,
) -> pl.DataFrame:
    """Overlay confirmed source bands on display bars without looking ahead.

    Weekly source bands become available on the following Monday.
    Monthly source bands become available on the first day of the following
    calendar month.
    """
    _validate_source_timeframe(source_timeframe)
    if source_bands is None or source_bands.is_empty():
        return _empty_df()
    if display_bars is None or display_bars.is_empty():
        return _empty_df()

    source = (
        source_bands.select(
            [
                "ticker",
                "as_of_date",
                "fair_value",
                "upper_band",
                "lower_band",
                "upper_dev",
                "lower_dev",
                "n_straddling_bars",
                "bar_count",
            ]
        )
        .rename({"as_of_date": "source_date"})
        .with_columns(
            _source_availability_expression(source_timeframe).alias("available_date")
        )
        .sort(["ticker", "available_date"])
    )
    display = display_bars.select(["ticker", "date", "close"]).sort(["ticker", "date"])
    joined = display.join_asof(
        source,
        left_on="date",
        right_on="available_date",
        by="ticker",
        strategy="backward",
        check_sortedness=False,
    ).filter(pl.col("fair_value").is_not_null())
    if joined.is_empty():
        return _empty_df()

    return (
        joined.with_columns(
            [
                pl.col("date").alias("as_of_date"),
                pl.col("close").cast(pl.Float32).alias("current_close"),
            ]
        )
        .with_columns(_zone_expression().alias("zone"))
        .select(list(FAIR_VALUE_BANDS_SCHEMA))
        .sort(["ticker", "as_of_date"])
    )
