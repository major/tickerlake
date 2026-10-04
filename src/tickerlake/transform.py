"""Transform market data into adjusted bars and aggregated period bars."""

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
