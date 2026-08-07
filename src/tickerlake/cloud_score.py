"""Pure polars data layer for the Ciovacco cloud-score report.

All 9 conditions are computed on the per-ETF relative (ETF/SPY) ratio series.
The relative chart makes the vs-SPY comparison intrinsic — there is no
separate "ETF vs SPY" comparison step.

1-5. Five cloud-timeframe columns (0.0-1.0 each): the ratio's close vs four
   Ichimoku lines (Tenkan-sen, Kijun-sen, Senkou Span A, Senkou Span B)
   computed on the ratio's daily, weekly, 2-week, 3-week, and monthly
   bars. Each cloud column is the count of "above" lines divided by 4, so
   values land in {0.0, 0.25, 0.5, 0.75, 1.0}.
6-9. Four MA columns (0/1 each): the ratio's own weekly 200-week / 300-week
   simple moving average — an above-MA check plus a rising-MA slope check.
   The MAs are computed on the ratio series, never compared across tickers.

The total is the sum of all 9 columns (max 9.0); null cells count as 0.

Reads daily bars from the consumer DuckDB, aggregates them to the five
timeframes, and renders a Rich scorecard. No CLI, no DB writes.
"""

from __future__ import annotations

import datetime
import tempfile
from pathlib import Path
from typing import TYPE_CHECKING, cast

import duckdb
import polars as pl
from rich.table import Table
from rich.text import Text

from tickerlake.race import compute_relative_ratio

if TYPE_CHECKING:
    from collections.abc import Sequence

# Schema of the daily bars frame read from the consumer DB for the report:
# the full split-adjusted OHLCV bar set (the daily_bars table schema).
CLOUD_BARS_SCHEMA: dict = {
    "date": pl.Date,
    "ticker": pl.Utf8,
    "open": pl.Float32,
    "high": pl.Float32,
    "low": pl.Float32,
    "close": pl.Float32,
    "volume": pl.Float32,
    "vwap": pl.Float32,
    "transactions": pl.UInt32,
}

# Schema of the Ichimoku frame: per-bar Tenkan/Kijun/Senkou values (with the
# Senkou values re-shifted to the current bar) for the four cloud lines.
ICHIMOKU_SCHEMA: dict = {
    "date": pl.Date,
    "ticker": pl.Utf8,
    "close": pl.Float32,
    "tenkan": pl.Float32,
    "kijun": pl.Float32,
    "senkou_a_at_current": pl.Float32,
    "senkou_b_at_current": pl.Float32,
}

# Per-ticker moving-average output: the latest N-period SMA of close and its
# slope (ma_value minus the value ``slope_lookback`` bars earlier).
MA_SLOPE_SCHEMA: dict = {
    "ticker": pl.Utf8,
    "ma_value": pl.Float32,
    "ma_slope": pl.Float32,
}

# Combined 200/300-week MA values + slopes used by the MA scoring.
MA_VALUES_SCHEMA: dict = {
    "ticker": pl.Utf8,
    "ma_200": pl.Float32,
    "slope_200": pl.Float32,
    "ma_300": pl.Float32,
    "slope_300": pl.Float32,
}

# The four MA conditions scored 0/1 (null when history is insufficient).
MA_SCORE_SCHEMA: dict = {
    "ticker": pl.Utf8,
    "score_200wk_ma": pl.Int64,
    "score_200wk_ma_slope": pl.Int64,
    "score_300wk_ma": pl.Int64,
    "score_300wk_ma_slope": pl.Int64,
}

# Final scorecard: 5 cloud-timeframe columns (0.0-1.0 in 0.25 steps), 4 MA
# columns (0/1), and the total (0.0-9.0; null cells count as 0 toward the
# total).
CLOUD_SCORE_SCHEMA: dict = {
    "ticker": pl.Utf8,
    "score_1d_cloud": pl.Float32,  # 0.0-1.0
    "score_weekly_cloud": pl.Float32,  # 0.0-1.0
    "score_2wk_cloud": pl.Float32,  # 0.0-1.0
    "score_3wk_cloud": pl.Float32,  # 0.0-1.0
    "score_monthly_cloud": pl.Float32,  # 0.0-1.0
    "score_200wk_ma": pl.Int64,  # 0 or 1
    "score_200wk_ma_slope": pl.Int64,  # 0 or 1
    "score_300wk_ma": pl.Int64,  # 0 or 1
    "score_300wk_ma_slope": pl.Int64,  # 0 or 1
    "total": pl.Float32,  # 0.0-9.0
}

# Ichimoku periods per timeframe as (tenkan, kijun, senkou_b). All five
# timeframes use the standard 9/26/52 — only the bar timeframe changes
# between columns (1d, 1w, 2w, 3w, 1mo). The deeper timeframes need more
# history: daily needs 78 bars (~4 months), weekly needs 78 bars (1.5y),
# 2-week needs 156 weeks (3y), 3-week needs 234 weeks (4.5y), monthly needs
# 78 months (6.5y). All five resolve within the 10-year backfill.
TIMEFRAME_ICHIMOKU_PERIODS: dict[str, tuple[int, int, int]] = {
    "1d": (9, 26, 52),
    "weekly": (9, 26, 52),
    "2wk": (9, 26, 52),
    "3wk": (9, 26, 52),
    "monthly": (9, 26, 52),
}

# Aggregation interval used to build each timeframe's bars from daily bars.
# "1d" is a pass-through (no aggregation); the rest are polars `every` strings.
_TIMEFRAME_EVERY: dict[str, str] = {
    "1d": "1d",
    "weekly": "1w",
    "2wk": "2w",
    "3wk": "3w",
    "monthly": "1mo",
}

# Output column for each timeframe's cloud score.
_CLOUD_SCORE_COLUMNS: dict[str, str] = {
    "1d": "score_1d_cloud",
    "weekly": "score_weekly_cloud",
    "2wk": "score_2wk_cloud",
    "3wk": "score_3wk_cloud",
    "monthly": "score_monthly_cloud",
}

# Senkou Span A/B are plotted 26 periods ahead of the bar they're computed
# on, so the value shown at the current bar is the one computed 26 bars ago.
_SENKOU_DISPLACEMENT = 26
# Weekly moving-average periods behind the four MA conditions.
_MA_200_PERIOD = 200
_MA_300_PERIOD = 300
# Slope window for the MA conditions: 26 weekly bars (~6 months).
_MA_SLOPE_LOOKBACK = 26

# Rendering: default display cap on the rendered scorecard.
_DEFAULT_MAX_ETFS = 50
# Cloud-cell styles by score; the dim variants keep the table scannable.
_CLOUD_STYLES: dict[float, str] = {
    1.0: "green",
    0.75: "dim green",
    0.5: "yellow",
    0.25: "dim yellow",
    0.0: "red",
}
# Total-cell color buckets (0-9 scale): >= _TOTAL_HIGH green,
# >= _TOTAL_LOW yellow, below red.
_TOTAL_HIGH = 7.0
_TOTAL_LOW = 4.0


def read_daily_bars(
    consumer_path: Path,
    *,
    tickers: Sequence[str],
    lookback_days: int,
) -> pl.DataFrame:
    """Read daily OHLCV bars for ``tickers`` over a lookback window.

    Reads the full split-adjusted daily bar set (``date``, ``ticker``, and
    the OHLCV columns) from the consumer-DB ``daily_bars`` table, filtered to
    the requested tickers (uppercased to match stored data) and to
    ``date >= today - lookback_days``. Returns rows sorted by (ticker, date)
    cast to ``CLOUD_BARS_SCHEMA``.
    """
    if not tickers:
        msg = "tickers must not be empty"
        raise ValueError(msg)
    if lookback_days < 1:
        msg = "lookback_days must be >= 1"
        raise ValueError(msg)
    if not consumer_path.exists():
        msg = f"Consumer DB not found: {consumer_path}"
        raise ValueError(msg)

    start_date = (
        datetime.date.today() - datetime.timedelta(days=lookback_days)  # noqa: DTZ011
    )
    placeholders = ", ".join(["?"] * len(tickers))
    params = [ticker.upper() for ticker in tickers] + [start_date]

    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as f:
        tmp = Path(f.name)
    try:
        con = duckdb.connect(str(consumer_path), read_only=True)
        try:
            try:
                con.execute(
                    "COPY ("  # noqa: S608 -- table and tmp path are internal constants
                    "SELECT date, ticker, open, high, low, close, volume, vwap, "
                    "transactions FROM daily_bars "
                    f"WHERE ticker IN ({placeholders}) AND date >= ? "
                    "ORDER BY ticker, date"
                    f") TO '{tmp}' (FORMAT PARQUET)",
                    params,
                )
            except duckdb.CatalogException as err:
                msg = f"daily_bars table not found in consumer DB: {consumer_path}"
                raise ValueError(msg) from err
        finally:
            con.close()
        return pl.read_parquet(tmp).cast(CLOUD_BARS_SCHEMA)
    finally:
        tmp.unlink(missing_ok=True)


def aggregate_daily_to_period(daily_bars: pl.DataFrame, *, every: str) -> pl.DataFrame:
    """Aggregate daily OHLCV bars into calendar periods per ticker.

    Supports ``1d`` (pass-through, no aggregation) and the week-based
    ``1w``/``2w``/``3w`` and month-based ``1mo``/``2mo`` ``every`` intervals.
    ``1d`` returns the input as-is; the others use polars ``group_by_dynamic``.
    Week-based bars are labeled with the period start (a Monday); month-based
    bars with the last trading day in the period. Returns a ``CLOUD_BARS_SCHEMA``
    frame sorted by (ticker, date).

    Week-based bars are re-aligned to a common Monday date sequence after
    aggregation. ``group_by_dynamic`` with ``start_by="monday"`` gives each
    ticker a different first-Monday (the Monday of the week containing that
    ticker's first observation), which makes the inner join on date fail for
    timeframes like ``3w`` where the period doesn't divide the start-date
    difference evenly. The realignment re-dates every bar to
    ``reference_monday + slot * offset_days`` where the reference is the
    earliest Monday in the aggregated data and the slot is the integer count
    of periods between the bar's date and the reference.
    """
    supported = {"1d", "1w", "2w", "3w", "1mo", "2mo"}
    if every not in supported:
        msg = f"every must be one of: {', '.join(sorted(supported))}"
        raise ValueError(msg)
    if daily_bars.is_empty():
        return pl.DataFrame(schema=CLOUD_BARS_SCHEMA)
    if every == "1d":
        return daily_bars.select(list(CLOUD_BARS_SCHEMA)).sort(
            ["ticker", "date"]
        ).cast(CLOUD_BARS_SCHEMA)

    is_week_based = every in {"1w", "2w", "3w"}
    aggregated = (
        daily_bars.sort(["ticker", "date"])
        .group_by_dynamic(
            "date",
            every=every,
            period=every,
            group_by="ticker",
            start_by="monday" if is_week_based else "window",
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
    )
    if is_week_based:
        offset_days = {"1w": 7, "2w": 14, "3w": 21}[every]
        # Re-align each ticker's bars to a common Monday date sequence so the
        # inner join on date works regardless of when each ticker started
        # trading. group_by_dynamic with start_by="monday" gives each ticker
        # a different first-Monday, which is fine for 1w (the 1-week period
        # is a divisor of any day offset) but fails for 3w when tickers
        # start on different weekdays. The realignment uses the earliest
        # Monday in the data as the reference; each bar gets a slot
        # index = (date - reference) // offset_days and is re-dated to
        # reference + slot * offset_days.
        min_date_raw = aggregated["date"].min()
        if min_date_raw is not None:
            min_date = cast("datetime.date", min_date_raw)
            reference_monday = min_date - datetime.timedelta(
                days=min_date.weekday()
            )
            aggregated = (
                aggregated.with_columns(
                    (
                        (pl.col("date") - pl.lit(reference_monday)).dt.total_days()
                        // offset_days
                    )
                    .cast(pl.Int64)
                    .alias("_slot")
                )
                .with_columns(
                    (
                        pl.lit(reference_monday)
                        + pl.duration(days=pl.col("_slot") * offset_days)
                    )
                    .alias("date")
                )
                .drop("_slot")
            )
        return (
            aggregated.drop("period_date")
            .sort(["ticker", "date"])
            .select(list(CLOUD_BARS_SCHEMA))
        )
    return (
        aggregated.drop("date")
        .rename({"period_date": "date"})
        .sort(["ticker", "date"])
        .select(list(CLOUD_BARS_SCHEMA))
    )


def _midpoint(window: int) -> pl.Expr:
    """Midpoint of the rolling high/low range over ``window`` bars."""
    return (
        pl.col("high").rolling_max(window_size=window)
        + pl.col("low").rolling_min(window_size=window)
    ) / 2


def compute_ichimoku(
    bars: pl.DataFrame,
    *,
    tenkan_period: int,
    kijun_period: int,
    senkou_b_period: int,
) -> pl.DataFrame:
    """Compute the four cloud Ichimoku lines per bar for every ticker.

    Returns ``(date, ticker, close, tenkan, kijun, senkou_a_at_current,
    senkou_b_at_current)``. ``senkou_a_at_current`` and
    ``senkou_b_at_current`` are the Senkou values computed
    ``_SENKOU_DISPLACEMENT`` bars earlier (the cloud is plotted that many
    periods ahead). Values are null until enough history exists for the
    requested period.
    """
    if tenkan_period < 1:
        msg = "tenkan_period must be >= 1"
        raise ValueError(msg)
    if kijun_period < 1:
        msg = "kijun_period must be >= 1"
        raise ValueError(msg)
    if senkou_b_period < 1:
        msg = "senkou_b_period must be >= 1"
        raise ValueError(msg)
    if bars.is_empty():
        return pl.DataFrame(schema=ICHIMOKU_SCHEMA)

    enriched = (
        bars.sort(["ticker", "date"])
        .with_columns(
            _midpoint(tenkan_period).over("ticker").alias("tenkan"),
            _midpoint(kijun_period).over("ticker").alias("kijun"),
            _midpoint(senkou_b_period).over("ticker").alias("senkou_b"),
        )
        .with_columns(
            ((pl.col("tenkan") + pl.col("kijun")) / 2).alias("senkou_a"),
        )
        .with_columns(
            pl.col("senkou_a")
            .shift(_SENKOU_DISPLACEMENT)
            .over("ticker")
            .alias("senkou_a_at_current"),
            pl.col("senkou_b")
            .shift(_SENKOU_DISPLACEMENT)
            .over("ticker")
            .alias("senkou_b_at_current"),
        )
    )
    return enriched.select(list(ICHIMOKU_SCHEMA)).cast(ICHIMOKU_SCHEMA)


def score_ichimoku(
    ichimoku_df: pl.DataFrame,
    *,
    tickers: Sequence[str],
    benchmark: str,
) -> pl.DataFrame:
    """Score each ticker 0.0-1.0 from its four cloud-line checks.

    For each ticker in ``tickers`` (excluding ``benchmark``) the score is
    evaluated on its most recent bar: one 0.25 weight for each of
    ``close > tenkan``, ``close > kijun``, ``close > senkou_a_at_current``,
    and ``close > senkou_b_at_current``. The score is null when history is
    insufficient to define any line. Returns one row per scored ticker as
    ``(ticker, score)`` with score a Float32 in {0.0, 0.25, 0.5, 0.75, 1.0}.
    """
    empty = pl.DataFrame(schema={"ticker": pl.Utf8, "score": pl.Float32})
    if ichimoku_df.is_empty():
        return empty

    scoring = [ticker for ticker in tickers if ticker != benchmark]
    if not scoring:
        return empty

    available = set(ichimoku_df["ticker"].unique().to_list())
    scoring = [ticker for ticker in scoring if ticker in available]
    if not scoring:
        return empty

    latest = (
        ichimoku_df.filter(pl.col("ticker").is_in(scoring))
        .sort(["ticker", "date"])
        .group_by("ticker")
        .last()
    )
    checks = [
        pl.col("close") > pl.col("tenkan"),
        pl.col("close") > pl.col("kijun"),
        pl.col("close") > pl.col("senkou_a_at_current"),
        pl.col("close") > pl.col("senkou_b_at_current"),
    ]
    all_defined = pl.all_horizontal([check.is_not_null() for check in checks])
    score = (
        pl.when(all_defined)
        .then(pl.sum_horizontal([check.cast(pl.Float32) for check in checks]) / 4)
        .otherwise(None)
        .cast(pl.Float32)
        .alias("score")
    )
    return latest.select("ticker", score)


def compute_ma_and_slope(
    bars: pl.DataFrame,
    *,
    period: int,
    slope_lookback: int = _MA_SLOPE_LOOKBACK,
) -> pl.DataFrame:
    """Return the latest N-period moving average and its slope per ticker.

    ``ma_value`` is the simple moving average of ``close`` over ``period``
    bars at each ticker's most recent bar; ``ma_slope`` is that value minus
    the moving average ``slope_lookback`` bars earlier. Both are null when
    there is insufficient history (early in the series the moving average is
    itself undefined). Returns ``(ticker, ma_value, ma_slope)``.
    """
    if period < 1:
        msg = "period must be >= 1"
        raise ValueError(msg)
    if slope_lookback < 1:
        msg = "slope_lookback must be >= 1"
        raise ValueError(msg)
    if bars.is_empty():
        return pl.DataFrame(schema=MA_SLOPE_SCHEMA)

    enriched = bars.sort(["ticker", "date"]).with_columns(
        pl.col("close").rolling_mean(window_size=period).over("ticker").alias("_ma"),
        pl.col("close")
        .rolling_mean(window_size=period)
        .shift(slope_lookback)
        .over("ticker")
        .alias("_ma_prev"),
    )
    return (
        enriched.group_by("ticker")
        .agg(
            pl.col("_ma").last().alias("ma_value"),
            (pl.col("_ma") - pl.col("_ma_prev")).last().alias("ma_slope"),
        )
        .select("ticker", "ma_value", "ma_slope")
        .cast(MA_SLOPE_SCHEMA)
    )


def _above_ratio_expression(
    ratio_column: str, ma_column: str, score_column: str
) -> pl.Expr:
    """Score 1 when the ratio close is above the ratio's own MA, 0 otherwise."""
    return (
        pl.when(pl.col(ratio_column).is_not_null() & pl.col(ma_column).is_not_null())
        .then((pl.col(ratio_column) > pl.col(ma_column)).cast(pl.Int64))
        .otherwise(None)
        .alias(score_column)
    )


def _slope_ratio_expression(
    ma_column: str, slope_column: str, score_column: str
) -> pl.Expr:
    """Score 1 when the ratio's MA is rising (slope > 0), 0 otherwise."""
    return (
        pl.when(pl.col(ma_column).is_not_null() & pl.col(slope_column).is_not_null())
        .then((pl.col(slope_column) > 0).cast(pl.Int64))
        .otherwise(None)
        .alias(score_column)
    )


def score_ma(
    ma_ratio_df: pl.DataFrame,
    *,
    latest_ratio: pl.DataFrame,
) -> pl.DataFrame:
    """Score 4 MA conditions per ticker from the ratio's MA + slope.

    The ratio is the latest ratio value per ticker (the current ETF/SPY ratio).
    The MA/slope columns come from ``compute_ma_and_slope`` run on the ratio series.
    All comparisons are self-comparisons on the ratio (no separate SPY frame).
    """
    if ma_ratio_df.is_empty():
        return pl.DataFrame(schema=MA_SCORE_SCHEMA)
    if latest_ratio.is_empty():
        score_columns = [name for name in MA_SCORE_SCHEMA if name != "ticker"]
        return pl.DataFrame(
            {
                "ticker": ma_ratio_df["ticker"].to_list(),
                **{column: [None] * ma_ratio_df.height for column in score_columns},
            },
            schema=MA_SCORE_SCHEMA,
        )

    joined = ma_ratio_df.join(latest_ratio, on="ticker", how="left")
    return joined.select(
        "ticker",
        _above_ratio_expression("ratio", "ma_200", "score_200wk_ma"),
        _slope_ratio_expression("ma_200", "slope_200", "score_200wk_ma_slope"),
        _above_ratio_expression("ratio", "ma_300", "score_300wk_ma"),
        _slope_ratio_expression("ma_300", "slope_300", "score_300wk_ma_slope"),
    ).cast(MA_SCORE_SCHEMA)


def _score_cloud_timeframes(
    daily_bars: pl.DataFrame,
    *,
    tickers: Sequence[str],
    benchmark: str,
) -> list[pl.DataFrame]:
    """Score all five cloud timeframes on the per-ETF relative (ETF/SPY) series."""
    cloud_scores = []
    for timeframe, (tenkan, kijun, senkou_b) in TIMEFRAME_ICHIMOKU_PERIODS.items():
        timeframe_bars = aggregate_daily_to_period(
            daily_bars, every=_TIMEFRAME_EVERY[timeframe]
        )
        ratio_bars = compute_relative_ratio(timeframe_bars, benchmark=benchmark)
        # The ratio series is (date, ticker, close); Ichimoku needs high/low,
        # so treat the ratio close as both — a ratio has no independent range.
        ratio_bars = ratio_bars.with_columns(
            pl.col("close").alias("high"),
            pl.col("close").alias("low"),
        )
        ichimoku = compute_ichimoku(
            ratio_bars,
            tenkan_period=tenkan,
            kijun_period=kijun,
            senkou_b_period=senkou_b,
        )
        scored = score_ichimoku(ichimoku, tickers=tickers, benchmark=benchmark)
        cloud_scores.append(scored.rename({"score": _CLOUD_SCORE_COLUMNS[timeframe]}))
    return cloud_scores


def _compute_ma_scores(
    daily_bars: pl.DataFrame,
    *,
    benchmark: str,
    slope_lookback: int,
) -> pl.DataFrame:
    """Score the four weekly MA conditions from the per-ETF relative (ETF/SPY) series."""  # noqa: E501
    weekly_bars = aggregate_daily_to_period(daily_bars, every="1w")
    ratio_bars = compute_relative_ratio(weekly_bars, benchmark=benchmark)

    if ratio_bars.is_empty():
        return pl.DataFrame(schema=MA_SCORE_SCHEMA)

    latest_ratio = ratio_bars.group_by("ticker").agg(
        pl.col("close").last().alias("ratio")
    )

    ma_200 = compute_ma_and_slope(
        ratio_bars, period=_MA_200_PERIOD, slope_lookback=slope_lookback
    )
    ma_300 = compute_ma_and_slope(
        ratio_bars, period=_MA_300_PERIOD, slope_lookback=slope_lookback
    )
    ma_all = (
        ma_200.rename({"ma_value": "ma_200", "ma_slope": "slope_200"})
        .join(
            ma_300.rename({"ma_value": "ma_300", "ma_slope": "slope_300"}),
            on="ticker",
            how="full",
        )
        .cast(MA_VALUES_SCHEMA)
    )
    return score_ma(ma_all, latest_ratio=latest_ratio)


def _join_score_frames(frames: Sequence[pl.DataFrame]) -> pl.DataFrame:
    """Outer-join per-ticker score frames into one frame on the ticker key."""
    result = frames[0]
    for frame in frames[1:]:
        result = result.join(frame, on="ticker", how="full", coalesce=True)
    return result


def compute_cloud_scores(
    daily_bars: pl.DataFrame,
    *,
    tickers: Sequence[str],
    benchmark: str,
    slope_lookback: int = _MA_SLOPE_LOOKBACK,
) -> pl.DataFrame:
    """Orchestrate the 9-condition Ciovacco scorecard.

    All 9 conditions are computed on the per-ETF relative (ETF/SPY) ratio
    series: each timeframe's bars are reduced to the ratio (ETF close divided
    by the benchmark close), and the five cloud scores (0.0-1.0 each) plus the
    weekly 200/300-week MA conditions (0/1 each) are evaluated on that ratio
    series. The MA conditions are self-comparisons — the ratio's close vs its
    own 200/300-week MA and the MA's own slope — never cross-ticker
    comparisons. Everything is summed into ``total`` (max 9.0; null cells
    count as 0). Returns ``CLOUD_SCORE_SCHEMA`` with one row per scored
    ticker; ``benchmark`` itself is never scored.
    """
    if daily_bars.is_empty():
        return pl.DataFrame(schema=CLOUD_SCORE_SCHEMA)
    if not tickers:
        msg = "tickers must not be empty"
        raise ValueError(msg)

    available = set(daily_bars["ticker"].unique().to_list())
    if benchmark not in available:
        msg = f"benchmark ticker {benchmark!r} not found in bars"
        raise ValueError(msg)

    scoring_tickers = [ticker for ticker in tickers if ticker != benchmark]
    if not scoring_tickers:
        return pl.DataFrame(schema=CLOUD_SCORE_SCHEMA)

    cloud_scores = _score_cloud_timeframes(
        daily_bars, tickers=scoring_tickers, benchmark=benchmark
    )
    ma_scores = _compute_ma_scores(
        daily_bars, benchmark=benchmark, slope_lookback=slope_lookback
    )

    result = _join_score_frames([*cloud_scores, ma_scores])
    if result.is_empty():
        return pl.DataFrame(schema=CLOUD_SCORE_SCHEMA)

    score_columns = [
        name for name in CLOUD_SCORE_SCHEMA if name not in {"ticker", "total"}
    ]
    result = result.with_columns(
        pl.sum_horizontal(score_columns).cast(pl.Float32).alias("total")
    )
    return result.select(list(CLOUD_SCORE_SCHEMA)).cast(CLOUD_SCORE_SCHEMA)


def _cloud_text(value: float | None) -> str:
    """Render a cloud cell as a 2-decimal value, or 'n/a' when null."""
    return "n/a" if value is None else f"{value:.2f}"


def _score_text(value: int | None) -> str:
    """Render an MA cell: the 0/1 value, or 'n/a' when null."""
    return "n/a" if value is None else str(value)


def _total_text(value: float | None) -> str:
    """Render the total cell as a 2-decimal value, or 'n/a' when null."""
    return "n/a" if value is None else f"{value:.2f}"


def _cloud_style(value: float | None) -> str | None:
    """Return a Rich style for a cloud-score cell (1.0 green .. 0.0 red)."""
    if value is None:
        return "dim"
    return _CLOUD_STYLES[round(float(value), 2)]


def _ma_style(value: int | None) -> str | None:
    """Return a Rich style for an MA cell: 1 green, 0 red, null dim."""
    if value is None:
        return "dim"
    return "green" if value == 1 else "red"


def _total_style(value: float | None) -> str | None:
    """Return a Rich style for the total cell (green/yellow/red buckets)."""
    if value is None:
        return "dim"
    if value >= _TOTAL_HIGH:
        return "green"
    if value >= _TOTAL_LOW:
        return "yellow"
    return "red"


def render_cloud_scorecard(
    scores: pl.DataFrame,
    *,
    benchmark: str,
    max_etfs: int | None = _DEFAULT_MAX_ETFS,
) -> Table:
    """Build a Rich scorecard: one row per ETF, 9 score columns + total.

    Rows are sorted by ``total`` descending (nulls last) and capped at
    ``max_etfs`` (default 50, None for unlimited). Cloud cells are tinted by
    score (1.0 green .. 0.0 red) and formatted to 2 decimals, MA cells
    green/red for 1/0, null cells render as ``n/a`` in dim, and the total
    cell is green >= 7, yellow 4-6.99, red below.
    """
    table = Table(
        title=f"Ciovacco cloud scorecard vs {benchmark}",
        header_style="bold",
    )
    table.add_column("Ticker", style="bold")
    for column in (
        "1D-Cloud", "W-Cloud", "2W-Cloud", "3W-Cloud", "Mo-Cloud"
    ):
        table.add_column(column, justify="center")
    for column in ("200W MA", "200W slope", "300W MA", "300W slope"):
        table.add_column(column, justify="center")
    table.add_column("Total", justify="center")

    if scores.is_empty():
        return table

    sorted_scores = scores.sort("total", descending=True, nulls_last=True)
    if max_etfs is not None:
        sorted_scores = sorted_scores.head(max_etfs)

    cloud_columns = (
        "score_1d_cloud",
        "score_weekly_cloud",
        "score_2wk_cloud",
        "score_3wk_cloud",
        "score_monthly_cloud",
    )
    ma_columns = (
        "score_200wk_ma",
        "score_200wk_ma_slope",
        "score_300wk_ma",
        "score_300wk_ma_slope",
    )

    for row in sorted_scores.iter_rows(named=True):
        cells = [row["ticker"]]
        cells.extend(
            Text(
                _cloud_text(row.get(column)),
                style=_cloud_style(row.get(column)),  # ty: ignore[invalid-argument-type]
            )
            for column in cloud_columns
        )
        cells.extend(
            Text(
                _score_text(row.get(column)),
                style=_ma_style(row.get(column)),  # ty: ignore[invalid-argument-type]
            )
            for column in ma_columns
        )
        cells.append(
            Text(
                _total_text(row.get("total")),
                style=_total_style(row.get("total")),  # ty: ignore[invalid-argument-type]
            )
        )
        table.add_row(*cells)

    return table
