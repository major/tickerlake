"""Pure polars data layer for the etf-race report.

Reads close prices for a set of tickers from the consumer DuckDB across
daily/weekly/monthly timeframes, then computes the race view:

1. Multi-asset race view (``rebase_to_100``): every ticker's closes are
   rebased to 100 at the first bar of its window so the leaderboard
   compares like-for-like across tickers with very different prices.

Also provides per-ticker metrics (``compute_race_metrics``), ranking
(``rank_by_current``), overtake detection (``detect_pending_overtakes``),
and Rich rendering helpers (``render_leaderboard``, ``render_overtakes``).

No CLI, no DB writes.
"""

from __future__ import annotations

import datetime
import tempfile
from pathlib import Path
from typing import TYPE_CHECKING

import duckdb
import polars as pl
from rich.table import Table

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence

# Schema of the race bars frame: the close prices read for a set of
# tickers over a lookback window, sorted by (ticker, date).
RACE_BARS_SCHEMA: dict = {
    "date": pl.Date,
    "ticker": pl.Utf8,
    "close": pl.Float32,
}

# Schema of the race metrics frame: one row per ticker with its current
# value, total/recent returns, and momentum.
# Schema of the relative momentum frame: one row per ticker with its
# relative strength ratio and momentum across three windows.
RELATIVE_MOMENTUM_SCHEMA: dict = {
    "ticker": pl.Utf8,
    "rs_ratio": pl.Float32,
    "momentum_short": pl.Float32,
    "momentum_medium": pl.Float32,
    "momentum_long": pl.Float32,
    "rate_short": pl.Float32,
    "rate_medium": pl.Float32,
    "rate_long": pl.Float32,
}

# Schema of the relative trend frame: relative momentum plus trend
# classification and building indicator.
RELATIVE_TREND_SCHEMA: dict = {
    "ticker": pl.Utf8,
    "rs_ratio": pl.Float32,
    "momentum_short": pl.Float32,
    "momentum_medium": pl.Float32,
    "momentum_long": pl.Float32,
    "rate_short": pl.Float32,
    "rate_medium": pl.Float32,
    "rate_long": pl.Float32,
    "trend": pl.Utf8,
    "building": pl.Boolean,
}

# Consumer-DB table that backs each supported timeframe. The weekly_bars
# and monthly_bars tables share the daily bars schema, so the same reader
# works for all three.
TIMEFRAME_TABLE: dict[str, str] = {
    "daily": "daily_bars",
    "weekly": "weekly_bars",
    "monthly": "monthly_bars",
}

# Default volume threshold for the dynamic ETF list: a ticker must have a
# 20-day simple moving average of volume at or above this on its most recent
# daily_metrics row to qualify.
_DEFAULT_MIN_VOL_SMA_20 = 250_000.0
# Default cap on the dynamic ETF list: only the top N most-liquid qualifying
# ETFs (by volume_sma_20) are shown. 50 is wide enough to cover every major
# sector/thematic ETF while keeping the leaderboard readable.
_DEFAULT_MAX_ETFS = 50
# Default cap on the pending-overtakes table. 50+ ETFs can produce a flood
# of gap-closing names; 25 is enough to see what's actionable without
# drowning the operator.
_DEFAULT_MAX_PENDING_OVERTAKES = 25

# Detection thresholds (kept module-level so ruff doesn't flag them as
# magic numbers and so the values are discoverable in one place).
_RS_RATIO_BASELINE = 100.0


def read_race_bars(
    consumer_path: Path,
    *,
    timeframe: str,
    tickers: Sequence[str],
    lookback_days: int,
) -> pl.DataFrame:
    """Read close prices for ``tickers`` over a lookback window.

    Reads ``date``, ``ticker``, ``close`` from the consumer-DB table for
    the given ``timeframe``, filtered to the requested tickers (uppercased
    to match stored data) and to ``date >= today - lookback_days``. Returns
    rows sorted by (ticker, date) with ``close`` as Float32.
    """
    if timeframe not in TIMEFRAME_TABLE:
        msg = f"timeframe must be one of: {', '.join(sorted(TIMEFRAME_TABLE))}"
        raise ValueError(msg)
    if not tickers:
        msg = "tickers must not be empty"
        raise ValueError(msg)
    if lookback_days < 1:
        msg = "lookback_days must be >= 1"
        raise ValueError(msg)
    if not consumer_path.exists():
        msg = f"Consumer DB not found: {consumer_path}"
        raise ValueError(msg)

    table = TIMEFRAME_TABLE[timeframe]
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
                    f"SELECT date, ticker, close FROM {table} "
                    f"WHERE ticker IN ({placeholders}) AND date >= ? "
                    "ORDER BY ticker, date"
                    f") TO '{tmp}' (FORMAT PARQUET)",
                    params,
                )
            except duckdb.CatalogException as err:
                msg = f"{table} table not found in consumer DB: {consumer_path}"
                raise ValueError(msg) from err
        finally:
            con.close()
        return pl.read_parquet(tmp).with_columns(pl.col("close").cast(pl.Float32))
    finally:
        tmp.unlink(missing_ok=True)


def read_qualifying_etfs(
    consumer_path: Path,
    *,
    min_volume_sma_20: float = _DEFAULT_MIN_VOL_SMA_20,
    limit: int | None = _DEFAULT_MAX_ETFS,
) -> list[str]:
    """Read active ETF tickers whose latest ``daily_metrics`` row qualifies.

    Filters the ``tickers`` table to ``type='ETF'`` and ``active=true``,
    joins each ticker to its most recent ``daily_metrics`` row, and keeps
    those whose ``volume_sma_20`` is at or above ``min_volume_sma_20``.
    Returns the top ``limit`` qualifying tickers ordered by
    ``volume_sma_20`` descending (most liquid first), then ticker ascending
    as a tiebreaker. Pass ``limit=None`` to return every qualifying ticker.
    Raises ``ValueError`` when the consumer DB is missing or the required
    tables aren't present.
    """
    if min_volume_sma_20 < 0:
        msg = "min_volume_sma_20 must be >= 0"
        raise ValueError(msg)
    if limit is not None and limit < 1:
        msg = "limit must be >= 1 or None"
        raise ValueError(msg)
    if not consumer_path.exists():
        msg = f"Consumer DB not found: {consumer_path}"
        raise ValueError(msg)

    limit_clause = f" LIMIT {limit}" if limit is not None else ""

    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as f:
        tmp = Path(f.name)
    try:
        con = duckdb.connect(str(consumer_path), read_only=True)
        try:
            con.execute(
                "COPY ("  # noqa: S608 -- tables, threshold, and limit are internal constants
                "SELECT t.ticker FROM tickers t "
                "JOIN ("
                "  SELECT m.ticker, m.volume_sma_20 FROM daily_metrics m "
                "  JOIN ("
                "    SELECT ticker, MAX(date) AS max_date "
                "    FROM daily_metrics GROUP BY ticker"
                "  ) latest "
                "  ON m.ticker = latest.ticker AND m.date = latest.max_date"
                ") m ON t.ticker = m.ticker "
                "WHERE t.type = 'ETF' AND t.active "
                f"  AND m.volume_sma_20 >= {min_volume_sma_20} "
                "ORDER BY m.volume_sma_20 DESC, t.ticker"
                f"{limit_clause}"
                f") TO '{tmp}' (FORMAT PARQUET)"
            )
        except duckdb.CatalogException as err:
            msg = (
                "tickers/daily_metrics tables not found in consumer DB: "
                f"{consumer_path}. Run backfill first."
            )
            raise ValueError(msg) from err
        finally:
            con.close()
        return pl.read_parquet(tmp)["ticker"].to_list()
    finally:
        tmp.unlink(missing_ok=True)


def rebase_to_100(bars: pl.DataFrame) -> pl.DataFrame:
    """Rebase every ticker's closes so its first close in the window is 100.

    Returns the input columns plus ``rebased`` (Float32). Empty input is
    returned unchanged (no ``rebased`` column added).
    """
    if bars.is_empty():
        return bars

    sorted_bars = bars.sort(["ticker", "date"])
    first_close = pl.col("close").first().over("ticker")
    rebased = (
        pl.when(first_close != 0)
        .then((pl.col("close").cast(pl.Float64) / first_close) * 100)
        .otherwise(None)
        .cast(pl.Float32)
        .alias("rebased")
    )
    return sorted_bars.with_columns(rebased)


# ---- vs-benchmark relative momentum ----------------------------------------


def compute_relative_ratio(bars: pl.DataFrame, *, benchmark: str) -> pl.DataFrame:
    """Compute ticker/benchmark close ratio for every ticker except benchmark.

    Takes a long-form ``(date, ticker, close)`` frame and returns the same
    shape with ``close`` replaced by ``ticker_close / benchmark_close`` for
    every ticker in ``bars`` except ``benchmark`` itself. Raises ``ValueError``
    if ``benchmark`` is not present in the input. Uses inner join to ensure
    date alignment and avoid nulls from mismatched bar coverage.
    """
    if bars.is_empty():
        return pl.DataFrame(schema=RACE_BARS_SCHEMA)

    available = set(bars["ticker"].to_list())
    if benchmark not in available:
        msg = f"benchmark ticker {benchmark!r} not found in bars"
        raise ValueError(msg)

    # Extract benchmark's date/close pairs.
    bench = bars.filter(pl.col("ticker") == benchmark).select(
        "date", bench_close="close"
    )

    # Inner join non-benchmark tickers against benchmark dates.
    result = (
        bars.filter(pl.col("ticker") != benchmark)
        .join(bench, on="date", how="inner")
        .with_columns(
            (pl.col("close").cast(pl.Float64) / pl.col("bench_close"))
            .cast(pl.Float32)
            .alias("close")
        )
        .select(["date", "ticker", "close"])
        .sort(["ticker", "date"])
    )
    # n-6: cast to schema for consistency
    return result.cast(RACE_BARS_SCHEMA)


def compute_relative_momentum(
    rebased_ratio_bars: pl.DataFrame,
    *,
    short_window: int,
    medium_window: int,
    long_window: int,
) -> pl.DataFrame:
    """Compute relative momentum across three windows from rebased ratio bars.

    Takes the output of ``rebase_to_100()`` applied to ``compute_relative_ratio()``
    (i.e. a frame with ``ticker``, ``date``, ``close``, ``rebased`` columns).
    Returns one row per ticker with ``rs_ratio`` (current rebased value),
    ``momentum_short/medium/long`` (cumulative: current minus value N bars back),
    and ``rate_short/medium/long`` (normalized: momentum divided by actual bars
    back, accounting for graceful clamping). Rates are used for the ``building``
    indicator; momenta are kept for display.
    """
    # Validate windows before empty check (N10).
    if short_window < 1:
        msg = "short_window must be >= 1"
        raise ValueError(msg)
    if medium_window < 1:
        msg = "medium_window must be >= 1"
        raise ValueError(msg)
    if long_window < 1:
        msg = "long_window must be >= 1"
        raise ValueError(msg)
    if not (short_window < medium_window < long_window):
        msg = (
            f"windows must be strictly increasing: "
            f"short_window={short_window} < medium_window={medium_window} < "
            f"long_window={long_window}"
        )
        raise ValueError(msg)

    if rebased_ratio_bars.is_empty() or "rebased" not in rebased_ratio_bars.columns:
        return pl.DataFrame(schema=RELATIVE_MOMENTUM_SCHEMA)

    sorted_bars = rebased_ratio_bars.sort(["ticker", "date"])
    current = pl.col("rebased").last()

    # Graceful clamp: tail(window+1).first() gives the value window bars back,
    # or the first value if the window exceeds available history.
    short_ref = pl.col("rebased").tail(short_window + 1).first()
    medium_ref = pl.col("rebased").tail(medium_window + 1).first()
    long_ref = pl.col("rebased").tail(long_window + 1).first()

    # Compute actual bars back (clamped distance) for rate normalization.
    # n_bars = count of rows per ticker; bars_back_w = min(window, n_bars - 1),
    # then clamped to >= 1.
    n_bars = pl.len()

    metrics = sorted_bars.group_by("ticker").agg(
        current.alias("rs_ratio"),
        (current - short_ref).alias("momentum_short"),
        (current - medium_ref).alias("momentum_medium"),
        (current - long_ref).alias("momentum_long"),
        (
            (current - short_ref).cast(pl.Float64)
            / pl.max_horizontal(1, pl.min_horizontal(short_window, n_bars - 1)).cast(
                pl.Float64
            )
        )
        .cast(pl.Float32)
        .alias("rate_short"),
        (
            (current - medium_ref).cast(pl.Float64)
            / pl.max_horizontal(1, pl.min_horizontal(medium_window, n_bars - 1)).cast(
                pl.Float64
            )
        )
        .cast(pl.Float32)
        .alias("rate_medium"),
        (
            (current - long_ref).cast(pl.Float64)
            / pl.max_horizontal(1, pl.min_horizontal(long_window, n_bars - 1)).cast(
                pl.Float64
            )
        )
        .cast(pl.Float32)
        .alias("rate_long"),
    )

    # Drop any rows with null rs_ratio (defensive backstop for B2).
    result = metrics.filter(pl.col("rs_ratio").is_not_null())

    return result.select(
        [
            "ticker",
            "rs_ratio",
            "momentum_short",
            "momentum_medium",
            "momentum_long",
            "rate_short",
            "rate_medium",
            "rate_long",
        ]
    ).cast(RELATIVE_MOMENTUM_SCHEMA)


def classify_relative_trend(relative_momentum: pl.DataFrame) -> pl.DataFrame:
    """Add trend classification and building indicator to relative momentum.

    Adds ``trend`` (Utf8) and ``building`` (Boolean) columns. Trend is one of:
    - ``"Unknown"``: rs_ratio or momentum_short is null
    - ``"Leading"``: rs_ratio >= 100 and momentum_short > 0
    - ``"Fading"``: rs_ratio >= 100 and momentum_short <= 0
    - ``"Improving"``: rs_ratio < 100 and momentum_short > 0
    - ``"Lagging"``: rs_ratio < 100 and momentum_short <= 0

    Building is ``True`` when rate_short > rate_medium > rate_long AND
    momentum_medium > 0 (outperformance accelerating, excluding decelerating
    declines), ``False`` otherwise (including when any rate/momentum is null).
    """
    if relative_momentum.is_empty():
        return pl.DataFrame(schema=RELATIVE_TREND_SCHEMA)

    # M4: null rs_ratio or momentum_short → "Unknown"
    trend = (
        pl.when(pl.col("rs_ratio").is_null() | pl.col("momentum_short").is_null())
        .then(pl.lit("Unknown"))
        .when(
            (pl.col("rs_ratio") >= _RS_RATIO_BASELINE) & (pl.col("momentum_short") > 0)
        )
        .then(pl.lit("Leading"))
        .when(
            (pl.col("rs_ratio") >= _RS_RATIO_BASELINE) & (pl.col("momentum_short") <= 0)
        )
        .then(pl.lit("Fading"))
        .when(
            (pl.col("rs_ratio") < _RS_RATIO_BASELINE) & (pl.col("momentum_short") > 0)
        )
        .then(pl.lit("Improving"))
        .otherwise(pl.lit("Lagging"))
        .alias("trend")
    )

    # B1 fix: building = rate_short > rate_medium > rate_long AND momentum_medium > 0
    # This flags accelerating outperformers, not decelerating decliners.
    building = (
        (
            (pl.col("rate_short") > pl.col("rate_medium"))
            & (pl.col("rate_medium") > pl.col("rate_long"))
            & (pl.col("momentum_medium") > 0)
        )
        .fill_null(value=False)
        .alias("building")
    )

    return relative_momentum.with_columns(trend, building)


def _fmt_momentum(value: float) -> str:
    """Format a momentum value with color but no hot/cold emoji.

    Green for positive, red for negative, white for zero.
    """
    color = "green" if value > 0 else ("red" if value < 0 else "white")
    return f"[{color}]{value:+.2f}[/]"


def _fmt_or_na(value: float | None, formatter: Callable[[float], str]) -> str:
    """Format a value using the given formatter, or return 'n/a' if null."""
    return "n/a" if value is None else formatter(value)


def render_relative_leaderboard(
    relative_trend: pl.DataFrame, *, benchmark: str
) -> Table:
    """Build a Rich Table for relative momentum vs a benchmark.

    Title is ``f"🐎 vs {benchmark} Momentum"``. Columns: Ticker (bold),
    RS-Ratio (right-justified, 2 decimals), Trend (with emoji prefix),
    Momentum Short/Medium/Long (colored, no emoji), and Building
    (🚀 when True, empty otherwise). Sorted by building descending, then
    momentum_short descending.
    """
    table = Table(title=f"🐎 vs {benchmark} Momentum", header_style="bold")
    table.add_column("Ticker", style="bold")
    table.add_column("RS-Ratio", justify="right")
    table.add_column("Trend")
    table.add_column("Momentum Short", justify="right")
    table.add_column("Momentum Medium", justify="right")
    table.add_column("Momentum Long", justify="right")
    table.add_column("Building")

    if relative_trend.is_empty():
        return table

    sorted_df = relative_trend.sort(
        ["building", "momentum_short"], descending=[True, True], nulls_last=True
    )

    trend_emoji = {
        "Leading": "🟢",
        "Fading": "🟠",
        "Improving": "🟡",
        "Lagging": "🔴",
        "Unknown": "❓",
    }

    for row in sorted_df.iter_rows(named=True):
        trend_label = row["trend"]
        emoji = trend_emoji.get(trend_label, "")
        trend_str = f"{emoji} {trend_label}" if emoji else trend_label

        building_marker = "🚀" if row["building"] else ""

        # m-4: null-safe rendering for rs_ratio and momentum values
        rs_ratio_str = "n/a" if row["rs_ratio"] is None else f"{row['rs_ratio']:.2f}"

        table.add_row(
            row["ticker"],
            rs_ratio_str,
            trend_str,
            _fmt_or_na(row["momentum_short"], _fmt_momentum),
            _fmt_or_na(row["momentum_medium"], _fmt_momentum),
            _fmt_or_na(row["momentum_long"], _fmt_momentum),
            building_marker,
        )

    return table
