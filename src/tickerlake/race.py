"""Pure polars data layer for the etf-race report.

Reads close prices for a set of tickers from the consumer DuckDB across
daily/weekly/monthly timeframes, then computes the race view:

1. Multi-asset race view (``rebase_to_100``): every ticker's closes are
   rebased to 100 at the first bar of its window for the diagnostic RS view.

Also provides relative race metrics (pace, position, places gained, and race
form) and Rich rendering helpers for the horse-race table.

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
from rich.text import Text

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

# Horse-race terminology is intentionally kept in the derived report layer;
# these values are not persisted in DuckDB.
HORSE_FORM_COLUMNS = {
    "position": pl.Int64,
    "places_gained": pl.Int64,
    "relative_return_short": pl.Float32,
    "relative_return_medium": pl.Float32,
    "relative_return_long": pl.Float32,
    "staying_power": pl.Float32,
    "momentum_score": pl.Float32,
    "closing_score": pl.Float32,
    "leadership_score": pl.Float32,
    "race_score": pl.Float32,
    "form": pl.Utf8,
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
# Default display cap on the rendered leaderboard (applied as `.head(max_etfs)`
# after sorting by `race_score`). 50 is wide enough to cover every major
# sector/thematic ETF while keeping the leaderboard readable.
_DEFAULT_MAX_ETFS = 50
# Default cap on the pending-overtakes table. 50+ ETFs can produce a flood
# of gap-closing names; 25 is enough to see what's actionable without
# drowning the operator.
_DEFAULT_MAX_PENDING_OVERTAKES = 25

# Detection thresholds (kept module-level so ruff doesn't flag them as
# magic numbers and so the values are discoverable in one place).
_RS_RATIO_BASELINE = 100.0
_MIN_PLACES_TO_CHARGE = 2

# Rendering styles for the horse-race leaderboard: form label -> (emoji,
# row style). The emoji is shown in the Form cell; the style tints the whole
# row. Style is None for forms that render unstyled.
FORM_STYLE: dict[str, tuple[str, str | None]] = {
    "Charging": ("🚀", "green"),
    "Front-runner": ("🏆", "cyan"),
    "Closing ground": ("⚡", "yellow"),
    "Steady": ("➖", None),  # noqa: RUF001 -- deliberate per design spec
    "Losing steam": ("📉", "red"),
    "Fading": ("🍂", "dark_orange"),
    "Back of field": ("🐢", "dim red"),
    "Unknown": ("❔", "dim"),
}

# Race-score color buckets: >= _RACE_SCORE_HIGH is green, >= _RACE_SCORE_LOW
# is yellow, below is red.
_RACE_SCORE_HIGH = 70.0
_RACE_SCORE_LOW = 40.0


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
) -> list[str]:
    """Read every qualifying active, non-leveraged ETF ticker from the consumer DB.

    Filters the ``tickers`` table to ``type='ETF'`` and ``active=true``,
    joins each ticker to its most recent ``daily_metrics`` row, and keeps
    those whose ``volume_sma_20`` is at or above ``min_volume_sma_20``.
    Returns all qualifying tickers (no cap) ordered by ``volume_sma_20``
    descending (most liquid first), then ticker ascending as a tiebreaker.
    Raises ``ValueError`` when the consumer DB is missing or the required
    tables aren't present.
    """
    if min_volume_sma_20 < 0:
        msg = "min_volume_sma_20 must be >= 0"
        raise ValueError(msg)
    if not consumer_path.exists():
        msg = f"Consumer DB not found: {consumer_path}"
        raise ValueError(msg)

    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as f:
        tmp = Path(f.name)
    try:
        con = duckdb.connect(str(consumer_path), read_only=True)
        try:
            con.execute(
                "COPY ("  # noqa: S608 -- tables and threshold are internal constants
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
                "  AND NOT regexp_matches(lower(t.name), "
                "'(^|[^a-z0-9])(1x|2x|3x|inverse|leverage|leveraged)"
                "([^a-z0-9]|$)') "
                "  AND NOT regexp_matches(lower(t.name), "
                "'(^|[^a-z0-9])proshares[[:space:]]+ultra"
                "(pro|short)?([^a-z0-9]|$)') "
                "ORDER BY m.volume_sma_20 DESC, t.ticker"
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


def compute_relative_race_metrics(
    ratio_bars: pl.DataFrame,
    *,
    short_window: int,
    medium_window: int,
    long_window: int,
) -> pl.DataFrame:
    """Add comparable pace, position, places-gained, and race scores.

    ``ratio_bars`` contains the raw ETF/benchmark ratio.  Returns are
    percentage changes in that ratio, so they are comparable across horses;
    unlike the rebased RS-Ratio level, they do not depend on when a ticker
    first appeared in the input.
    """
    if ratio_bars.is_empty():
        return pl.DataFrame(schema={"ticker": pl.Utf8, **HORSE_FORM_COLUMNS})

    windows = {
        "short": short_window,
        "medium": medium_window,
        "long": long_window,
    }
    working = ratio_bars.sort(["ticker", "date"])
    for label, window in windows.items():
        working = working.with_columns(
            ((pl.col("close") / pl.col("close").shift(window).over("ticker")) - 1)
            .mul(100)
            .cast(pl.Float32)
            .alias(f"relative_return_{label}")
        )
        working = working.with_columns(
            pl.col(f"relative_return_{label}")
            .rank("ordinal", descending=True)
            .over("date")
            .cast(pl.Int64)
            .alias(f"position_{label}")
        )

    latest_date = working.select(pl.col("date").max()).item()
    current = working.filter(pl.col("date") == latest_date)
    if current.is_empty():
        return pl.DataFrame(schema={"ticker": pl.Utf8, **HORSE_FORM_COLUMNS})

    n_horses = current.height
    score_denominator = max(1, n_horses - 1)
    current = current.with_columns(
        pl.col("position_long").alias("position"),
        (
            pl.col("position_medium").shift(medium_window).over("ticker")
            - pl.col("position_medium")
        ).alias("places_gained"),
    )

    # The current rows have no prior rows after filtering to latest_date, so
    # obtain the historical medium-race positions before joining current rows.
    historical = working.select(
        "ticker",
        "date",
        places_gained=pl.col("position_medium"),
    )
    prior = (
        historical.with_columns(
            pl.col("places_gained").shift(medium_window).over("ticker").alias("prior")
        )
        .filter(pl.col("date") == latest_date)
        .select("ticker", places_gained=pl.col("prior") - pl.col("places_gained"))
    )
    current = current.drop("places_gained").join(prior, on="ticker", how="left")

    staying = working.group_by("ticker").agg(
        pl.col("relative_return_medium")
        .gt(0)
        .cast(pl.Float32)
        .mean()
        .mul(100)
        .alias("staying_power")
    )
    current = current.join(staying, on="ticker", how="left")

    for label in windows:
        current = current.with_columns(
            (
                (n_horses - pl.col(f"position_{label}")).cast(pl.Float32)
                / score_denominator
                * 100
            )
            .clip(0, 100)
            .alias(f"{label}_score")
        )

    current = (
        current.with_columns(
            (50 + pl.col("places_gained").cast(pl.Float32) / score_denominator * 50)
            .clip(0, 100)
            .alias("closing_score"),
            pl.col("long_score").alias("leadership_score"),
        )
        .with_columns(
            (pl.col("short_score") + pl.col("medium_score") + pl.col("long_score"))
            .truediv(3)
            .alias("momentum_score")
        )
        .with_columns(
            (
                pl.col("momentum_score") * 0.45
                + pl.col("closing_score") * 0.35
                + pl.col("staying_power") * 0.20
            )
            .cast(pl.Float32)
            .alias("race_score")
        )
    )

    rebased = rebase_to_100(ratio_bars)
    momentum = compute_relative_momentum(
        rebased,
        short_window=short_window,
        medium_window=medium_window,
        long_window=long_window,
    )
    return momentum.join(
        current.select(
            "ticker",
            "position",
            "places_gained",
            "relative_return_short",
            "relative_return_medium",
            "relative_return_long",
            "staying_power",
            "momentum_score",
            "closing_score",
            "leadership_score",
            "race_score",
        ),
        on="ticker",
        how="inner",
    )


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


def classify_horse_form(metrics: pl.DataFrame) -> pl.DataFrame:
    """Classify the field using accessible horse-racing terminology."""
    if metrics.is_empty():
        return metrics.with_columns(pl.lit(None, dtype=pl.Utf8).alias("form"))

    field_size = metrics.height
    front_cutoff = max(1, (field_size + 4) // 5)
    form = (
        pl.when(pl.col("position").is_null())
        .then(pl.lit("Unknown"))
        .when((pl.col("places_gained") >= _MIN_PLACES_TO_CHARGE) & pl.col("building"))
        .then(pl.lit("Charging"))
        .when(
            (pl.col("position") <= front_cutoff)
            & (pl.col("relative_return_short") >= 0)
            & (pl.col("relative_return_medium") >= 0)
        )
        .then(pl.lit("Front-runner"))
        .when(pl.col("places_gained") >= _MIN_PLACES_TO_CHARGE)
        .then(pl.lit("Closing ground"))
        .when(
            (pl.col("position") <= front_cutoff) & (pl.col("relative_return_short") < 0)
        )
        .then(pl.lit("Losing steam"))
        .when((pl.col("places_gained") < 0) & (pl.col("relative_return_short") < 0))
        .then(pl.lit("Losing steam"))
        .when(pl.col("places_gained") < 0)
        .then(pl.lit("Fading"))
        .when(pl.col("position") > field_size * 0.8)
        .then(pl.lit("Back of field"))
        .otherwise(pl.lit("Steady"))
        .alias("form")
    )
    return metrics.with_columns(form)


def _fmt_or_na(value: float | None, formatter: Callable[[float], str]) -> str:
    """Format a value using the given formatter, or return 'n/a' if null."""
    return "n/a" if value is None else formatter(value)


def _form_style(form: str | None) -> tuple[str, str | None]:
    """Return (emoji, row style) for a horse form; Unknown for null/unknown."""
    return FORM_STYLE.get(form, FORM_STYLE["Unknown"])


def _pace_style(value: float | None) -> str | None:
    """Return a Rich style for a pace value: green for gains, red for losses."""
    if value is None or value == 0:
        return None
    return "green" if value > 0 else "red"


def _race_score_style(value: float | None) -> str | None:
    """Return a Rich style for a race score bucket (green/yellow/red)."""
    if value is None:
        return None
    if value >= _RACE_SCORE_HIGH:
        return "green"
    if value >= _RACE_SCORE_LOW:
        return "yellow"
    return "red"


def render_relative_leaderboard(
    relative_trend: pl.DataFrame,
    *,
    benchmark: str,
    max_etfs: int | None = _DEFAULT_MAX_ETFS,
) -> Table:
    """Build a Rich Table for relative momentum vs a benchmark.

    The table uses horse-race language: position, places gained, pace over
    three windows, race score, and form. Diagnostic RS-Ratio, trend, raw
    momentum, and building columns are intentionally omitted. ``max_etfs``
    caps the displayed rows after sorting (default: ``_DEFAULT_MAX_ETFS``,
    pass ``None`` for unlimited); it is a display limit only, and does not
    restrict the underlying computation.
    """
    table = Table(title=f"🐎 vs {benchmark} Momentum", header_style="bold")
    table.add_column("Ticker", style="bold")
    table.add_column("Pos", justify="right")
    table.add_column("Places", justify="right")
    table.add_column("Pace Short", justify="right")
    table.add_column("Pace Medium", justify="right")
    table.add_column("Pace Long", justify="right")
    table.add_column("Race", justify="right")
    table.add_column("Form")

    if relative_trend.is_empty():
        return table

    if "race_score" in relative_trend.columns:
        sorted_df = relative_trend.sort("race_score", descending=True, nulls_last=True)
    else:
        sorted_df = relative_trend.sort(
            ["building", "momentum_short"], descending=[True, True], nulls_last=True
        )
    if max_etfs is not None:
        sorted_df = sorted_df.head(max_etfs)

    for row in sorted_df.iter_rows(named=True):
        emoji, row_style = _form_style(row.get("form"))
        form_label = row.get("form") or "Unknown"
        table.add_row(
            row["ticker"],
            _fmt_or_na(row.get("position"), lambda value: str(int(value))),
            _fmt_or_na(row.get("places_gained"), lambda value: f"{int(value):+d}"),
            Text(
                _fmt_or_na(
                    row.get("relative_return_short"), lambda value: f"{value:+.1f}%"
                ),
                style=_pace_style(row.get("relative_return_short")),  # ty: ignore[invalid-argument-type]
            ),
            Text(
                _fmt_or_na(
                    row.get("relative_return_medium"), lambda value: f"{value:+.1f}%"
                ),
                style=_pace_style(row.get("relative_return_medium")),  # ty: ignore[invalid-argument-type]
            ),
            Text(
                _fmt_or_na(
                    row.get("relative_return_long"), lambda value: f"{value:+.1f}%"
                ),
                style=_pace_style(row.get("relative_return_long")),  # ty: ignore[invalid-argument-type]
            ),
            Text(
                _fmt_or_na(row.get("race_score"), lambda value: f"{value:.0f}"),
                style=_race_score_style(row.get("race_score")),  # ty: ignore[invalid-argument-type]
            ),
            f"{emoji} {form_label}",
            style=row_style,
        )

    return table
