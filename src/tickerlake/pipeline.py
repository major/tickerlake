"""ETL pipeline orchestration: backfill, update, and info commands."""

from __future__ import annotations

import logging
import tempfile
from pathlib import Path
from typing import TYPE_CHECKING

import duckdb
import polars as pl
from rich.table import Table

from tickerlake import console
from tickerlake.calendar import get_trading_days
from tickerlake.client import MassiveClient
from tickerlake.extract import extract_daily_aggs, extract_splits, extract_tickers
from tickerlake.fib_zones import WEEKLY_FIB_ZONES_SCHEMA, compute_weekly_fib_zones_all
from tickerlake.load import (
    append_raw_db,
    compact_raw_db,
    delete_raw_dates,
    get_db_info,
    get_existing_dates,
    read_adjusted_daily_bars_for_ticker,
    read_raw_db,
    read_weekly_fib_zones,
    write_consumer_db,
    write_raw_db,
    write_splits,
    write_weekly_fib_zones,
)
from tickerlake.transform import (
    VALID_TIMEFRAMES,
    adjust_splits,
    aggregate_to_monthly,
    aggregate_to_weekly,
    bars_for_timeframe,
    compute_metrics,
    filter_tickers,
    find_pivots,
)

if TYPE_CHECKING:
    import datetime

    from tickerlake.config import Config

logger = logging.getLogger(__name__)

_SPOT_CHECK_SAMPLE_SIZE = 5
_SPOT_CHECK_TOLERANCE = 1e-3
# Massive revises published daily bars up to this many trading days after
# initial publication; always refresh this trailing window on every run.
_REVISION_WINDOW_DAYS = 5
# Liquidity gate for weekly fib zones: a ticker must average at least this many
# shares per week on its latest weekly_metrics row to be eligible.
_LIQUIDITY_VOLUME_THRESHOLD = 1_000_000.0
# Zone values shown by default in `screen` (zone="all"); rows whose primary
# degree is void are additionally excluded because their swing-low setup has
# already broken.
_ACTIONABLE_ZONES = ["in_ibz", "in_smz", "below_smz"]


def _verify_split_adjustment(
    raw_bars: pl.DataFrame, adjusted_bars: pl.DataFrame, splits: pl.DataFrame
) -> None:
    """Spot-check that split adjustment factors were applied correctly.

    Samples tickers with the most extreme (smallest) cumulative adjustment
    factors and verifies the adjusted/raw close ratio matches the expected
    factor within tolerance. Raises ValueError on mismatch.
    """
    if splits.is_empty():
        return

    sample = splits.filter(
        pl.col("adjustment_factor").is_between(0.02, 0.5, closed="left")
    ).sort("adjustment_factor")
    seen: set[str] = set()
    verified = 0

    for row in sample.iter_rows(named=True):
        ticker = row["ticker"]
        if ticker in seen:
            continue
        seen.add(ticker)

        pre_split = raw_bars.filter(
            (pl.col("ticker") == ticker) & (pl.col("date") < row["execution_date"])
        )
        if pre_split.is_empty():
            continue

        check_date = pre_split["date"].max()
        raw_close = float(pre_split.filter(pl.col("date") == check_date)["close"][0])
        adj_row = adjusted_bars.filter(
            (pl.col("ticker") == ticker) & (pl.col("date") == check_date)
        )
        if adj_row.is_empty():
            continue

        adj_close = float(adj_row["close"][0])
        expected = float(row["adjustment_factor"])
        actual = adj_close / raw_close

        if abs(actual - expected) / abs(expected) > _SPOT_CHECK_TOLERANCE:
            msg = (
                f"Split adjustment spot check failed: {ticker} on {check_date} "
                f"expected factor {expected:.6f}, got {actual:.6f} "
                f"(raw={raw_close:.2f}, adjusted={adj_close:.2f})"
            )
            raise ValueError(msg)
        verified += 1
        if verified >= _SPOT_CHECK_SAMPLE_SIZE:
            break

    if verified > 0:
        logger.info(
            "Split adjustment spot check passed (%d tickers verified).", verified
        )


def _run_backfill(config: Config, *, bars_start: datetime.date | None = None) -> None:
    """Execute the full extract-transform-load backfill sequence.

    Args:
        config: Configuration object.
        bars_start: Optional start date for bars extraction. If None, uses
                   config.start_date. Splits and tickers extraction always use
                   config.start_date/config.end_date.
    """
    dates = get_trading_days(bars_start or config.start_date, config.end_date)
    if not dates:
        logger.warning("No trading days in the requested date range.")
        return

    raw_path = config.output_dir / "raw.duckdb"
    consumer_path = config.output_dir / "tickerlake.duckdb"

    requested_dates = set(dates)
    existing_dates = get_existing_dates(raw_path)
    cached_dates = existing_dates & requested_dates
    refresh_dates = set(sorted(cached_dates)[-_REVISION_WINDOW_DAYS:])
    fetch_dates = (requested_dates - cached_dates) | refresh_dates

    logger.info(
        "Backfill: %s to %s (%d trading days, %d cached, %d to fetch)",
        bars_start or config.start_date,
        config.end_date,
        len(dates),
        len(cached_dates),
        len(fetch_dates),
    )
    client = MassiveClient(config)

    if fetch_dates and not existing_dates:
        logger.info("Extracting daily bars...")
        raw_bars = extract_daily_aggs(client, sorted(fetch_dates))
        logger.info("Writing raw DB to %s...", raw_path)
        write_raw_db(raw_bars, raw_path)
    elif fetch_dates:
        logger.info(
            "Extracting %d dates (missing + refresh window)...", len(fetch_dates)
        )
        new_raw_bars = extract_daily_aggs(client, sorted(fetch_dates))

        # Delete only the dates that are both actually present in the newly-fetched
        # data AND already in the table (never delete a date the table doesn't have).
        fetched_dates = set(new_raw_bars["date"].unique().to_list())
        dates_to_delete = fetched_dates & existing_dates
        if dates_to_delete:
            logger.info(
                "Deleting %d refreshed dates from raw DB before appending.",
                len(dates_to_delete),
            )
            delete_raw_dates(raw_path, dates_to_delete)

        logger.info("Appending to raw DB at %s...", raw_path)
        append_raw_db(new_raw_bars, raw_path)
    else:
        logger.info("All dates cached, skipping extraction.")

    logger.info("Loading raw bars for transform...")
    all_bars = read_raw_db(raw_path)

    logger.info("Extracting splits (%s to %s)...", config.start_date, config.end_date)
    splits = extract_splits(client, config.start_date, config.end_date)
    logger.info("Persisting %d splits to %s...", len(splits), raw_path)
    write_splits(splits, raw_path)
    logger.info("Extracting tickers (types: %s)...", ", ".join(config.ticker_types))
    tickers = extract_tickers(client, config.ticker_types)

    logger.info("Adjusting for %d splits...", len(splits))
    bars = adjust_splits(all_bars, splits)
    _verify_split_adjustment(all_bars, bars, splits)
    logger.info("Filtering to known tickers...")
    bars = filter_tickers(bars, tickers)
    logger.info("Computing metrics (SMA-50, SMA-200, ATR-14, ATR%%)...")
    metrics = compute_metrics(bars)
    logger.info("Aggregating weekly bars...")
    weekly_bars = aggregate_to_weekly(bars)
    logger.info("Computing weekly metrics...")
    weekly_metrics = compute_metrics(weekly_bars)
    logger.info("Aggregating monthly bars...")
    monthly_bars = aggregate_to_monthly(bars)
    logger.info("Computing monthly metrics...")
    monthly_metrics = compute_metrics(monthly_bars)

    logger.info("Writing consumer DB to %s...", consumer_path)
    write_consumer_db(
        bars,
        metrics,
        tickers,
        consumer_path,
        weekly_bars=weekly_bars,
        weekly_metrics=weekly_metrics,
        monthly_bars=monthly_bars,
        monthly_metrics=monthly_metrics,
    )

    n_tickers = bars["ticker"].n_unique()
    logger.info(
        "Backfill complete: %s bars, %s tickers",
        f"{len(bars):,}",
        f"{n_tickers:,}",
    )


def backfill(config: Config) -> None:
    """Run a full backfill of the ETL pipeline from scratch."""
    _require_api_key(config)
    _run_backfill(config)


def update(config: Config) -> None:
    """Incrementally update raw.duckdb with new trading days, then rebuild consumer db."""  # noqa: E501
    _require_api_key(config)
    raw_path = config.output_dir / "raw.duckdb"

    if not raw_path.exists():
        logger.warning("No raw.duckdb found, running backfill...")
        _run_backfill(config)
        return

    cached_dates = get_existing_dates(raw_path)
    if not cached_dates:
        logger.warning("raw.duckdb exists but is empty, running backfill...")
        _run_backfill(config)
        return

    # Compute the start of the revision window: the earliest date in the last
    # _REVISION_WINDOW_DAYS cached dates. Re-fetch from there to pick up any
    # revisions Massive made to those dates.
    window_start = min(sorted(cached_dates)[-_REVISION_WINDOW_DAYS:])
    logger.info(
        "Update: re-fetching revision window from %s, then new dates through %s",
        window_start,
        config.end_date,
    )
    _run_backfill(config, bars_start=window_start)


def find_ticker_pivots(
    config: Config, ticker: str, timeframe: str = "weekly", k: int = 4
) -> pl.DataFrame:
    """Return pivots for a ticker/timeframe from adjusted consumer daily bars."""
    if k < 1:
        msg = "k must be >= 1"
        raise ValueError(msg)
    if timeframe not in VALID_TIMEFRAMES:
        msg = f"timeframe must be one of: {', '.join(sorted(VALID_TIMEFRAMES))}"
        raise ValueError(msg)
    bars = read_adjusted_daily_bars_for_ticker(
        config.output_dir / "tickerlake.duckdb", ticker
    )
    timeframe_bars = bars_for_timeframe(bars, timeframe)
    return find_pivots(timeframe_bars, k=k)


def _require_api_key(config: Config) -> None:
    """Raise a clear error when a Massive API command lacks credentials."""
    if not config.api_key:
        msg = "MASSIVE_API_KEY environment variable is required"
        raise ValueError(msg)


def pivots(config: Config, ticker: str, timeframe: str = "weekly", k: int = 4) -> None:
    """Log confirmed pivots for a ticker/timeframe without writing any data."""
    ticker = ticker.upper()
    result = find_ticker_pivots(config, ticker, timeframe, k)
    if result.is_empty():
        logger.warning("No pivots found for %s (%s, k=%d).", ticker, timeframe, k)
        return

    logger.info("Pivots for %s (%s, k=%d):", ticker, timeframe, k)
    for row in result.iter_rows(named=True):
        logger.info(
            "%s %-4s %10.2f confirmed_at=%s",
            row["date"],
            row["pivot_type"],
            row["price"],
            row["confirmed_at"],
        )


def _log_db_info(label: str, path: Path) -> None:
    """Log database info for a single DuckDB file."""
    if not path.exists():
        logger.info("%s: not found (%s)", label, path)
        return

    db_info = get_db_info(path)
    logger.info("%s: %s", label, path)
    for table in db_info["tables"]:
        row_count = db_info["row_counts"].get(table, 0)
        logger.info("  %s: %s rows", table, f"{row_count:,}")
        if table in db_info.get("date_range", {}):
            dr = db_info["date_range"][table]
            logger.info("    dates: %s to %s", dr["min"], dr["max"])
    size_mb = db_info["file_size_bytes"] / (1024 * 1024)
    logger.info("  size: %.1f MB", size_mb)


def compact(config: Config) -> None:
    """Rebuild raw.duckdb to reclaim space and optimize compression."""
    raw_path = config.output_dir / "raw.duckdb"

    if not raw_path.exists():
        logger.warning("No raw.duckdb found at %s", raw_path)
        return

    size_before = raw_path.stat().st_size
    logger.info("Compacting %s (%.1f MB)...", raw_path, size_before / (1024 * 1024))
    compact_raw_db(raw_path)
    size_after = raw_path.stat().st_size

    saved = size_before - size_after
    pct = (saved / size_before * 100) if size_before > 0 else 0
    logger.info(
        "Done: %.1f MB (saved %.1f MB, %.0f%%)",
        size_after / (1024 * 1024),
        saved / (1024 * 1024),
        pct,
    )


def info(config: Config) -> None:
    """Print metadata about existing DuckDB files."""
    _log_db_info("raw.duckdb", config.output_dir / "raw.duckdb")
    _log_db_info("tickerlake.duckdb", config.output_dir / "tickerlake.duckdb")


def _read_weekly_fib_inputs(consumer_path: Path) -> tuple[pl.DataFrame, set[str]]:
    """Read weekly_bars and the set of liquid tickers from the consumer DB.

    A ticker is liquid when its most recent weekly_metrics row has
    volume_sma_20 >= _LIQUIDITY_VOLUME_THRESHOLD.
    """
    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as bars_file:
        bars_tmp = Path(bars_file.name)
    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as elig_file:
        eligible_tmp = Path(elig_file.name)
    try:
        con = duckdb.connect(str(consumer_path), read_only=True)
        try:
            con.execute(
                "COPY (SELECT * FROM weekly_bars ORDER BY ticker, date) "
                f"TO '{bars_tmp}' (FORMAT PARQUET)"
            )
            con.execute(
                "COPY (SELECT m.ticker FROM weekly_metrics m "
                "JOIN (SELECT ticker, MAX(date) AS max_date "
                "      FROM weekly_metrics GROUP BY ticker) latest "
                "  ON m.ticker = latest.ticker AND m.date = latest.max_date "
                f"WHERE m.volume_sma_20 >= {_LIQUIDITY_VOLUME_THRESHOLD}) "
                f"TO '{eligible_tmp}' (FORMAT PARQUET)"
            )
        except duckdb.CatalogException as err:
            msg = (
                "weekly_bars/weekly_metrics tables not found in consumer DB: "
                f"{consumer_path}. Run backfill first."
            )
            raise ValueError(msg) from err
        finally:
            con.close()
        bars = pl.read_parquet(bars_tmp)
        eligible = set(pl.read_parquet(eligible_tmp)["ticker"].to_list())
        return bars, eligible
    finally:
        bars_tmp.unlink(missing_ok=True)
        eligible_tmp.unlink(missing_ok=True)


def _validate_weekly_fib_zones_schema(df: pl.DataFrame) -> None:
    """Raise ValueError when df does not match WEEKLY_FIB_ZONES_SCHEMA."""
    actual = dict(df.schema)
    mismatches = [
        f"{col}: expected {dtype}, got {actual[col]}"
        for col, dtype in WEEKLY_FIB_ZONES_SCHEMA.items()
        if col in actual and actual[col] != dtype
    ]
    missing = sorted(set(WEEKLY_FIB_ZONES_SCHEMA) - set(actual))
    extra = sorted(set(actual) - set(WEEKLY_FIB_ZONES_SCHEMA))
    problems = [
        *([f"missing columns: {', '.join(missing)}"] if missing else []),
        *([f"unexpected columns: {', '.join(extra)}"] if extra else []),
        *mismatches,
    ]
    if problems:
        msg = "weekly_fib_zones schema mismatch: " + "; ".join(problems)
        raise ValueError(msg)


def compute_weekly_fib_zones(config: Config) -> None:
    """Compute weekly fib zones for all eligible tickers and persist them.

    Eligible tickers are those whose latest weekly_metrics row has
    volume_sma_20 >= _LIQUIDITY_VOLUME_THRESHOLD. All available weekly bars
    are considered (no lookback cap).
    Logs the number of eligible tickers, per-zone counts, void count, and
    rows written.
    """
    consumer_path = config.output_dir / "tickerlake.duckdb"
    if not consumer_path.exists():
        msg = f"Consumer DB not found: {consumer_path}. Run backfill first."
        raise ValueError(msg)

    weekly_bars, eligible_tickers = _read_weekly_fib_inputs(consumer_path)
    logger.info(
        "Weekly fib zones: %d eligible tickers (volume_sma_20 >= %s).",
        len(eligible_tickers),
        f"{_LIQUIDITY_VOLUME_THRESHOLD:,.0f}",
    )

    zones = compute_weekly_fib_zones_all(weekly_bars, eligible_tickers=eligible_tickers)
    _validate_weekly_fib_zones_schema(zones)

    n_in_ibz = int(zones.filter(pl.col("zone") == "in_ibz").height)
    n_in_smz = int(zones.filter(pl.col("zone") == "in_smz").height)
    n_below_smz = int(zones.filter(pl.col("zone") == "below_smz").height)
    n_above_ibz = int(zones.filter(pl.col("zone") == "above_ibz").height)
    n_void = int(zones.filter(pl.col("primary_status") == "void").height)

    write_weekly_fib_zones(zones, consumer_path)

    logger.info(
        "Weekly fib zones written: n_in_ibz=%d, n_in_smz=%d, n_below_smz=%d, "
        "n_above_ibz=%d, n_void=%d, n_written=%d.",
        n_in_ibz,
        n_in_smz,
        n_below_smz,
        n_above_ibz,
        n_void,
        len(zones),
    )


def screen_fib_zones(
    config: Config,
    *,
    zone: str = "all",
    limit: int | None = None,
    min_swing_low: float = 5.0,
) -> None:
    """Print a screen of persisted weekly fib zones to the console.

    zone="all" restricts to the actionable zones (in_ibz, in_smz, below_smz)
    with primary_status != 'void'. Any other zone value filters to that single
    zone regardless of status. Rows whose swing_low is below min_swing_low are
    excluded (default $5 minimum). Results are sorted by ticker and optionally
    capped at `limit` rows.
    """
    consumer_path = config.output_dir / "tickerlake.duckdb"
    if not consumer_path.exists():
        msg = f"Consumer DB not found: {consumer_path}. Run fib-zones compute first."
        raise ValueError(msg)

    if zone == "all":
        result = read_weekly_fib_zones(consumer_path, zone=_ACTIONABLE_ZONES)
        result = result.filter(pl.col("primary_status") != "void")
    else:
        result = read_weekly_fib_zones(consumer_path, zone=zone)
    result = result.filter(pl.col("swing_low") >= min_swing_low)
    result = result.sort("ticker")

    total = result.height
    displayed = result.head(limit) if limit is not None else result

    table = Table(title="Weekly fib zones screen", header_style="bold")
    table.add_column("ticker", style="bold")
    table.add_column("current_price", justify="right")
    table.add_column("zone")
    table.add_column("pct_retracement", justify="right")
    table.add_column("swing_low", justify="right")
    table.add_column("swing_high", justify="right")
    table.add_column("primary_degree", justify="right")
    table.add_column("primary_status")

    for row in displayed.iter_rows(named=True):
        table.add_row(
            row["ticker"],
            f"{row['current_price']:.2f}",
            row["zone"],
            f"{row['pct_retracement']:.2f}%",
            f"{row['swing_low']:.2f}",
            f"{row['swing_high']:.2f}",
            str(row["primary_degree"]),
            row["primary_status"],
        )
    console.print(table)

    limit_label = str(limit) if limit is not None else "unlimited"
    logger.info(
        "Screen: %d total matches, %d displayed (zone=%s, limit=%s, min_swing_low=%s).",
        total,
        displayed.height,
        zone,
        limit_label,
        min_swing_low,
    )
