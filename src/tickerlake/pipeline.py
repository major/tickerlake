"""ETL pipeline orchestration: backfill, update, and info commands."""

from __future__ import annotations

import datetime
import logging
from typing import TYPE_CHECKING

import duckdb
import polars as pl

from tickerlake.calendar import get_trading_days
from tickerlake.client import MassiveClient
from tickerlake.extract import TICKERS_SCHEMA, extract_daily_aggs, extract_splits, extract_tickers
from tickerlake.load import (
    compact_raw_db,
    get_db_info,
    get_existing_dates,
    read_raw_db,
    read_splits,
    replace_raw_dates,
    write_consumer_db,
    write_raw_db,
    write_splits,
)
from tickerlake.outcomes import FetchOutcome, FetchStatus
from tickerlake.transform import (
    adjust_splits,
    aggregate_to_monthly,
    aggregate_to_weekly,
    compute_metrics,
    filter_tickers,
)

if TYPE_CHECKING:
    from pathlib import Path

    from tickerlake.config import Config

logger = logging.getLogger(__name__)

_SPOT_CHECK_SAMPLE_SIZE = 5
_SPOT_CHECK_TOLERANCE = 1e-3
# Massive revises published daily bars up to this many trading days after
# initial publication; always refresh this trailing window on every run.
_REVISION_WINDOW_DAYS = 5


class ExtractionIncompleteError(RuntimeError):
    """Raised when source outcomes do not support a complete consumer rebuild."""

    def __init__(self, source: str, outcomes: list[FetchOutcome]) -> None:
        """Build a concise domain error from the source outcomes that blocked publication."""
        self.outcomes = outcomes
        super().__init__(f"{source} extraction did not validate all expected data: {_outcome_summary(outcomes)}")


def _outcome_summary(outcomes: list[FetchOutcome]) -> str:
    """Describe non-publishable outcomes without exposing source records."""
    return "; ".join(
        f"{outcome.requested_date or 'reference'}={outcome.status.value}"
        + (f" ({outcome.diagnostic})" if outcome.diagnostic else "")
        for outcome in outcomes
        if outcome.status in {FetchStatus.failed, FetchStatus.quarantined, FetchStatus.successful_empty}
    )


def _read_previous_tickers(path: Path, types: list[str]) -> pl.DataFrame | None:
    """Read only the previously published ticker rows in the requested catalog scope."""
    if not path.exists():
        return None

    con = duckdb.connect(str(path), read_only=True)
    try:
        tables = {row[0] for row in con.execute("SHOW TABLES").fetchall()}
        if "tickers" not in tables:
            return None
        rows = con.execute(
            "SELECT ticker, name, type, primary_exchange, cik, active FROM tickers WHERE type IN ?",
            [types],
        ).fetchall()
        return pl.DataFrame(rows, schema=TICKERS_SCHEMA, orient="row")
    finally:
        con.close()


def _split_fetch_bounds(
    config: Config, retained_start: datetime.date, retained_end: datetime.date, previous: pl.DataFrame | None
) -> tuple[datetime.date, datetime.date]:
    """Cover configured dates, all retained bars, and every cached split event."""
    starts: list[datetime.date] = [config.start_date, retained_start]
    ends: list[datetime.date] = [config.end_date, retained_end]
    if previous is not None and not previous.is_empty():
        previous_start = previous["execution_date"].min()
        previous_end = previous["execution_date"].max()
        if not isinstance(previous_start, datetime.date) or not isinstance(previous_end, datetime.date):
            raise ExtractionIncompleteError("Split", [])
        starts.append(previous_start)
        ends.append(previous_end)
    return min(starts), max(ends)


def _verify_split_adjustment(raw_bars: pl.DataFrame, adjusted_bars: pl.DataFrame, splits: pl.DataFrame) -> None:
    """Spot-check that split adjustment factors were applied correctly.

    Samples tickers with the most extreme (smallest) cumulative adjustment
    factors and verifies the adjusted/raw close ratio matches the expected
    factor within tolerance. Raises ValueError on mismatch.
    """
    if splits.is_empty():
        return

    sample = splits.filter(pl.col("adjustment_factor").is_between(0.02, 0.5, closed="left")).sort("adjustment_factor")
    seen: set[str] = set()
    verified = 0

    for row in sample.iter_rows(named=True):
        ticker = row["ticker"]
        if ticker in seen:
            continue
        seen.add(ticker)

        pre_split = raw_bars.filter((pl.col("ticker") == ticker) & (pl.col("date") < row["execution_date"]))
        if pre_split.is_empty():
            continue

        check_date = pre_split["date"].max()
        raw_close = float(pre_split.filter(pl.col("date") == check_date)["close"][0])
        adj_row = adjusted_bars.filter((pl.col("ticker") == ticker) & (pl.col("date") == check_date))
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
        logger.info("Split adjustment spot check passed (%d tickers verified).", verified)


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

    daily_outcomes: list[FetchOutcome] = []
    if fetch_dates:
        logger.info("Extracting %d dates (missing + refresh window)...", len(fetch_dates))
        previous = read_raw_db(raw_path) if raw_path.exists() and existing_dates else None
        daily_outcomes = extract_daily_aggs(client, sorted(fetch_dates), previous=previous)
        populated = [outcome for outcome in daily_outcomes if outcome.status is FetchStatus.populated]
        if populated:
            new_raw_bars = pl.concat([outcome.frame for outcome in populated])
            dates_to_delete = {
                date for outcome in populated if (date := outcome.requested_date) is not None and date in existing_dates
            }
            if raw_path.exists() and existing_dates:
                replace_raw_dates(new_raw_bars, raw_path, dates_to_delete)
            else:
                write_raw_db(new_raw_bars, raw_path)
    else:
        logger.info("All dates cached, skipping extraction.")

    unacceptable = [outcome for outcome in daily_outcomes if outcome.status is not FetchStatus.populated]
    if unacceptable:
        raise ExtractionIncompleteError("Daily", unacceptable)

    _rebuild_consumer_database(config, client, raw_path, consumer_path)


def _rebuild_consumer_database(config: Config, client: MassiveClient, raw_path: Path, consumer_path: Path) -> None:
    """Rebuild the consumer database from raw bars and current reference data."""
    logger.info("Loading raw bars for transform...")
    all_bars = read_raw_db(raw_path)

    retained_start = all_bars["date"].min()
    retained_end = all_bars["date"].max()
    if not isinstance(retained_start, datetime.date) or not isinstance(retained_end, datetime.date):
        raise ExtractionIncompleteError("Daily", [])

    previous_splits = (
        read_splits(raw_path) if raw_path.exists() and "splits" in get_db_info(raw_path)["tables"] else None
    )
    splits_start, splits_end = _split_fetch_bounds(config, retained_start, retained_end, previous_splits)
    logger.info("Extracting splits (%s to %s)...", splits_start, splits_end)
    split_outcome = extract_splits(client, splits_start, splits_end, previous=previous_splits)
    logger.info("Extracting tickers (types: %s)...", ", ".join(config.ticker_types))
    previous_tickers = _read_previous_tickers(consumer_path, config.ticker_types)
    ticker_outcome = extract_tickers(client, config.ticker_types, previous=previous_tickers)
    if split_outcome.status in {FetchStatus.failed, FetchStatus.quarantined}:
        raise ExtractionIncompleteError("Split", [split_outcome])
    if ticker_outcome.status is not FetchStatus.populated:
        raise ExtractionIncompleteError("Ticker", [ticker_outcome])
    splits = split_outcome.frame
    tickers = ticker_outcome.frame
    if split_outcome.status is FetchStatus.populated:
        logger.info("Persisting %d splits to %s...", len(splits), raw_path)
        write_splits(splits, raw_path)
    elif previous_splits is None or previous_splits.is_empty():
        logger.info("Persisting empty split cache to %s...", raw_path)
        write_splits(splits, raw_path)

    logger.info("Adjusting for %d splits...", len(splits))
    bars = adjust_splits(all_bars, splits)
    _verify_split_adjustment(all_bars, bars, splits)
    logger.info("Filtering to known tickers...")
    bars = filter_tickers(bars, tickers)
    logger.info("Computing metrics (SMA-50, SMA-200, ATR-14, ATR%%)...")
    metrics = compute_metrics(bars)
    logger.info("Aggregating weekly bars...")
    weekly_bars = aggregate_to_weekly(bars, collection_start=retained_start, target=config.end_date)
    logger.info("Computing weekly metrics...")
    weekly_metrics = compute_metrics(weekly_bars)
    logger.info("Aggregating monthly bars...")
    monthly_bars = aggregate_to_monthly(bars, collection_start=retained_start, target=config.end_date)
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
    """Incrementally update raw.duckdb with new trading days, then rebuild consumer db."""
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


def _require_api_key(config: Config) -> None:
    """Raise a clear error when a Massive API command lacks credentials."""
    if not config.api_key:
        msg = "MASSIVE_API_KEY environment variable is required"
        raise ValueError(msg)


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
