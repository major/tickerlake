"""ETL pipeline orchestration: backfill, update, and info commands."""

import datetime
import logging
from pathlib import Path

from tickerlake.calendar import get_trading_days
from tickerlake.client import MassiveClient
from tickerlake.config import Config
from tickerlake.extract import extract_daily_aggs, extract_splits, extract_tickers
from tickerlake.load import (
    append_raw_db,
    compact_raw_db,
    get_db_info,
    read_raw_db,
    write_consumer_db,
    write_raw_db,
)
from tickerlake.transform import adjust_splits, compute_metrics, filter_tickers

logger = logging.getLogger(__name__)


def _run_backfill(config: Config) -> None:
    """Execute the full extract-transform-load backfill sequence."""
    dates = get_trading_days(config.start_date, config.end_date)
    if not dates:
        logger.warning("No trading days in the requested date range.")
        return

    logger.info(
        "Backfill: %s to %s (%d trading days)",
        config.start_date,
        config.end_date,
        len(dates),
    )
    client = MassiveClient(config)

    logger.info("Extracting daily bars...")
    bars = extract_daily_aggs(client, dates)
    logger.info("Extracting splits (%s to %s)...", config.start_date, config.end_date)
    splits = extract_splits(client, config.start_date, config.end_date)
    logger.info("Extracting tickers (types: %s)...", ", ".join(config.ticker_types))
    tickers = extract_tickers(client, config.ticker_types)

    logger.info("Adjusting for %d splits...", len(splits))
    bars = adjust_splits(bars, splits)
    logger.info("Filtering to known tickers...")
    bars = filter_tickers(bars, tickers)
    logger.info("Computing metrics (SMA-50, SMA-200, ATR-14, ATR%%, RS, VARS)...")
    metrics = compute_metrics(bars)

    raw_path = config.output_dir / "raw.duckdb"
    consumer_path = config.output_dir / "tickerlake.duckdb"

    logger.info("Writing raw DB to %s...", raw_path)
    write_raw_db(bars, raw_path)
    logger.info("Writing consumer DB to %s...", consumer_path)
    write_consumer_db(bars, metrics, tickers, consumer_path)

    n_tickers = bars["ticker"].n_unique()
    logger.info(
        "Backfill complete: %s bars, %s tickers",
        f"{len(bars):,}",
        f"{n_tickers:,}",
    )


def backfill(config: Config) -> None:
    """Run a full backfill of the ETL pipeline from scratch."""
    _run_backfill(config)


def update(config: Config) -> None:
    """Incrementally update raw.duckdb with new trading days, then rebuild consumer db."""
    raw_path = config.output_dir / "raw.duckdb"

    if not raw_path.exists():
        logger.warning("No raw.duckdb found, running backfill...")
        _run_backfill(config)
        return

    existing_bars = read_raw_db(raw_path)
    max_date: datetime.date = existing_bars["date"].max()  # type: ignore[assignment]
    next_day = max_date + datetime.timedelta(days=1)

    dates = get_trading_days(next_day, config.end_date)
    if not dates:
        logger.warning("Already up to date.")
        return

    logger.info(
        "Update: %s to %s (%d new trading days)",
        next_day,
        config.end_date,
        len(dates),
    )
    client = MassiveClient(config)

    logger.info("Extracting new daily bars...")
    new_bars = extract_daily_aggs(client, dates)
    logger.info("Appending to raw DB...")
    append_raw_db(new_bars, raw_path)

    logger.info("Rebuilding consumer DB from full raw dataset...")
    all_bars = read_raw_db(raw_path)
    logger.info("Extracting splits (%s to %s)...", config.start_date, config.end_date)
    splits = extract_splits(client, config.start_date, config.end_date)
    logger.info("Extracting tickers (types: %s)...", ", ".join(config.ticker_types))
    tickers = extract_tickers(client, config.ticker_types)

    logger.info("Adjusting for %d splits...", len(splits))
    all_bars = adjust_splits(all_bars, splits)
    logger.info("Filtering to known tickers...")
    all_bars = filter_tickers(all_bars, tickers)
    logger.info("Computing metrics (SMA-50, SMA-200, ATR-14, ATR%%, RS, VARS)...")
    metrics = compute_metrics(all_bars)

    consumer_path = config.output_dir / "tickerlake.duckdb"
    logger.info("Writing consumer DB to %s...", consumer_path)
    write_consumer_db(all_bars, metrics, tickers, consumer_path)

    n_tickers = all_bars["ticker"].n_unique()
    logger.info(
        "Update complete: %s bars, %s tickers",
        f"{len(all_bars):,}",
        f"{n_tickers:,}",
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
