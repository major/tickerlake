"""ETL pipeline orchestration: backfill, update, and info commands."""

import datetime
import logging
from pathlib import Path

import polars as pl

from tickerlake.calendar import get_trading_days
from tickerlake.client import MassiveClient
from tickerlake.config import Config
from tickerlake.extract import extract_daily_aggs, extract_splits, extract_tickers
from tickerlake.load import (
    append_raw_db,
    compact_raw_db,
    delete_raw_dates,
    get_db_info,
    get_existing_dates,
    read_raw_db,
    write_consumer_db,
    write_raw_db,
    write_splits,
)
from tickerlake.transform import (
    adjust_splits,
    aggregate_to_weekly,
    compute_hvc_vwap_anchors,
    compute_metrics,
    detect_hvcs,
    filter_tickers,
)

logger = logging.getLogger(__name__)

_SPOT_CHECK_SAMPLE_SIZE = 5
_SPOT_CHECK_TOLERANCE = 1e-3
_BACKFILL_REFRESH_DAYS = 5


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
            raise ValueError(
                f"Split adjustment spot check failed: {ticker} on {check_date} "
                f"expected factor {expected:.6f}, got {actual:.6f} "
                f"(raw={raw_close:.2f}, adjusted={adj_close:.2f})"
            )
        verified += 1
        if verified >= _SPOT_CHECK_SAMPLE_SIZE:
            break

    if verified > 0:
        logger.info(
            "Split adjustment spot check passed (%d tickers verified).", verified
        )


def _run_backfill(config: Config) -> None:
    """Execute the full extract-transform-load backfill sequence."""
    dates = get_trading_days(config.start_date, config.end_date)
    if not dates:
        logger.warning("No trading days in the requested date range.")
        return

    raw_path = config.output_dir / "raw.duckdb"
    consumer_path = config.output_dir / "tickerlake.duckdb"

    requested_dates = set(dates)
    existing_dates = get_existing_dates(raw_path)
    cached_dates = existing_dates & requested_dates
    refresh_dates = set(sorted(cached_dates)[-_BACKFILL_REFRESH_DAYS:])

    if refresh_dates:
        logger.info(
            "Dropping %d cached trading days from raw DB for refresh (%s to %s).",
            len(refresh_dates),
            min(refresh_dates),
            max(refresh_dates),
        )
        delete_raw_dates(raw_path, refresh_dates)
        cached_dates -= refresh_dates

    missing_dates = [d for d in dates if d not in cached_dates]

    logger.info(
        "Backfill: %s to %s (%d trading days, %d cached, %d to fetch)",
        config.start_date,
        config.end_date,
        len(dates),
        len(cached_dates),
        len(missing_dates),
    )
    client = MassiveClient(config)

    if missing_dates and not existing_dates:
        logger.info("Extracting daily bars...")
        raw_bars = extract_daily_aggs(client, missing_dates)
        logger.info("Writing raw DB to %s...", raw_path)
        write_raw_db(raw_bars, raw_path)
    elif missing_dates:
        logger.info("Extracting %d missing dates...", len(missing_dates))
        new_raw_bars = extract_daily_aggs(client, missing_dates)
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
    logger.info("Computing metrics (SMA-50, SMA-200, ATR-14, ATR%%, RS, VARS)...")
    metrics = compute_metrics(bars)
    hvcs = detect_hvcs(bars, metrics)
    logger.info("Detected %d high-volume catalyst events.", len(hvcs))
    logger.info("Computing HVC-anchored VWAPs...")
    hvc_vwap_anchors = compute_hvc_vwap_anchors(bars, hvcs)
    logger.info("Computed %d HVC VWAP anchor rows.", len(hvc_vwap_anchors))
    logger.info("Aggregating weekly bars...")
    weekly_bars = aggregate_to_weekly(bars)
    logger.info("Computing weekly metrics...")
    weekly_metrics = compute_metrics(weekly_bars)
    logger.info("Detecting weekly HVCs...")
    weekly_hvcs = detect_hvcs(weekly_bars, weekly_metrics)

    logger.info("Writing consumer DB to %s...", consumer_path)
    write_consumer_db(
        bars,
        metrics,
        tickers,
        consumer_path,
        hvcs=hvcs,
        hvc_vwap_anchors=hvc_vwap_anchors,
        weekly_bars=weekly_bars,
        weekly_metrics=weekly_metrics,
        weekly_hvcs=weekly_hvcs,
    )

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
    max_date_val = existing_bars["date"].max()
    assert isinstance(max_date_val, datetime.date), f"Expected date, got {type(max_date_val)}"
    max_date: datetime.date = max_date_val
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
    logger.info("Persisting %d splits to %s...", len(splits), raw_path)
    write_splits(splits, raw_path)
    logger.info("Extracting tickers (types: %s)...", ", ".join(config.ticker_types))
    tickers = extract_tickers(client, config.ticker_types)

    logger.info("Adjusting for %d splits...", len(splits))
    raw_bars = all_bars
    all_bars = adjust_splits(all_bars, splits)
    _verify_split_adjustment(raw_bars, all_bars, splits)
    del raw_bars
    logger.info("Filtering to known tickers...")
    all_bars = filter_tickers(all_bars, tickers)
    logger.info("Computing metrics (SMA-50, SMA-200, ATR-14, ATR%%, RS, VARS)...")
    metrics = compute_metrics(all_bars)
    hvcs = detect_hvcs(all_bars, metrics)
    logger.info("Detected %d high-volume catalyst events.", len(hvcs))
    logger.info("Computing HVC-anchored VWAPs...")
    hvc_vwap_anchors = compute_hvc_vwap_anchors(all_bars, hvcs)
    logger.info("Computed %d HVC VWAP anchor rows.", len(hvc_vwap_anchors))
    logger.info("Aggregating weekly bars...")
    weekly_bars = aggregate_to_weekly(all_bars)
    logger.info("Computing weekly metrics...")
    weekly_metrics = compute_metrics(weekly_bars)
    logger.info("Detecting weekly HVCs...")
    weekly_hvcs = detect_hvcs(weekly_bars, weekly_metrics)

    consumer_path = config.output_dir / "tickerlake.duckdb"
    logger.info("Writing consumer DB to %s...", consumer_path)
    write_consumer_db(
        all_bars,
        metrics,
        tickers,
        consumer_path,
        hvcs=hvcs,
        hvc_vwap_anchors=hvc_vwap_anchors,
        weekly_bars=weekly_bars,
        weekly_metrics=weekly_metrics,
        weekly_hvcs=weekly_hvcs,
    )

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
