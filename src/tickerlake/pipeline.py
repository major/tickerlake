"""ETL pipeline orchestration: backfill, update, and info commands."""

import datetime
from pathlib import Path

from tickerlake.calendar import get_trading_days
from tickerlake.client import MassiveClient
from tickerlake.config import Config
from tickerlake.extract import extract_daily_aggs, extract_splits, extract_tickers
from tickerlake.load import (
    append_raw_db,
    get_db_info,
    read_raw_db,
    write_consumer_db,
    write_raw_db,
)
from tickerlake.transform import adjust_splits, compute_metrics, filter_tickers


def _run_backfill(config: Config) -> None:
    """Execute the full extract-transform-load backfill sequence."""
    dates = get_trading_days(config.start_date, config.end_date)
    if not dates:
        print("No trading days in the requested date range.")
        return

    print(
        f"Backfill: {config.start_date} to {config.end_date} ({len(dates)} trading days)"
    )
    client = MassiveClient(config)

    print("Extracting daily bars...")
    bars = extract_daily_aggs(client, dates)
    print(f"Extracting splits ({config.start_date} to {config.end_date})...")
    splits = extract_splits(client, config.start_date, config.end_date)
    print(f"Extracting tickers (types: {', '.join(config.ticker_types)})...")
    tickers = extract_tickers(client, config.ticker_types)

    print(f"Adjusting for {len(splits)} splits...")
    bars = adjust_splits(bars, splits)
    print("Filtering to known tickers...")
    bars = filter_tickers(bars, tickers)
    print("Computing metrics (SMA-50, SMA-200)...")
    metrics = compute_metrics(bars)

    raw_path = config.output_dir / "raw.duckdb"
    consumer_path = config.output_dir / "tickerlake.duckdb"

    print(f"Writing raw DB to {raw_path}...")
    write_raw_db(bars, raw_path)
    print(f"Writing consumer DB to {consumer_path}...")
    write_consumer_db(bars, metrics, tickers, consumer_path)

    n_tickers = bars["ticker"].n_unique()
    print(f"Backfill complete: {len(bars):,} bars, {n_tickers:,} tickers")


def backfill(config: Config) -> None:
    """Run a full backfill of the ETL pipeline from scratch."""
    _run_backfill(config)


def update(config: Config) -> None:
    """Incrementally update raw.duckdb with new trading days, then rebuild consumer db."""
    raw_path = config.output_dir / "raw.duckdb"

    if not raw_path.exists():
        print("No raw.duckdb found, running backfill...")
        _run_backfill(config)
        return

    existing_bars = read_raw_db(raw_path)
    max_date: datetime.date = existing_bars["date"].max()  # type: ignore[assignment]
    next_day = max_date + datetime.timedelta(days=1)

    dates = get_trading_days(next_day, config.end_date)
    if not dates:
        print("Already up to date.")
        return

    print(f"Update: {next_day} to {config.end_date} ({len(dates)} new trading days)")
    client = MassiveClient(config)

    print("Extracting new daily bars...")
    new_bars = extract_daily_aggs(client, dates)
    print("Appending to raw DB...")
    append_raw_db(new_bars, raw_path)

    print("Rebuilding consumer DB from full raw dataset...")
    all_bars = read_raw_db(raw_path)
    print(f"Extracting splits ({config.start_date} to {config.end_date})...")
    splits = extract_splits(client, config.start_date, config.end_date)
    print(f"Extracting tickers (types: {', '.join(config.ticker_types)})...")
    tickers = extract_tickers(client, config.ticker_types)

    print(f"Adjusting for {len(splits)} splits...")
    all_bars = adjust_splits(all_bars, splits)
    print("Filtering to known tickers...")
    all_bars = filter_tickers(all_bars, tickers)
    print("Computing metrics (SMA-50, SMA-200)...")
    metrics = compute_metrics(all_bars)

    consumer_path = config.output_dir / "tickerlake.duckdb"
    print(f"Writing consumer DB to {consumer_path}...")
    write_consumer_db(all_bars, metrics, tickers, consumer_path)

    n_tickers = all_bars["ticker"].n_unique()
    print(f"Update complete: {len(all_bars):,} bars, {n_tickers:,} tickers")


def _print_db_info(label: str, path: Path) -> None:
    """Print database info for a single DuckDB file."""
    if not path.exists():
        print(f"{label}: not found ({path})")
        return

    db_info = get_db_info(path)
    print(f"{label}: {path}")
    for table in db_info["tables"]:
        row_count = db_info["row_counts"].get(table, 0)
        print(f"  {table}: {row_count:,} rows")
        if table in db_info.get("date_range", {}):
            dr = db_info["date_range"][table]
            print(f"    dates: {dr['min']} to {dr['max']}")
    size_mb = db_info["file_size_bytes"] / (1024 * 1024)
    print(f"  size: {size_mb:.1f} MB")


def info(config: Config) -> None:
    """Print metadata about existing DuckDB files."""
    _print_db_info("raw.duckdb", config.output_dir / "raw.duckdb")
    _print_db_info("tickerlake.duckdb", config.output_dir / "tickerlake.duckdb")
