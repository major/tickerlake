"""Convert raw Massive API objects into polars DataFrames with correct dtypes."""

import datetime
import logging

import polars as pl
from rich.progress import (
    BarColumn,
    Progress,
    SpinnerColumn,
    TextColumn,
    TimeElapsedColumn,
)

from tickerlake import console
from tickerlake.client import MassiveClient

logger = logging.getLogger(__name__)

DAILY_AGGS_SCHEMA = {
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

SPLITS_SCHEMA = {
    "ticker": pl.Utf8,
    "execution_date": pl.Date,
    "split_from": pl.Float32,
    "split_to": pl.Float32,
    "adjustment_factor": pl.Float64,
    "adjustment_type": pl.Utf8,
}

TICKERS_SCHEMA = {
    "ticker": pl.Utf8,
    "name": pl.Utf8,
    "type": pl.Utf8,
    "primary_exchange": pl.Utf8,
    "cik": pl.Utf8,
    "active": pl.Boolean,
}


def _agg_to_row(agg) -> dict:
    return {
        "date": datetime.datetime.fromtimestamp(
            agg.timestamp / 1000, tz=datetime.UTC
        ).date(),
        "ticker": agg.ticker,
        "open": agg.open,
        "high": agg.high,
        "low": agg.low,
        "close": agg.close,
        "volume": agg.volume,
        "vwap": agg.vwap,
        "transactions": agg.transactions,
    }


def _split_to_row(split) -> dict:
    return {
        "ticker": split.ticker,
        "execution_date": datetime.date.fromisoformat(split.execution_date),
        "split_from": split.split_from,
        "split_to": split.split_to,
        "adjustment_factor": split.historical_adjustment_factor,
        "adjustment_type": split.adjustment_type,
    }


def _ticker_to_row(ticker) -> dict:
    return {
        "ticker": ticker.ticker,
        "name": ticker.name,
        "type": ticker.type,
        "primary_exchange": ticker.primary_exchange,
        "cik": ticker.cik,
        "active": ticker.active,
    }


def _rows_to_df(rows: list[dict], schema: dict) -> pl.DataFrame:
    if not rows:
        return pl.DataFrame(schema=schema)
    return pl.DataFrame(rows).cast(schema)


def extract_daily_aggs(
    client: MassiveClient, dates: list[datetime.date]
) -> pl.DataFrame:
    """Extract daily aggregate bars for each date into a single DataFrame."""
    frames: list[pl.DataFrame] = []
    if not dates:
        return pl.DataFrame(schema=DAILY_AGGS_SCHEMA)
    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
        TimeElapsedColumn(),
        console=console,
        transient=True,
    ) as progress:
        task = progress.add_task("Fetching bars...", total=len(dates))
        for date in dates:
            try:
                aggs = client.fetch_daily_aggs(date)
            except Exception as e:  # noqa: BLE001
                logger.warning("Skipping %s (API error: %s)", date, e)
                progress.update(task, description=f"Skipping {date}...", advance=1)
                continue
            logger.debug("Fetching %s... %d tickers", date, len(aggs))
            if aggs:
                frames.append(
                    _rows_to_df([_agg_to_row(a) for a in aggs], DAILY_AGGS_SCHEMA)
                )
            progress.update(task, description=f"Fetching {date}...", advance=1)
    if not frames:
        return pl.DataFrame(schema=DAILY_AGGS_SCHEMA)
    return pl.concat(frames)


def extract_splits(
    client: MassiveClient, start_date: datetime.date, end_date: datetime.date
) -> pl.DataFrame:
    splits = client.fetch_splits(start_date, end_date)
    return _rows_to_df([_split_to_row(s) for s in splits], SPLITS_SCHEMA)


def extract_tickers(client: MassiveClient, types: list[str]) -> pl.DataFrame:
    tickers = client.fetch_tickers(types)
    return _rows_to_df([_ticker_to_row(t) for t in tickers], TICKERS_SCHEMA)
