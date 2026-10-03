"""Tests for tickerlake.extract — raw API data → polars DataFrames."""

import datetime
from dataclasses import dataclass
from unittest.mock import MagicMock

import polars as pl

from tickerlake.extract import extract_daily_aggs, extract_splits, extract_tickers

# ── Helpers ──────────────────────────────────────────────────────────────────


@dataclass
class DailyAgg:
    """Concrete record matching the daily aggregate fields used by extraction."""

    ticker: str
    timestamp: int
    open: float
    high: float
    low: float
    close: float
    volume: float
    vwap: float
    transactions: int


@dataclass
class StockSplit:
    """Concrete record matching the split fields used by extraction."""

    ticker: str
    execution_date: str
    split_from: float
    split_to: float
    historical_adjustment_factor: float
    adjustment_type: str


@dataclass
class Ticker:
    """Concrete record matching the ticker fields used by extraction."""

    ticker: str
    name: str
    type: str
    primary_exchange: str
    cik: str
    active: bool


SAMPLE_AGG = {
    "ticker": "AAPL",
    "timestamp": 1704153600000,
    "open": 185.0,
    "high": 186.0,
    "low": 184.0,
    "close": 185.5,
    "volume": 50_000_000.0,
    "vwap": 185.2,
    "transactions": 1000,
}


def _make_agg(record):
    return DailyAgg(**record)


def _make_split(record):
    return StockSplit(**record)


def _make_ticker(record):
    return Ticker(**record)


# ── Daily aggs ────────────────────────────────────────────────────────────────


EXPECTED_DAILY_AGGS_SCHEMA = {
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


def test_extract_daily_aggs_schema():
    """Returned DataFrame must have exact column names and dtypes."""
    client = MagicMock()
    # 1704153600000 ms = 2024-01-02 UTC
    client.fetch_daily_aggs.return_value = [
        _make_agg(SAMPLE_AGG),
    ]
    dates = [datetime.date(2024, 1, 2)]
    df = extract_daily_aggs(client, dates)

    assert df.schema == EXPECTED_DAILY_AGGS_SCHEMA


def test_extract_daily_aggs_timestamp_conversion():
    """Ms epoch timestamp must convert to pl.Date correctly."""
    client = MagicMock()
    # 1704153600000 ms = 2024-01-02 00:00:00 UTC
    client.fetch_daily_aggs.return_value = [
        _make_agg(SAMPLE_AGG),
    ]
    df = extract_daily_aggs(client, [datetime.date(2024, 1, 2)])

    assert df["date"][0] == datetime.date(2024, 1, 2)


def test_extract_daily_aggs_empty_response():
    """Empty API response must return empty DataFrame with correct schema (no crash)."""
    client = MagicMock()
    client.fetch_daily_aggs.return_value = []
    df = extract_daily_aggs(client, [datetime.date(2024, 1, 2)])

    assert df.is_empty()
    assert df.schema == EXPECTED_DAILY_AGGS_SCHEMA


def test_extract_daily_aggs_multiple_dates():
    """Multiple dates must be concatenated into a single DataFrame."""
    client = MagicMock()
    client.fetch_daily_aggs.side_effect = [
        [_make_agg(SAMPLE_AGG)],
        [
            _make_agg(
                {
                    **SAMPLE_AGG,
                    "timestamp": 1704240000000,
                    "open": 186.0,
                    "high": 187.0,
                    "low": 185.0,
                    "close": 186.5,
                    "volume": 51e6,
                    "vwap": 186.2,
                    "transactions": 1100,
                }
            )
        ],
    ]
    dates = [datetime.date(2024, 1, 2), datetime.date(2024, 1, 3)]
    df = extract_daily_aggs(client, dates)

    expected_record_count = 2
    expected_fetch_count = 2
    assert len(df) == expected_record_count
    assert client.fetch_daily_aggs.call_count == expected_fetch_count


def test_extract_daily_aggs_progress_output():
    """extract_daily_aggs runs without error for a single date."""
    client = MagicMock()
    client.fetch_daily_aggs.return_value = [
        _make_agg(SAMPLE_AGG),
    ]
    dates = [datetime.date(2024, 1, 2)]
    extract_daily_aggs(client, dates)


# ── Splits ────────────────────────────────────────────────────────────────────


EXPECTED_SPLITS_SCHEMA = {
    "ticker": pl.Utf8,
    "execution_date": pl.Date,
    "split_from": pl.Float32,
    "split_to": pl.Float32,
    "adjustment_factor": pl.Float64,
    "adjustment_type": pl.Utf8,
}


def test_extract_splits_schema():
    """Returned splits DataFrame must have exact column names and dtypes."""
    client = MagicMock()
    client.fetch_splits.return_value = [
        _make_split(
            {
                "ticker": "AAPL",
                "execution_date": "2024-08-31",
                "split_from": 1.0,
                "split_to": 4.0,
                "historical_adjustment_factor": 4.0,
                "adjustment_type": "forward",
            }
        ),
    ]
    df = extract_splits(client, datetime.date(2024, 1, 1), datetime.date(2024, 12, 31))

    assert df.schema == EXPECTED_SPLITS_SCHEMA


def test_extract_splits_execution_date_parsing():
    """String execution_date must be parsed to pl.Date."""
    client = MagicMock()
    client.fetch_splits.return_value = [
        _make_split(
            {
                "ticker": "AAPL",
                "execution_date": "2024-08-31",
                "split_from": 1.0,
                "split_to": 4.0,
                "historical_adjustment_factor": 4.0,
                "adjustment_type": "forward",
            }
        ),
    ]
    df = extract_splits(client, datetime.date(2024, 1, 1), datetime.date(2024, 12, 31))

    assert df["execution_date"][0] == datetime.date(2024, 8, 31)


def test_extract_splits_empty_response():
    """Empty splits response must return empty DataFrame with correct schema."""
    client = MagicMock()
    client.fetch_splits.return_value = []
    df = extract_splits(client, datetime.date(2024, 1, 1), datetime.date(2024, 12, 31))

    assert df.is_empty()
    assert df.schema == EXPECTED_SPLITS_SCHEMA


# ── Tickers ───────────────────────────────────────────────────────────────────


EXPECTED_TICKERS_SCHEMA = {
    "ticker": pl.Utf8,
    "name": pl.Utf8,
    "type": pl.Utf8,
    "primary_exchange": pl.Utf8,
    "cik": pl.Utf8,
    "active": pl.Boolean,
}


def test_extract_tickers_schema():
    """Returned tickers DataFrame must have exact column names and dtypes."""
    client = MagicMock()
    client.fetch_tickers.return_value = [
        _make_ticker(
            {
                "ticker": "AAPL",
                "name": "Apple Inc.",
                "type": "CS",
                "primary_exchange": "XNAS",
                "cik": "0000320193",
                "active": True,
            }
        ),
    ]
    df = extract_tickers(client, ["CS"])

    assert df.schema == EXPECTED_TICKERS_SCHEMA


def test_extract_tickers_empty_response():
    """Empty tickers response must return empty DataFrame with correct schema."""
    client = MagicMock()
    client.fetch_tickers.return_value = []
    df = extract_tickers(client, ["CS"])

    assert df.is_empty()
    assert df.schema == EXPECTED_TICKERS_SCHEMA


def test_extract_daily_aggs_empty_dates_list():
    """Empty dates list returns empty DataFrame without entering Progress."""
    client = MagicMock()
    df = extract_daily_aggs(client, [])

    assert df.is_empty()
    assert df.schema == EXPECTED_DAILY_AGGS_SCHEMA
    # Client should never be called
    client.fetch_daily_aggs.assert_not_called()


def test_extract_daily_aggs_skips_failed_date():
    """extract_daily_aggs skips failed dates and continues with others."""
    client = MagicMock()
    # First date raises, second date succeeds
    client.fetch_daily_aggs.side_effect = [
        Exception("API error"),
        [
            _make_agg(
                {
                    **SAMPLE_AGG,
                    "timestamp": 1704240000000,
                    "open": 186.0,
                    "high": 187.0,
                    "low": 185.0,
                    "close": 186.5,
                    "volume": 51e6,
                    "vwap": 186.2,
                    "transactions": 1100,
                }
            )
        ],
    ]
    dates = [datetime.date(2024, 1, 2), datetime.date(2024, 1, 3)]
    df = extract_daily_aggs(client, dates)

    # Should have data from the second date only
    assert len(df) == 1
    assert df["date"][0] == datetime.date(2024, 1, 3)
    assert df["ticker"][0] == "AAPL"
    # Both dates should have been attempted
    expected_fetch_count = 2
    assert client.fetch_daily_aggs.call_count == expected_fetch_count
