"""Tests for tickerlake.extract — raw API data → polars DataFrames."""

import datetime
from unittest.mock import MagicMock

import polars as pl

from tickerlake.extract import extract_daily_aggs, extract_splits, extract_tickers

# ── Helpers ──────────────────────────────────────────────────────────────────


def make_mock_agg(record):
    """Build a mock GroupedDailyAgg object."""
    agg = MagicMock()
    for field, value in record.items():
        setattr(agg, field, value)
    return agg


def make_mock_split(record):
    """Build a mock StockSplit object."""
    s = MagicMock()
    for field, value in record.items():
        setattr(s, field, value)
    return s


def make_mock_ticker(record):
    """Build a mock Ticker object."""
    t = MagicMock()
    for field, value in record.items():
        setattr(t, field, value)
    return t


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
        make_mock_agg(SAMPLE_AGG),
    ]
    dates = [datetime.date(2024, 1, 2)]
    df = extract_daily_aggs(client, dates)

    assert df.schema == EXPECTED_DAILY_AGGS_SCHEMA


def test_extract_daily_aggs_timestamp_conversion():
    """Ms epoch timestamp must convert to pl.Date correctly."""
    client = MagicMock()
    # 1704153600000 ms = 2024-01-02 00:00:00 UTC
    client.fetch_daily_aggs.return_value = [
        make_mock_agg(SAMPLE_AGG),
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
        [make_mock_agg(SAMPLE_AGG)],
        [
            make_mock_agg(
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
        make_mock_agg(SAMPLE_AGG),
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
        make_mock_split(
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
        make_mock_split(
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
        make_mock_ticker(
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
            make_mock_agg(
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
