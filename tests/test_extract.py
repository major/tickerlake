"""Tests for tickerlake.extract — raw API data → polars DataFrames."""

import datetime
from unittest.mock import MagicMock

import polars as pl

from tickerlake.extract import extract_daily_aggs, extract_splits, extract_tickers

# ── Helpers ──────────────────────────────────────────────────────────────────


def make_mock_agg(ticker, ts_ms, o, h, low, c, vol, vwap, txns):
    """Build a mock GroupedDailyAgg object."""
    agg = MagicMock()
    agg.ticker = ticker
    agg.timestamp = ts_ms
    agg.open = o
    agg.high = h
    agg.low = low
    agg.close = c
    agg.volume = vol
    agg.vwap = vwap
    agg.transactions = txns
    return agg


def make_mock_split(
    ticker, execution_date_str, split_from, split_to, adj_factor, adj_type
):
    """Build a mock StockSplit object."""
    s = MagicMock()
    s.ticker = ticker
    s.execution_date = execution_date_str
    s.split_from = split_from
    s.split_to = split_to
    s.historical_adjustment_factor = adj_factor
    s.adjustment_type = adj_type
    return s


def make_mock_ticker(ticker, name, type_, exchange, cik, active):
    """Build a mock Ticker object."""
    t = MagicMock()
    t.ticker = ticker
    t.name = name
    t.type = type_
    t.primary_exchange = exchange
    t.cik = cik
    t.active = active
    return t


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
        make_mock_agg(
            "AAPL", 1704153600000, 185.0, 186.0, 184.0, 185.5, 50_000_000.0, 185.2, 1000
        ),
    ]
    dates = [datetime.date(2024, 1, 2)]
    df = extract_daily_aggs(client, dates)

    assert df.schema == EXPECTED_DAILY_AGGS_SCHEMA


def test_extract_daily_aggs_timestamp_conversion():
    """ms epoch timestamp must convert to pl.Date correctly."""
    client = MagicMock()
    # 1704153600000 ms = 2024-01-02 00:00:00 UTC
    client.fetch_daily_aggs.return_value = [
        make_mock_agg(
            "AAPL", 1704153600000, 185.0, 186.0, 184.0, 185.5, 50_000_000.0, 185.2, 1000
        ),
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
        [
            make_mock_agg(
                "AAPL", 1704153600000, 185.0, 186.0, 184.0, 185.5, 50e6, 185.2, 1000
            )
        ],
        [
            make_mock_agg(
                "AAPL", 1704240000000, 186.0, 187.0, 185.0, 186.5, 51e6, 186.2, 1100
            )
        ],
    ]
    dates = [datetime.date(2024, 1, 2), datetime.date(2024, 1, 3)]
    df = extract_daily_aggs(client, dates)

    assert len(df) == 2
    assert client.fetch_daily_aggs.call_count == 2


def test_extract_daily_aggs_progress_output():
    """extract_daily_aggs runs without error for a single date."""
    client = MagicMock()
    client.fetch_daily_aggs.return_value = [
        make_mock_agg(
            "AAPL", 1704153600000, 185.0, 186.0, 184.0, 185.5, 50e6, 185.2, 1000
        ),
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
        make_mock_split("AAPL", "2024-08-31", 1.0, 4.0, 4.0, "forward"),
    ]
    df = extract_splits(client, datetime.date(2024, 1, 1), datetime.date(2024, 12, 31))

    assert df.schema == EXPECTED_SPLITS_SCHEMA


def test_extract_splits_execution_date_parsing():
    """String execution_date must be parsed to pl.Date."""
    client = MagicMock()
    client.fetch_splits.return_value = [
        make_mock_split("AAPL", "2024-08-31", 1.0, 4.0, 4.0, "forward"),
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
        make_mock_ticker("AAPL", "Apple Inc.", "CS", "XNAS", "0000320193", True),
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
                "AAPL", 1704240000000, 186.0, 187.0, 185.0, 186.5, 51e6, 186.2, 1100
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
    assert client.fetch_daily_aggs.call_count == 2
