"""Pytest configuration and shared fixtures for tickerlake tests."""

import datetime

import polars as pl
import pytest


@pytest.fixture
def sample_bars_df() -> pl.DataFrame:
    """Create a sample OHLCV bars DataFrame with realistic test data.

    Returns a polars DataFrame with columns matching the pipeline's bars schema:
    - date: pl.Date
    - ticker: pl.Utf8
    - open: pl.Float32
    - high: pl.Float32
    - low: pl.Float32
    - close: pl.Float32
    - volume: pl.Float32
    - vwap: pl.Float32
    - transactions: pl.UInt32
    """
    return pl.DataFrame(
        {
            "date": [
                datetime.date(2024, 1, 1),
                datetime.date(2024, 1, 1),
                datetime.date(2024, 1, 2),
                datetime.date(2024, 1, 2),
                datetime.date(2024, 1, 3),
                datetime.date(2024, 1, 3),
            ],
            "ticker": ["AAPL", "MSFT", "AAPL", "MSFT", "AAPL", "MSFT"],
            "open": [150.0, 380.0, 151.0, 381.0, 152.0, 382.0],
            "high": [152.0, 382.0, 153.0, 383.0, 154.0, 384.0],
            "low": [149.0, 379.0, 150.0, 380.0, 151.0, 381.0],
            "close": [151.5, 381.5, 152.5, 382.5, 153.5, 383.5],
            "volume": [
                1000000.0,
                1200000.0,
                1100000.0,
                1300000.0,
                1050000.0,
                1250000.0,
            ],
            "vwap": [151.2, 381.2, 152.2, 382.2, 153.2, 383.2],
            "transactions": [5000, 6000, 5500, 6500, 5250, 6250],
        }
    ).with_columns(
        pl.col("date").cast(pl.Date),
        pl.col("ticker").cast(pl.Utf8),
        pl.col("open").cast(pl.Float32),
        pl.col("high").cast(pl.Float32),
        pl.col("low").cast(pl.Float32),
        pl.col("close").cast(pl.Float32),
        pl.col("volume").cast(pl.Float32),
        pl.col("vwap").cast(pl.Float32),
        pl.col("transactions").cast(pl.UInt32),
    )


@pytest.fixture
def sample_splits_df() -> pl.DataFrame:
    """Create a sample stock splits DataFrame.

    Returns a polars DataFrame with columns matching the pipeline's splits schema:
    - ticker: pl.Utf8
    - execution_date: pl.Date
    - split_from: pl.Float32
    - split_to: pl.Float32
    - adjustment_factor: pl.Float64
    - adjustment_type: pl.Utf8
    """
    return pl.DataFrame(
        {
            "ticker": ["AAPL", "MSFT"],
            "execution_date": [
                datetime.date(2024, 1, 15),
                datetime.date(2024, 2, 1),
            ],
            "split_from": [1.0, 1.0],
            "split_to": [2.0, 3.0],
            "adjustment_factor": [2.0, 3.0],
            "adjustment_type": ["forward", "forward"],
        }
    ).with_columns(
        pl.col("ticker").cast(pl.Utf8),
        pl.col("execution_date").cast(pl.Date),
        pl.col("split_from").cast(pl.Float32),
        pl.col("split_to").cast(pl.Float32),
        pl.col("adjustment_factor").cast(pl.Float64),
        pl.col("adjustment_type").cast(pl.Utf8),
    )


@pytest.fixture
def sample_tickers_df() -> pl.DataFrame:
    """Create a sample tickers reference DataFrame.

    Returns a polars DataFrame with columns matching the pipeline's tickers schema:
    - ticker: pl.Utf8
    - name: pl.Utf8
    - type: pl.Utf8
    - primary_exchange: pl.Utf8
    - cik: pl.Utf8
    - active: pl.Boolean
    """
    return pl.DataFrame(
        {
            "ticker": ["AAPL", "MSFT", "SPY"],
            "name": ["Apple Inc.", "Microsoft Corporation", "SPDR S&P 500 ETF Trust"],
            "type": ["CS", "CS", "ETF"],
            "primary_exchange": ["XNAS", "XNAS", "XNYS"],
            "cik": ["0000320193", "0000789019", ""],
            "active": [True, True, True],
        }
    ).with_columns(
        pl.col("ticker").cast(pl.Utf8),
        pl.col("name").cast(pl.Utf8),
        pl.col("type").cast(pl.Utf8),
        pl.col("primary_exchange").cast(pl.Utf8),
        pl.col("cik").cast(pl.Utf8),
        pl.col("active").cast(pl.Boolean),
    )


@pytest.fixture
def sample_daily_bars_df() -> pl.DataFrame:
    """Create ~8 years of weekday daily OHLCV bars for the ciovacco report.

    Four tickers with deterministic close paths:
    - UP: steadily rising close (100 * 1.0005^i)
    - DOWN: steadily falling close (100 * 0.9995^i)
    - FLAT: constant close (100)
    - SPY: the benchmark, moderately rising (200 * 1.0002^i)

    High/low are derived from close (high = close*1.001, low = close*0.999).
    The 8-year span is long enough for every cloud timeframe and the weekly
    300-week MA conditions to resolve (the deepest cloud timeframe needs
    ~6.5 years of daily bars).
    """
    dates = pl.date_range(
        datetime.date(2017, 1, 2),
        datetime.date(2024, 12, 31),
        interval="1d",
        eager=True,
    )
    dates = dates.filter(dates.dt.weekday() < 5).to_list()
    paths = {
        "UP": lambda i: 100.0 * (1.0005**i),
        "DOWN": lambda i: 100.0 * (0.9995**i),
        "FLAT": lambda i: 100.0,
        "SPY": lambda i: 200.0 * (1.0002**i),
    }
    rows = []
    for ticker, path in paths.items():
        for index, day in enumerate(dates):
            close = path(index)
            rows.append(
                {
                    "date": day,
                    "ticker": ticker,
                    "open": close * 0.999,
                    "high": close * 1.001,
                    "low": close * 0.999,
                    "close": close,
                    "volume": 1_000_000.0,
                    "vwap": close,
                    "transactions": 5000,
                }
            )
    return pl.DataFrame(rows).with_columns(
        pl.col("date").cast(pl.Date),
        pl.col("ticker").cast(pl.Utf8),
        pl.col("open").cast(pl.Float32),
        pl.col("high").cast(pl.Float32),
        pl.col("low").cast(pl.Float32),
        pl.col("close").cast(pl.Float32),
        pl.col("volume").cast(pl.Float32),
        pl.col("vwap").cast(pl.Float32),
        pl.col("transactions").cast(pl.UInt32),
    )


@pytest.fixture
def sample_ichimoku_df() -> pl.DataFrame:
    """Create an Ichimoku frame for the score_ichimoku grid tests.

    Each ticker has two bars; the second bar (2024-01-02) drives the score:
    T1 scores 1.0 (above all 4 lines), T2 0.75 (senkou_b equal/below), T3 0.5
    (exactly on senkou_a and senkou_b — the canonical "inside the cloud"
    case), T4 0.25 (only tenkan above), T5 0.0, T6 null (senkou_a undefined),
    and SPY (the benchmark) is present but must never be scored.
    """
    last_bar = {
        "T1": (10.0, 9.0, 9.0, 9.0, 9.0),
        "T2": (10.0, 9.0, 9.0, 9.0, 10.0),
        "T3": (10.0, 9.0, 9.0, 10.0, 10.0),
        "T4": (10.0, 9.0, 11.0, 11.0, 11.0),
        "T5": (10.0, 11.0, 11.0, 11.0, 11.0),
        "T6": (10.0, 9.0, 9.0, None, 9.0),
        "SPY": (10.0, 9.0, 9.0, 9.0, 9.0),
    }
    rows = []
    for ticker, (close, tenkan, kijun, sa, sb) in last_bar.items():
        rows.append({"date": datetime.date(2024, 1, 1), "ticker": ticker})
        rows.append(
            {
                "date": datetime.date(2024, 1, 2),
                "ticker": ticker,
                "close": close,
                "tenkan": tenkan,
                "kijun": kijun,
                "senkou_a_at_current": sa,
                "senkou_b_at_current": sb,
            }
        )
    return pl.DataFrame(
        rows,
        schema={
            "date": pl.Date,
            "ticker": pl.Utf8,
            "close": pl.Float32,
            "tenkan": pl.Float32,
            "kijun": pl.Float32,
            "senkou_a_at_current": pl.Float32,
            "senkou_b_at_current": pl.Float32,
        },
    )
