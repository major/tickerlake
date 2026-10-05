"""Pytest configuration and shared fixtures for tickerlake tests."""

import datetime
import json
from typing import Any

import polars as pl
import pytest
from urllib3.response import HTTPResponse

from tickerlake.client import SdkMassiveClient
from tickerlake.config import Config


class FakeTransport:
    """Serve queued HTTP responses through urllib3's request boundary."""

    def __init__(self, responses: list[HTTPResponse]) -> None:
        """Store the response sequence and begin recording requests."""
        self.responses = iter(responses)
        self.requests: list[tuple[str, str, dict[str, Any] | None]] = []

    def request(self, method: str, url: str, *, fields: dict[str, Any] | None, headers: Any) -> HTTPResponse:
        """Return the next queued response and record the request."""
        del headers
        self.requests.append((method, url, fields))
        return next(self.responses)


def response(payload: Any, *, status: int = 200) -> HTTPResponse:
    """Create a real urllib3 response with a JSON body."""
    body = payload if isinstance(payload, bytes) else json.dumps(payload).encode()
    return HTTPResponse(body=body, status=status)


@pytest.fixture
def client_and_transport() -> tuple[SdkMassiveClient, FakeTransport]:
    """Build a real SdkMassiveClient wired to a deterministic transport.

    The real ``RESTClient`` runs end to end; only its urllib3 transport is
    swapped, so request construction, pagination, and model parsing all execute.
    """
    client = SdkMassiveClient(Config(api_key="test-key"))
    transport = FakeTransport([])
    client._client.client = transport  # noqa: SLF001
    return client, transport


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
    - volume: pl.Float64
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
        }
    ).with_columns(
        pl.col("date").cast(pl.Date),
        pl.col("ticker").cast(pl.Utf8),
        pl.col("open").cast(pl.Float32),
        pl.col("high").cast(pl.Float32),
        pl.col("low").cast(pl.Float32),
        pl.col("close").cast(pl.Float32),
        pl.col("volume").cast(pl.Float64),
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
