"""Tests for the Massive API client wrapper."""

import datetime
import json
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

import pytest
from urllib3.response import HTTPResponse

from tickerlake.client import MassiveClient
from tickerlake.config import Config

if TYPE_CHECKING:
    from pathlib import Path


@dataclass(frozen=True)
class DailyAgg:
    """Representative grouped daily aggregate record."""

    ticker: str
    close: float


@dataclass(frozen=True)
class Split:
    """Representative stock split record."""

    ticker: str
    execution_date: str
    split_from: float
    split_to: float


@dataclass(frozen=True)
class Ticker:
    """Representative ticker reference record."""

    ticker: str
    type: str


class FakeSdk:
    """Small SDK-boundary fake with generator-backed endpoint responses."""

    def __init__(self) -> None:
        """Initialize empty endpoint responses and request records."""
        self.daily_aggs: list[DailyAgg] = []
        self.splits: list[Split] = []
        self.tickers_by_type: dict[str, list[Ticker]] = {}
        self.daily_params: dict[str, Any] | None = None
        self.split_params: dict[str, Any] | None = None
        self.ticker_params: list[dict[str, Any]] = []
        self.BASE = "https://api.massive.com"
        self.json = json

    def get_grouped_daily_aggs(self, **params: Any) -> list[DailyAgg]:
        """Return configured aggregate records and store the request."""
        self.daily_params = {key: value for key, value in params.items() if key != "raw"}
        rows = [{"T": row.ticker, "c": row.close} for row in self.daily_aggs]
        return HTTPResponse(body=json.dumps({"results": rows}).encode(), status=200)  # type: ignore[return-value]

    def list_stocks_splits(self, **params: Any) -> HTTPResponse:
        """Yield configured split records and store the request."""
        self.split_params = {key: value for key, value in params.items() if key != "raw"}
        rows = [
            {
                "ticker": row.ticker,
                "execution_date": row.execution_date,
                "split_from": row.split_from,
                "split_to": row.split_to,
            }
            for row in self.splits
        ]
        return HTTPResponse(body=json.dumps({"results": rows}).encode(), status=200)

    def list_tickers(self, **params: Any) -> HTTPResponse:
        """Yield configured records for the requested type."""
        params = {key: value for key, value in params.items() if key != "raw"}
        self.ticker_params.append(params)
        rows = [
            {"ticker": row.ticker, "type": row.type, "active": True} for row in self.tickers_by_type[params["type"]]
        ]
        return HTTPResponse(body=json.dumps({"results": rows}).encode(), status=200)


@pytest.fixture
def sample_config(tmp_path: Path) -> Config:
    """Build a representative client configuration."""
    return Config(
        api_key="test-api-key",
        start_date=datetime.date(2024, 1, 1),
        end_date=datetime.date(2024, 12, 31),
        ticker_types=["CS", "ETF", "ETV", "ETN", "ADRC"],
    )


def client_with_sdk(monkeypatch: pytest.MonkeyPatch, config: Config, sdk: FakeSdk) -> MassiveClient:
    """Construct a client using the supplied SDK-boundary fake."""

    def create_client(*, api_key: str) -> FakeSdk:
        assert api_key == config.api_key
        return sdk

    monkeypatch.setattr("tickerlake.client.RESTClient", create_client)
    return MassiveClient(config)


def test_init_requires_api_key(monkeypatch: pytest.MonkeyPatch) -> None:
    """Reject missing credentials before constructing the SDK client."""
    with monkeypatch.context() as context:
        context.delenv("MASSIVE_API_KEY", raising=False)
        context.setattr("tickerlake.client.RESTClient", lambda **_: pytest.fail("SDK should not be created"))
        with pytest.raises(ValueError, match="MASSIVE_API_KEY environment variable is required"):
            MassiveClient(Config(api_key=""))


def test_fetch_daily_aggs_preserves_sdk_records_and_request(
    sample_config: Config, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Return representative SDK data and send the grouped-aggregate filters."""
    sdk = FakeSdk()
    expected = [DailyAgg(ticker="AAPL", close=150.25)]
    sdk.daily_aggs = expected
    client = client_with_sdk(monkeypatch, sample_config, sdk)
    requested_date = datetime.date(2024, 1, 15)

    result = client.fetch_daily_aggs(requested_date)

    assert len(result) == 1
    assert result[0].ticker == "AAPL"
    assert result[0].close == expected[0].close
    assert sdk.daily_params == {
        "date": requested_date,
        "adjusted": False,
        "market_type": "stocks",
        "include_otc": False,
    }


def test_fetch_splits_materializes_sdk_generator_and_formats_date_filters(
    sample_config: Config, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Materialize split records and send inclusive date filters as strings."""
    sdk = FakeSdk()
    expected = [
        Split(ticker="AAPL", execution_date="2024-01-15", split_from=1.0, split_to=2.0),
        Split(ticker="MSFT", execution_date="2024-02-01", split_from=1.0, split_to=3.0),
    ]
    sdk.splits = expected
    client = client_with_sdk(monkeypatch, sample_config, sdk)

    result = client.fetch_splits(datetime.date(2024, 1, 1), datetime.date(2024, 12, 31))

    assert len(result) == len(expected)
    assert isinstance(result, list)
    assert [split.ticker for split in result] == ["AAPL", "MSFT"]
    assert sdk.split_params == {
        "execution_date_gte": "2024-01-01",
        "execution_date_lte": "2024-12-31",
    }


def test_fetch_tickers_materializes_and_combines_each_type_response(
    sample_config: Config, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Combine generator responses and preserve each ticker record's values."""
    sdk = FakeSdk()
    cs_tickers = [Ticker(ticker="AAPL", type="CS"), Ticker(ticker="MSFT", type="CS")]
    etf_tickers = [Ticker(ticker="SPY", type="ETF"), Ticker(ticker="QQQ", type="ETF")]
    sdk.tickers_by_type = {"CS": cs_tickers, "ETF": etf_tickers}
    client = client_with_sdk(monkeypatch, sample_config, sdk)

    result = client.fetch_tickers(["CS", "ETF"])

    assert [ticker.ticker for ticker in result] == [ticker.ticker for ticker in cs_tickers + etf_tickers]
    assert isinstance(result, list)
    assert [ticker.ticker for ticker in result] == ["AAPL", "MSFT", "SPY", "QQQ"]
    assert sdk.ticker_params == [
        {"market": "stocks", "type": "CS", "active": True, "limit": 1000},
        {"market": "stocks", "type": "ETF", "active": True, "limit": 1000},
    ]


def test_fetch_tickers_with_no_types_returns_empty_list(sample_config: Config, monkeypatch: pytest.MonkeyPatch) -> None:
    """Return an empty result without issuing ticker requests for no types."""
    sdk = FakeSdk()
    client = client_with_sdk(monkeypatch, sample_config, sdk)

    assert client.fetch_tickers([]) == []
    assert sdk.ticker_params == []


def test_fetch_splits_with_empty_sdk_generator_returns_empty_list(
    sample_config: Config, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Return an empty list when the SDK split generator yields no records."""
    client = client_with_sdk(monkeypatch, sample_config, FakeSdk())

    result = client.fetch_splits(datetime.date(2024, 1, 1), datetime.date(2024, 12, 31))

    assert result == []
    assert isinstance(result, list)
