"""Adapter mapping and Protocol conformance tests for the Massive client seam.

These tests drive the real ``SdkMassiveClient`` through the shared
``FakeTransport`` urllib3 harness defined in ``tests/conftest.py``. They lock
in the exact wire-to-record mapping for each fetch method and prove that both
the production adapter and a minimal fake satisfy the ``MassiveClient``
protocol.
"""

import datetime
from dataclasses import replace
from typing import Any

import pytest

from tests.conftest import FakeTransport, response
from tickerlake.client import DailyAgg, MassiveClient, SdkMassiveClient, SplitRecord, TickerRecord
from tickerlake.config import Config

DAILY_AGG_PAYLOAD: dict[str, Any] = {
    "T": "AAPL",
    "o": 1.0,
    "h": 2.0,
    "l": 0.5,
    "c": 1.5,
    "v": 100.0,
    "t": 1700000000000,
}
DAILY_AGG_RECORD = DailyAgg(
    ticker="AAPL",
    open=1.0,
    high=2.0,
    low=0.5,
    close=1.5,
    volume=100.0,
    timestamp=1700000000000,
)

SPLIT_PAYLOAD: dict[str, Any] = {
    "ticker": "AAPL",
    "execution_date": "2024-01-15",
    "split_from": 1.0,
    "split_to": 2.0,
    "historical_adjustment_factor": 2.0,
    "adjustment_type": "forward",
}
SPLIT_RECORD = SplitRecord(
    ticker="AAPL",
    execution_date="2024-01-15",
    split_from=1.0,
    split_to=2.0,
    historical_adjustment_factor=2.0,
    adjustment_type="forward",
)

TICKER_PAYLOAD: dict[str, Any] = {
    "ticker": "AAPL",
    "name": "Apple Inc.",
    "type": "CS",
    "primary_exchange": "XNAS",
    "cik": "0000320193",
    "active": True,
}
TICKER_RECORD = TickerRecord(
    ticker="AAPL",
    name="Apple Inc.",
    type="CS",
    primary_exchange="XNAS",
    cik="0000320193",
    active=True,
)


class FakeMassiveClient:
    """Minimal domain fake implementing the three ``MassiveClient`` methods."""

    def fetch_daily_aggs(self, date: datetime.date) -> list[DailyAgg]:
        """Return no grouped daily aggregates."""
        return []

    def fetch_splits(self, start_date: datetime.date, end_date: datetime.date) -> list[SplitRecord]:
        """Return no split records."""
        return []

    def fetch_tickers(self, types: list[str]) -> list[TickerRecord]:
        """Return no ticker reference records."""
        return []


def test_init_requires_api_key(monkeypatch: pytest.MonkeyPatch) -> None:
    """Reject missing credentials before constructing the SDK client."""
    # Arrange
    monkeypatch.delenv("MASSIVE_API_KEY", raising=False)

    # Act / Assert
    with pytest.raises(ValueError, match="MASSIVE_API_KEY environment variable is required"):
        SdkMassiveClient(Config(api_key=""))


def test_fetch_daily_aggs_maps_full_payload_to_record(
    client_and_transport: tuple[SdkMassiveClient, FakeTransport],
) -> None:
    """Map every populated wire field onto the exact DailyAgg record."""
    # Arrange
    client, transport = client_and_transport
    transport.responses = iter([response({"results": [dict(DAILY_AGG_PAYLOAD)]})])

    # Act
    result = client.fetch_daily_aggs(datetime.date(2024, 1, 15))

    # Assert
    assert result == [DAILY_AGG_RECORD]


@pytest.mark.parametrize(
    ("wire_key", "field_name"),
    [
        ("T", "ticker"),
        ("o", "open"),
        ("h", "high"),
        ("l", "low"),
        ("c", "close"),
        ("v", "volume"),
        ("t", "timestamp"),
    ],
)
def test_fetch_daily_aggs_absent_optional_key_maps_to_none(
    client_and_transport: tuple[SdkMassiveClient, FakeTransport],
    wire_key: str,
    field_name: str,
) -> None:
    """Leave the DailyAgg field None when its optional wire key is absent."""
    # Arrange
    client, transport = client_and_transport
    payload = dict(DAILY_AGG_PAYLOAD)
    del payload[wire_key]
    transport.responses = iter([response({"results": [payload]})])

    # Act
    result = client.fetch_daily_aggs(datetime.date(2024, 1, 15))

    # Assert
    assert result == [replace(DAILY_AGG_RECORD, **{field_name: None})]


def test_fetch_daily_aggs_maps_sdk_ticker_and_timestamp_keys(
    client_and_transport: tuple[SdkMassiveClient, FakeTransport],
) -> None:
    """Map the SDK's T wire key to ticker and t to the epoch-millisecond timestamp."""
    # Arrange
    client, transport = client_and_transport
    transport.responses = iter([response({"results": [{"T": "AAPL", "t": 1700000000000}]})])

    # Act
    result = client.fetch_daily_aggs(datetime.date(2023, 11, 14))

    # Assert
    assert result == [DailyAgg(ticker="AAPL", timestamp=1700000000000)]


def test_fetch_splits_maps_full_payload_to_record(
    client_and_transport: tuple[SdkMassiveClient, FakeTransport],
) -> None:
    """Map every populated split wire field onto the exact SplitRecord."""
    # Arrange
    client, transport = client_and_transport
    transport.responses = iter([response({"results": [dict(SPLIT_PAYLOAD)]})])

    # Act
    result = client.fetch_splits(datetime.date(2024, 1, 1), datetime.date(2024, 12, 31))

    # Assert
    assert result == [SPLIT_RECORD]


@pytest.mark.parametrize(
    ("wire_key", "field_name"),
    [
        ("ticker", "ticker"),
        ("execution_date", "execution_date"),
        ("split_from", "split_from"),
        ("split_to", "split_to"),
        ("historical_adjustment_factor", "historical_adjustment_factor"),
        ("adjustment_type", "adjustment_type"),
    ],
)
def test_fetch_splits_absent_optional_key_maps_to_none(
    client_and_transport: tuple[SdkMassiveClient, FakeTransport],
    wire_key: str,
    field_name: str,
) -> None:
    """Leave the SplitRecord field None when its optional wire key is absent."""
    # Arrange
    client, transport = client_and_transport
    payload = dict(SPLIT_PAYLOAD)
    del payload[wire_key]
    transport.responses = iter([response({"results": [payload]})])

    # Act
    result = client.fetch_splits(datetime.date(2024, 1, 1), datetime.date(2024, 12, 31))

    # Assert
    assert result == [replace(SPLIT_RECORD, **{field_name: None})]


def test_fetch_tickers_maps_full_payload_to_record(
    client_and_transport: tuple[SdkMassiveClient, FakeTransport],
) -> None:
    """Map every populated ticker wire field onto the exact TickerRecord."""
    # Arrange
    client, transport = client_and_transport
    transport.responses = iter([response({"results": [dict(TICKER_PAYLOAD)]})])

    # Act
    result = client.fetch_tickers(["CS"])

    # Assert
    assert result == [TICKER_RECORD]


@pytest.mark.parametrize(
    ("wire_key", "field_name"),
    [
        ("ticker", "ticker"),
        ("name", "name"),
        ("type", "type"),
        ("primary_exchange", "primary_exchange"),
        ("cik", "cik"),
        ("active", "active"),
    ],
)
def test_fetch_tickers_absent_optional_key_maps_to_none(
    client_and_transport: tuple[SdkMassiveClient, FakeTransport],
    wire_key: str,
    field_name: str,
) -> None:
    """Leave the TickerRecord field None when its optional wire key is absent."""
    # Arrange
    client, transport = client_and_transport
    payload = dict(TICKER_PAYLOAD)
    del payload[wire_key]
    transport.responses = iter([response({"results": [payload]})])

    # Act
    result = client.fetch_tickers(["CS"])

    # Assert
    assert result == [replace(TICKER_RECORD, **{field_name: None})]


def test_fetch_tickers_iterates_types_and_maps_each_response(
    client_and_transport: tuple[SdkMassiveClient, FakeTransport],
) -> None:
    """Issue one typed request per type and map each response to TickerRecords."""
    # Arrange
    client, transport = client_and_transport
    transport.responses = iter(
        [
            response({"results": [{"ticker": "AAPL", "type": "CS", "active": True}]}),
            response({"results": [{"ticker": "SPY", "type": "ETF", "active": True}]}),
        ]
    )

    # Act
    result = client.fetch_tickers(["CS", "ETF"])

    # Assert
    assert result == [
        TickerRecord(ticker="AAPL", type="CS", active=True),
        TickerRecord(ticker="SPY", type="ETF", active=True),
    ]
    assert len(transport.requests) == 2  # noqa: PLR2004
    requested_types = [request[2]["type"] for request in transport.requests]
    assert requested_types == ["CS", "ETF"]


def test_fetch_tickers_with_no_types_returns_empty_list(
    client_and_transport: tuple[SdkMassiveClient, FakeTransport],
) -> None:
    """Return an empty list and issue no requests when no types are requested."""
    # Arrange
    client, transport = client_and_transport

    # Act
    result = client.fetch_tickers([])

    # Assert
    assert result == []
    assert transport.requests == []


def test_sdk_massive_client_satisfies_protocol() -> None:
    """SdkMassiveClient conforms to the MassiveClient protocol."""
    # Arrange / Act
    client = SdkMassiveClient(Config(api_key="test-key"))

    # Assert
    assert isinstance(client, MassiveClient)


def test_fake_massive_client_satisfies_protocol() -> None:
    """A minimal fake implementing the three methods also satisfies the protocol."""
    # Arrange / Act
    fake = FakeMassiveClient()

    # Assert
    assert isinstance(fake, MassiveClient)
