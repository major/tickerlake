"""Behavioral tests for strict responses from the real Massive SDK adapter."""

# ruff: noqa: D103

import datetime
import json
from typing import Any

import pytest
from urllib3.response import HTTPResponse

from tickerlake.client import MassiveClient
from tickerlake.config import Config
from tickerlake.extract import extract_daily_aggs, extract_splits, extract_tickers
from tickerlake.outcomes import FetchStatus


class FakeTransport:
    """Serve queued HTTP responses through urllib3's request boundary."""

    def __init__(self, responses: list[HTTPResponse]) -> None:
        """Store response sequence and begin recording requests."""
        self.responses = iter(responses)
        self.requests: list[tuple[str, str, dict[str, Any] | None]] = []

    def request(self, method: str, url: str, *, fields: dict[str, Any] | None, headers: Any) -> HTTPResponse:
        """Return the next response from the deterministic queue."""
        del headers
        self.requests.append((method, url, fields))
        return next(self.responses)


def response(payload: Any, *, status: int = 200) -> HTTPResponse:
    """Create a real urllib3 response with a JSON body."""
    body = payload if isinstance(payload, bytes) else json.dumps(payload).encode()
    return HTTPResponse(body=body, status=status)


@pytest.fixture
def client_and_transport(tmp_path: Any) -> tuple[MassiveClient, FakeTransport]:
    config = Config(api_key="test-key", output_dir=tmp_path)
    client = MassiveClient(config)
    transport = FakeTransport([])
    client._client.client = transport  # noqa: SLF001
    return client, transport


def test_daily_aggs_parses_two_pages_and_uses_raw_endpoint_flags(
    client_and_transport: tuple[MassiveClient, FakeTransport],
) -> None:
    client, transport = client_and_transport
    transport.responses = iter(
        [
            response({"results": [{"T": "AAPL", "c": 150}], "next_url": "https://api.massive.com/page?cursor=2"}),
            response({"results": [{"T": "MSFT", "c": 200}]}),
        ]
    )

    result = client.fetch_daily_aggs(datetime.date(2024, 1, 15))

    assert [row.ticker for row in result] == ["AAPL", "MSFT"]
    assert transport.requests[0] == (
        "GET",
        "https://api.massive.com/v2/aggs/grouped/locale/us/market/stocks/2024-01-15",
        {"adjusted": "false", "locale": "us", "market_type": "stocks", "include_otc": "false"},
    )
    assert transport.requests[1][1:] == ("https://api.massive.com/page", {"cursor": "2"})


@pytest.mark.parametrize("payload", [b"not json", {"status": "OK"}, {"results": {"bad": 1}}])
def test_malformed_initial_response_fails_extraction(
    client_and_transport: tuple[MassiveClient, FakeTransport], payload: Any
) -> None:
    client, transport = client_and_transport
    transport.responses = iter([response(payload)])

    outcome = extract_daily_aggs(client, [datetime.date(2024, 1, 15)])[0]

    assert outcome.status is FetchStatus.failed
    assert outcome.diagnostic == "transport_error"


@pytest.mark.parametrize(
    ("kind", "payload", "extract"),
    [
        ("daily", b"broken", lambda c: extract_daily_aggs(c, [datetime.date(2024, 1, 15)])[0]),
        ("ticker", b"broken", lambda c: extract_tickers(c, ["CS"])),
        ("split", b"broken", lambda c: extract_splits(c, datetime.date(2024, 1, 1), datetime.date(2024, 12, 31))),
    ],
)
def test_malformed_page_response_fails_extraction_without_returning_partial_records(
    client_and_transport: tuple[MassiveClient, FakeTransport], kind: str, payload: Any, extract: Any
) -> None:
    client, transport = client_and_transport
    first = {"results": [{"T": "AAPL", "c": 150}], "next_url": "https://api.massive.com/page2"}
    if kind == "ticker":
        first = {
            "results": [{"ticker": "AAPL", "type": "CS", "active": True}],
            "next_url": "https://api.massive.com/page2",
        }
    elif kind == "split":
        first = {"results": [], "next_url": "https://api.massive.com/page2"}
    transport.responses = iter([response(first), response(payload)])

    outcome = extract(client)

    assert outcome.status is FetchStatus.failed
    assert outcome.diagnostic == "transport_error"


def test_explicit_empty_results_is_successful_empty(client_and_transport: tuple[MassiveClient, FakeTransport]) -> None:
    client, transport = client_and_transport
    transport.responses = iter([response({"results": []})])

    assert client.fetch_tickers(["CS"]) == []


@pytest.mark.parametrize("payload", [{}, {"results": None}, {"results": "bad"}])
def test_missing_or_invalid_results_fails_closed(
    client_and_transport: tuple[MassiveClient, FakeTransport], payload: Any
) -> None:
    # No verified SDK contract says an omitted results field means zero records.
    client, transport = client_and_transport
    transport.responses = iter([response(payload)])

    with pytest.raises(TypeError, match="results list"):
        client.fetch_tickers(["CS"])


def test_model_parse_error_propagates(client_and_transport: tuple[MassiveClient, FakeTransport]) -> None:
    client, transport = client_and_transport
    transport.responses = iter([response({"results": [None]})])

    with pytest.raises((TypeError, AttributeError)):
        client.fetch_tickers(["CS"])


def test_non_200_response_propagates_sdk_error(client_and_transport: tuple[MassiveClient, FakeTransport]) -> None:
    client, transport = client_and_transport
    transport.responses = iter([response({"error": "no"}, status=503)])

    with pytest.raises(Exception, match="no"):
        client.fetch_tickers(["CS"])


@pytest.mark.parametrize(
    "url",
    [
        "http://api.massive.com/path",
        "https://evil.example/path",
        "https://user@api.massive.com/path",
        "https://api.massive.com/path#fragment",
        "not a URL",
    ],
)
def test_invalid_next_url_is_rejected_before_request(
    client_and_transport: tuple[MassiveClient, FakeTransport], url: str
) -> None:
    client, transport = client_and_transport
    transport.responses = iter([response({"results": [], "next_url": url})])

    with pytest.raises(ValueError, match="pagination URL"):
        client.fetch_tickers(["CS"])
    assert len(transport.requests) == 1


def test_repeated_next_page_is_rejected(client_and_transport: tuple[MassiveClient, FakeTransport]) -> None:
    client, transport = client_and_transport
    page = "https://api.massive.com/page2"
    transport.responses = iter(
        [response({"results": [], "next_url": page}), response({"results": [], "next_url": page})]
    )

    with pytest.raises(ValueError, match="Repeated pagination URL"):
        client.fetch_tickers(["CS"])


def test_valid_split_pages_are_deserialized(client_and_transport: tuple[MassiveClient, FakeTransport]) -> None:
    client, transport = client_and_transport
    transport.responses = iter(
        [
            response(
                {
                    "results": [{"ticker": "AAPL", "execution_date": "2024-01-01"}],
                    "next_url": "https://api.massive.com/p2",
                }
            ),
            response({"results": [{"ticker": "MSFT", "execution_date": "2024-02-01"}]}),
        ]
    )

    result = client.fetch_splits(datetime.date(2024, 1, 1), datetime.date(2024, 12, 31))
    assert [row.ticker for row in result] == ["AAPL", "MSFT"]
