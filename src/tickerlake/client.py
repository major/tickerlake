"""Massive API client seam: domain records, protocol, and SDK adapter.

This module is the boundary between the tickerlake pipeline and the massive
SDK. Pipelines depend only on the :class:`MassiveClient` protocol and the
frozen records it returns. The concrete :class:`SdkMassiveClient` maps the
SDK's models onto those records, and nothing here coerces or validates data:
``extract.py`` remains the validator.
"""

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable
from urllib.parse import parse_qsl, urlsplit

from massive import RESTClient
from massive.rest.models.aggs import GroupedDailyAgg
from massive.rest.models.splits import StockSplit
from massive.rest.models.tickers import Ticker

if TYPE_CHECKING:
    import datetime

    from tickerlake.config import Config


@dataclass(frozen=True, slots=True)
class DailyAgg:
    """One grouped daily aggregate bar for a single ticker and session.

    Fields mirror the SDK model honestly, so every value is optional and only
    the fields ``extract.py`` reads are present.
    """

    ticker: str | None = None
    open: float | None = None
    high: float | None = None
    low: float | None = None
    close: float | None = None
    volume: float | None = None
    timestamp: int | None = None


@dataclass(frozen=True, slots=True)
class SplitRecord:
    """One stock split event with its ratio and adjustment metadata.

    ``execution_date`` stays an ISO string; ``extract.py`` parses and validates
    it rather than letting this adapter coerce it.
    """

    ticker: str | None = None
    execution_date: str | None = None
    split_from: float | None = None
    split_to: float | None = None
    historical_adjustment_factor: float | None = None
    adjustment_type: str | None = None


@dataclass(frozen=True, slots=True)
class TickerRecord:
    """One ticker reference record for the reference catalog."""

    ticker: str | None = None
    name: str | None = None
    type: str | None = None
    primary_exchange: str | None = None
    cik: str | None = None
    active: bool | None = None


@runtime_checkable
class MassiveClient(Protocol):
    """Domain seam for the Massive market-data source.

    Callers depend on this protocol instead of the concrete SDK adapter, which
    keeps the pipeline testable with plain fakes.
    """

    def fetch_daily_aggs(self, date: datetime.date) -> list[DailyAgg]:
        """Fetch grouped daily aggregates for every ticker on one date."""
        ...

    def fetch_splits(self, start_date: datetime.date, end_date: datetime.date) -> list[SplitRecord]:
        """Fetch stock split events in an inclusive date range."""
        ...

    def fetch_tickers(self, types: list[str]) -> list[TickerRecord]:
        """Fetch ticker reference records for each requested ticker type."""
        ...


class SdkMassiveClient:
    """Adapter over ``massive.RESTClient`` returning domain records.

    Each fetch method performs an explicit model-to-record mapping. The adapter
    never coerces values; ``extract.py`` is the validator.
    """

    def __init__(self, config: Config) -> None:
        """Initialize with Config, creating the underlying RESTClient."""
        if not config.api_key:
            msg = "MASSIVE_API_KEY environment variable is required"
            raise ValueError(msg)
        self._client = RESTClient(api_key=config.api_key)

    # Why we keep our own pagination follower instead of the SDK's pagination:
    #
    # 1. ``get_grouped_daily_aggs`` bypasses the SDK's ``_paginate`` helper
    #    entirely, so the SDK cannot paginate our largest and most important
    #    call at all.
    # 2. The SDK's ``_paginate_iter`` silently swallows malformed JSON and
    #    responses without a ``results`` list. Turning a transport fault into a
    #    truncated page list would publish silent data holes in the raw tables.
    # 3. Our local follower validates every page before returning any records
    #    and fails closed, so a bad page surfaces as an extraction failure.
    #
    # Conclusion: keep the strict local pagination follower below.
    def _fetch_pages(self, path: str, params: dict[str, Any], model: Any, first_response: Any) -> list[Any]:
        """Fetch and validate every page before returning any records."""
        result: list[Any] = []
        visited: set[str] = set()
        next_url: str | None = None
        response = first_response
        while True:
            if next_url is not None:
                parsed = urlsplit(next_url)
                origin = urlsplit(self._client.BASE)
                if (
                    parsed.scheme != "https"
                    or parsed.hostname != origin.hostname
                    or parsed.port != origin.port
                    or parsed.username is not None
                    or parsed.password is not None
                    or parsed.fragment
                    or (parsed.path and not parsed.path.startswith("/"))
                ):
                    raise ValueError("Invalid pagination URL from Massive API")  # noqa: TRY003
                path = parsed.path or "/"
                params = dict(parse_qsl(parsed.query))
                response = self._client._get(path, params=params, raw=True)  # noqa: SLF001

            try:
                payload = self._client.json.loads(response.data.decode("utf-8"))
            except (UnicodeDecodeError, ValueError) as exc:
                raise ValueError("Invalid JSON response from Massive API") from exc  # noqa: TRY003
            if not isinstance(payload, dict) or not isinstance(payload.get("results"), list):
                raise TypeError("Massive API response must contain a results list")  # noqa: TRY003
            result.extend(model.from_dict(row) for row in payload["results"])
            next_url = payload.get("next_url")
            if next_url is None:
                return result
            if not isinstance(next_url, str) or not next_url:
                raise ValueError("Invalid pagination URL from Massive API")  # noqa: TRY003
            if next_url in visited:
                raise ValueError("Repeated pagination URL from Massive API")  # noqa: TRY003
            visited.add(next_url)

    def fetch_daily_aggs(self, date: datetime.date) -> list[DailyAgg]:
        """Fetch grouped daily aggregates for all tickers on a given date."""
        path = f"/v2/aggs/grouped/locale/us/market/stocks/{date}"
        response = self._client.get_grouped_daily_aggs(
            date=date,
            adjusted=False,
            market_type="stocks",
            include_otc=False,
            raw=True,
        )
        # GroupedDailyAgg.from_dict maps the wire keys T -> ticker and t -> timestamp.
        records = self._fetch_pages(path, {}, GroupedDailyAgg, response)
        return [
            DailyAgg(
                ticker=record.ticker,
                open=record.open,
                high=record.high,
                low=record.low,
                close=record.close,
                volume=record.volume,
                timestamp=record.timestamp,
            )
            for record in records
        ]

    def fetch_splits(self, start_date: datetime.date, end_date: datetime.date) -> list[SplitRecord]:
        """Fetch stock splits in the given date range."""
        response = self._client.list_stocks_splits(
            execution_date_gte=str(start_date),
            execution_date_lte=str(end_date),
            raw=True,
        )
        records = self._fetch_pages(
            "/stocks/v1/splits",
            {},
            StockSplit,
            response,
        )
        return [
            SplitRecord(
                ticker=record.ticker,
                execution_date=record.execution_date,
                split_from=record.split_from,
                split_to=record.split_to,
                historical_adjustment_factor=record.historical_adjustment_factor,
                adjustment_type=record.adjustment_type,
            )
            for record in records
        ]

    def fetch_tickers(self, types: list[str]) -> list[TickerRecord]:
        """Fetch ticker reference data for the given ticker types (e.g. CS, ETF)."""
        result: list[TickerRecord] = []
        for ticker_type in types:
            response = self._client.list_tickers(
                market="stocks",
                type=ticker_type,
                active=True,
                limit=1000,
                raw=True,
            )
            records = self._fetch_pages(
                "/v3/reference/tickers",
                {},
                Ticker,
                response,
            )
            result.extend(
                TickerRecord(
                    ticker=record.ticker,
                    name=record.name,
                    type=record.type,
                    primary_exchange=record.primary_exchange,
                    cik=record.cik,
                    active=record.active,
                )
                for record in records
            )
        return result


def make_massive_client(config: Config) -> MassiveClient:
    """Build the production Massive client behind the domain protocol."""
    return SdkMassiveClient(config)
