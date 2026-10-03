"""Thin wrapper around massive.RESTClient for the tickerlake pipeline."""

from typing import TYPE_CHECKING, Any
from urllib.parse import parse_qsl, urlsplit

from massive import RESTClient
from massive.rest.models.aggs import GroupedDailyAgg
from massive.rest.models.splits import StockSplit
from massive.rest.models.tickers import Ticker

if TYPE_CHECKING:
    import datetime

    from tickerlake.config import Config


class MassiveClient:
    """Thin wrapper around massive.RESTClient for the tickerlake pipeline."""

    def __init__(self, config: Config) -> None:
        """Initialize with Config, creating the underlying RESTClient."""
        if not config.api_key:
            msg = "MASSIVE_API_KEY environment variable is required"
            raise ValueError(msg)
        self._client = RESTClient(api_key=config.api_key)

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

    def fetch_daily_aggs(self, date: datetime.date) -> list[Any]:
        """Fetch grouped daily aggregates for all tickers on a given date."""
        path = f"/v2/aggs/grouped/locale/us/market/stocks/{date}"
        response = self._client.get_grouped_daily_aggs(
            date=date,
            adjusted=False,
            market_type="stocks",
            include_otc=False,
            raw=True,
        )
        return self._fetch_pages(path, {}, GroupedDailyAgg, response)

    def fetch_splits(self, start_date: datetime.date, end_date: datetime.date) -> list[Any]:
        """Fetch stock splits in the given date range."""
        response = self._client.list_stocks_splits(
            execution_date_gte=str(start_date),
            execution_date_lte=str(end_date),
            raw=True,
        )
        return self._fetch_pages(
            "/stocks/v1/splits",
            {},
            StockSplit,
            response,
        )

    def fetch_tickers(self, types: list[str]) -> list[Any]:
        """Fetch ticker reference data for the given ticker types (e.g. CS, ETF)."""
        result: list[Any] = []
        for ticker_type in types:
            response = self._client.list_tickers(
                market="stocks",
                type=ticker_type,
                active=True,
                limit=1000,
                raw=True,
            )
            result.extend(
                self._fetch_pages(
                    "/v3/reference/tickers",
                    {},
                    Ticker,
                    response,
                )
            )
        return result
