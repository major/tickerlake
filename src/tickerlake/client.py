"""Thin wrapper around massive.RESTClient for the tickerlake pipeline."""

from typing import TYPE_CHECKING, Any, cast

from massive import RESTClient

if TYPE_CHECKING:
    import datetime

    from tickerlake.config import Config


class MassiveClient:
    """Thin wrapper around massive.RESTClient for the tickerlake pipeline."""

    def __init__(self, config: Config) -> None:
        """Initialize with Config, creating the underlying RESTClient."""
        self._client = RESTClient(api_key=config.api_key)

    def fetch_daily_aggs(self, date: datetime.date) -> list[Any]:
        """Fetch grouped daily aggregates for all tickers on a given date."""
        return cast(
            "list[Any]",
            self._client.get_grouped_daily_aggs(
                date=date,
                adjusted=False,
                market_type="stocks",
                include_otc=False,
            ),
        )

    def fetch_splits(
        self, start_date: datetime.date, end_date: datetime.date
    ) -> list[Any]:
        """Fetch stock splits in the given date range."""
        return list(
            self._client.list_stocks_splits(
                execution_date_gte=str(start_date),
                execution_date_lte=str(end_date),
            )
        )

    def fetch_tickers(self, types: list[str]) -> list[Any]:
        """Fetch ticker reference data for the given ticker types (e.g. CS, ETF)."""
        result: list[Any] = []
        for ticker_type in types:
            result.extend(
                self._client.list_tickers(
                    market="stocks",
                    type=ticker_type,
                    active=True,
                    limit=1000,
                )
            )
        return result
