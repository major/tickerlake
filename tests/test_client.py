"""Tests for the Massive API client wrapper."""

import datetime
from unittest.mock import MagicMock, patch

import pytest

from tickerlake.client import MassiveClient
from tickerlake.config import Config


@pytest.fixture
def sample_config() -> Config:
    """Create a sample Config for testing."""
    return Config(
        api_key="test-api-key",
        output_dir="/tmp",
        start_date=datetime.date(2024, 1, 1),
        end_date=datetime.date(2024, 12, 31),
        ticker_types=["CS", "ETF", "ETV"],
    )


class TestMassiveClientInit:
    """Tests for MassiveClient initialization."""

    @patch("tickerlake.client.RESTClient")
    def test_init_creates_rest_client(
        self, mock_rest_class: MagicMock, sample_config: Config
    ) -> None:
        """MassiveClient(config) creates RESTClient with config.api_key."""
        mock_rest_class.return_value = MagicMock()

        client = MassiveClient(sample_config)

        mock_rest_class.assert_called_once_with(api_key="test-api-key")
        assert client._client is not None


class TestFetchDailyAggs:
    """Tests for fetch_daily_aggs method."""

    @patch("tickerlake.client.RESTClient")
    def test_fetch_daily_aggs_correct_params(
        self, mock_rest_class: MagicMock, sample_config: Config
    ) -> None:
        """fetch_daily_aggs calls get_grouped_daily_aggs with correct parameters."""
        mock_rest = MagicMock()
        mock_rest_class.return_value = mock_rest
        mock_rest.get_grouped_daily_aggs.return_value = []

        client = MassiveClient(sample_config)
        test_date = datetime.date(2024, 1, 15)
        client.fetch_daily_aggs(test_date)

        mock_rest.get_grouped_daily_aggs.assert_called_once_with(
            date=test_date,
            adjusted=False,
            market_type="stocks",
            include_otc=False,
        )

    @patch("tickerlake.client.RESTClient")
    def test_fetch_daily_aggs_returns_list(
        self, mock_rest_class: MagicMock, sample_config: Config
    ) -> None:
        """fetch_daily_aggs returns the list from the underlying API call."""
        mock_rest = MagicMock()
        mock_rest_class.return_value = mock_rest
        expected_result = [{"ticker": "AAPL", "close": 150.0}]
        mock_rest.get_grouped_daily_aggs.return_value = expected_result

        client = MassiveClient(sample_config)
        result = client.fetch_daily_aggs(datetime.date(2024, 1, 15))

        assert result == expected_result


class TestFetchSplits:
    """Tests for fetch_splits method."""

    @patch("tickerlake.client.RESTClient")
    def test_fetch_splits_correct_params(
        self, mock_rest_class: MagicMock, sample_config: Config
    ) -> None:
        """fetch_splits calls list_stocks_splits with correct parameters as strings."""
        mock_rest = MagicMock()
        mock_rest_class.return_value = mock_rest
        mock_rest.list_stocks_splits.return_value = iter([])

        client = MassiveClient(sample_config)
        start_date = datetime.date(2024, 1, 1)
        end_date = datetime.date(2024, 12, 31)
        client.fetch_splits(start_date, end_date)

        mock_rest.list_stocks_splits.assert_called_once_with(
            execution_date_gte="2024-01-01",
            execution_date_lte="2024-12-31",
        )

    @patch("tickerlake.client.RESTClient")
    def test_fetch_splits_returns_list(
        self, mock_rest_class: MagicMock, sample_config: Config
    ) -> None:
        """fetch_splits materializes the iterator to a list."""
        mock_rest = MagicMock()
        mock_rest_class.return_value = mock_rest
        expected_splits = [
            {
                "ticker": "AAPL",
                "execution_date": "2024-01-15",
                "split_from": 1.0,
                "split_to": 2.0,
            },
            {
                "ticker": "MSFT",
                "execution_date": "2024-02-01",
                "split_from": 1.0,
                "split_to": 3.0,
            },
        ]
        mock_rest.list_stocks_splits.return_value = iter(expected_splits)

        client = MassiveClient(sample_config)
        result = client.fetch_splits(
            datetime.date(2024, 1, 1), datetime.date(2024, 12, 31)
        )

        assert result == expected_splits
        assert isinstance(result, list)


class TestFetchTickers:
    """Tests for fetch_tickers method."""

    @patch("tickerlake.client.RESTClient")
    def test_fetch_tickers_two_calls(
        self, mock_rest_class: MagicMock, sample_config: Config
    ) -> None:
        """fetch_tickers(["CS", "ETF"]) makes exactly 2 calls to list_tickers."""
        mock_rest = MagicMock()
        mock_rest_class.return_value = mock_rest
        mock_rest.list_tickers.return_value = iter([])

        client = MassiveClient(sample_config)
        client.fetch_tickers(["CS", "ETF"])

        assert mock_rest.list_tickers.call_count == 2

    @patch("tickerlake.client.RESTClient")
    def test_fetch_tickers_correct_params(
        self, mock_rest_class: MagicMock, sample_config: Config
    ) -> None:
        """fetch_tickers calls list_tickers with correct parameters for each type."""
        mock_rest = MagicMock()
        mock_rest_class.return_value = mock_rest
        mock_rest.list_tickers.return_value = iter([])

        client = MassiveClient(sample_config)
        client.fetch_tickers(["CS", "ETF"])

        calls = mock_rest.list_tickers.call_args_list
        assert len(calls) == 2

        # First call for CS
        assert calls[0][1] == {
            "market": "stocks",
            "type": "CS",
            "active": True,
            "limit": 1000,
        }

        # Second call for ETF
        assert calls[1][1] == {
            "market": "stocks",
            "type": "ETF",
            "active": True,
            "limit": 1000,
        }

    @patch("tickerlake.client.RESTClient")
    def test_fetch_tickers_concatenates_results(
        self, mock_rest_class: MagicMock, sample_config: Config
    ) -> None:
        """fetch_tickers concatenates results from both calls into one list."""
        mock_rest = MagicMock()
        mock_rest_class.return_value = mock_rest

        cs_tickers = [
            {"ticker": "AAPL", "type": "CS"},
            {"ticker": "MSFT", "type": "CS"},
        ]
        etf_tickers = [
            {"ticker": "SPY", "type": "ETF"},
            {"ticker": "QQQ", "type": "ETF"},
        ]

        # Return different iterators for each call
        mock_rest.list_tickers.side_effect = [iter(cs_tickers), iter(etf_tickers)]

        client = MassiveClient(sample_config)
        result = client.fetch_tickers(["CS", "ETF"])

        expected = cs_tickers + etf_tickers
        assert result == expected
        assert len(result) == 4
