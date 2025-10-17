"""Tests for the bronze tickers module."""

from unittest.mock import MagicMock, patch

import polars as pl
import pytest

from tickerlake.bronze.tickers import get_tickers, load_tickers


@pytest.fixture
def mock_settings():
    """Create a mock settings object for testing."""
    with patch("tickerlake.bronze.tickers.settings") as mock:
        mock.bronze_unified_storage_path = "s3://tickerlake/unified/bronze"
        yield mock


@pytest.fixture
def mock_polygon_client():
    """Create a mock Polygon API client."""
    mock_client = MagicMock()
    return mock_client


@pytest.fixture
def sample_ticker_data():
    """Sample ticker data from Polygon API."""
    # Create mock ticker objects matching the schema
    ticker1 = {
        "active": True,
        "base_currency_name": "US Dollar",
        "base_currency_symbol": "USD",
        "cik": "0000320193",
        "composite_figi": "BBG000B9XRY4",
        "currency_name": "US Dollar",
        "currency_symbol": "USD",
        "delisted_utc": None,
        "last_updated_utc": "2024-01-15T00:00:00Z",
        "locale": "us",
        "market": "stocks",
        "name": "Apple Inc.",
        "primary_exchange": "XNAS",
        "share_class_figi": "BBG001S5N8V8",
        "ticker": "AAPL",
        "type": "CS",
    }

    ticker2 = {
        "active": True,
        "base_currency_name": "US Dollar",
        "base_currency_symbol": "USD",
        "cik": "0001318605",
        "composite_figi": "BBG000N9MNX3",
        "currency_name": "US Dollar",
        "currency_symbol": "USD",
        "delisted_utc": None,
        "last_updated_utc": "2024-01-15T00:00:00Z",
        "locale": "us",
        "market": "stocks",
        "name": "Tesla, Inc.",
        "primary_exchange": "XNAS",
        "share_class_figi": "BBG001SQKGD7",
        "ticker": "TSLA",
        "type": "CS",
    }

    ticker3 = {
        "active": True,
        "base_currency_name": "US Dollar",
        "base_currency_symbol": "USD",
        "cik": "0001045810",
        "composite_figi": "BBG000BBJQV0",
        "currency_name": "US Dollar",
        "currency_symbol": "USD",
        "delisted_utc": None,
        "last_updated_utc": "2024-01-15T00:00:00Z",
        "locale": "us",
        "market": "stocks",
        "name": "NVIDIA Corporation",
        "primary_exchange": "XNAS",
        "share_class_figi": "BBG001S5PQL7",
        "ticker": "NVDA",
        "type": "CS",
    }

    return [ticker1, ticker2, ticker3]


class TestGetTickers:
    """Test cases for get_tickers function."""

    @patch("tickerlake.bronze.tickers.setup_polygon_api_client")
    def test_get_tickers_returns_dataframe(
        self, mock_setup_client, mock_polygon_client, sample_ticker_data
    ):
        """Test get_tickers returns a Polars DataFrame with correct structure."""
        mock_setup_client.return_value = mock_polygon_client
        mock_polygon_client.list_tickers.return_value = sample_ticker_data

        result = get_tickers()

        # Should return a DataFrame
        assert isinstance(result, pl.DataFrame)
        # Should have ticker column at minimum
        assert "ticker" in result.columns
        # Should have correct number of rows
        assert len(result) == 3

    @patch("tickerlake.bronze.tickers.setup_polygon_api_client")
    def test_get_tickers_api_parameters(
        self, mock_setup_client, mock_polygon_client, sample_ticker_data
    ):
        """Test get_tickers calls Polygon API with correct parameters."""
        mock_setup_client.return_value = mock_polygon_client
        mock_polygon_client.list_tickers.return_value = sample_ticker_data

        get_tickers()

        # Verify API was called with correct parameters
        mock_polygon_client.list_tickers.assert_called_once_with(
            market="stocks",
            active=True,
            order="asc",
            sort="ticker",
            limit=1000,
        )

    @patch("tickerlake.bronze.tickers.setup_polygon_api_client")
    def test_get_tickers_data_content(
        self, mock_setup_client, mock_polygon_client, sample_ticker_data
    ):
        """Test get_tickers correctly processes API data."""
        mock_setup_client.return_value = mock_polygon_client
        mock_polygon_client.list_tickers.return_value = sample_ticker_data

        result = get_tickers()

        # Check ticker symbols
        tickers = result["ticker"].to_list()
        assert "AAPL" in tickers
        assert "TSLA" in tickers
        assert "NVDA" in tickers

        # Check first row data if available
        if "name" in result.columns:
            first_row = result.filter(pl.col("ticker") == "AAPL")
            if len(first_row) > 0:
                assert first_row["name"][0] == "Apple Inc."

    @patch("tickerlake.bronze.tickers.setup_polygon_api_client")
    def test_get_tickers_active_only(
        self, mock_setup_client, mock_polygon_client, sample_ticker_data
    ):
        """Test get_tickers only requests active tickers."""
        mock_setup_client.return_value = mock_polygon_client
        mock_polygon_client.list_tickers.return_value = sample_ticker_data

        get_tickers()

        # Verify active=True was passed
        call_kwargs = mock_polygon_client.list_tickers.call_args.kwargs
        assert call_kwargs["active"] is True

    @patch("tickerlake.bronze.tickers.setup_polygon_api_client")
    def test_get_tickers_stocks_market_only(
        self, mock_setup_client, mock_polygon_client, sample_ticker_data
    ):
        """Test get_tickers only requests stocks market."""
        mock_setup_client.return_value = mock_polygon_client
        mock_polygon_client.list_tickers.return_value = sample_ticker_data

        get_tickers()

        # Verify market="stocks" was passed
        call_kwargs = mock_polygon_client.list_tickers.call_args.kwargs
        assert call_kwargs["market"] == "stocks"

    @patch("tickerlake.bronze.tickers.setup_polygon_api_client")
    def test_get_tickers_empty_response(self, mock_setup_client, mock_polygon_client):
        """Test get_tickers handles empty API response."""
        mock_setup_client.return_value = mock_polygon_client
        mock_polygon_client.list_tickers.return_value = []

        result = get_tickers()

        # Should return empty DataFrame
        assert isinstance(result, pl.DataFrame)
        assert len(result) == 0

    @patch("tickerlake.bronze.tickers.setup_polygon_api_client")
    def test_get_tickers_sorted_by_ticker(
        self, mock_setup_client, mock_polygon_client, sample_ticker_data
    ):
        """Test get_tickers requests tickers sorted by ticker symbol."""
        mock_setup_client.return_value = mock_polygon_client
        mock_polygon_client.list_tickers.return_value = sample_ticker_data

        get_tickers()

        # Verify sort parameters
        call_kwargs = mock_polygon_client.list_tickers.call_args.kwargs
        assert call_kwargs["sort"] == "ticker"
        assert call_kwargs["order"] == "asc"

    @pytest.mark.parametrize(
        "ticker_type,description",
        [
            ("CS", "Common Stock"),
            ("ETF", "Exchange Traded Fund"),
            ("ADRC", "American Depositary Receipt Common"),
        ],
    )
    @patch("tickerlake.bronze.tickers.setup_polygon_api_client")
    def test_get_tickers_various_types(
        self,
        mock_setup_client,
        mock_polygon_client,
        ticker_type,
        description,
    ):
        """Test get_tickers handles various ticker types."""
        ticker_data = [
            {
                "ticker": "TEST",
                "name": f"Test {description}",
                "type": ticker_type,
                "active": True,
                "market": "stocks",
            }
        ]

        mock_setup_client.return_value = mock_polygon_client
        mock_polygon_client.list_tickers.return_value = ticker_data

        result = get_tickers()

        assert len(result) == 1
        assert result["ticker"][0] == "TEST"
        if "type" in result.columns:
            assert result["type"][0] == ticker_type


class TestLoadTickers:
    """Test cases for load_tickers function."""

    @patch("tickerlake.bronze.tickers.s3_storage_options", {"option": "value"})
    @patch("tickerlake.bronze.tickers.get_tickers")
    def test_load_tickers_writes_parquet(
        self, mock_get_tickers, mock_settings, sample_ticker_data
    ):
        """Test load_tickers writes DataFrame to Parquet file."""
        mock_df = pl.DataFrame(sample_ticker_data)
        mock_get_tickers.return_value = mock_df

        # Mock the write_parquet method
        with patch.object(pl.DataFrame, "write_parquet") as mock_write:
            load_tickers()

            # Verify write_parquet was called with correct parameters
            mock_write.assert_called_once_with(
                "s3://tickerlake/unified/bronze/tickers/tickers.parquet",
                storage_options={"option": "value"},
            )

    @patch("tickerlake.bronze.tickers.s3_storage_options", {"option": "value"})
    @patch("tickerlake.bronze.tickers.get_tickers")
    def test_load_tickers_calls_get_tickers(self, mock_get_tickers, mock_settings):
        """Test load_tickers calls get_tickers to retrieve data."""
        mock_df = pl.DataFrame(
            {
                "ticker": ["TEST"],
                "name": ["Test Corp"],
                "active": [True],
            }
        )
        mock_get_tickers.return_value = mock_df

        with patch.object(pl.DataFrame, "write_parquet"):
            load_tickers()

            # Verify get_tickers was called
            mock_get_tickers.assert_called_once()

    @patch("tickerlake.bronze.tickers.s3_storage_options", {"option": "value"})
    @patch("tickerlake.bronze.tickers.get_tickers")
    def test_load_tickers_empty_dataframe(self, mock_get_tickers, mock_settings):
        """Test load_tickers handles empty DataFrame correctly."""
        mock_df = pl.DataFrame(schema={"ticker": pl.Utf8, "active": pl.Boolean})
        mock_get_tickers.return_value = mock_df

        with patch.object(pl.DataFrame, "write_parquet") as mock_write:
            load_tickers()

            # Should still write even if empty
            mock_write.assert_called_once()

    @patch("tickerlake.bronze.tickers.s3_storage_options", {"aws_region": "us-east-1"})
    @patch("tickerlake.bronze.tickers.get_tickers")
    def test_load_tickers_storage_options(self, mock_get_tickers, mock_settings):
        """Test load_tickers uses correct storage options."""
        mock_df = pl.DataFrame(
            {
                "ticker": ["TEST"],
                "name": ["Test Corp"],
                "active": [True],
            }
        )
        mock_get_tickers.return_value = mock_df

        with patch.object(pl.DataFrame, "write_parquet") as mock_write:
            load_tickers()

            # Verify storage options were passed
            call_kwargs = mock_write.call_args.kwargs
            assert "storage_options" in call_kwargs
            assert call_kwargs["storage_options"]["aws_region"] == "us-east-1"

    @patch("tickerlake.bronze.tickers.s3_storage_options", {"option": "value"})
    @patch("tickerlake.bronze.tickers.get_tickers")
    def test_load_tickers_destination_path(self, mock_get_tickers, mock_settings):
        """Test load_tickers writes to correct destination path."""
        mock_df = pl.DataFrame({"ticker": ["TEST"]})
        mock_get_tickers.return_value = mock_df

        with patch.object(pl.DataFrame, "write_parquet") as mock_write:
            load_tickers()

            # Verify the path structure
            written_path = mock_write.call_args.args[0]
            assert written_path.endswith("/tickers/tickers.parquet")
            assert "bronze" in written_path
            assert written_path.startswith("s3://")


class TestIntegration:
    """Integration tests for tickers module."""

    @patch("tickerlake.bronze.tickers.s3_storage_options", {"option": "value"})
    @patch("tickerlake.bronze.tickers.setup_polygon_api_client")
    def test_end_to_end_tickers_loading(
        self, mock_setup_client, mock_polygon_client, sample_ticker_data, mock_settings
    ):
        """Test complete flow from API to Parquet file."""
        mock_setup_client.return_value = mock_polygon_client
        mock_polygon_client.list_tickers.return_value = sample_ticker_data

        with patch.object(pl.DataFrame, "write_parquet") as mock_write:
            load_tickers()

            # Verify the complete chain
            mock_setup_client.assert_called_once()
            mock_polygon_client.list_tickers.assert_called_once()
            mock_write.assert_called_once()

            # Verify correct API parameters
            api_kwargs = mock_polygon_client.list_tickers.call_args.kwargs
            assert api_kwargs["market"] == "stocks"
            assert api_kwargs["active"] is True

    @patch("tickerlake.bronze.tickers.s3_storage_options", {"option": "value"})
    @patch("tickerlake.bronze.tickers.setup_polygon_api_client")
    def test_large_ticker_list(
        self, mock_setup_client, mock_polygon_client, mock_settings
    ):
        """Test handling of large ticker lists."""
        # Create a large list of mock tickers
        large_ticker_list = [
            {"ticker": f"TICK{i:04d}", "active": True, "market": "stocks"}
            for i in range(1000)
        ]

        mock_setup_client.return_value = mock_polygon_client
        mock_polygon_client.list_tickers.return_value = large_ticker_list

        result = get_tickers()

        # Should handle all 1000 tickers
        assert len(result) == 1000
        assert result["ticker"][0] == "TICK0000"
        assert result["ticker"][999] == "TICK0999"
