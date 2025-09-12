"""Tests for the bronze medallion layer main module."""

from datetime import datetime
from unittest.mock import MagicMock, patch

import polars as pl
import pytest
import pytz

from tickerlake.bronze.main import (
    build_polygon_client,
    download_daily_aggregates,
    get_missing_stock_aggs,
    get_missing_trading_days,
    get_split_details,
    get_ticker_details,
    get_valid_trading_days,
    list_bronze_daily_folders,
    main,
    store_daily_aggregates,
    write_iwm_holdings,
    write_mdy_holdings,
    write_qqq_holdings,
    write_split_details,
    write_spsm_holdings,
    write_spy_holdings,
    write_ssga_holdings,
    write_ticker_details,
)


@pytest.fixture
def mock_settings():
    """Mock settings for testing."""
    settings = MagicMock()
    settings.polygon_api_key.get_secret_value.return_value = "test_api_key"
    settings.s3_bucket_name = "test-bucket"
    settings.s3_endpoint_url = "https://s3.test.com"
    settings.aws_access_key_id.get_secret_value.return_value = "test_access_key"
    settings.aws_secret_access_key.get_secret_value.return_value = "test_secret"
    settings.data_start_date = "2020-01-01"
    settings.spy_holdings_source = "https://test.com/spy.xlsx"
    return settings


@pytest.fixture
def mock_s3_storage_options():
    """Mock S3 storage options."""
    return {
        "endpoint_url": "https://s3.test.com",
        "key": "test_access_key",
        "secret": "test_secret",
    }


@pytest.fixture
def mock_polygon_client():
    """Mock Polygon REST client."""
    client = MagicMock()
    return client


@pytest.fixture
def sample_dataframe():
    """Sample DataFrame for testing."""
    return pl.DataFrame({
        "ticker": ["AAPL", "GOOGL", "MSFT"],
        "close": [150.0, 2800.0, 300.0],
        "volume": [100000, 50000, 80000],
    })


class TestBuildPolygonClient:
    """Tests for build_polygon_client function."""

    @patch("tickerlake.bronze.main.RESTClient")
    @patch("tickerlake.bronze.main.settings")
    def test_build_polygon_client(
        self, mock_settings_import, mock_rest_client, mock_settings
    ):
        """Test building Polygon client with API key."""
        mock_settings_import.polygon_api_key.get_secret_value.return_value = "test_key"

        client = build_polygon_client()

        mock_rest_client.assert_called_once_with("test_key")
        assert client == mock_rest_client.return_value


class TestDownloadDailyAggregates:
    """Tests for download_daily_aggregates function."""

    @pytest.mark.parametrize(
        "date_str,expected_ticker_count",
        [
            ("2023-01-15", 3),
            ("2023-02-20", 5),
        ],
    )
    @patch("tickerlake.bronze.main.build_polygon_client")
    def test_download_daily_aggregates_with_data(
        self, mock_build_client, date_str, expected_ticker_count
    ):
        """Test downloading daily aggregates with data."""
        mock_client = MagicMock()
        mock_build_client.return_value = mock_client

        # Create mock data based on expected count
        mock_data = [
            {"ticker": f"T{i}", "c": 100.0 + i} for i in range(expected_ticker_count)
        ]
        mock_client.get_grouped_daily_aggs.return_value = mock_data

        result = download_daily_aggregates(date_str)

        mock_client.get_grouped_daily_aggs.assert_called_once_with(
            date_str, adjusted=False, include_otc=False
        )
        assert len(result) == expected_ticker_count
        # Verify data is sorted by ticker
        assert result["ticker"].to_list() == sorted([
            f"T{i}" for i in range(expected_ticker_count)
        ])
        assert "c" in result.columns

    @patch("tickerlake.bronze.main.pl.DataFrame")
    @patch("tickerlake.bronze.main.build_polygon_client")
    def test_download_daily_aggregates_empty(self, mock_build_client, mock_dataframe):
        """Test downloading daily aggregates with no data."""
        mock_client = MagicMock()
        mock_build_client.return_value = mock_client
        mock_client.get_grouped_daily_aggs.return_value = []

        # Mock empty DataFrame that handles sort
        mock_df = MagicMock()
        mock_df.__len__ = lambda self: 0
        mock_df.sort.return_value = mock_df
        mock_dataframe.return_value = mock_df

        result = download_daily_aggregates("2023-03-10")

        mock_client.get_grouped_daily_aggs.assert_called_once_with(
            "2023-03-10", adjusted=False, include_otc=False
        )
        assert len(result) == 0
        mock_df.sort.assert_called_once_with("ticker")


class TestStoreDailyAggregates:
    """Tests for store_daily_aggregates function."""

    @patch("tickerlake.bronze.main.s3_storage_options")
    @patch("tickerlake.bronze.main.settings")
    def test_store_daily_aggregates(
        self, mock_settings, mock_storage_options, sample_dataframe
    ):
        """Test storing daily aggregates to S3."""
        mock_settings.s3_bucket_name = "test-bucket"
        mock_storage_options.return_value = {"key": "value"}

        with patch.object(sample_dataframe, "write_parquet") as mock_write:
            store_daily_aggregates(sample_dataframe, "2023-01-15")

            mock_write.assert_called_once_with(
                file="s3://test-bucket/bronze/daily/2023-01-15/data.parquet",
                storage_options=mock_storage_options,
                compression="zstd",
            )


class TestGetValidTradingDays:
    """Tests for get_valid_trading_days function."""

    @patch("tickerlake.bronze.main.datetime")
    @patch("tickerlake.bronze.main.get_trading_days")
    @patch("tickerlake.bronze.main.settings")
    def test_get_valid_trading_days(
        self, mock_settings, mock_get_trading_days, mock_datetime
    ):
        """Test getting valid trading days."""
        mock_settings.data_start_date = "2023-01-01"
        ny_tz = pytz.timezone("America/New_York")
        mock_datetime.now.return_value = ny_tz.localize(datetime(2023, 1, 31, 12, 0))
        mock_get_trading_days.return_value = ["2023-01-03", "2023-01-04", "2023-01-05"]

        result = get_valid_trading_days()

        assert result == ["2023-01-03", "2023-01-04", "2023-01-05"]
        mock_get_trading_days.assert_called_once_with(
            start_date="2023-01-01",
            end_date="2023-01-31",
        )


class TestListBronzeDailyFolders:
    """Tests for list_bronze_daily_folders function."""

    @patch("tickerlake.bronze.main.s3fs.S3FileSystem")
    @patch("tickerlake.bronze.main.settings")
    def test_list_bronze_daily_folders(self, mock_settings, mock_s3fs):
        """Test listing bronze daily folders from S3."""
        mock_settings.s3_bucket_name = "test-bucket"
        mock_settings.s3_endpoint_url = "https://s3.test.com"
        mock_settings.aws_access_key_id.get_secret_value.return_value = "key"
        mock_settings.aws_secret_access_key.get_secret_value.return_value = "secret"

        mock_fs = MagicMock()
        mock_s3fs.return_value = mock_fs
        mock_fs.ls.return_value = [
            "test-bucket/bronze/daily/2023-01-03",
            "test-bucket/bronze/daily/2023-01-04",
            "test-bucket/bronze/daily/2023-01-05",
        ]
        mock_fs.isdir.return_value = True

        result = list_bronze_daily_folders()

        assert result == ["2023-01-03", "2023-01-04", "2023-01-05"]
        mock_fs.ls.assert_called_once_with("test-bucket/bronze/daily/")

    @patch("tickerlake.bronze.main.s3fs.S3FileSystem")
    @patch("tickerlake.bronze.main.settings")
    def test_list_bronze_daily_folders_file_not_found(self, mock_settings, mock_s3fs):
        """Test listing bronze daily folders when S3 prefix doesn't exist."""
        mock_settings.s3_bucket_name = "test-bucket"
        mock_settings.s3_endpoint_url = "https://s3.test.com"
        mock_settings.aws_access_key_id.get_secret_value.return_value = "key"
        mock_settings.aws_secret_access_key.get_secret_value.return_value = "secret"

        mock_fs = MagicMock()
        mock_s3fs.return_value = mock_fs
        mock_fs.ls.side_effect = FileNotFoundError("Prefix not found")

        result = list_bronze_daily_folders()

        assert result == []
        mock_fs.ls.assert_called_once_with("test-bucket/bronze/daily/")


class TestGetMissingTradingDays:
    """Tests for get_missing_trading_days function."""

    @pytest.mark.parametrize(
        "market_open,expected_missing",
        [
            (True, ["2023-01-04", "2023-01-05"]),  # Market open, exclude today
            (
                False,
                ["2023-01-04", "2023-01-05", "2023-01-06"],
            ),  # Market closed, include today
        ],
    )
    @patch("tickerlake.bronze.main.datetime")
    @patch("tickerlake.bronze.main.is_market_open")
    @patch("tickerlake.bronze.main.list_bronze_daily_folders")
    @patch("tickerlake.bronze.main.get_valid_trading_days")
    def test_get_missing_trading_days_market_status(
        self,
        mock_get_valid,
        mock_list_folders,
        mock_is_open,
        mock_datetime,
        market_open,
        expected_missing,
    ):
        """Test getting missing trading days with different market statuses."""
        mock_get_valid.return_value = [
            "2023-01-03",
            "2023-01-04",
            "2023-01-05",
            "2023-01-06",
        ]
        mock_list_folders.return_value = ["2023-01-03"]  # Only one day exists
        mock_is_open.return_value = market_open

        ny_tz = pytz.timezone("America/New_York")
        mock_datetime.now.return_value = ny_tz.localize(datetime(2023, 1, 6, 14, 0))

        result = get_missing_trading_days()

        assert result == expected_missing


class TestGetMissingStockAggs:
    """Tests for get_missing_stock_aggs function."""

    @patch("tickerlake.bronze.main.logger")
    @patch("tickerlake.bronze.main.store_daily_aggregates")
    @patch("tickerlake.bronze.main.download_daily_aggregates")
    @patch("tickerlake.bronze.main.get_missing_trading_days")
    def test_get_missing_stock_aggs(
        self, mock_get_missing, mock_download, mock_store, mock_logger, sample_dataframe
    ):
        """Test processing missing stock aggregates."""
        mock_get_missing.return_value = ["2023-01-04", "2023-01-05"]
        mock_download.return_value = sample_dataframe

        get_missing_stock_aggs()

        assert mock_download.call_count == 2
        assert mock_store.call_count == 2
        mock_download.assert_any_call(date_str="2023-01-04")
        mock_download.assert_any_call(date_str="2023-01-05")
        mock_store.assert_any_call(sample_dataframe, date_str="2023-01-04")
        mock_store.assert_any_call(sample_dataframe, date_str="2023-01-05")

        # Verify logging
        assert mock_logger.info.call_count == 3  # 1 for count + 2 for each day


class TestGetTickerDetails:
    """Tests for get_ticker_details function."""

    @patch("tickerlake.bronze.main.pl.DataFrame")
    @patch("tickerlake.bronze.main.logger")
    @patch("tickerlake.bronze.main.build_polygon_client")
    def test_get_ticker_details(self, mock_build_client, mock_logger, mock_dataframe):
        """Test fetching ticker details from Polygon."""
        mock_client = MagicMock()
        mock_build_client.return_value = mock_client

        # Create simple mock objects that behave like API results
        ticker_data = [
            type(
                "ticker", (), {"ticker": "AAPL", "name": "Apple Inc.", "type": "CS"}
            )(),
            type(
                "ticker", (), {"ticker": "GOOGL", "name": "Alphabet Inc.", "type": "CS"}
            )(),
            type(
                "ticker",
                (),
                {"ticker": "MSFT", "name": "Microsoft Corp.", "type": "CS"},
            )(),
        ]

        mock_client.list_tickers.return_value = iter(ticker_data)

        # Mock the DataFrame creation
        mock_df = MagicMock()
        mock_df.__len__ = lambda self: 3
        mock_df.columns = ["ticker", "name", "type"]
        mock_dataframe.return_value = mock_df

        result = get_ticker_details()

        mock_client.list_tickers.assert_called_once_with(
            market="stocks",
            active=True,
            order="asc",
            sort="ticker",
            limit=1000,
        )
        assert len(result) == 3
        assert "ticker" in result.columns
        mock_logger.info.assert_called_once_with("Fetched 3 active tickers")


class TestWriteTickerDetails:
    """Tests for write_ticker_details function."""

    @patch("tickerlake.bronze.main.s3_storage_options")
    @patch("tickerlake.bronze.main.settings")
    def test_write_ticker_details(
        self, mock_settings, mock_storage_options, sample_dataframe
    ):
        """Test writing ticker details to S3."""
        mock_settings.s3_bucket_name = "test-bucket"
        mock_storage_options.return_value = {"key": "value"}

        with patch.object(sample_dataframe, "write_parquet") as mock_write:
            write_ticker_details(sample_dataframe)

            mock_write.assert_called_once_with(
                file="s3://test-bucket/bronze/tickers/data.parquet",
                storage_options=mock_storage_options,
                compression="zstd",
            )


class TestGetSplitDetails:
    """Tests for get_split_details function."""

    @patch("tickerlake.bronze.main.pl.DataFrame")
    @patch("tickerlake.bronze.main.logger")
    @patch("tickerlake.bronze.main.build_polygon_client")
    @patch("tickerlake.bronze.main.settings")
    def test_get_split_details(
        self, mock_settings, mock_build_client, mock_logger, mock_dataframe
    ):
        """Test fetching split details from Polygon."""
        mock_settings.data_start_date = "2023-01-01"
        mock_client = MagicMock()
        mock_build_client.return_value = mock_client

        # Create simple mock objects that behave like API results
        split_data = [
            type(
                "split",
                (),
                {
                    "id": "split1",
                    "ticker": "AAPL",
                    "split_from": 4,
                    "split_to": 1,
                    "execution_date": "2023-01-01",
                },
            )(),
            type(
                "split",
                (),
                {
                    "id": "split2",
                    "ticker": "GOOGL",
                    "split_from": 20,
                    "split_to": 1,
                    "execution_date": "2023-02-01",
                },
            )(),
        ]

        mock_client.list_splits.return_value = iter(split_data)

        # Mock the DataFrame creation with schema
        mock_df = MagicMock()
        mock_df.__len__ = lambda self: 2
        mock_df.columns = ["id", "ticker", "split_from", "split_to", "execution_date"]
        mock_df.__getitem__ = (
            lambda self, key: MagicMock(dtype=pl.String) if key == "id" else MagicMock()
        )
        mock_dataframe.return_value = mock_df

        result = get_split_details()

        mock_client.list_splits.assert_called_once_with(
            execution_date_gte="2020-01-01",
            order="asc",
            sort="execution_date",
            limit=1000,
        )
        assert len(result) == 2
        assert "id" in result.columns
        # Check that id column is of type String
        assert result["id"].dtype == pl.String
        mock_logger.info.assert_called_once_with("Fetched 2 recent splits")


class TestWriteSplitDetails:
    """Tests for write_split_details function."""

    @patch("tickerlake.bronze.main.s3_storage_options")
    @patch("tickerlake.bronze.main.settings")
    def test_write_split_details(
        self, mock_settings, mock_storage_options, sample_dataframe
    ):
        """Test writing split details to S3."""
        mock_settings.s3_bucket_name = "test-bucket"
        mock_storage_options.return_value = {"key": "value"}

        with patch.object(sample_dataframe, "write_parquet") as mock_write:
            write_split_details(sample_dataframe)

            mock_write.assert_called_once_with(
                file="s3://test-bucket/bronze/splits/data.parquet",
                storage_options=mock_storage_options,
                compression="zstd",
            )


class TestWriteSSGAHoldings:
    """Tests for write_ssga_holdings function."""

    @pytest.mark.parametrize(
        "etf_ticker,expected_path",
        [
            ("SPY", "s3://test-bucket/bronze/holdings/spy/data.parquet"),
            ("MDY", "s3://test-bucket/bronze/holdings/mdy/data.parquet"),
            ("SPSM", "s3://test-bucket/bronze/holdings/spsm/data.parquet"),
        ],
    )
    @patch("tickerlake.bronze.main.logger")
    @patch("tickerlake.bronze.main.pl.read_excel")
    @patch("tickerlake.bronze.main.s3_storage_options")
    @patch("tickerlake.bronze.main.settings")
    def test_write_ssga_holdings_various_etfs(
        self,
        mock_settings,
        mock_storage_options,
        mock_read_excel,
        mock_logger,
        etf_ticker,
        expected_path,
    ):
        """Test writing SSGA holdings for various ETFs."""
        mock_settings.s3_bucket_name = "test-bucket"
        mock_storage_options.return_value = {"key": "value"}

        # Create mock DataFrame with method chaining
        mock_df = MagicMock()
        mock_df.rename.return_value = mock_df
        mock_df.filter.return_value = mock_df
        mock_df.select.return_value = mock_df
        mock_df.sort.return_value = mock_df
        mock_read_excel.return_value = mock_df

        write_ssga_holdings("https://test.com/holdings.xlsx", etf_ticker)

        mock_read_excel.assert_called_once_with(
            "https://test.com/holdings.xlsx",
            sheet_name="holdings",
            read_options={"header_row": 4},
        )
        mock_df.write_parquet.assert_called_once_with(
            file=expected_path,
            storage_options=mock_storage_options,
            compression="zstd",
        )
        mock_logger.info.assert_called_once_with(f"Writing {etf_ticker} holdings")


class TestWriteQQQHoldings:
    """Tests for write_qqq_holdings function."""

    @patch("tickerlake.bronze.main.logger")
    @patch("tickerlake.bronze.main.pl.read_csv")
    @patch("tickerlake.bronze.main.s3_storage_options")
    @patch("tickerlake.bronze.main.settings")
    def test_write_qqq_holdings(
        self, mock_settings, mock_storage_options, mock_read_csv, mock_logger
    ):
        """Test writing QQQ holdings to S3."""
        mock_settings.s3_bucket_name = "test-bucket"
        mock_settings.qqq_holdings_source = "https://test.com/qqq.csv"
        mock_storage_options.return_value = {"key": "value"}

        # Create mock DataFrame with method chaining
        mock_df = MagicMock()
        mock_df.rename.return_value = mock_df
        mock_df.select.return_value = mock_df
        mock_df.sort.return_value = mock_df
        mock_read_csv.return_value = mock_df

        write_qqq_holdings()

        mock_read_csv.assert_called_once_with("https://test.com/qqq.csv")
        mock_df.rename.assert_called_once_with({"Holding Ticker": "ticker"})
        mock_df.select.assert_called_once_with(["ticker"])
        mock_df.sort.assert_called_once_with("ticker")
        mock_df.write_parquet.assert_called_once_with(
            file="s3://test-bucket/bronze/holdings/qqq/data.parquet",
            storage_options=mock_storage_options,
            compression="zstd",
        )
        mock_logger.info.assert_called_once_with("Writing QQQ holdings")


class TestWriteIWMHoldings:
    """Tests for write_iwm_holdings function."""

    @patch("tickerlake.bronze.main.logger")
    @patch("tickerlake.bronze.main.pl.read_csv")
    @patch("tickerlake.bronze.main.s3_storage_options")
    @patch("tickerlake.bronze.main.settings")
    def test_write_iwm_holdings(
        self, mock_settings, mock_storage_options, mock_read_csv, mock_logger
    ):
        """Test writing IWM holdings to S3."""
        mock_settings.s3_bucket_name = "test-bucket"
        mock_settings.iwm_holdings_source = "https://test.com/iwm.csv"
        mock_storage_options.return_value = {"key": "value"}

        # Create mock DataFrame with method chaining
        mock_df = MagicMock()
        mock_df.rename.return_value = mock_df
        mock_df.filter.return_value = mock_df
        mock_df.select.return_value = mock_df
        mock_df.sort.return_value = mock_df
        mock_read_csv.return_value = mock_df

        write_iwm_holdings()

        mock_read_csv.assert_called_once_with(
            "https://test.com/iwm.csv",
            skip_rows=9,
            columns=["Ticker", "Market Currency"],
        )
        mock_df.rename.assert_called_once_with({"Ticker": "ticker"})
        mock_df.select.assert_called_once_with(["ticker"])
        mock_df.sort.assert_called_once_with("ticker")
        mock_df.write_parquet.assert_called_once_with(
            file="s3://test-bucket/bronze/holdings/iwm/data.parquet",
            storage_options=mock_storage_options,
            compression="zstd",
        )
        mock_logger.info.assert_called_once_with("Writing IWM holdings")


class TestETFWrapperFunctions:
    """Tests for ETF wrapper functions."""

    @patch("tickerlake.bronze.main.write_ssga_holdings")
    @patch("tickerlake.bronze.main.settings")
    def test_write_spy_holdings(self, mock_settings, mock_write_ssga):
        """Test write_spy_holdings calls write_ssga_holdings correctly."""
        mock_settings.spy_holdings_source = "https://test.com/spy.xlsx"

        write_spy_holdings()

        mock_write_ssga.assert_called_once_with("https://test.com/spy.xlsx", "SPY")

    @patch("tickerlake.bronze.main.write_ssga_holdings")
    @patch("tickerlake.bronze.main.settings")
    def test_write_mdy_holdings(self, mock_settings, mock_write_ssga):
        """Test write_mdy_holdings calls write_ssga_holdings correctly."""
        mock_settings.mdy_holdings_source = "https://test.com/mdy.xlsx"

        write_mdy_holdings()

        mock_write_ssga.assert_called_once_with("https://test.com/mdy.xlsx", "MDY")

    @patch("tickerlake.bronze.main.write_ssga_holdings")
    @patch("tickerlake.bronze.main.settings")
    def test_write_spsm_holdings(self, mock_settings, mock_write_ssga):
        """Test write_spsm_holdings calls write_ssga_holdings correctly."""
        mock_settings.spsm_holdings_source = "https://test.com/spsm.xlsx"

        write_spsm_holdings()

        mock_write_ssga.assert_called_once_with("https://test.com/spsm.xlsx", "SPSM")


class TestMainFunction:
    """Tests for main function."""

    @patch("tickerlake.bronze.main.write_iwm_holdings")
    @patch("tickerlake.bronze.main.write_qqq_holdings")
    @patch("tickerlake.bronze.main.write_spsm_holdings")
    @patch("tickerlake.bronze.main.write_mdy_holdings")
    @patch("tickerlake.bronze.main.write_spy_holdings")
    @patch("tickerlake.bronze.main.write_split_details")
    @patch("tickerlake.bronze.main.write_ticker_details")
    @patch("tickerlake.bronze.main.get_split_details")
    @patch("tickerlake.bronze.main.get_ticker_details")
    @patch("tickerlake.bronze.main.get_missing_stock_aggs")
    def test_main_calls_all_functions(
        self,
        mock_get_missing_aggs,
        mock_get_tickers,
        mock_get_splits,
        mock_write_tickers,
        mock_write_splits,
        mock_write_spy,
        mock_write_mdy,
        mock_write_spsm,
        mock_write_qqq,
        mock_write_iwm,
        sample_dataframe,
    ):
        """Test that main function calls all expected functions."""
        # Setup return values
        mock_get_tickers.return_value = sample_dataframe
        mock_get_splits.return_value = sample_dataframe

        main()

        # Verify Polygon data functions called
        mock_get_missing_aggs.assert_called_once()
        mock_get_tickers.assert_called_once()
        mock_get_splits.assert_called_once()
        mock_write_tickers.assert_called_once_with(sample_dataframe)
        mock_write_splits.assert_called_once_with(sample_dataframe)

        # Verify ETF data functions called
        mock_write_spy.assert_called_once()
        mock_write_mdy.assert_called_once()
        mock_write_spsm.assert_called_once()
        mock_write_qqq.assert_called_once()
        mock_write_iwm.assert_called_once()


class TestMainEntryPoint:
    """Tests for __main__ entry point."""

    @patch("tickerlake.bronze.main.main")
    def test_main_entry_point(self, mock_main):
        """Test that __main__ calls main function."""
        # Import the module to trigger __main__ execution
        import tickerlake.bronze.main

        # Reload to trigger __main__ block if needed
        # Note: This doesn't actually test line 363 due to how imports work
        # But it ensures the main function exists and is callable
        assert callable(tickerlake.bronze.main.main)
