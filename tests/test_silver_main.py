"""Tests for silver medallion layer."""

from datetime import date
from unittest.mock import patch

import polars as pl
import pytest


@pytest.fixture
def sample_ticker_data():
    """Create sample ticker details data."""
    return pl.DataFrame({
        "ticker": ["AAPL", "MSFT", "SPY", "QQQ"],
        "name": ["Apple Inc.", "Microsoft Corp.", "SPDR S&P 500", "Invesco QQQ"],
        "type": ["CS", "CS", "ETF", "ETF"],
    })


@pytest.fixture
def sample_split_data():
    """Create sample split details data."""
    return pl.DataFrame({
        "ticker": ["AAPL", "AAPL"],
        "execution_date": [date(2024, 6, 1), date(2020, 8, 31)],
        "split_from": [4.0, 4.0],
        "split_to": [1.0, 1.0],
    })


@pytest.fixture
def sample_daily_data():
    """Create sample daily aggregates data."""
    return pl.DataFrame({
        "ticker": ["AAPL", "AAPL", "MSFT", "MSFT"],
        "timestamp": [1735689600000, 1735776000000, 1735689600000, 1735776000000],
        "open": [150.0, 151.0, 300.0, 301.0],
        "high": [152.0, 153.0, 305.0, 306.0],
        "low": [149.0, 150.0, 298.0, 299.0],
        "close": [151.0, 152.0, 302.0, 303.0],
        "volume": [50000000, 48000000, 30000000, 32000000],
        "transactions": [1500, 1400, 1200, 1300],
    })


@pytest.fixture
def sample_etf_holdings():
    """Create sample ETF holdings data."""
    return pl.DataFrame({
        "ticker": ["AAPL", "MSFT", "GOOGL"],
        "weight": [7.0, 6.5, 4.2],
        "etf": ["spy", "spy", "spy"],
    })


@pytest.fixture
def mock_s3_storage():
    """Mock S3 storage configuration."""
    with patch("tickerlake.silver.main.s3_storage_options") as mock_storage:
        mock_storage.return_value = {
            "key": "test_key",
            "secret": "test_secret",
            "endpoint_url": "http://localhost:9000",
        }
        yield mock_storage


@pytest.fixture
def mock_settings():
    """Mock application settings."""
    with patch("tickerlake.silver.main.settings") as mock_settings:
        mock_settings.s3_bucket_name = "test-bucket"
        mock_settings.etfs = ["SPY", "QQQ", "IWM"]
        yield mock_settings


class TestTickerDetails:
    """Test ticker details functions."""

    @patch("tickerlake.silver.main.pl.scan_parquet")
    def test_read_ticker_details(
        self, mock_scan, sample_ticker_data, mock_s3_storage, mock_settings
    ):
        """Test reading ticker details filters for CS and ETF types."""
        mock_scan.return_value.select.return_value.filter.return_value.collect.return_value = sample_ticker_data

        from tickerlake.silver.main import read_ticker_details

        result = read_ticker_details()

        mock_scan.assert_called_once_with(
            "s3://test-bucket/bronze/tickers/data.parquet",
            storage_options=mock_s3_storage,
        )
        assert isinstance(result, pl.DataFrame)
        # Check that ticker_type column is included
        assert "ticker_type" in result.columns

    @patch("tickerlake.silver.main.write_delta_table")
    @patch("tickerlake.silver.main.delta_table_exists")
    def test_write_ticker_details(
        self,
        mock_exists,
        mock_write_delta,
        sample_ticker_data,
        mock_s3_storage,
        mock_settings,
    ):
        """Test writing ticker details to Delta table."""
        from tickerlake.silver.main import write_ticker_details

        # Test overwrite mode when table doesn't exist
        mock_exists.return_value = False
        write_ticker_details(sample_ticker_data)
        mock_write_delta.assert_called_once_with(
            sample_ticker_data, "tickers", mode="overwrite"
        )


class TestSplitDetails:
    """Test split details functions."""

    @patch("tickerlake.silver.main.pl.scan_parquet")
    def test_read_split_details(
        self, mock_scan, sample_split_data, mock_s3_storage, mock_settings
    ):
        """Test reading split details with ticker filtering."""
        mock_scan.return_value.filter.return_value.select.return_value.collect.return_value = sample_split_data

        from tickerlake.silver.main import read_split_details

        valid_tickers = ["AAPL", "MSFT"]
        result = read_split_details(valid_tickers)

        mock_scan.assert_called_once_with(
            "s3://test-bucket/bronze/splits/data.parquet",
            storage_options=mock_s3_storage,
        )
        assert isinstance(result, pl.DataFrame)

    @patch("tickerlake.silver.main.write_delta_table")
    @patch("tickerlake.silver.main.delta_table_exists")
    def test_write_split_details(
        self,
        mock_exists,
        mock_write_delta,
        sample_split_data,
        mock_s3_storage,
        mock_settings,
    ):
        """Test writing split details to Delta table."""
        from tickerlake.silver.main import write_split_details

        # Test overwrite mode when table doesn't exist
        mock_exists.return_value = False
        write_split_details(sample_split_data)
        mock_write_delta.assert_called_once_with(
            sample_split_data, "splits", mode="overwrite"
        )


class TestETFHoldings:
    """Test ETF holdings functions."""

    @patch("tickerlake.silver.main.pl.read_parquet")
    def test_read_etf_holdings(
        self, mock_read, sample_etf_holdings, mock_s3_storage, mock_settings
    ):
        """Test reading ETF holdings for a specific ETF."""
        mock_read.return_value = sample_etf_holdings.drop("etf")

        from tickerlake.silver.main import read_etf_holdings

        result = read_etf_holdings("SPY")

        mock_read.assert_called_once_with(
            "s3://test-bucket/bronze/holdings/spy/data.parquet",
            storage_options=mock_s3_storage,
        )
        assert isinstance(result, pl.DataFrame)

    @patch("tickerlake.silver.main.read_etf_holdings")
    def test_read_all_etf_holdings(
        self, mock_read_etf, sample_etf_holdings, mock_settings
    ):
        """Test reading and aggregating all ETF holdings."""
        mock_read_etf.return_value = sample_etf_holdings

        from tickerlake.silver.main import read_all_etf_holdings

        result = read_all_etf_holdings()

        assert mock_read_etf.call_count == 3  # Called for SPY, QQQ, IWM
        assert isinstance(result, pl.DataFrame)


class TestDailyAggregates:
    """Test daily aggregates functions."""

    @patch("tickerlake.silver.main.pl.scan_parquet")
    def test_read_daily_aggs(
        self, mock_scan, sample_daily_data, mock_s3_storage, mock_settings
    ):
        """Test reading daily aggregates with ticker filtering."""
        mock_scan.return_value.filter.return_value.collect.return_value = (
            sample_daily_data
        )

        from tickerlake.silver.main import read_daily_aggs

        valid_tickers = ["AAPL", "MSFT"]
        result = read_daily_aggs(valid_tickers)

        mock_scan.assert_called_once_with(
            "s3://test-bucket/bronze/daily/*/data.parquet",
            storage_options=mock_s3_storage,
        )
        assert isinstance(result, pl.DataFrame)

    @patch("tickerlake.silver.main.pl.scan_parquet")
    def test_read_daily_aggs_with_ticker_details(
        self,
        mock_scan,
        sample_daily_data,
        sample_ticker_data,
        mock_s3_storage,
        mock_settings,
    ):
        """Test reading daily aggregates with ticker details for ticker_type."""
        mock_scan.return_value.filter.return_value.collect.return_value = (
            sample_daily_data
        )

        from tickerlake.silver.main import read_daily_aggs

        # Create ticker details with ticker_type
        ticker_details = sample_ticker_data.select([
            "ticker",
            pl.col("type").alias("ticker_type"),
        ])
        valid_tickers = ["AAPL", "MSFT"]
        result = read_daily_aggs(valid_tickers, ticker_details)

        assert isinstance(result, pl.DataFrame)
        # Check that ticker_type column is added
        assert "ticker_type" in result.columns

    def test_apply_splits(self, sample_daily_data, sample_split_data):
        """Test applying split adjustments to daily data."""
        from tickerlake.silver.main import apply_splits

        # Add date column to daily data
        daily_with_date = sample_daily_data.with_columns(
            pl.from_epoch(pl.col("timestamp"), time_unit="ms")
            .cast(pl.Date)
            .alias("date")
        )

        result = apply_splits(daily_with_date, sample_split_data)

        assert isinstance(result, pl.DataFrame)
        assert "date" in result.columns
        assert "ticker" in result.columns

    def test_apply_splits_with_ticker_type(
        self, sample_daily_data, sample_split_data, sample_ticker_data
    ):
        """Test applying split adjustments preserves ticker_type column."""
        from tickerlake.silver.main import apply_splits

        # Add date and ticker_type columns to daily data
        daily_with_date_and_type = sample_daily_data.with_columns(
            pl.from_epoch(pl.col("timestamp"), time_unit="ms")
            .cast(pl.Date)
            .alias("date")
        ).join(
            sample_ticker_data.select(["ticker", pl.col("type").alias("ticker_type")]),
            on="ticker",
            how="left",
        )

        result = apply_splits(daily_with_date_and_type, sample_split_data)

        assert isinstance(result, pl.DataFrame)
        assert "date" in result.columns
        assert "ticker" in result.columns
        assert "ticker_type" in result.columns

    @patch("tickerlake.silver.main.write_delta_table")
    def test_write_daily_aggs(
        self, mock_write_delta, sample_daily_data, mock_s3_storage, mock_settings
    ):
        """Test writing daily aggregates to Delta table."""
        from tickerlake.silver.main import write_daily_aggs

        write_daily_aggs(sample_daily_data, mode="overwrite")
        mock_write_delta.assert_called_once_with(
            sample_daily_data, "daily", mode="overwrite"
        )


class TestTimeAggregates:
    """Test time-based aggregate functions."""

    @pytest.mark.parametrize(
        "period,table_name,function_name",
        [
            ("1w", "weekly", "write_weekly_aggs"),
            ("1mo", "monthly", "write_monthly_aggs"),
        ],
    )
    @patch("tickerlake.silver.main.write_delta_table")
    def test_write_time_aggregates(
        self,
        mock_write_delta,
        sample_daily_data,
        mock_s3_storage,
        mock_settings,
        period,
        table_name,
        function_name,
    ):
        """Test writing weekly and monthly aggregates to Delta tables."""
        from tickerlake.silver import main

        # Add date column required for aggregation
        daily_with_date = sample_daily_data.with_columns(
            pl.from_epoch(pl.col("timestamp"), time_unit="ms")
            .cast(pl.Date)
            .alias("date")
        )

        # Get the appropriate function
        write_function = getattr(main, function_name)
        write_function(daily_with_date)

        # Verify write_delta_table was called with correct table name
        assert mock_write_delta.called
        call_args = mock_write_delta.call_args
        assert table_name in str(call_args)

    @patch("tickerlake.silver.main.write_delta_table")
    def test_write_time_aggs_direct(
        self, mock_write_delta, sample_daily_data, mock_s3_storage, mock_settings
    ):
        """Test write_time_aggs function directly."""
        from tickerlake.silver.main import write_time_aggs

        # Add date column required for aggregation
        daily_with_date = sample_daily_data.with_columns(
            pl.from_epoch(pl.col("timestamp"), time_unit="ms")
            .cast(pl.Date)
            .alias("date")
        )

        write_time_aggs(daily_with_date, "1w", "weekly")

        mock_write_delta.assert_called_once()
        call_args = mock_write_delta.call_args
        assert "weekly" in str(call_args)


class TestMainFunction:
    """Test main pipeline function."""

    @patch("tickerlake.silver.main.SilverLayer")
    def test_main_pipeline_full_rebuild(
        self,
        mock_silver_layer_class,
        sample_ticker_data,
        sample_split_data,
        sample_daily_data,
        sample_etf_holdings,
    ):
        """Test complete silver layer pipeline execution in full rebuild mode."""
        # Create a mock instance
        mock_silver_instance = mock_silver_layer_class.return_value

        from tickerlake.silver.main import main

        main(full_rebuild=True)

        # Verify SilverLayer was instantiated and run was called with full_rebuild=True
        mock_silver_layer_class.assert_called_once()
        mock_silver_instance.run.assert_called_once_with(full_rebuild=True)


class TestDataTransformations:
    """Test data transformation logic."""

    def test_daily_data_type_conversions(self):
        """Test that daily data types are properly converted."""
        raw_data = pl.DataFrame({
            "ticker": ["AAPL", "MSFT"],
            "timestamp": [1735689600000, 1735689600000],
            "open": ["150.5", "300.25"],
            "high": ["152.75", "305.50"],
            "low": ["149.25", "298.75"],
            "close": ["151.50", "302.00"],
            "volume": ["50000000", "30000000"],
        })

        # Simulate the transformations done in read_daily_aggs
        result = raw_data.with_columns(
            pl.from_epoch(pl.col("timestamp"), time_unit="ms")
            .cast(pl.Date)
            .alias("date"),
            pl.col("volume").cast(pl.UInt64),
            pl.col("open").cast(pl.Float64),
            pl.col("close").cast(pl.Float64),
            pl.col("high").cast(pl.Float64),
            pl.col("low").cast(pl.Float64),
        )

        assert result["date"].dtype == pl.Date
        assert result["ticker"].dtype == pl.String
        assert result["volume"].dtype == pl.UInt64
        assert result["open"].dtype == pl.Float64
