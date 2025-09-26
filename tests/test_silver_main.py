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
        assert result["ticker_type"].dtype == pl.Categorical

    @patch("polars.DataFrame.write_parquet")
    def test_write_ticker_details(
        self, mock_write, sample_ticker_data, mock_s3_storage, mock_settings
    ):
        """Test writing ticker details to S3."""
        from tickerlake.silver.main import write_ticker_details

        write_ticker_details(sample_ticker_data)

        mock_write.assert_called_once_with(
            file="s3://test-bucket/silver/tickers/data.parquet",
            storage_options=mock_s3_storage,
            compression="zstd",
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

    @patch("polars.DataFrame.write_parquet")
    def test_write_split_details(
        self, mock_write, sample_split_data, mock_s3_storage, mock_settings
    ):
        """Test writing split details to S3."""
        from tickerlake.silver.main import write_split_details

        write_split_details(sample_split_data)

        mock_write.assert_called_once_with(
            file="s3://test-bucket/silver/splits/data.parquet",
            storage_options=mock_s3_storage,
            compression="zstd",
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

    @patch("polars.DataFrame.write_parquet")
    def test_write_daily_aggs(
        self, mock_write, sample_daily_data, mock_s3_storage, mock_settings
    ):
        """Test writing daily aggregates to S3."""
        from tickerlake.silver.main import write_daily_aggs

        write_daily_aggs(sample_daily_data)

        mock_write.assert_called_once_with(
            file="s3://test-bucket/silver/daily/data.parquet",
            storage_options=mock_s3_storage,
            compression="zstd",
            row_group_size=100_000,
        )


class TestTimeAggregates:
    """Test time-based aggregate functions."""

    @pytest.mark.parametrize(
        "period,output_dir,function_name",
        [
            ("1w", "weekly", "write_weekly_aggs"),
            ("1mo", "monthly", "write_monthly_aggs"),
        ],
    )
    @patch("polars.DataFrame.write_parquet")
    def test_write_time_aggregates(
        self,
        mock_write,
        sample_daily_data,
        mock_s3_storage,
        mock_settings,
        period,
        output_dir,
        function_name,
    ):
        """Test writing weekly and monthly aggregates."""
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

        # Verify write was called with correct path
        call_args = mock_write.call_args
        assert f"s3://test-bucket/silver/{output_dir}/data.parquet" in str(call_args)

    @patch("polars.DataFrame.write_parquet")
    def test_write_time_aggs_direct(
        self, mock_write, sample_daily_data, mock_s3_storage, mock_settings
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

        mock_write.assert_called_once()
        call_args = mock_write.call_args
        assert "s3://test-bucket/silver/weekly/data.parquet" in str(call_args)


class TestMainFunction:
    """Test main pipeline function."""

    @patch("tickerlake.silver.main.write_monthly_aggs")
    @patch("tickerlake.silver.main.write_weekly_aggs")
    @patch("tickerlake.silver.main.write_daily_aggs")
    @patch("tickerlake.silver.main.apply_splits")
    @patch("tickerlake.silver.main.read_daily_aggs")
    @patch("tickerlake.silver.main.write_split_details")
    @patch("tickerlake.silver.main.read_split_details")
    @patch("tickerlake.silver.main.write_ticker_details")
    @patch("tickerlake.silver.main.read_all_etf_holdings")
    @patch("tickerlake.silver.main.read_ticker_details")
    def test_main_pipeline(
        self,
        mock_read_ticker,
        mock_read_etf,
        mock_write_ticker,
        mock_read_split,
        mock_write_split,
        mock_read_daily,
        mock_apply_splits,
        mock_write_daily,
        mock_write_weekly,
        mock_write_monthly,
        sample_ticker_data,
        sample_split_data,
        sample_daily_data,
        sample_etf_holdings,
    ):
        """Test complete silver layer pipeline execution."""
        # Setup mocks
        mock_ticker_details = sample_ticker_data.select([
            "ticker",
            "name",
            pl.col("type").alias("ticker_type"),
        ])
        mock_read_ticker.return_value = mock_ticker_details
        mock_read_etf.return_value = pl.DataFrame({
            "ticker": ["AAPL", "MSFT"],
            "etfs": [["spy", "qqq"], ["spy"]],
        })
        mock_read_split.return_value = sample_split_data
        mock_read_daily.return_value = sample_daily_data.with_columns(
            pl.from_epoch(pl.col("timestamp"), time_unit="ms")
            .cast(pl.Date)
            .alias("date")
        )
        mock_apply_splits.return_value = mock_read_daily.return_value

        from tickerlake.silver.main import main

        main()

        # Verify all steps were called
        mock_read_ticker.assert_called_once()
        mock_read_etf.assert_called_once()
        mock_write_ticker.assert_called_once()
        mock_read_split.assert_called_once()
        mock_write_split.assert_called_once()
        mock_read_daily.assert_called_once()
        mock_apply_splits.assert_called_once()
        mock_write_daily.assert_called_once()
        mock_write_weekly.assert_called_once()
        mock_write_monthly.assert_called_once()


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
            pl.col("ticker").cast(pl.Categorical),
            pl.col("volume").cast(pl.UInt64),
            pl.col("open").cast(pl.Float64),
            pl.col("close").cast(pl.Float64),
            pl.col("high").cast(pl.Float64),
            pl.col("low").cast(pl.Float64),
        )

        assert result["date"].dtype == pl.Date
        assert result["ticker"].dtype == pl.Categorical
        assert result["volume"].dtype == pl.UInt64
        assert result["open"].dtype == pl.Float64
