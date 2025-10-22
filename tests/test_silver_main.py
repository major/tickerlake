"""Tests for silver medallion layer."""

from datetime import date
from unittest.mock import patch

import polars as pl
import pytest


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
def mock_settings():
    """Mock application settings."""
    with patch("tickerlake.silver.main.settings") as mock_settings:
        mock_settings.bronze_storage_path = "./data/bronze"
        mock_settings.silver_storage_path = "./data/silver"
        yield mock_settings


class TestReadSplits:
    """Test read_splits function."""

    @patch("tickerlake.silver.main.pl.read_parquet")
    def test_read_splits(self, mock_read, sample_split_data, mock_settings):
        """Test reading splits data from bronze layer."""
        mock_read.return_value = sample_split_data

        from tickerlake.silver.main import read_splits

        result = read_splits()

        mock_read.assert_called_once_with(
            "./data/bronze/splits/splits.parquet"
        )
        assert isinstance(result, pl.DataFrame)


class TestReadTickers:
    """Test read_tickers function."""

    @patch("tickerlake.silver.main.pl.read_parquet")
    def test_read_tickers(self, mock_read, mock_settings):
        """Test reading tickers data from bronze layer."""
        sample_tickers = pl.DataFrame({
            "ticker": ["AAPL", "SPY", "AAPL.WS"],
            "type": ["CS", "ETF", "WARRANT"],
            "name": ["Apple Inc.", "SPDR S&P 500 ETF Trust", "Apple Warrant"],
            "primary_exchange": ["NASDAQ", "NYSE", "NASDAQ"],
            "active": [True, True, False],
            "cik": ["0000320193", "0001064642", "0000320193"],
        })
        mock_read.return_value = sample_tickers

        from tickerlake.silver.main import read_tickers

        result = read_tickers()

        mock_read.assert_called_once_with(
            "./data/bronze/tickers/tickers.parquet"
        )
        assert isinstance(result, pl.DataFrame)


class TestReadStocksLazy:
    """Test read_stocks_lazy function."""

    @patch("tickerlake.silver.main.pl.scan_parquet")
    def test_read_stocks_lazy(self, mock_scan, mock_settings):
        """Test reading stocks data lazily from bronze layer."""
        mock_lf = pl.LazyFrame()
        mock_scan.return_value = mock_lf

        from tickerlake.silver.main import read_stocks_lazy

        result = read_stocks_lazy()

        mock_scan.assert_called_once_with(
            "./data/bronze/stocks/date=*/*.parquet"
        )
        assert isinstance(result, pl.LazyFrame)


class TestMain:
    """Test main pipeline function."""

    @patch("tickerlake.silver.main.pl.DataFrame.write_parquet")
    @patch("tickerlake.silver.main.aggregate_to_monthly")
    @patch("tickerlake.silver.main.aggregate_to_weekly")
    @patch("tickerlake.silver.main.calculate_all_indicators")
    @patch("tickerlake.silver.main.calculate_weinstein_stage")
    @patch("tickerlake.silver.main.pl.read_parquet")
    @patch("tickerlake.silver.main.apply_splits_lazy")
    @patch("tickerlake.silver.main.read_stocks_lazy")
    @patch("tickerlake.silver.main.read_splits")
    @patch("tickerlake.silver.main.read_tickers")
    @patch("tickerlake.silver.main.create_ticker_metadata")
    def test_main_calls_functions(
        self,
        mock_create_metadata,
        mock_read_tickers,
        mock_read_splits,
        mock_read_stocks,
        mock_apply_splits,
        mock_read_parquet,
        mock_calc_stages,
        mock_calc_indicators,
        mock_agg_weekly,
        mock_agg_monthly,
        mock_write_parquet,
        mock_settings,
    ):
        """Test main function calls the correct processing functions and filters splits."""
        # Mock tickers data with different types
        mock_tickers_df = pl.DataFrame({
            "ticker": ["AAPL", "SPY", "AAPL.WS"],
            "type": ["CS", "ETF", "WARRANT"],
            "name": ["Apple Inc.", "SPDR S&P 500 ETF Trust", "Apple Warrant"],
            "primary_exchange": ["NASDAQ", "NYSE", "NASDAQ"],
            "active": [True, True, False],
            "cik": ["0000320193", "0001064642", "0000320193"],
        })
        mock_read_tickers.return_value = mock_tickers_df

        # Mock splits data including splits for a warrant (should be filtered out)
        mock_splits_df = pl.DataFrame({
            "ticker": ["AAPL", "SPY", "AAPL.WS"],
            "execution_date": [date(2024, 1, 1), date(2024, 2, 1), date(2024, 3, 1)],
            "split_from": [2.0, 3.0, 4.0],
            "split_to": [1.0, 1.0, 1.0],
        })
        mock_read_splits.return_value = mock_splits_df

        # Create a LazyFrame with sample stock data
        stock_data = pl.DataFrame({
            "ticker": ["AAPL", "SPY", "AAPL.WS"],
            "date": [date(2024, 1, 1)] * 3,
            "open": [99.0, 199.0, 49.0],
            "high": [101.0, 201.0, 51.0],
            "low": [98.0, 198.0, 48.0],
            "close": [100.0, 200.0, 50.0],
            "volume": [1000000, 2000000, 500000],
            "transactions": [10000, 20000, 5000],
        })
        mock_read_stocks.return_value = stock_data.lazy()

        # Mock read_parquet to return the same stock data for daily aggregates
        mock_read_parquet.return_value = stock_data

        # Mock indicator calculations to return simple DataFrames
        mock_indicators = pl.DataFrame({
            "ticker": ["AAPL", "SPY"],
            "date": [date(2024, 1, 1)] * 2,
            "sma_20": [100.0, 200.0],
            "sma_50": [100.0, 200.0],
            "sma_200": [100.0, 200.0],
            "atr_14": [2.0, 4.0],
            "volume_ma_20": [1000000, 2000000],
            "volume_ratio": [1.0, 1.0],
            "is_hvc": [False, False],
        })
        mock_calc_indicators.return_value = mock_indicators

        # Mock Weinstein stage calculation - needs to return input df + stage columns
        # The function receives weekly_with_close which has close, volume, and all indicators
        def mock_weinstein(df):
            return df.with_columns([
                pl.lit(2, dtype=pl.UInt8).alias("stage"),
                pl.lit(1, dtype=pl.UInt8).alias("raw_stage"),
                pl.lit(100.0).alias("ma_30"),
                pl.lit(5.0).alias("price_vs_ma_pct"),
                pl.lit(1.0).alias("ma_slope_pct"),
                pl.lit(True).alias("stage_changed"),
                pl.lit(3, dtype=pl.UInt8).alias("weeks_in_stage"),
            ])
        mock_calc_stages.side_effect = mock_weinstein

        # Mock aggregations to return simple DataFrames
        mock_agg_weekly.return_value = stock_data.head(2)
        mock_agg_monthly.return_value = stock_data.head(2)

        from tickerlake.silver.main import main

        main()

        mock_read_tickers.assert_called_once()
        mock_read_splits.assert_called_once()
        mock_read_stocks.assert_called_once()

        # Verify apply_splits was called and check that splits were filtered
        mock_apply_splits.assert_called_once()
        call_args = mock_apply_splits.call_args
        splits_passed = call_args.kwargs['splits_df']

        # Verify that only CS and ETF splits were passed (AAPL.WS should be filtered out)
        assert len(splits_passed) == 2
        assert "AAPL.WS" not in splits_passed["ticker"].to_list()

        # Verify indicators and aggregations were calculated
        assert mock_calc_indicators.call_count == 3  # daily, weekly, monthly
        assert mock_agg_weekly.call_count == 1
        assert mock_agg_monthly.call_count == 1
