"""Tests for silver medallion layer."""

from datetime import date
from unittest.mock import MagicMock, patch

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
    # Settings are now accessed via config module, not directly in silver.main
    # No need to mock anymore, but keep fixture for backward compatibility
    yield None


class TestReadSplits:
    """Test read_splits function."""

    @patch("tickerlake.silver.main.get_engine")
    def test_read_splits(self, mock_get_engine, sample_split_data, mock_settings):
        """Test reading splits data from bronze Postgres layer."""
        from tickerlake.silver.main import read_splits

        # Mock engine and connection
        mock_conn = MagicMock()
        mock_engine = MagicMock()
        mock_engine.connect.return_value.__enter__.return_value = mock_conn
        mock_get_engine.return_value = mock_engine

        # Mock query results
        mock_result = [
            ("AAPL", sample_split_data["execution_date"][0], 4.0, 1.0),
            ("AAPL", sample_split_data["execution_date"][1], 4.0, 1.0),
        ]
        mock_conn.execute.return_value.fetchall.return_value = mock_result

        result = read_splits()

        mock_conn.execute.assert_called_once()
        assert isinstance(result, pl.DataFrame)
        assert len(result) == 2


class TestReadTickers:
    """Test read_tickers function."""

    @patch("tickerlake.silver.main.get_engine")
    def test_read_tickers(self, mock_get_engine, mock_settings):
        """Test reading tickers data from bronze Postgres layer."""
        from tickerlake.silver.main import read_tickers

        # Mock engine and connection
        mock_conn = MagicMock()
        mock_engine = MagicMock()
        mock_engine.connect.return_value.__enter__.return_value = mock_conn
        mock_get_engine.return_value = mock_engine

        # Mock query results
        mock_result = [
            ("AAPL", "Apple Inc.", "CS", True, "us", "stocks", "NASDAQ", "usd", "USD", "0000320193", None, None, None, None, None, None),
            ("SPY", "SPDR S&P 500 ETF Trust", "ETF", True, "us", "stocks", "NYSE", "usd", "USD", "0001064642", None, None, None, None, None, None),
            ("AAPL.U", "Apple Unit", "UNIT", False, "us", "stocks", "NASDAQ", "usd", "USD", "0000320193", None, None, None, None, None, None),
        ]
        mock_conn.execute.return_value.fetchall.return_value = mock_result

        result = read_tickers()

        mock_conn.execute.assert_called_once()
        assert isinstance(result, pl.DataFrame)
        assert len(result) == 3


class TestReadStocks:
    """Test read_stocks function."""

    @patch("tickerlake.silver.main.get_engine")
    def test_read_stocks(self, mock_get_engine, mock_settings):
        """Test reading stocks data from bronze Postgres layer."""
        from datetime import date
        from tickerlake.silver.main import read_stocks

        # Mock engine and connection
        mock_conn = MagicMock()
        mock_engine = MagicMock()
        mock_engine.connect.return_value.__enter__.return_value = mock_conn
        mock_get_engine.return_value = mock_engine

        # Mock query results
        mock_result = [
            ("AAPL", date(2024, 1, 1), 99.0, 101.0, 98.0, 100.0, 1000000, 10000),
            ("SPY", date(2024, 1, 1), 199.0, 201.0, 198.0, 200.0, 2000000, 20000),
        ]
        mock_conn.execute.return_value.fetchall.return_value = mock_result

        result = read_stocks()

        mock_conn.execute.assert_called_once()
        assert isinstance(result, pl.DataFrame)
        assert len(result) == 2


class TestMain:
    """Test main pipeline function."""

    @patch("tickerlake.silver.main.bulk_load")
    @patch("tickerlake.silver.main.clear_all_tables")
    @patch("tickerlake.silver.main.init_silver_schema")
    @patch("tickerlake.silver.main.aggregate_to_monthly")
    @patch("tickerlake.silver.main.aggregate_to_weekly")
    @patch("tickerlake.silver.main.calculate_all_indicators")
    @patch("tickerlake.silver.main.apply_splits")
    @patch("tickerlake.silver.main.read_stocks")
    @patch("tickerlake.silver.main.read_splits")
    @patch("tickerlake.silver.main.read_tickers")
    def test_main_calls_functions(
        self,
        mock_read_tickers,
        mock_read_splits,
        mock_read_stocks,
        mock_apply_splits,
        mock_calc_indicators,
        mock_agg_weekly,
        mock_agg_monthly,
        mock_init_schema,
        mock_clear_tables,
        mock_bulk_load,
        mock_settings,
    ):
        """Test main function calls the correct processing functions and filters splits."""
        # Mock tickers data with different types
        mock_tickers_df = pl.DataFrame({
            "ticker": ["AAPL", "SPY", "AAPL.U"],
            "type": ["CS", "ETF", "UNIT"],
            "name": ["Apple Inc.", "SPDR S&P 500 ETF Trust", "Apple Unit"],
            "primary_exchange": ["NASDAQ", "NYSE", "NASDAQ"],
            "active": [True, True, False],
            "cik": ["0000320193", "0001064642", "0000320193"],
        }).with_columns(pl.col("ticker").cast(pl.Categorical))
        mock_read_tickers.return_value = mock_tickers_df

        # Mock splits data including splits for a unit (should be filtered out)
        mock_splits_df = pl.DataFrame({
            "ticker": ["AAPL", "SPY", "AAPL.U"],
            "execution_date": [date(2024, 1, 1), date(2024, 2, 1), date(2024, 3, 1)],
            "split_from": [2.0, 3.0, 4.0],
            "split_to": [1.0, 1.0, 1.0],
        }).with_columns(pl.col("ticker").cast(pl.Categorical))
        mock_read_splits.return_value = mock_splits_df

        # Sample stock data
        stock_data = pl.DataFrame({
            "ticker": ["AAPL", "SPY", "AAPL.U"],
            "date": [date(2024, 1, 1)] * 3,
            "open": [99.0, 199.0, 49.0],
            "high": [101.0, 201.0, 51.0],
            "low": [98.0, 198.0, 48.0],
            "close": [100.0, 200.0, 50.0],
            "volume": [1000000, 2000000, 500000],
            "transactions": [10000, 20000, 5000],
        }).with_columns(pl.col("ticker").cast(pl.Categorical))
        mock_read_stocks.return_value = stock_data

        # Mock split-adjusted data (just return filtered stock data)
        adjusted_data = stock_data.filter(pl.col("ticker").is_in(["AAPL", "SPY"]))
        mock_apply_splits.return_value = adjusted_data

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
        }).with_columns(pl.col("ticker").cast(pl.Categorical))
        mock_calc_indicators.return_value = mock_indicators

        # Mock aggregations to return simple DataFrames
        mock_agg_weekly.return_value = adjusted_data
        mock_agg_monthly.return_value = adjusted_data

        from tickerlake.silver.main import main

        main()

        # Verify schema initialization and table clearing
        mock_init_schema.assert_called_once()
        mock_clear_tables.assert_called_once()

        # Verify data reading
        mock_read_tickers.assert_called_once()
        mock_read_splits.assert_called_once()
        mock_read_stocks.assert_called_once()

        # Verify apply_splits was called with filtered splits
        mock_apply_splits.assert_called_once()
        call_args = mock_apply_splits.call_args
        splits_passed = call_args.kwargs['splits_df']

        # Verify that only tradeable securities splits were passed (AAPL.U should be filtered out)
        assert len(splits_passed) == 2
        assert "AAPL.U" not in splits_passed["ticker"].to_list()

        # Verify indicators and aggregations were calculated
        assert mock_calc_indicators.call_count == 3  # daily, weekly, monthly
        assert mock_agg_weekly.call_count == 1
        assert mock_agg_monthly.call_count == 1

        # Verify bulk_load was called for all tables
        # ticker_metadata, daily_aggregates, daily_indicators, weekly_aggregates, weekly_indicators, monthly_aggregates, monthly_indicators
        assert mock_bulk_load.call_count == 7
