"""Tests for the bronze layer main module."""

from datetime import date
from unittest.mock import MagicMock, Mock, patch

import polars as pl
import pytest

from tickerlake.bronze.main import (
    get_missing_trading_days,
    get_required_trading_days,
    load_grouped_daily_aggs,
    previously_stored_dates,
    validate_bronze_data,
)


@pytest.fixture
def mock_settings():
    """Create a mock settings object for testing."""
    with patch("tickerlake.bronze.main.settings") as mock:
        mock.data_start_year = 2024
        mock.bronze_storage_path = "./data/bronze"
        yield mock


@pytest.fixture
def mock_polygon_client():
    """Create a mock Polygon API client."""
    mock_client = MagicMock()
    return mock_client


@pytest.fixture
def mock_api_response():
    """Create mock API response objects."""
    mock_result_1 = MagicMock()
    mock_result_1.ticker = "AAPL"
    mock_result_1.volume = 100000
    mock_result_1.open = 150.0
    mock_result_1.close = 152.0
    mock_result_1.high = 153.0
    mock_result_1.low = 149.0
    mock_result_1.timestamp = 1704153600000  # 2024-01-02 in ms
    mock_result_1.transactions = 5000

    mock_result_2 = MagicMock()
    mock_result_2.ticker = "MSFT"
    mock_result_2.volume = 80000
    mock_result_2.open = 375.0
    mock_result_2.close = 378.0
    mock_result_2.high = 379.0
    mock_result_2.low = 374.0
    mock_result_2.timestamp = 1704153600000  # 2024-01-02 in ms
    mock_result_2.transactions = 3000

    return [mock_result_1, mock_result_2]


class TestGetRequiredTradingDays:
    """Test cases for getting required trading days."""

    @patch("tickerlake.bronze.main.is_data_available_for_today")
    @patch("tickerlake.bronze.main.get_trading_days")
    @patch("tickerlake.bronze.main.date")
    def test_get_required_trading_days_includes_today(
        self, mock_date, mock_get_trading_days, mock_is_available, mock_settings
    ):
        """Test get_required_trading_days includes today when data available."""
        mock_date.today.return_value = date(2024, 6, 15)
        mock_date.side_effect = lambda *args, **kw: date(*args, **kw)
        mock_is_available.return_value = True
        mock_get_trading_days.return_value = ["2024-01-02", "2024-01-03"]

        result = get_required_trading_days()

        mock_get_trading_days.assert_called_once_with(
            date(2024, 1, 1), date(2024, 6, 15)
        )
        assert result == ["2024-01-02", "2024-01-03"]

    @patch("tickerlake.bronze.main.is_data_available_for_today")
    @patch("tickerlake.bronze.main.get_trading_days")
    @patch("tickerlake.bronze.main.date")
    def test_get_required_trading_days_excludes_today(
        self, mock_date, mock_get_trading_days, mock_is_available, mock_settings
    ):
        """Test get_required_trading_days excludes today when data not available."""
        mock_date.today.return_value = date(2024, 6, 15)
        mock_date.side_effect = lambda *args, **kw: date(*args, **kw)
        mock_is_available.return_value = False
        mock_get_trading_days.return_value = ["2024-01-02", "2024-01-03"]

        result = get_required_trading_days()

        # Should still pass today to get_trading_days
        mock_get_trading_days.assert_called_once_with(
            date(2024, 1, 1), date(2024, 6, 15)
        )
        assert result == ["2024-01-02", "2024-01-03"]


class TestGetMissingTradingDays:
    """Test cases for identifying missing trading days."""

    @pytest.mark.parametrize(
        "required,stored,expected",
        [
            # No dates stored yet - all are missing
            (
                ["2024-01-02", "2024-01-03", "2024-01-04"],
                [],
                ["2024-01-02", "2024-01-03", "2024-01-04"],
            ),
            # Some dates already stored
            (
                ["2024-01-02", "2024-01-03", "2024-01-04"],
                ["2024-01-02", "2024-01-03"],
                ["2024-01-04"],
            ),
            # All dates already stored
            (
                ["2024-01-02", "2024-01-03"],
                ["2024-01-02", "2024-01-03"],
                [],
            ),
            # Stored dates not in required list (edge case)
            (
                ["2024-01-02", "2024-01-03"],
                ["2024-01-05", "2024-01-06"],
                ["2024-01-02", "2024-01-03"],
            ),
        ],
    )
    def test_get_missing_trading_days_scenarios(self, required, stored, expected):
        """Test get_missing_trading_days with various scenarios."""
        result = get_missing_trading_days(required, stored)
        assert result == expected

    def test_get_missing_trading_days_maintains_sort_order(self):
        """Test get_missing_trading_days maintains chronological order."""
        required = ["2024-01-05", "2024-01-02", "2024-01-08", "2024-01-03"]
        stored = ["2024-01-03"]

        result = get_missing_trading_days(required, stored)

        # Should be sorted
        assert result == ["2024-01-02", "2024-01-05", "2024-01-08"]


class TestPreviouslyStoredDates:
    """Test cases for retrieving previously stored dates."""

    @patch("tickerlake.bronze.main.pl.scan_parquet")
    def test_previously_stored_dates_success(self, mock_scan_parquet):
        """Test previously_stored_dates retrieves dates from Parquet files."""
        # Create mock DataFrame with dates
        mock_dates = pl.DataFrame({"date": [date(2024, 1, 2), date(2024, 1, 3)]})
        test_schema = {"ticker": pl.Utf8, "date": pl.Date}

        # Setup mock lazy frame
        mock_lf = MagicMock()
        mock_lf.select.return_value = mock_lf
        mock_lf.unique.return_value = mock_lf
        mock_lf.sort.return_value = mock_lf
        mock_lf.collect.return_value = mock_dates.with_columns(
            pl.col("date").dt.strftime("%Y-%m-%d").alias("date")
        )

        mock_scan_parquet.return_value = mock_lf

        result = previously_stored_dates("./data/test/destination", test_schema)

        assert result == ["2024-01-02", "2024-01-03"]
        mock_scan_parquet.assert_called_once()

    @patch("tickerlake.bronze.main.pl.scan_parquet")
    def test_previously_stored_dates_no_data(self, mock_scan_parquet):
        """Test previously_stored_dates handles missing data gracefully."""
        mock_scan_parquet.side_effect = Exception("No parquet files found")
        test_schema = {"ticker": pl.Utf8, "date": pl.Date}

        result = previously_stored_dates("./data/test/destination", test_schema)

        assert result == []


class TestLoadGroupedDailyAggs:
    """Test cases for loading grouped daily aggregates from API."""

    @patch("tickerlake.bronze.main.pl.DataFrame")
    @patch("tickerlake.bronze.main.setup_polygon_api_client")
    @patch("tickerlake.bronze.main.tqdm")
    def test_load_grouped_daily_aggs_single_date(
        self,
        mock_tqdm,
        mock_setup_client,
        mock_polars_df,
        mock_polygon_client,
        mock_api_response,
    ):
        """Test load_grouped_daily_aggs fetches data for a single date."""
        dates = ["2024-01-02"]
        destination = "./data/bronze/stocks"

        mock_setup_client.return_value = mock_polygon_client
        mock_polygon_client.get_grouped_daily_aggs.return_value = mock_api_response

        # Mock Polars DataFrame operations
        mock_df = MagicMock()
        mock_df.with_columns.return_value = mock_df
        mock_df.drop.return_value = mock_df
        mock_polars_df.return_value = mock_df

        # Mock tqdm
        mock_pbar = MagicMock()
        mock_tqdm.return_value.__enter__.return_value = mock_pbar
        mock_pbar.__iter__ = Mock(return_value=iter(dates))

        load_grouped_daily_aggs(dates, destination)

        # Verify API was called with correct parameters
        mock_polygon_client.get_grouped_daily_aggs.assert_called_once_with(
            "2024-01-02",
            adjusted=False,
            include_otc=False,
        )

        # Verify DataFrame was written
        mock_df.write_parquet.assert_called_once_with(
            destination,
            partition_by=["date"],
        )

    @patch("tickerlake.bronze.main.pl.DataFrame")
    @patch("tickerlake.bronze.main.setup_polygon_api_client")
    @patch("tickerlake.bronze.main.tqdm")
    def test_load_grouped_daily_aggs_multiple_dates(
        self,
        mock_tqdm,
        mock_setup_client,
        mock_polars_df,
        mock_polygon_client,
        mock_api_response,
    ):
        """Test load_grouped_daily_aggs fetches data for multiple dates."""
        dates = ["2024-01-02", "2024-01-03"]
        destination = "./data/bronze/stocks"

        mock_setup_client.return_value = mock_polygon_client
        mock_polygon_client.get_grouped_daily_aggs.return_value = mock_api_response

        # Mock Polars DataFrame
        mock_df = MagicMock()
        mock_df.with_columns.return_value = mock_df
        mock_df.drop.return_value = mock_df
        mock_polars_df.return_value = mock_df

        # Mock tqdm
        mock_pbar = MagicMock()
        mock_tqdm.return_value.__enter__.return_value = mock_pbar
        mock_pbar.__iter__ = Mock(return_value=iter(dates))

        load_grouped_daily_aggs(dates, destination)

        # Should call API for each date
        assert mock_polygon_client.get_grouped_daily_aggs.call_count == 2
        # Should write parquet for each date
        assert mock_df.write_parquet.call_count == 2

    @patch("tickerlake.bronze.main.setup_polygon_api_client")
    @patch("tickerlake.bronze.main.tqdm")
    def test_load_grouped_daily_aggs_empty_list(
        self, mock_tqdm, mock_setup_client, mock_polygon_client
    ):
        """Test load_grouped_daily_aggs handles empty date list."""
        dates = []
        destination = "./data/bronze/stocks"

        load_grouped_daily_aggs(dates, destination)

        # Should not call API at all
        mock_setup_client.assert_not_called()
        mock_tqdm.assert_not_called()

    @patch("tickerlake.bronze.main.setup_polygon_api_client")
    @patch("tickerlake.bronze.main.tqdm")
    def test_load_grouped_daily_aggs_handles_api_error(
        self, mock_tqdm, mock_setup_client, mock_polygon_client
    ):
        """Test load_grouped_daily_aggs handles API errors gracefully."""
        dates = ["2024-01-02", "2024-01-03"]
        destination = "./data/bronze/stocks"

        mock_setup_client.return_value = mock_polygon_client
        # First call fails, second succeeds
        mock_polygon_client.get_grouped_daily_aggs.side_effect = [
            Exception("API Error"),
            [],
        ]

        # Mock tqdm
        mock_pbar = MagicMock()
        mock_tqdm.return_value.__enter__.return_value = mock_pbar
        mock_pbar.__iter__ = Mock(return_value=iter(dates))

        # Should not raise, just log and continue
        load_grouped_daily_aggs(dates, destination)

        # Should have tried both dates
        assert mock_polygon_client.get_grouped_daily_aggs.call_count == 2

    @patch("tickerlake.bronze.main.setup_polygon_api_client")
    @patch("tickerlake.bronze.main.tqdm")
    def test_load_grouped_daily_aggs_handles_empty_response(
        self, mock_tqdm, mock_setup_client, mock_polygon_client
    ):
        """Test load_grouped_daily_aggs handles empty API response."""
        dates = ["2024-01-02"]
        destination = "./data/bronze/stocks"

        mock_setup_client.return_value = mock_polygon_client
        mock_polygon_client.get_grouped_daily_aggs.return_value = []

        # Mock tqdm
        mock_pbar = MagicMock()
        mock_tqdm.return_value.__enter__.return_value = mock_pbar
        mock_pbar.__iter__ = Mock(return_value=iter(dates))

        load_grouped_daily_aggs(dates, destination)

        # Should call API but not write anything
        mock_polygon_client.get_grouped_daily_aggs.assert_called_once()

    @patch("tickerlake.bronze.main.setup_polygon_api_client")
    @patch("tickerlake.bronze.main.tqdm")
    def test_load_grouped_daily_aggs_stops_on_403_error(
        self, mock_tqdm, mock_setup_client, mock_polygon_client
    ):
        """Test load_grouped_daily_aggs stops fetching when 403 error encountered."""
        # Reversed dates (newest first)
        dates = ["2024-01-05", "2024-01-04", "2024-01-03"]
        destination = "./data/bronze/stocks"

        mock_setup_client.return_value = mock_polygon_client
        # First two succeed, third gets 403
        mock_polygon_client.get_grouped_daily_aggs.side_effect = [
            [],  # 2024-01-05 succeeds
            [],  # 2024-01-04 succeeds
            Exception("403 Forbidden"),  # 2024-01-03 hits limit
        ]

        # Mock tqdm
        mock_pbar = MagicMock()
        mock_tqdm.return_value.__enter__.return_value = mock_pbar
        mock_pbar.__iter__ = Mock(return_value=iter(dates))

        load_grouped_daily_aggs(dates, destination)

        # Should stop after the 403, so only 3 calls (not continuing after)
        assert mock_polygon_client.get_grouped_daily_aggs.call_count == 3


class TestValidateBronzeData:
    """Test cases for bronze data validation."""

    @patch("tickerlake.bronze.main.pl.scan_parquet")
    def test_validate_bronze_data_all_normal(self, mock_scan_parquet, mock_settings):
        """Test validate_bronze_data when all record counts are normal."""
        # Create mock data with consistent record counts
        mock_data = pl.DataFrame({
            "date": [date(2024, 1, 2), date(2024, 1, 3), date(2024, 1, 4)],
            "record_count": [10000, 10100, 9900],
        })

        # Setup mock lazy frame
        mock_lf = MagicMock()
        mock_lf.select.return_value = mock_lf
        mock_lf.group_by.return_value = mock_lf
        mock_lf.agg.return_value = mock_lf
        mock_lf.sort.return_value = mock_lf
        mock_lf.collect.return_value = mock_data

        mock_scan_parquet.return_value = mock_lf

        # Should not raise any exceptions
        validate_bronze_data()

        mock_scan_parquet.assert_called_once()

    @patch("tickerlake.bronze.main.pl.scan_parquet")
    def test_validate_bronze_data_detects_low_count(self, mock_scan_parquet, mock_settings):
        """Test validate_bronze_data detects abnormally low record counts."""
        # Create mock data with one suspiciously low count (< 50% of mean)
        mock_data = pl.DataFrame({
            "date": [date(2024, 1, 2), date(2024, 1, 3), date(2024, 1, 4)],
            "record_count": [10000, 4000, 10100],  # 4000 is < 50% of mean (~6700)
        })

        mock_lf = MagicMock()
        mock_lf.select.return_value = mock_lf
        mock_lf.group_by.return_value = mock_lf
        mock_lf.agg.return_value = mock_lf
        mock_lf.sort.return_value = mock_lf
        mock_lf.collect.return_value = mock_data

        mock_scan_parquet.return_value = mock_lf

        # Should log warnings but not raise
        validate_bronze_data()

    @patch("tickerlake.bronze.main.pl.scan_parquet")
    def test_validate_bronze_data_detects_high_count(self, mock_scan_parquet, mock_settings):
        """Test validate_bronze_data detects abnormally high record counts."""
        # Create mock data with one suspiciously high count (> 200% of mean)
        mock_data = pl.DataFrame({
            "date": [date(2024, 1, 2), date(2024, 1, 3), date(2024, 1, 4)],
            "record_count": [10000, 22000, 10100],  # 22000 is > 200% of mean (~13367)
        })

        mock_lf = MagicMock()
        mock_lf.select.return_value = mock_lf
        mock_lf.group_by.return_value = mock_lf
        mock_lf.agg.return_value = mock_lf
        mock_lf.sort.return_value = mock_lf
        mock_lf.collect.return_value = mock_data

        mock_scan_parquet.return_value = mock_lf

        # Should log warnings but not raise
        validate_bronze_data()

    @patch("tickerlake.bronze.main.pl.scan_parquet")
    def test_validate_bronze_data_handles_no_data(self, mock_scan_parquet, mock_settings):
        """Test validate_bronze_data handles case with no data gracefully."""
        # Empty DataFrame
        mock_data = pl.DataFrame({"date": [], "record_count": []})

        mock_lf = MagicMock()
        mock_lf.select.return_value = mock_lf
        mock_lf.group_by.return_value = mock_lf
        mock_lf.agg.return_value = mock_lf
        mock_lf.sort.return_value = mock_lf
        mock_lf.collect.return_value = mock_data

        mock_scan_parquet.return_value = mock_lf

        # Should not raise, just log warning
        validate_bronze_data()

    @patch("tickerlake.bronze.main.pl.scan_parquet")
    def test_validate_bronze_data_handles_scan_error(self, mock_scan_parquet, mock_settings):
        """Test validate_bronze_data handles parquet scan errors gracefully."""
        mock_scan_parquet.side_effect = Exception("No parquet files found")

        # Should not raise, just log warning
        validate_bronze_data()
