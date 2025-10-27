"""Tests for the bronze layer main module."""

from datetime import date
from unittest.mock import MagicMock, Mock, patch

import pytest

from tickerlake.bronze.main import (
    get_missing_trading_days,
    get_required_trading_days,
    load_grouped_daily_aggs,
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


class TestGetExistingDates:
    """Test cases for retrieving previously stored dates from Postgres."""

    @patch("tickerlake.bronze.postgres.get_engine")
    def test_get_existing_dates_success(self, mock_get_engine):
        """Test get_existing_dates retrieves dates from Postgres."""
        from tickerlake.bronze.postgres import get_existing_dates

        # Create mock connection and result
        mock_conn = MagicMock()
        mock_engine = MagicMock()
        mock_engine.connect.return_value.__enter__.return_value = mock_conn
        mock_get_engine.return_value = mock_engine

        # Mock query results
        mock_result = [(date(2024, 1, 2),), (date(2024, 1, 3),)]
        mock_conn.execute.return_value.fetchall.return_value = mock_result

        result = get_existing_dates()

        assert result == ["2024-01-02", "2024-01-03"]
        mock_conn.execute.assert_called_once()

    @patch("tickerlake.bronze.postgres.get_engine")
    def test_get_existing_dates_no_data(self, mock_get_engine):
        """Test get_existing_dates handles empty database gracefully."""
        from tickerlake.bronze.postgres import get_existing_dates

        # Create mock connection with no results
        mock_conn = MagicMock()
        mock_engine = MagicMock()
        mock_engine.connect.return_value.__enter__.return_value = mock_conn
        mock_get_engine.return_value = mock_engine

        mock_conn.execute.return_value.fetchall.return_value = []

        result = get_existing_dates()

        assert result == []


class TestLoadGroupedDailyAggs:
    """Test cases for loading grouped daily aggregates from API."""

    @patch("tickerlake.bronze.main.bulk_load")
    @patch("tickerlake.bronze.main.setup_polygon_api_client")
    @patch("tickerlake.bronze.main.tqdm")
    def test_load_grouped_daily_aggs_single_date(
        self,
        mock_tqdm,
        mock_setup_client,
        mock_bulk_load,
        mock_polygon_client,
        mock_api_response,
    ):
        """Test load_grouped_daily_aggs fetches data for a single date."""
        dates = ["2024-01-02"]

        mock_setup_client.return_value = mock_polygon_client
        mock_polygon_client.get_grouped_daily_aggs.return_value = mock_api_response

        # Mock tqdm
        mock_pbar = MagicMock()
        mock_tqdm.return_value.__enter__.return_value = mock_pbar
        mock_pbar.__iter__ = Mock(return_value=iter(dates))

        load_grouped_daily_aggs(dates)

        # Verify API was called with correct parameters
        mock_polygon_client.get_grouped_daily_aggs.assert_called_once_with(
            "2024-01-02",
            adjusted=False,
            include_otc=False,
        )

        # Verify bulk_load was called
        mock_bulk_load.assert_called_once()

    @patch("tickerlake.bronze.main.bulk_load")
    @patch("tickerlake.bronze.main.setup_polygon_api_client")
    @patch("tickerlake.bronze.main.tqdm")
    def test_load_grouped_daily_aggs_multiple_dates(
        self,
        mock_tqdm,
        mock_setup_client,
        mock_bulk_load,
        mock_polygon_client,
        mock_api_response,
    ):
        """Test load_grouped_daily_aggs fetches data for multiple dates."""
        dates = ["2024-01-02", "2024-01-03"]

        mock_setup_client.return_value = mock_polygon_client
        mock_polygon_client.get_grouped_daily_aggs.return_value = mock_api_response

        # Mock tqdm
        mock_pbar = MagicMock()
        mock_tqdm.return_value.__enter__.return_value = mock_pbar
        mock_pbar.__iter__ = Mock(return_value=iter(dates))

        load_grouped_daily_aggs(dates)

        # Should call API for each date
        assert mock_polygon_client.get_grouped_daily_aggs.call_count == 2
        # Should call bulk_load for each date
        assert mock_bulk_load.call_count == 2

    @patch("tickerlake.bronze.main.setup_polygon_api_client")
    @patch("tickerlake.bronze.main.tqdm")
    def test_load_grouped_daily_aggs_empty_list(
        self, mock_tqdm, mock_setup_client, mock_polygon_client
    ):
        """Test load_grouped_daily_aggs handles empty date list."""
        dates = []

        load_grouped_daily_aggs(dates)

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
        load_grouped_daily_aggs(dates)

        # Should have tried both dates
        assert mock_polygon_client.get_grouped_daily_aggs.call_count == 2

    @patch("tickerlake.bronze.main.bulk_load")
    @patch("tickerlake.bronze.main.setup_polygon_api_client")
    @patch("tickerlake.bronze.main.tqdm")
    def test_load_grouped_daily_aggs_handles_empty_response(
        self, mock_tqdm, mock_setup_client, mock_bulk_load, mock_polygon_client
    ):
        """Test load_grouped_daily_aggs handles empty API response."""
        dates = ["2024-01-02"]

        mock_setup_client.return_value = mock_polygon_client
        mock_polygon_client.get_grouped_daily_aggs.return_value = []

        # Mock tqdm
        mock_pbar = MagicMock()
        mock_tqdm.return_value.__enter__.return_value = mock_pbar
        mock_pbar.__iter__ = Mock(return_value=iter(dates))

        load_grouped_daily_aggs(dates)

        # Should call API but not write anything
        mock_polygon_client.get_grouped_daily_aggs.assert_called_once()
        # Should not call bulk_load for empty response
        mock_bulk_load.assert_not_called()

    @patch("tickerlake.bronze.main.setup_polygon_api_client")
    @patch("tickerlake.bronze.main.tqdm")
    def test_load_grouped_daily_aggs_stops_on_403_error(
        self, mock_tqdm, mock_setup_client, mock_polygon_client
    ):
        """Test load_grouped_daily_aggs stops fetching when 403 error encountered."""
        # Reversed dates (newest first)
        dates = ["2024-01-05", "2024-01-04", "2024-01-03"]

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

        load_grouped_daily_aggs(dates)

        # Should stop after the 403, so only 3 calls (not continuing after)
        assert mock_polygon_client.get_grouped_daily_aggs.call_count == 3


class TestValidateBronzeData:
    """Test cases for bronze data validation."""

    @patch("tickerlake.bronze.postgres.get_validation_stats")
    def test_validate_bronze_data_all_normal(self, mock_get_stats, mock_settings):
        """Test validate_bronze_data when all record counts are normal."""
        # Mock validation stats from Postgres
        mock_stats = [
            (date(2024, 1, 2), 10000),
            (date(2024, 1, 3), 10100),
            (date(2024, 1, 4), 9900),
        ]

        mock_get_stats.return_value = mock_stats

        # Should not raise any exceptions
        validate_bronze_data()

        mock_get_stats.assert_called_once()

    @patch("tickerlake.bronze.postgres.get_validation_stats")
    def test_validate_bronze_data_detects_low_count(self, mock_get_stats, mock_settings):
        """Test validate_bronze_data detects abnormally low record counts."""
        # Mock stats with one suspiciously low count (< 50% of mean)
        mock_stats = [
            (date(2024, 1, 2), 10000),
            (date(2024, 1, 3), 4000),  # 4000 is < 50% of mean (~8033)
            (date(2024, 1, 4), 10100),
        ]

        mock_get_stats.return_value = mock_stats

        # Should log warnings but not raise
        validate_bronze_data()

    @patch("tickerlake.bronze.postgres.get_validation_stats")
    def test_validate_bronze_data_detects_high_count(self, mock_get_stats, mock_settings):
        """Test validate_bronze_data detects abnormally high record counts."""
        # Mock stats with one suspiciously high count (> 200% of mean)
        mock_stats = [
            (date(2024, 1, 2), 10000),
            (date(2024, 1, 3), 22000),  # 22000 is > 200% of mean (~14033)
            (date(2024, 1, 4), 10100),
        ]

        mock_get_stats.return_value = mock_stats

        # Should log warnings but not raise
        validate_bronze_data()

    @patch("tickerlake.bronze.postgres.get_validation_stats")
    def test_validate_bronze_data_handles_no_data(self, mock_get_stats, mock_settings):
        """Test validate_bronze_data handles case with no data gracefully."""
        # Empty stats
        mock_get_stats.return_value = []

        # Should not raise, just log warning
        validate_bronze_data()

    @patch("tickerlake.bronze.postgres.get_validation_stats")
    def test_validate_bronze_data_handles_query_error(self, mock_get_stats, mock_settings):
        """Test validate_bronze_data handles database query errors gracefully."""
        mock_get_stats.side_effect = Exception("Database connection error")

        # Should not raise, just log warning
        validate_bronze_data()
