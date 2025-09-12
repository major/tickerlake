"""Tests for utility functions in tickerlake.utils module."""

from datetime import datetime
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
import pytz

from tickerlake.utils import get_trading_days, is_market_open


@pytest.fixture
def mock_calendar():
    """Create a mock NYSE calendar for testing."""
    mock_cal = MagicMock()
    mock_cal.tz = pytz.timezone("America/New_York")
    return mock_cal


@pytest.fixture
def mock_schedule_open():
    """Create a mock schedule for when market is open."""
    ny_tz = pytz.timezone("America/New_York")
    schedule = pd.DataFrame({
        "market_open": [datetime(2025, 1, 15, 9, 30, tzinfo=ny_tz)],
        "market_close": [datetime(2025, 1, 15, 16, 0, tzinfo=ny_tz)],
    })
    return schedule


@pytest.fixture
def mock_schedule_closed():
    """Create an empty schedule for when market is closed."""
    return pd.DataFrame()


class TestGetTradingDays:
    """Test cases for get_trading_days function."""

    @pytest.mark.parametrize(
        "start_date,end_date,expected_days",
        [
            # Test normal weekday range
            (
                "2025-01-13",
                "2025-01-17",
                ["2025-01-13", "2025-01-14", "2025-01-15", "2025-01-16", "2025-01-17"],
            ),
            # Test range including weekend
            ("2025-01-17", "2025-01-21", ["2025-01-17", "2025-01-21"]),
            # Test single day
            ("2025-01-15", "2025-01-15", ["2025-01-15"]),
            # Test range with holiday (MLK Day 2025-01-20)
            ("2025-01-17", "2025-01-22", ["2025-01-17", "2025-01-21", "2025-01-22"]),
        ],
    )
    @patch("tickerlake.utils.mcal.get_calendar")
    def test_trading_days_various_ranges(
        self, mock_get_calendar, start_date, end_date, expected_days
    ):
        """Test get_trading_days with various date ranges."""
        mock_calendar = MagicMock()
        mock_get_calendar.return_value = mock_calendar

        # Create mock trading days based on expected output
        mock_days = pd.DatetimeIndex([pd.Timestamp(day) for day in expected_days])
        mock_calendar.valid_days.return_value = mock_days

        result = get_trading_days(start_date, end_date)

        assert result == expected_days
        mock_get_calendar.assert_called_once_with("NYSE")
        mock_calendar.valid_days.assert_called_once_with(
            start_date=start_date, end_date=end_date
        )

    @patch("tickerlake.utils.mcal.get_calendar")
    def test_trading_days_empty_range(self, mock_get_calendar):
        """Test get_trading_days with a weekend range that has no trading days."""
        mock_calendar = MagicMock()
        mock_get_calendar.return_value = mock_calendar

        # Return empty DatetimeIndex for weekend
        mock_calendar.valid_days.return_value = pd.DatetimeIndex([])

        result = get_trading_days("2025-01-18", "2025-01-19")

        assert result == []
        mock_get_calendar.assert_called_once_with("NYSE")

    @patch("tickerlake.utils.mcal.get_calendar")
    def test_trading_days_with_datetime_objects(self, mock_get_calendar):
        """Test get_trading_days with datetime objects instead of strings."""
        mock_calendar = MagicMock()
        mock_get_calendar.return_value = mock_calendar

        start = datetime(2025, 1, 15)
        end = datetime(2025, 1, 16)

        mock_days = pd.DatetimeIndex([
            pd.Timestamp("2025-01-15"),
            pd.Timestamp("2025-01-16"),
        ])
        mock_calendar.valid_days.return_value = mock_days

        result = get_trading_days(start, end)

        assert result == ["2025-01-15", "2025-01-16"]
        mock_calendar.valid_days.assert_called_once_with(start_date=start, end_date=end)


class TestIsMarketOpen:
    """Test cases for is_market_open function."""

    @pytest.mark.parametrize(
        "current_time,market_open_time,market_close_time,expected",
        [
            # Market open - middle of trading day
            (
                datetime(2025, 1, 15, 12, 0),
                datetime(2025, 1, 15, 9, 30),
                datetime(2025, 1, 15, 16, 0),
                True,
            ),
            # Market open - right at open
            (
                datetime(2025, 1, 15, 9, 30),
                datetime(2025, 1, 15, 9, 30),
                datetime(2025, 1, 15, 16, 0),
                True,
            ),
            # Market open - right at close
            (
                datetime(2025, 1, 15, 16, 0),
                datetime(2025, 1, 15, 9, 30),
                datetime(2025, 1, 15, 16, 0),
                True,
            ),
            # Market closed - before open
            (
                datetime(2025, 1, 15, 9, 0),
                datetime(2025, 1, 15, 9, 30),
                datetime(2025, 1, 15, 16, 0),
                False,
            ),
            # Market closed - after close
            (
                datetime(2025, 1, 15, 16, 30),
                datetime(2025, 1, 15, 9, 30),
                datetime(2025, 1, 15, 16, 0),
                False,
            ),
        ],
    )
    @patch("tickerlake.utils.datetime")
    @patch("tickerlake.utils.mcal.get_calendar")
    def test_market_open_various_times(
        self,
        mock_get_calendar,
        mock_datetime,
        current_time,
        market_open_time,
        market_close_time,
        expected,
    ):
        """Test is_market_open at various times of the trading day."""
        ny_tz = pytz.timezone("America/New_York")

        # Setup mock calendar
        mock_calendar = MagicMock()
        mock_calendar.tz = pytz.timezone("America/New_York")
        mock_get_calendar.return_value = mock_calendar

        # Setup mock current time
        mock_datetime.now.return_value = ny_tz.localize(current_time)

        # Setup mock schedule
        schedule = pd.DataFrame({
            "market_open": [ny_tz.localize(market_open_time)],
            "market_close": [ny_tz.localize(market_close_time)],
        })
        mock_calendar.schedule.return_value = schedule

        result = is_market_open()

        assert result == expected
        mock_get_calendar.assert_called_once_with("NYSE")

    @patch("tickerlake.utils.datetime")
    @patch("tickerlake.utils.mcal.get_calendar")
    def test_market_closed_weekend(self, mock_get_calendar, mock_datetime):
        """Test is_market_open on a weekend when market is closed."""
        ny_tz = pytz.timezone("America/New_York")

        # Setup mock calendar
        mock_calendar = MagicMock()
        mock_calendar.tz = pytz.timezone("America/New_York")
        mock_get_calendar.return_value = mock_calendar

        # Saturday
        mock_datetime.now.return_value = ny_tz.localize(datetime(2025, 1, 18, 12, 0))

        # Return empty schedule for weekend
        mock_calendar.schedule.return_value = pd.DataFrame()

        result = is_market_open()

        assert result is False
        mock_get_calendar.assert_called_once_with("NYSE")

    @patch("tickerlake.utils.datetime")
    @patch("tickerlake.utils.mcal.get_calendar")
    def test_market_closed_holiday(self, mock_get_calendar, mock_datetime):
        """Test is_market_open on a holiday when market is closed."""
        ny_tz = pytz.timezone("America/New_York")

        # Setup mock calendar
        mock_calendar = MagicMock()
        mock_calendar.tz = pytz.timezone("America/New_York")
        mock_get_calendar.return_value = mock_calendar

        # MLK Day 2025
        mock_datetime.now.return_value = ny_tz.localize(datetime(2025, 1, 20, 12, 0))

        # Return empty schedule for holiday
        mock_calendar.schedule.return_value = pd.DataFrame()

        result = is_market_open()

        assert result is False
        mock_get_calendar.assert_called_once_with("NYSE")

    @patch("tickerlake.utils.datetime")
    @patch("tickerlake.utils.mcal.get_calendar")
    def test_market_timezone_handling(self, mock_get_calendar, mock_datetime):
        """Test that is_market_open correctly handles timezone conversions."""
        ny_tz = pytz.timezone("America/New_York")
        utc_tz = pytz.UTC

        # Setup mock calendar with string timezone representation
        mock_calendar = MagicMock()
        mock_calendar.tz = "America/New_York"  # Test string conversion
        mock_get_calendar.return_value = mock_calendar

        # Current time in UTC (5 PM UTC = 12 PM EST during winter)
        mock_datetime.now.return_value = ny_tz.localize(datetime(2025, 1, 15, 12, 0))

        # Schedule with UTC times that will be converted
        schedule = pd.DataFrame({
            "market_open": [
                utc_tz.localize(datetime(2025, 1, 15, 14, 30))
            ],  # 9:30 AM EST
            "market_close": [
                utc_tz.localize(datetime(2025, 1, 15, 21, 0))
            ],  # 4:00 PM EST
        })
        mock_calendar.schedule.return_value = schedule

        result = is_market_open()

        assert result is True
        mock_get_calendar.assert_called_once_with("NYSE")
