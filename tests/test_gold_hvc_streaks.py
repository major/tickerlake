"""Tests for gold layer HVC (High Volume Close) streak analysis. 📊"""

from datetime import date
from unittest.mock import MagicMock, patch

import polars as pl
import pytest

from tickerlake.gold.hvc_streaks import detect_most_recent_streak


@pytest.fixture
def sample_hvc_data():
    """Sample data with volume ratios for HVC testing."""
    return pl.DataFrame({
        "ticker": ["AAPL"] * 10,
        "date": [date(2024, 1, i) for i in range(1, 11)],
        "close": [150.0, 155.0, 160.0, 165.0, 170.0, 175.0, 180.0, 185.0, 190.0, 195.0],
        "volume_ratio": [1.5, 3.2, 3.5, 2.0, 3.8, 3.1, 2.5, 3.6, 3.9, 4.2],
    })


@pytest.fixture
def sample_down_streak():
    """Sample data showing a down streak."""
    return pl.DataFrame({
        "ticker": ["TEST"] * 5,
        "date": [date(2024, 1, i) for i in range(1, 6)],
        "close": [100.0, 90.0, 80.0, 70.0, 60.0],
        "volume_ratio": [3.5, 3.2, 3.8, 3.1, 3.9],
    })


@pytest.fixture
def sample_broken_streak():
    """Sample data with an old streak and a new streak (most recent)."""
    return pl.DataFrame({
        "ticker": ["BROKEN"] * 8,
        "date": [date(2024, 1, i) for i in range(1, 9)],
        # Old up streak: 10→20→30, then down to 10, then new up streak: 10→20→30→40
        "close": [10.0, 20.0, 30.0, 10.0, 20.0, 30.0, 40.0, 50.0],
        "volume_ratio": [3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0],
    })


@pytest.fixture
def sample_equal_prices():
    """Sample data with equal prices that should continue the streak."""
    return pl.DataFrame({
        "ticker": ["EQUAL"] * 6,
        "date": [date(2024, 1, i) for i in range(1, 7)],
        # Up streak with equal price in middle: 10→20→20→30
        "close": [10.0, 20.0, 20.0, 30.0, 40.0, 50.0],
        "volume_ratio": [3.0, 3.0, 3.0, 3.0, 3.0, 3.0],
    })


# Note: identify_hvc_periods tests removed - HVCs now cached in gold layer


class TestDetectMostRecentStreak:
    """Test detect_most_recent_streak function. 📈"""

    def test_up_streak(self):
        """Should detect an upward streak correctly."""
        # Example from user: 5, 10, 20, 10, 20, 30, 40, 50 → streak of 5 going up
        df = pl.DataFrame({
            "ticker": ["TEST"] * 8,
            "date": [date(2024, 1, i) for i in range(1, 9)],
            "close": [5.0, 10.0, 20.0, 10.0, 20.0, 30.0, 40.0, 50.0],
            "volume_ratio": [3.0] * 8,
        })

        result = detect_most_recent_streak("TEST", df)

        assert result is not None
        assert result["streak_length"] == 5
        assert result["direction"] == "up"
        assert result["latest_close"] == 50.0
        assert result["streak_price_change_pct"] == pytest.approx(400.0)  # 10→50 = 400%

    def test_down_streak(self):
        """Should detect a downward streak correctly."""
        # Example from user: 10, 20, 30, 40, 30, 40, 30, 20, 10 → streak of 4 going down
        df = pl.DataFrame({
            "ticker": ["TEST"] * 9,
            "date": [date(2024, 1, i) for i in range(1, 10)],
            "close": [10.0, 20.0, 30.0, 40.0, 30.0, 40.0, 30.0, 20.0, 10.0],
            "volume_ratio": [3.0] * 9,
        })

        result = detect_most_recent_streak("TEST", df)

        assert result is not None
        assert result["streak_length"] == 4
        assert result["direction"] == "down"
        assert result["latest_close"] == 10.0
        assert result["streak_price_change_pct"] == pytest.approx(-75.0)  # 40→10 = -75%

    def test_equal_prices_continue_streak(self, sample_equal_prices):
        """Equal prices should continue the current direction."""
        result = detect_most_recent_streak("EQUAL", sample_equal_prices)

        assert result is not None
        # Should capture all 6: 10→20→20→30→40→50 (equal at 20 continues up)
        assert result["streak_length"] == 6
        assert result["direction"] == "up"

    def test_only_most_recent_streak(self, sample_broken_streak):
        """Should only capture the most recent streak, not old broken ones."""
        result = detect_most_recent_streak("BROKEN", sample_broken_streak)

        assert result is not None
        # Most recent streak: 10→20→30→40→50 = 5 periods
        assert result["streak_length"] == 5
        assert result["direction"] == "up"

    def test_single_hvc_period(self):
        """Single HVC should return streak of 1."""
        df = pl.DataFrame({
            "ticker": ["SINGLE"],
            "date": [date(2024, 1, 1)],
            "close": [100.0],
            "volume_ratio": [3.5],
        })

        result = detect_most_recent_streak("SINGLE", df)

        assert result is not None
        assert result["streak_length"] == 1
        assert result["direction"] == "up"  # Default for single period
        assert result["latest_close"] == 100.0
        assert result["streak_price_change_pct"] == 0.0

    def test_empty_dataframe(self):
        """Empty DataFrame should return None."""
        df = pl.DataFrame({
            "ticker": pl.Series([], dtype=pl.String),
            "date": pl.Series([], dtype=pl.Date),
            "close": pl.Series([], dtype=pl.Float64),
            "volume_ratio": pl.Series([], dtype=pl.Float64),
        })

        result = detect_most_recent_streak("EMPTY", df)

        assert result is None

    def test_streak_metadata_fields(self, sample_down_streak):
        """Should return all required metadata fields."""
        result = detect_most_recent_streak("TEST", sample_down_streak)

        assert result is not None
        assert "ticker" in result
        assert "streak_length" in result
        assert "direction" in result
        assert "streak_start_date" in result
        assert "streak_end_date" in result
        assert "latest_close" in result
        assert "streak_price_change_pct" in result


class TestCalculateHVCStreaksForTimeframe:
    """Test calculate_hvc_streaks_for_timeframe integration. ⏱️"""

    @patch("tickerlake.gold.hvc_streaks.get_engine")
    def test_calculates_streaks(self, mock_get_engine):
        """Should calculate streaks from cached HVC data."""
        from tickerlake.gold.hvc_streaks import calculate_hvc_streaks_for_timeframe

        # Mock database connection
        mock_conn = MagicMock()
        mock_engine = MagicMock()
        mock_engine.connect.return_value.__enter__.return_value = mock_conn
        mock_get_engine.return_value = mock_engine

        # Mock query results - reading from gold HVC cache
        mock_result = [
            ("AAPL", date(2024, 1, 1), 100.0, 3.5),
            ("AAPL", date(2024, 1, 2), 110.0, 3.8),
            ("AAPL", date(2024, 1, 3), 120.0, 4.0),
            ("TSLA", date(2024, 1, 1), 200.0, 3.2),
            ("TSLA", date(2024, 1, 2), 190.0, 3.1),
        ]
        mock_conn.execute.return_value.fetchall.return_value = mock_result

        result = calculate_hvc_streaks_for_timeframe(
            timeframe="daily",
            min_streak_length=2
        )

        # Should return DataFrame with streaks
        assert isinstance(result, pl.DataFrame)
        assert len(result) == 2  # Two tickers
        assert "ticker" in result.columns
        assert "streak_length" in result.columns

    @patch("tickerlake.gold.hvc_streaks.get_engine")
    def test_no_hvc_periods_found(self, mock_get_engine):
        """Should handle case with no HVCs in cache."""
        from tickerlake.gold.hvc_streaks import calculate_hvc_streaks_for_timeframe

        mock_conn = MagicMock()
        mock_engine = MagicMock()
        mock_engine.connect.return_value.__enter__.return_value = mock_conn
        mock_get_engine.return_value = mock_engine

        # No results from cache
        mock_conn.execute.return_value.fetchall.return_value = []

        result = calculate_hvc_streaks_for_timeframe(
            timeframe="daily",
            min_streak_length=2
        )

        assert isinstance(result, pl.DataFrame)
        assert len(result) == 0

    def test_invalid_timeframe(self):
        """Should raise error for invalid timeframe."""
        from tickerlake.gold.hvc_streaks import calculate_hvc_streaks_for_timeframe

        with pytest.raises(ValueError, match="Invalid timeframe"):
            calculate_hvc_streaks_for_timeframe(
                timeframe="invalid",
                min_streak_length=2
            )

    @patch("tickerlake.gold.hvc_streaks.get_engine")
    def test_min_streak_length_filter(self, mock_get_engine):
        """Should filter out streaks below minimum length."""
        from tickerlake.gold.hvc_streaks import calculate_hvc_streaks_for_timeframe

        mock_conn = MagicMock()
        mock_engine = MagicMock()
        mock_engine.connect.return_value.__enter__.return_value = mock_conn
        mock_get_engine.return_value = mock_engine

        # Single HVC period (streak length = 1)
        mock_result = [
            ("SHORT", date(2024, 1, 1), 100.0, 3.5),
        ]
        mock_conn.execute.return_value.fetchall.return_value = mock_result

        result = calculate_hvc_streaks_for_timeframe(
            timeframe="daily",
            min_streak_length=2  # Should filter out length-1 streak
        )

        assert len(result) == 0  # Filtered out
