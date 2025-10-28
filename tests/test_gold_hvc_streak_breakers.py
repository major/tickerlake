"""Tests for gold layer HVC Streak Breaker analysis. 🔔"""

from datetime import date

import polars as pl
import pytest

from tickerlake.gold.hvc_streak_breakers import detect_streak_breakers


class TestDetectStreakBreakers:
    """Test detect_streak_breakers function. 🔔"""

    def test_single_breaker(self):
        """Should detect when a 3-period up streak is broken by down."""
        df = pl.DataFrame({
            "ticker": ["TEST"] * 5,
            "date": [date(2024, 1, i) for i in range(1, 6)],
            "close": [10.0, 20.0, 30.0, 40.0, 30.0],  # Up, up, up, DOWN
            "volume_ratio": [3.0] * 5,
        })

        result = detect_streak_breakers("TEST", df, min_streak_length=3)

        assert len(result) == 1
        breaker = result[0]
        assert breaker["ticker"] == "TEST"
        assert breaker["streak_direction"] == "up"
        assert breaker["streak_length"] == 3
        assert breaker["streak_start_date"] == date(2024, 1, 1)
        assert breaker["streak_end_date"] == date(2024, 1, 4)
        assert breaker["breaker_date"] == date(2024, 1, 5)
        assert breaker["streak_start_close"] == 10.0
        assert breaker["streak_end_close"] == 40.0
        assert breaker["breaker_close"] == 30.0
        assert breaker["streak_price_change_pct"] == pytest.approx(300.0)  # 10→40

    def test_down_streak_broken_by_up(self):
        """Should detect when a down streak is broken by up."""
        df = pl.DataFrame({
            "ticker": ["TEST"] * 6,
            "date": [date(2024, 1, i) for i in range(1, 7)],
            "close": [100.0, 90.0, 80.0, 70.0, 60.0, 70.0],  # Down×4, then UP
            "volume_ratio": [3.0] * 6,
        })

        result = detect_streak_breakers("TEST", df, min_streak_length=3)

        assert len(result) == 1
        breaker = result[0]
        assert breaker["streak_direction"] == "down"
        assert breaker["streak_length"] == 4
        assert breaker["breaker_close"] == 70.0

    def test_multiple_breakers(self):
        """Should detect multiple breakers in sequence.

        Note: This function returns ALL breakers for analysis. The calling
        function (calculate_streak_breakers_for_timeframe) filters to only
        the most recent breaker per ticker. 🎯
        """
        df = pl.DataFrame({
            "ticker": ["TEST"] * 10,
            "date": [date(2024, 1, i) for i in range(1, 11)],
            # Up×3, Down×3, Up×3 → 2 breakers
            "close": [10.0, 20.0, 30.0, 40.0, 30.0, 20.0, 10.0, 20.0, 30.0, 40.0],
            "volume_ratio": [3.0] * 10,
        })

        result = detect_streak_breakers("TEST", df, min_streak_length=3)

        assert len(result) == 2
        # First breaker: up streak broken by down
        assert result[0]["streak_direction"] == "up"
        assert result[0]["streak_length"] == 3
        # Second breaker: down streak broken by up (most recent)
        assert result[1]["streak_direction"] == "down"
        assert result[1]["streak_length"] == 3

    def test_streak_too_short(self):
        """Should not detect breakers if streak is below minimum."""
        df = pl.DataFrame({
            "ticker": ["TEST"] * 4,
            "date": [date(2024, 1, i) for i in range(1, 5)],
            "close": [10.0, 20.0, 30.0, 20.0],  # Only 2-period streak
            "volume_ratio": [3.0] * 4,
        })

        result = detect_streak_breakers("TEST", df, min_streak_length=3)

        assert len(result) == 0  # Streak too short

    def test_no_direction_change(self):
        """Should not detect breakers if direction never changes."""
        df = pl.DataFrame({
            "ticker": ["TEST"] * 5,
            "date": [date(2024, 1, i) for i in range(1, 6)],
            "close": [10.0, 20.0, 30.0, 40.0, 50.0],  # All up
            "volume_ratio": [3.0] * 5,
        })

        result = detect_streak_breakers("TEST", df, min_streak_length=3)

        assert len(result) == 0  # No direction change

    def test_equal_prices_continue_streak(self):
        """Equal prices should continue current direction."""
        df = pl.DataFrame({
            "ticker": ["TEST"] * 6,
            "date": [date(2024, 1, i) for i in range(1, 7)],
            # Up, up, equal, up, DOWN → streak of 4
            "close": [10.0, 20.0, 30.0, 30.0, 40.0, 30.0],
            "volume_ratio": [3.0] * 6,
        })

        result = detect_streak_breakers("TEST", df, min_streak_length=3)

        assert len(result) == 1
        assert result[0]["streak_length"] == 4  # Equal price counted

    def test_insufficient_data(self):
        """Should return empty list if not enough data."""
        df = pl.DataFrame({
            "ticker": ["TEST"] * 3,
            "date": [date(2024, 1, i) for i in range(1, 4)],
            "close": [10.0, 20.0, 30.0],
            "volume_ratio": [3.0] * 3,
        })

        # Need at least min_streak_length + 1 for a breaker
        result = detect_streak_breakers("TEST", df, min_streak_length=3)

        assert len(result) == 0

    def test_empty_dataframe(self):
        """Empty DataFrame should return empty list."""
        df = pl.DataFrame({
            "ticker": pl.Series([], dtype=pl.String),
            "date": pl.Series([], dtype=pl.Date),
            "close": pl.Series([], dtype=pl.Float64),
            "volume_ratio": pl.Series([], dtype=pl.Float64),
        })

        result = detect_streak_breakers("EMPTY", df, min_streak_length=3)

        assert result == []
