"""Tests for gold layer HVC Returns analysis. 🔄"""

from datetime import date
from unittest.mock import MagicMock, patch

import polars as pl
import pytest

from tickerlake.gold.hvc_returns import detect_hvc_returns_for_ticker


@pytest.fixture
def sample_daily_hvc_returns():
    """Sample daily HVC data where price returns to previous HVC ranges."""
    # Day 1: HVC at 100, range [95-105]
    # Day 2: HVC at 110, range [108-112]
    # Day 7: HVC at 102, range [100-104] -> Returns to Day 1 (6 days ago)
    # Day 10: HVC at 109, range [107-111] -> Returns to Day 2 (8 days ago)
    return pl.DataFrame({
        "ticker": ["AAPL"] * 4,
        "date": [
            date(2024, 1, 1),
            date(2024, 1, 2),
            date(2024, 1, 7),
            date(2024, 1, 10),
        ],
        "low": [95.0, 108.0, 100.0, 107.0],
        "high": [105.0, 112.0, 104.0, 111.0],
        "close": [102.0, 110.0, 102.0, 109.0],
        "volume_ratio": [3.5, 3.8, 3.2, 3.6],
    })


@pytest.fixture
def sample_weekly_hvc_returns():
    """Sample weekly HVC data with appropriate weekly gaps."""
    # Week 1 (Jan 5): HVC at 100, range [95-105]
    # Week 2 (Jan 12): HVC at 110, range [108-112]
    # Week 6 (Feb 9): HVC at 102, range [100-104] -> Returns to Week 1 (5 weeks)
    return pl.DataFrame({
        "ticker": ["TSLA"] * 3,
        "date": [
            date(2024, 1, 5),   # Week 1
            date(2024, 1, 12),  # Week 2
            date(2024, 2, 9),   # Week 6 (35 days later = 5 weeks)
        ],
        "low": [95.0, 108.0, 100.0],
        "high": [105.0, 112.0, 104.0],
        "close": [100.0, 110.0, 102.0],
        "volume_ratio": [3.5, 3.8, 3.2],
    })


@pytest.fixture
def sample_no_returns():
    """Sample data where no returns occur (prices never revisit previous HVCs)."""
    return pl.DataFrame({
        "ticker": ["MSFT"] * 5,
        "date": [date(2024, 1, i) for i in range(1, 6)],
        "low": [100.0, 110.0, 120.0, 130.0, 140.0],
        "high": [105.0, 115.0, 125.0, 135.0, 145.0],
        "close": [103.0, 113.0, 123.0, 133.0, 143.0],
        "volume_ratio": [3.5, 3.8, 3.2, 3.6, 3.9],
    })


@pytest.fixture
def sample_gap_too_small():
    """Sample data where returns occur but gap is too small."""
    # Day 1: HVC at 100, range [95-105]
    # Day 3: HVC at 102 -> Returns to Day 1 but only 2 days ago (< 5)
    return pl.DataFrame({
        "ticker": ["GOOG"] * 2,
        "date": [date(2024, 1, 1), date(2024, 1, 3)],
        "low": [95.0, 100.0],
        "high": [105.0, 104.0],
        "close": [100.0, 102.0],
        "volume_ratio": [3.5, 3.2],
    })


class TestDetectHVCReturnsForTicker:
    """Test detect_hvc_returns_for_ticker function. 🎯"""

    def test_daily_returns_detected(self, sample_daily_hvc_returns):
        """Should detect when daily close returns to previous HVC range."""
        results = detect_hvc_returns_for_ticker(
            sample_daily_hvc_returns,
            min_periods_ago=5,
            timeframe="daily"
        )

        # Day 7 returns to Day 1 (6 days ago)
        # Day 10 returns to Day 2 (8 days ago)
        assert len(results) == 2

        # Check first return (Day 7 → Day 1)
        first_return = results[0]
        assert first_return["ticker"] == "AAPL"
        assert first_return["return_date"] == date(2024, 1, 7)
        assert first_return["hvc_date"] == date(2024, 1, 1)
        assert first_return["current_close"] == 102.0
        assert first_return["hvc_low"] == 95.0
        assert first_return["hvc_high"] == 105.0
        assert first_return["days_since_hvc"] == 6

        # Check second return (Day 10 → Day 2)
        second_return = results[1]
        assert second_return["return_date"] == date(2024, 1, 10)
        assert second_return["hvc_date"] == date(2024, 1, 2)
        assert second_return["days_since_hvc"] == 8

    def test_weekly_returns_detected(self, sample_weekly_hvc_returns):
        """Should detect when weekly close returns to previous HVC range."""
        results = detect_hvc_returns_for_ticker(
            sample_weekly_hvc_returns,
            min_periods_ago=4,
            timeframe="weekly"
        )

        # Week 6 returns to Week 1 (5 weeks ago)
        assert len(results) == 1

        result = results[0]
        assert result["ticker"] == "TSLA"
        assert result["return_date"] == date(2024, 2, 9)
        assert result["hvc_date"] == date(2024, 1, 5)
        assert result["current_close"] == 102.0
        assert result["weeks_since_hvc"] == 5

    def test_no_returns_found(self, sample_no_returns):
        """Should return empty list when no price returns occur."""
        results = detect_hvc_returns_for_ticker(
            sample_no_returns,
            min_periods_ago=5,
            timeframe="daily"
        )

        assert len(results) == 0
        assert isinstance(results, list)

    def test_gap_too_small_filtered_out(self, sample_gap_too_small):
        """Should filter out returns where time gap is insufficient."""
        results = detect_hvc_returns_for_ticker(
            sample_gap_too_small,
            min_periods_ago=5,
            timeframe="daily"
        )

        # Return exists but only 2 days ago (< 5), should be filtered
        assert len(results) == 0

    def test_empty_dataframe(self):
        """Empty DataFrame should return empty list."""
        df = pl.DataFrame({
            "ticker": pl.Series([], dtype=pl.String),
            "date": pl.Series([], dtype=pl.Date),
            "low": pl.Series([], dtype=pl.Float64),
            "high": pl.Series([], dtype=pl.Float64),
            "close": pl.Series([], dtype=pl.Float64),
            "volume_ratio": pl.Series([], dtype=pl.Float64),
        })

        results = detect_hvc_returns_for_ticker(
            df,
            min_periods_ago=5,
            timeframe="daily"
        )

        assert len(results) == 0
        assert isinstance(results, list)

    def test_single_hvc_no_returns(self):
        """Single HVC should return empty list (no previous HVCs to return to)."""
        df = pl.DataFrame({
            "ticker": ["SINGLE"],
            "date": [date(2024, 1, 1)],
            "low": [95.0],
            "high": [105.0],
            "close": [100.0],
            "volume_ratio": [3.5],
        })

        results = detect_hvc_returns_for_ticker(
            df,
            min_periods_ago=5,
            timeframe="daily"
        )

        assert len(results) == 0

    def test_multiple_returns_same_date(self):
        """Single date can return to multiple previous HVCs."""
        # Day 1: HVC at 100, range [95-105]
        # Day 10: HVC at 101, range [98-104] → Returns to Day 1
        # Day 20: HVC at 102, range [100-104] → Returns to BOTH Day 1 and Day 10
        df = pl.DataFrame({
            "ticker": ["MULTI"] * 3,
            "date": [date(2024, 1, 1), date(2024, 1, 10), date(2024, 1, 20)],
            "low": [95.0, 98.0, 100.0],
            "high": [105.0, 104.0, 104.0],
            "close": [100.0, 101.0, 102.0],
            "volume_ratio": [3.5, 3.8, 3.2],
        })

        results = detect_hvc_returns_for_ticker(
            df,
            min_periods_ago=5,
            timeframe="daily"
        )

        # Total: Day 10→Day 1, Day 20→Day 1, Day 20→Day 10 = 3 returns
        assert len(results) == 3

        # Verify Day 20 returns to both previous HVCs
        day_20_returns = [r for r in results if r["return_date"] == date(2024, 1, 20)]
        assert len(day_20_returns) == 2
        day_20_hvc_dates = {r["hvc_date"] for r in day_20_returns}
        assert date(2024, 1, 1) in day_20_hvc_dates
        assert date(2024, 1, 10) in day_20_hvc_dates

    def test_close_at_boundary_low(self):
        """Current close exactly at HVC low boundary should be included."""
        df = pl.DataFrame({
            "ticker": ["EDGE"] * 2,
            "date": [date(2024, 1, 1), date(2024, 1, 10)],
            "low": [95.0, 100.0],
            "high": [105.0, 110.0],
            "close": [100.0, 95.0],  # Exactly at low boundary
            "volume_ratio": [3.5, 3.2],
        })

        results = detect_hvc_returns_for_ticker(
            df,
            min_periods_ago=5,
            timeframe="daily"
        )

        assert len(results) == 1
        assert results[0]["current_close"] == 95.0

    def test_close_at_boundary_high(self):
        """Current close exactly at HVC high boundary should be included."""
        df = pl.DataFrame({
            "ticker": ["EDGE"] * 2,
            "date": [date(2024, 1, 1), date(2024, 1, 10)],
            "low": [95.0, 100.0],
            "high": [105.0, 110.0],
            "close": [100.0, 105.0],  # Exactly at high boundary
            "volume_ratio": [3.5, 3.2],
        })

        results = detect_hvc_returns_for_ticker(
            df,
            min_periods_ago=5,
            timeframe="daily"
        )

        assert len(results) == 1
        assert results[0]["current_close"] == 105.0

    def test_close_outside_range(self):
        """Current close outside HVC range should not be detected."""
        df = pl.DataFrame({
            "ticker": ["OUT"] * 2,
            "date": [date(2024, 1, 1), date(2024, 1, 10)],
            "low": [95.0, 106.0],  # Outside the range
            "high": [105.0, 110.0],
            "close": [100.0, 108.0],
            "volume_ratio": [3.5, 3.2],
        })

        results = detect_hvc_returns_for_ticker(
            df,
            min_periods_ago=5,
            timeframe="daily"
        )

        # 108.0 is outside [95-105] range
        assert len(results) == 0

    def test_unsorted_data_gets_sorted(self):
        """Function should sort data by date before processing."""
        # Provide data in random order
        df = pl.DataFrame({
            "ticker": ["UNSORTED"] * 3,
            "date": [date(2024, 1, 15), date(2024, 1, 1), date(2024, 1, 10)],
            "low": [100.0, 95.0, 98.0],
            "high": [104.0, 105.0, 104.0],
            "close": [102.0, 100.0, 101.0],
            "volume_ratio": [3.2, 3.5, 3.8],
        })

        results = detect_hvc_returns_for_ticker(
            df,
            min_periods_ago=5,
            timeframe="daily"
        )

        # Day 15 should return to Day 1 (despite unsorted input)
        assert len(results) >= 1
        # Check that it found the return correctly
        day_15_returns = [r for r in results if r["return_date"] == date(2024, 1, 15)]
        assert len(day_15_returns) > 0

    def test_return_dict_has_all_fields(self, sample_daily_hvc_returns):
        """Returned dictionaries should have all required fields."""
        results = detect_hvc_returns_for_ticker(
            sample_daily_hvc_returns,
            min_periods_ago=5,
            timeframe="daily"
        )

        assert len(results) > 0
        result = results[0]

        required_fields = [
            "ticker",
            "return_date",
            "current_close",
            "hvc_date",
            "hvc_low",
            "hvc_high",
            "hvc_close",
            "hvc_volume_ratio",
            "days_since_hvc",
        ]

        for field in required_fields:
            assert field in result

    def test_weekly_timeframe_uses_weeks_field(self, sample_weekly_hvc_returns):
        """Weekly timeframe should use 'weeks_since_hvc' field."""
        results = detect_hvc_returns_for_ticker(
            sample_weekly_hvc_returns,
            min_periods_ago=4,
            timeframe="weekly"
        )

        assert len(results) > 0
        result = results[0]

        assert "weeks_since_hvc" in result
        assert "days_since_hvc" not in result


class TestCalculateHVCReturnsForTimeframe:
    """Test calculate_hvc_returns_for_timeframe integration. ⏱️"""

    @patch("tickerlake.gold.hvc_returns.get_engine")
    def test_calculates_returns(self, mock_get_engine):
        """Should calculate returns from cached HVC data."""
        from tickerlake.gold.hvc_returns import calculate_hvc_returns_for_timeframe

        # Mock database connection
        mock_conn = MagicMock()
        mock_engine = MagicMock()
        mock_engine.connect.return_value.__enter__.return_value = mock_conn
        mock_get_engine.return_value = mock_engine

        # Mock query results - reading from gold HVC cache
        # AAPL has 3 HVCs where Day 10 returns to Day 1
        mock_result = [
            ("AAPL", date(2024, 1, 1), 95.0, 105.0, 100.0, 3.5),
            ("AAPL", date(2024, 1, 2), 108.0, 112.0, 110.0, 3.8),
            ("AAPL", date(2024, 1, 10), 98.0, 104.0, 102.0, 3.2),
        ]
        mock_conn.execute.return_value.fetchall.return_value = mock_result

        result = calculate_hvc_returns_for_timeframe(
            timeframe="daily",
            min_periods_ago=5
        )

        # Should return DataFrame with HVC returns
        assert isinstance(result, pl.DataFrame)
        assert len(result) >= 1  # At least one return detected
        assert "ticker" in result.columns
        assert "return_date" in result.columns
        assert "hvc_date" in result.columns

    @patch("tickerlake.gold.hvc_returns.get_engine")
    def test_no_hvc_periods_found(self, mock_get_engine):
        """Should handle case with no HVCs in cache."""
        from tickerlake.gold.hvc_returns import calculate_hvc_returns_for_timeframe

        mock_conn = MagicMock()
        mock_engine = MagicMock()
        mock_engine.connect.return_value.__enter__.return_value = mock_conn
        mock_get_engine.return_value = mock_engine

        # No results from cache
        mock_conn.execute.return_value.fetchall.return_value = []

        result = calculate_hvc_returns_for_timeframe(
            timeframe="daily",
            min_periods_ago=5
        )

        assert isinstance(result, pl.DataFrame)
        assert len(result) == 0

    def test_invalid_timeframe(self):
        """Should raise error for invalid timeframe."""
        from tickerlake.gold.hvc_returns import calculate_hvc_returns_for_timeframe

        with pytest.raises(ValueError, match="Invalid timeframe"):
            calculate_hvc_returns_for_timeframe(
                timeframe="invalid",
                min_periods_ago=5
            )

    @patch("tickerlake.gold.hvc_returns.get_engine")
    def test_weekly_uses_correct_gap(self, mock_get_engine):
        """Weekly timeframe should use weeks for gap calculation."""
        from tickerlake.gold.hvc_returns import calculate_hvc_returns_for_timeframe

        mock_conn = MagicMock()
        mock_engine = MagicMock()
        mock_engine.connect.return_value.__enter__.return_value = mock_conn
        mock_get_engine.return_value = mock_engine

        # Week 1 and Week 6 (35 days = 5 weeks apart)
        mock_result = [
            ("TSLA", date(2024, 1, 5), 95.0, 105.0, 100.0, 3.5),
            ("TSLA", date(2024, 2, 9), 98.0, 104.0, 102.0, 3.2),
        ]
        mock_conn.execute.return_value.fetchall.return_value = mock_result

        result = calculate_hvc_returns_for_timeframe(
            timeframe="weekly",
            min_periods_ago=4  # 4 weeks minimum
        )

        # Should detect the return (5 weeks >= 4 weeks)
        assert len(result) >= 1
        if len(result) > 0:
            assert "weeks_since_hvc" in result.columns
