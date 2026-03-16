"""Tests for the trading day calendar module."""

import datetime

import pytest

from tickerlake.calendar import get_trading_days


class TestGetTradingDays:
    """Test suite for get_trading_days function."""

    def test_excludes_weekends(self) -> None:
        """Weekend-only range returns empty list."""
        # Jan 6-7, 2024 is Saturday-Sunday
        result = get_trading_days(
            datetime.date(2024, 1, 6),
            datetime.date(2024, 1, 7),
        )
        assert result == []

    def test_excludes_holidays(self) -> None:
        """New Year's Day 2024-01-01 is not in results."""
        result = get_trading_days(
            datetime.date(2024, 1, 1),
            datetime.date(2024, 1, 1),
        )
        assert result == []

    def test_excludes_mlk_day(self) -> None:
        """MLK Day 2024-01-15 is not in results."""
        result = get_trading_days(
            datetime.date(2024, 1, 15),
            datetime.date(2024, 1, 15),
        )
        assert result == []

    def test_january_2024_count(self) -> None:
        """Jan 2-31 2024 has exactly 21 trading days."""
        result = get_trading_days(
            datetime.date(2024, 1, 2),
            datetime.date(2024, 1, 31),
        )
        assert len(result) == 21

    def test_returns_date_objects(self) -> None:
        """Returns list of datetime.date, not pd.Timestamp."""
        result = get_trading_days(
            datetime.date(2024, 1, 2),
            datetime.date(2024, 1, 3),
        )
        assert len(result) > 0
        for item in result:
            assert isinstance(item, datetime.date)
            assert not hasattr(item, "tz_localize")  # not a pd.Timestamp

    def test_end_date_none(self) -> None:
        """When end_date=None, returns days up to today."""
        today = datetime.datetime.now(tz=datetime.timezone.utc).date()
        result = get_trading_days(
            datetime.date(2024, 1, 2),
            end_date=None,
        )
        # Should have at least some trading days from Jan 2024 to today
        assert len(result) > 0
        # All dates should be <= today
        assert all(d <= today for d in result)

    def test_early_close_christmas_eve_2024(self) -> None:
        """2024-12-24 IS a trading day (even though early close)."""
        result = get_trading_days(
            datetime.date(2024, 12, 24),
            datetime.date(2024, 12, 24),
        )
        # Christmas Eve 2024 is a Tuesday and a trading day
        # (it's an early close, but still a trading day)
        assert len(result) == 1
        assert result[0] == datetime.date(2024, 12, 24)

    def test_excludes_christmas_2024(self) -> None:
        """2024-12-25 (Wednesday) is NOT a trading day."""
        result = get_trading_days(
            datetime.date(2024, 12, 25),
            datetime.date(2024, 12, 25),
        )
        assert result == []

    def test_single_trading_day(self) -> None:
        """A range containing exactly one trading day returns list of length 1."""
        result = get_trading_days(
            datetime.date(2024, 1, 2),
            datetime.date(2024, 1, 2),
        )
        assert len(result) == 1
        assert result[0] == datetime.date(2024, 1, 2)
