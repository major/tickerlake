"""Tests for the trading day calendar module."""

import datetime
from types import SimpleNamespace

import pandas as pd
import pytest

import tickerlake.calendar as market_calendar
from tickerlake.calendar import get_trading_days


class TestGetTradingDays:
    """Test suite for get_trading_days function."""

    @staticmethod
    def freeze_calendar_clocks(monkeypatch: pytest.MonkeyPatch, instant: pd.Timestamp) -> None:
        """Replace only the calendar module's clock references."""

        class FrozenDateTime(datetime.datetime):
            """Datetime class fixed to one instant for calendar defaults."""

            @classmethod
            def now(cls, tz: datetime.tzinfo | None = None) -> datetime.datetime:
                current = instant.to_pydatetime()
                return current.astimezone(tz) if tz else current

        class FrozenTimestamp(pd.Timestamp):
            """Timestamp class fixed to one instant for session comparisons."""

            @classmethod
            def now(cls, tz: str | datetime.tzinfo | None = None) -> pd.Timestamp:
                return instant.tz_convert(tz) if tz else instant.tz_localize(None)

        monkeypatch.setattr(
            market_calendar,
            "datetime",
            SimpleNamespace(datetime=FrozenDateTime, UTC=datetime.UTC),
        )
        monkeypatch.setattr(
            market_calendar,
            "pd",
            SimpleNamespace(Timestamp=FrozenTimestamp),
        )

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
        expected_trading_days = 21
        assert len(result) == expected_trading_days

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

    @pytest.mark.parametrize(
        ("session", "close_time", "expected"),
        [
            (datetime.date(2024, 1, 2), "20:59:59", []),
            (datetime.date(2024, 1, 2), "21:00:00", [datetime.date(2024, 1, 2)]),
            (datetime.date(2024, 1, 2), "21:00:01", [datetime.date(2024, 1, 2)]),
            (datetime.date(2024, 12, 24), "17:59:59", []),
            (datetime.date(2024, 12, 24), "18:00:00", [datetime.date(2024, 12, 24)]),
            (datetime.date(2024, 12, 24), "18:00:01", [datetime.date(2024, 12, 24)]),
        ],
        ids=[
            "regular-before-close",
            "regular-at-close",
            "regular-after-close",
            "early-before-close",
            "early-at-close",
            "early-after-close",
        ],
    )
    def test_end_date_none_includes_only_closed_sessions(
        self,
        monkeypatch: pytest.MonkeyPatch,
        session: datetime.date,
        close_time: str,
        expected: list[datetime.date],
    ) -> None:
        """A session enters results exactly when its independently known UTC close passes."""
        self.freeze_calendar_clocks(monkeypatch, pd.Timestamp(f"{session} {close_time}", tz="UTC"))
        assert get_trading_days(session, end_date=None) == expected

    def test_end_date_none_excludes_future_sessions(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """A future session is excluded even when the requested range includes it."""
        self.freeze_calendar_clocks(monkeypatch, pd.Timestamp("2024-01-02 22:00:00", tz="UTC"))
        assert get_trading_days(datetime.date(2024, 1, 2), end_date=datetime.date(2024, 1, 3)) == [
            datetime.date(2024, 1, 2)
        ]

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
