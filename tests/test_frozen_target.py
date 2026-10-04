"""Frozen-now tests for closed NYSE session resolution.

These tests use the real exchange_calendars XNYS calendar and inject ``now``
explicitly, so they are deterministic without patching the module clock.
"""

import datetime

import pytest

from tickerlake.calendar import get_closed_sessions, resolve_closed_target

UTC = datetime.UTC


def _utc(*args: int) -> datetime.datetime:
    """Build a timezone-aware UTC instant from positional datetime fields."""
    return datetime.datetime(*args, tzinfo=UTC)


# ---------------------------------------------------------------------------
# resolve_closed_target
# ---------------------------------------------------------------------------


def test_historic_session_resolves_to_itself() -> None:
    """A past session date resolves to itself once now is well after its close."""
    assert resolve_closed_target(datetime.date(2015, 6, 15), now=_utc(2026, 1, 1)) == datetime.date(2015, 6, 15)


def test_historic_observed_holiday_resolves_to_preceding_session() -> None:
    """2015-07-03 observed Independence Day resolves to the prior Thursday."""
    assert resolve_closed_target(datetime.date(2015, 7, 3), now=_utc(2026, 1, 1)) == datetime.date(2015, 7, 2)


def test_weekend_target_resolves_to_preceding_session() -> None:
    """Saturday 2024-01-06 resolves to Friday 2024-01-05."""
    assert resolve_closed_target(datetime.date(2024, 1, 6), now=_utc(2024, 1, 8)) == datetime.date(2024, 1, 5)


def test_holiday_target_resolves_to_preceding_session() -> None:
    """MLK Day 2024-01-15 resolves to Friday 2024-01-12."""
    assert resolve_closed_target(datetime.date(2024, 1, 15), now=_utc(2024, 1, 16)) == datetime.date(2024, 1, 12)


@pytest.mark.parametrize(
    ("now", "expected"),
    [
        (_utc(2024, 1, 2, 20, 59, 59), datetime.date(2023, 12, 29)),
        (_utc(2024, 1, 2, 21, 0, 0), datetime.date(2024, 1, 2)),
        (_utc(2024, 1, 2, 21, 0, 1), datetime.date(2024, 1, 2)),
    ],
    ids=["before-close", "at-close", "after-close"],
)
def test_ordinary_close_boundary(now: datetime.datetime, expected: datetime.date) -> None:
    """The 2024-01-02 session enters exactly when its 21:00 UTC close passes."""
    assert resolve_closed_target(datetime.date(2024, 1, 2), now=now) == expected


@pytest.mark.parametrize(
    ("now", "expected"),
    [
        (_utc(2024, 12, 24, 17, 59, 59), datetime.date(2024, 12, 23)),
        (_utc(2024, 12, 24, 18, 0, 0), datetime.date(2024, 12, 24)),
    ],
    ids=["before-early-close", "at-early-close"],
)
def test_early_close_boundary(now: datetime.datetime, expected: datetime.date) -> None:
    """Christmas Eve 2024 closes early at 18:00 UTC and is honored exactly."""
    assert resolve_closed_target(datetime.date(2024, 12, 24), now=now) == expected


@pytest.mark.parametrize(
    ("now", "expected"),
    [
        (_utc(2024, 1, 2, 0, 30), datetime.date(2023, 12, 29)),
        (_utc(2024, 1, 3, 1, 0), datetime.date(2024, 1, 2)),
    ],
    ids=["utc-day-before-close", "utc-day-after-close"],
)
def test_utc_day_boundary_uses_absolute_close(now: datetime.datetime, expected: datetime.date) -> None:
    """A now whose UTC date differs from the session's ET date still resolves correctly."""
    assert resolve_closed_target(datetime.date(2024, 1, 2), now=now) == expected


def test_future_target_resolves_to_latest_closed_session() -> None:
    """A far-future target falls back to the latest session closed at now."""
    assert resolve_closed_target(datetime.date(2030, 1, 1), now=_utc(2024, 1, 3, 22, 0)) == (datetime.date(2024, 1, 3))


def test_resolve_rejects_naive_now() -> None:
    """A naive now is rejected before any calendar work happens."""
    with pytest.raises(ValueError, match="timezone-aware"):
        resolve_closed_target(datetime.date(2024, 1, 2), now=datetime.datetime(2024, 1, 2, 22, 0))  # noqa: DTZ001


# ---------------------------------------------------------------------------
# get_closed_sessions
# ---------------------------------------------------------------------------


def test_range_returns_only_sessions_closed_by_now() -> None:
    """A partly closed range returns only the sessions already finished."""
    assert get_closed_sessions(
        datetime.date(2024, 1, 2),
        datetime.date(2024, 1, 3),
        now=_utc(2024, 1, 2, 22, 0),
    ) == [datetime.date(2024, 1, 2)]


def test_range_includes_all_sessions_after_their_closes() -> None:
    """Once now passes both closes, both sessions are returned in order."""
    assert get_closed_sessions(
        datetime.date(2024, 1, 2),
        datetime.date(2024, 1, 3),
        now=_utc(2024, 1, 3, 22, 0),
    ) == [datetime.date(2024, 1, 2), datetime.date(2024, 1, 3)]


def test_range_excludes_weekend_and_holiday() -> None:
    """A Saturday-through-holiday range yields no closed sessions."""
    assert (
        get_closed_sessions(
            datetime.date(2024, 1, 13),
            datetime.date(2024, 1, 15),
            now=_utc(2024, 1, 16),
        )
        == []
    )


def test_range_rejects_reversed_bounds() -> None:
    """End before start is rejected instead of silently returning nothing."""
    with pytest.raises(ValueError, match="end_date"):
        get_closed_sessions(
            datetime.date(2024, 1, 3),
            datetime.date(2024, 1, 2),
            now=_utc(2024, 1, 4),
        )


def test_range_rejects_naive_now() -> None:
    """A naive now is rejected before any calendar work happens."""
    with pytest.raises(ValueError, match="timezone-aware"):
        get_closed_sessions(
            datetime.date(2024, 1, 2),
            datetime.date(2024, 1, 3),
            now=datetime.datetime(2024, 1, 4),  # noqa: DTZ001
        )
