"""NYSE trading day calendar using exchange_calendars."""

import datetime
from typing import Literal

import exchange_calendars as ec
import pandas as pd


def period_session_bounds(
    date: datetime.date,
    period: Literal["week", "month"],
) -> tuple[datetime.date, datetime.date]:
    """Return the first and last scheduled XNYS sessions in the containing period."""
    if period == "week":
        start_date = date - datetime.timedelta(days=date.weekday())
        end_date = start_date + datetime.timedelta(days=6)
    elif period == "month":
        start_date = date.replace(day=1)
        next_month = start_date + datetime.timedelta(days=32)
        next_month = next_month.replace(day=1)
        end_date = next_month - datetime.timedelta(days=1)
    else:
        raise ValueError

    # Include padding because exchange_calendars requires query dates within
    # the calendar's session bounds, though period endpoints may be holidays.
    cal_start = start_date - datetime.timedelta(days=7)
    cal_end = end_date + datetime.timedelta(days=7)
    cal = ec.get_calendar("XNYS", start=cal_start, end=cal_end)
    # Use the calendar's schedule directly, not get_trading_days, which removes
    # sessions whose closes are still in the future.
    sessions = cal.sessions_in_range(str(start_date), str(end_date))
    if len(sessions) == 0:
        raise ValueError
    return sessions[0].date(), sessions[-1].date()


# Capture the concrete stdlib types at import time. Tests replace the module's
# ``datetime`` attribute with a frozen clock, and these aliases keep validation
# independent of that replacement.
_DATE = datetime.date

# The market can be closed for a long holiday weekend, so a preceding session
# may sit several days before the requested target.
_SESSION_LOOKBACK = datetime.timedelta(days=7)
# Padding used to extend the calendar's supported range past requested dates.
_CALENDAR_BOUNDS_PADDING = datetime.timedelta(days=7)


def _require_aware_now(now: datetime.datetime) -> None:
    """Reject naive instants; session closes are compared as absolute UTC times."""
    if now.tzinfo is None or now.utcoffset() is None:
        msg = "now must be a timezone-aware datetime"
        raise ValueError(msg)


def _require_date(value: datetime.date, name: str) -> None:
    """Reject non-dates (including datetime subclasses) before side effects."""
    if type(value) is not _DATE:
        msg = f"{name} must be a datetime.date, got {type(value).__name__}"
        raise TypeError(msg)


def _closed_sessions(
    start_date: datetime.date,
    end_date: datetime.date,
    now: datetime.datetime,
) -> list[datetime.date]:
    """Return closed XNYS sessions in an inclusive range, ascending.

    The calendar is created with explicit padded string bounds so sessions
    outside exchange_calendars' default range remain available. Query inputs
    are strings and the close comparison uses a tz-aware instant.
    """
    cal = ec.get_calendar(
        "XNYS",
        start=str(start_date - _CALENDAR_BOUNDS_PADDING),
        end=str(end_date + _CALENDAR_BOUNDS_PADDING),
    )
    sessions = cal.sessions_in_range(str(start_date), str(end_date))
    instant = pd.Timestamp(now.astimezone(datetime.UTC))
    return [session.date() for session in sessions if cal.session_close(session) <= instant]


def get_closed_sessions(
    start_date: datetime.date,
    end_date: datetime.date,
    *,
    now: datetime.datetime,
) -> list[datetime.date]:
    """Return XNYS sessions in [start_date, end_date] already closed at now.

    Args:
        start_date: First date to consider (inclusive).
        end_date: Last date to consider (inclusive).
        now: Timezone-aware instant used to decide which sessions have closed.

    Returns:
        Ascending list of session dates whose real close is at or before now.

    Raises:
        ValueError: If now is naive or end_date precedes start_date.
        TypeError: If start_date or end_date is not a datetime.date.
    """
    _require_aware_now(now)
    _require_date(start_date, "start_date")
    _require_date(end_date, "end_date")
    if end_date < start_date:
        msg = "end_date must not precede start_date"
        raise ValueError(msg)
    return _closed_sessions(start_date, end_date, now)


def resolve_closed_target(
    target: datetime.date,
    *,
    now: datetime.datetime,
) -> datetime.date:
    """Return the latest XNYS session at or before target that closed by now.

    Resolution does not depend on any requested correction range. A target that
    is not a session (weekend or holiday) or whose own session has not closed
    yet resolves to the preceding closed session. Early closes are honored by
    comparing the session's actual close against now.

    Raises:
        ValueError: If now is naive, or no closed session exists near target.
        TypeError: If target is not a datetime.date.
    """
    _require_aware_now(now)
    _require_date(target, "target")
    # A session cannot have closed before its own date, so now's UTC date and
    # target both upper-bound the search. The range is padded to reach the
    # preceding session; an empty result is an explicit error, never a guess.
    upper = min(target, now.astimezone(datetime.UTC).date())
    lower = upper - _SESSION_LOOKBACK
    sessions = _closed_sessions(lower, upper, now)
    if not sessions:
        msg = f"no closed XNYS session found at or before {target}"
        raise ValueError(msg)
    return sessions[-1]


def get_trading_days(
    start_date: datetime.date,
    end_date: datetime.date | None = None,
) -> list[datetime.date]:
    """Return NYSE trading days between start_date and end_date (inclusive).

    Only returns days where the market has already closed (session_close <= now).

    Args:
        start_date: First date to consider (inclusive).
        end_date: Last date to consider (inclusive). If None, uses today.

    Returns:
        List of datetime.date objects representing trading days.
    """
    if end_date is None:
        end_date = datetime.datetime.now(tz=datetime.UTC).date()

    # Preserve the historical contract: a reversed range yields no sessions.
    if end_date < start_date:
        return []

    return get_closed_sessions(
        start_date,
        end_date,
        now=datetime.datetime.now(tz=datetime.UTC),
    )
