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


def get_trading_days(
    start_date: datetime.date,
    end_date: datetime.date | None = None,
) -> list[datetime.date]:
    """Return NYSE trading days between start_date and end_date (inclusive).

    Only returns days where the market has already closed (session_close <= now).
    Uses tz-naive pd.Timestamp objects to avoid exchange_calendars crash with
    stdlib datetime.timezone.utc.

    Args:
        start_date: First date to consider (inclusive).
        end_date: Last date to consider (inclusive). If None, uses today.

    Returns:
        List of datetime.date objects representing trading days.
    """
    if end_date is None:
        end_date = datetime.datetime.now(tz=datetime.UTC).date()

    cal = ec.get_calendar("XNYS")

    # CRITICAL: use string dates — stdlib datetime.timezone.utc
    # causes AttributeError: 'datetime.timezone' object has no attribute 'key'
    # and pd.Timestamp can return NaTType which exchange_calendars doesn't accept
    sessions = cal.sessions_in_range(str(start_date), str(end_date))

    # Current time as tz-aware for comparison with session_close (which is tz-aware UTC)
    now = pd.Timestamp.now(tz="UTC")

    return [session.date() for session in sessions if cal.session_close(session) <= now]
