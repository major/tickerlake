"""NYSE trading day calendar using exchange_calendars."""

import datetime

import exchange_calendars as ec
import pandas as pd


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
        end_date = datetime.datetime.now(tz=datetime.timezone.utc).date()

    cal = ec.get_calendar("XNYS")

    # CRITICAL: use tz-naive Timestamps — stdlib datetime.timezone.utc
    # causes AttributeError: 'datetime.timezone' object has no attribute 'key'
    start_ts = pd.Timestamp(start_date)
    end_ts = pd.Timestamp(end_date)

    sessions = cal.sessions_in_range(start_ts, end_ts)

    # Current time as tz-aware for comparison with session_close (which is tz-aware UTC)
    now = pd.Timestamp.now(tz="UTC")

    result = []
    for session in sessions:
        if cal.session_close(session) <= now:
            result.append(session.date())

    return result
