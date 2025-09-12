"""Utility functions for trading calendar and market status operations."""

from datetime import datetime

import pandas_market_calendars as mcal
import pytz


def get_trading_days(start_date, end_date):
    """Get list of trading days between start and end dates.

    Args:
        start_date: Start date (string or date object).
        end_date: End date (string or date object).

    Returns:
        Trading days in YYYY-MM-DD format.

    """
    calendar = mcal.get_calendar("NYSE")
    trading_days = calendar.valid_days(start_date=start_date, end_date=end_date)
    return [day.strftime("%Y-%m-%d") for day in trading_days]


def is_market_open():
    """Check if the market is currently open.

    Returns:
        True if market is open, False otherwise.

    """
    nyse = mcal.get_calendar("NYSE")

    # Get current time in the market's timezone
    market_tz = pytz.timezone(nyse.tz.zone)
    now_market_time = datetime.now(market_tz)

    # Get today's trading schedule
    schedule = nyse.schedule(
        start_date=now_market_time.date(), end_date=now_market_time.date()
    )

    if schedule.empty:
        return False

    # Get market open and close times for today
    market_open = schedule.iloc[0]["market_open"].tz_convert(market_tz)
    market_close = schedule.iloc[0]["market_close"].tz_convert(market_tz)

    # Check if current time is between market open and close
    return market_open <= now_market_time <= market_close
