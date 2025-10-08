"""Utility functions for trading calendar and market status operations."""

from datetime import datetime, timedelta

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
    # nyse.tz is a ZoneInfo object, get its key for pytz
    market_tz = pytz.timezone(str(nyse.tz))
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


def is_data_available_for_today():
    """Check if today's market data should be available from the API.

    Returns:
        True if today's data should be available, False otherwise.

    Note:
        Data is considered available if the market is closed and at least
        30 minutes have passed since market close to allow for data processing.

    """
    nyse = mcal.get_calendar("NYSE")
    market_tz = pytz.timezone(str(nyse.tz))
    now_market_time = datetime.now(market_tz)

    # Get today's trading schedule
    schedule = nyse.schedule(
        start_date=now_market_time.date(), end_date=now_market_time.date()
    )

    # Not a trading day
    if schedule.empty:
        return False

    # Market is still open
    if is_market_open():
        return False

    # Check if enough time has passed since market close
    market_close = schedule.iloc[0]["market_close"].tz_convert(market_tz)
    time_since_close = now_market_time - market_close

    # Wait at least 30 minutes after close for data to be processed
    return time_since_close >= timedelta(minutes=30)
