"""Configuration management for tickerlake."""

import datetime
import os
from dataclasses import dataclass, field

import psycopg
from psycopg.conninfo import conninfo_to_dict


def _default_start_date() -> datetime.date:
    """Return today minus 10 years, falling back to Feb 28 on leap-day edge."""
    today = datetime.datetime.now(tz=datetime.UTC).date()
    target_year = today.year - 10
    try:
        return today.replace(year=target_year)
    except ValueError:
        # Today is Feb 29 and target_year is not a leap year.
        return datetime.date(target_year, 2, 28)


@dataclass
class Config:
    """Configuration for tickerlake ETL pipeline.

    Attributes:
        api_key: MASSIVE API key (loaded from MASSIVE_API_KEY env var when set;
            may be empty for read-only commands. Massive-backed commands enforce
            the requirement at their own boundary.)
        start_date: Start date for data collection (defaults to 10 years ago)
        end_date: End date for data collection (defaults to today)
        ticker_types: List of ticker types to process (defaults to
            ["CS", "ETF", "ETV", "ETN", "ADRC"])
    """

    api_key: str = field(default="", repr=False)
    start_date: datetime.date = field(default_factory=_default_start_date)
    end_date: datetime.date = field(default_factory=lambda: datetime.datetime.now(tz=datetime.UTC).date())
    ticker_types: list[str] = field(default_factory=lambda: ["CS", "ETF", "ETV", "ETN", "ADRC"])
    database_url: str | None = field(default=None, repr=False)

    def __post_init__(self) -> None:
        """Validate and normalize configuration after initialization."""
        if not self.api_key:
            self.api_key = os.environ.get("MASSIVE_API_KEY", "")
        if self.database_url is None:
            self.database_url = os.environ.get("DATABASE_URL")
        if self.database_url is not None:
            if not self.database_url.strip():
                message = "blank DATABASE_URL"
                raise ValueError(message)
            try:
                conninfo_to_dict(self.database_url)
            except psycopg.Error:
                message = "invalid DATABASE_URL"
                raise ValueError(message) from None
