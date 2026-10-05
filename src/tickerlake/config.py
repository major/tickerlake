"""Configuration management for tickerlake."""

import datetime
import os
from dataclasses import dataclass, field

import psycopg
from psycopg.conninfo import conninfo_to_dict

DATABASE_URL_DEFAULT = "postgresql://localhost/tickerlake"

# Canonical ticker types accepted by the market.ticker CHECK constraint.
# Keep in sync with the ARRAY literal in migrations/0001_foundation.sql.
_ALLOWED_TICKER_TYPES: tuple[str, ...] = ("CS", "ETF", "ETV", "ETN", "ADRC")


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
        database_url: PostgreSQL connection URL. Falls back to the
            DATABASE_URL environment variable, then to DATABASE_URL_DEFAULT
            (``postgresql://localhost/tickerlake``) for local development.
    """

    api_key: str = field(default="", repr=False)
    start_date: datetime.date = field(default_factory=_default_start_date)
    end_date: datetime.date = field(default_factory=lambda: datetime.datetime.now(tz=datetime.UTC).date())
    ticker_types: list[str] = field(default_factory=lambda: list(_ALLOWED_TICKER_TYPES))
    database_url: str | None = field(default=None, repr=False)

    def __post_init__(self) -> None:
        """Validate and normalize configuration after initialization."""
        if not all(ticker_type in _ALLOWED_TICKER_TYPES for ticker_type in self.ticker_types):
            message = "unsupported ticker type"
            raise ValueError(message)
        if not self.api_key:
            self.api_key = os.environ.get("MASSIVE_API_KEY", "")
        if self.database_url is None:
            self.database_url = os.environ.get("DATABASE_URL") or DATABASE_URL_DEFAULT
        if self.database_url is not None:
            if not self.database_url.strip():
                message = "blank DATABASE_URL"
                raise ValueError(message)
            try:
                conninfo_to_dict(self.database_url)
            except psycopg.Error:
                message = "invalid DATABASE_URL"
                raise ValueError(message) from None
