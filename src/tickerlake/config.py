"""Configuration management for tickerlake."""

import datetime
import os
from dataclasses import dataclass, field
from pathlib import Path


@dataclass
class Config:
    """Configuration for tickerlake ETL pipeline.

    Attributes:
        api_key: MASSIVE API key (loaded from MASSIVE_API_KEY env var when set;
            may be empty for read-only commands. Massive-backed commands enforce
            the requirement at their own boundary.)
        output_dir: Directory for output files (defaults to current working directory)
        start_date: Start date for data collection (defaults to 10 years ago)
        end_date: End date for data collection (defaults to today)
        ticker_types: List of ticker types to process (defaults to
            ["CS", "ETF", "ETV", "ETN", "ADRC"])
    """

    api_key: str = field(default="")
    output_dir: Path = field(default_factory=Path.cwd)
    start_date: datetime.date = field(
        default_factory=lambda: (
            datetime.datetime.now(tz=datetime.UTC)
            .date()
            .replace(year=datetime.datetime.now(tz=datetime.UTC).date().year - 10)
        )
    )
    end_date: datetime.date = field(
        default_factory=lambda: datetime.datetime.now(tz=datetime.UTC).date()
    )
    ticker_types: list[str] = field(
        default_factory=lambda: ["CS", "ETF", "ETV", "ETN", "ADRC"]
    )

    def __post_init__(self) -> None:
        """Validate and normalize configuration after initialization."""
        if not self.api_key:
            self.api_key = os.environ.get("MASSIVE_API_KEY", "")
        self.output_dir = Path(self.output_dir).resolve()
