"""Configuration management for tickerlake."""

import datetime
import os
from dataclasses import dataclass, field
from pathlib import Path


@dataclass
class Config:
    """Configuration for tickerlake ETL pipeline.

    Attributes:
        api_key: MASSIVE API key (required, from MASSIVE_API_KEY env var)
        output_dir: Directory for output files (defaults to current working directory)
        start_date: Start date for data collection (defaults to 1 year ago)
        end_date: End date for data collection (defaults to today)
        ticker_types: List of ticker types to process (defaults to ["CS", "ETF", "ETV"])
    """

    api_key: str = field(default="")
    output_dir: Path = field(default_factory=Path.cwd)
    start_date: datetime.date = field(
        default_factory=lambda: datetime.date.today().replace(
            year=datetime.date.today().year - 5
        )
    )
    end_date: datetime.date = field(default_factory=datetime.date.today)
    ticker_types: list[str] = field(default_factory=lambda: ["CS", "ETF", "ETV"])

    def __post_init__(self) -> None:
        """Validate and normalize configuration after initialization."""
        if not self.api_key:
            self.api_key = os.environ.get("MASSIVE_API_KEY", "")
        if not self.api_key:
            raise ValueError("MASSIVE_API_KEY environment variable is required")
        self.output_dir = Path(self.output_dir).resolve()
