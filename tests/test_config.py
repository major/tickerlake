"""Tests for tickerlake configuration module."""

import datetime
import os
from pathlib import Path
from unittest.mock import patch

import pytest

from tickerlake.config import Config


class TestApiKey:
    """Test API key configuration."""

    def test_api_key_from_env(self) -> None:
        """Config reads MASSIVE_API_KEY from environment."""
        with patch.dict(os.environ, {"MASSIVE_API_KEY": "test-key-123"}):
            config = Config()
            assert config.api_key == "test-key-123"

    def test_missing_api_key_raises(self) -> None:
        """Config() without env var raises ValueError with clear message."""
        with patch.dict(os.environ, {}, clear=True):
            with pytest.raises(
                ValueError, match="MASSIVE_API_KEY environment variable is required"
            ):
                Config()


class TestDates:
    """Test date configuration."""

    def test_start_date_default(self) -> None:
        """start_date defaults to 1 year ago from today."""
        with patch.dict(os.environ, {"MASSIVE_API_KEY": "test"}):
            config = Config()
            today = datetime.date.today()
            expected = today.replace(year=today.year - 1)
            assert config.start_date == expected
            assert isinstance(config.start_date, datetime.date)

    def test_end_date_default(self) -> None:
        """end_date defaults to today."""
        with patch.dict(os.environ, {"MASSIVE_API_KEY": "test"}):
            config = Config()
            today = datetime.date.today()
            assert config.end_date == today
            assert isinstance(config.end_date, datetime.date)

    def test_custom_dates(self) -> None:
        """start_date and end_date can be overridden via constructor."""
        custom_start = datetime.date(2020, 1, 1)
        custom_end = datetime.date(2023, 12, 31)
        with patch.dict(os.environ, {"MASSIVE_API_KEY": "test"}):
            config = Config(start_date=custom_start, end_date=custom_end)
            assert config.start_date == custom_start
            assert config.end_date == custom_end


class TestOutputDir:
    """Test output directory configuration."""

    def test_output_dir_default(self) -> None:
        """output_dir defaults to current working directory."""
        with patch.dict(os.environ, {"MASSIVE_API_KEY": "test"}):
            config = Config()
            assert config.output_dir == Path.cwd().resolve()
            assert isinstance(config.output_dir, Path)

    def test_output_dir_absolute(self) -> None:
        """output_dir is always absolute (resolve relative paths)."""
        with patch.dict(os.environ, {"MASSIVE_API_KEY": "test"}):
            config = Config(output_dir=Path("./relative/path"))
            assert config.output_dir.is_absolute()
            assert config.output_dir == Path("./relative/path").resolve()


class TestTickerTypes:
    """Test ticker types configuration."""

    def test_ticker_types_default(self) -> None:
        """ticker_types defaults to ["CS", "ETF", "ETV"]."""
        with patch.dict(os.environ, {"MASSIVE_API_KEY": "test"}):
            config = Config()
            assert config.ticker_types == ["CS", "ETF", "ETV"]

    def test_ticker_types_custom(self) -> None:
        """ticker_types can be overridden."""
        custom_types = ["CS", "ETF", "FUND"]
        with patch.dict(os.environ, {"MASSIVE_API_KEY": "test"}):
            config = Config(ticker_types=custom_types)
            assert config.ticker_types == custom_types
