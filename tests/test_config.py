"""Tests for tickerlake configuration module."""

import datetime
import os
import traceback
import types
from unittest.mock import patch

import psycopg
import pytest

from tickerlake.config import DATABASE_URL_DEFAULT, Config


def _config_datetime_on(day: datetime.date) -> types.SimpleNamespace:
    """Build a stand-in for the config module's ``datetime`` with a pinned today."""

    class _FakeDateTime(datetime.datetime):
        """A ``datetime`` whose ``now`` always returns the pinned day."""

        @classmethod
        def now(cls, tz: datetime.tzinfo | None = None) -> datetime.datetime:
            """Return the pinned day using the requested timezone."""
            return cls(day.year, day.month, day.day, tzinfo=tz)

    return types.SimpleNamespace(datetime=_FakeDateTime, UTC=datetime.UTC, date=datetime.date)


class TestApiKey:
    """Test API key configuration."""

    def test_api_key_from_env(self) -> None:
        """Config reads MASSIVE_API_KEY from environment."""
        with patch.dict(os.environ, {"MASSIVE_API_KEY": "test-key-123"}):
            config = Config()
            assert config.api_key == "test-key-123"

    def test_missing_api_key_allowed(self) -> None:
        """Config() without env var is valid for read-only commands."""
        with patch.dict(os.environ, {}, clear=True):
            config = Config()
        assert config.api_key == ""


class TestDates:
    """Test date configuration."""

    @pytest.mark.parametrize(
        ("instant", "local_timezone", "expected_start", "expected_end"),
        [
            (
                datetime.datetime(2026, 1, 1, 0, 30, tzinfo=datetime.UTC),
                datetime.timezone(datetime.timedelta(hours=-5)),
                datetime.date(2016, 1, 1),
                datetime.date(2026, 1, 1),
            ),
            (
                datetime.datetime(2026, 12, 31, 23, 30, tzinfo=datetime.UTC),
                datetime.timezone(datetime.timedelta(hours=5)),
                datetime.date(2016, 12, 31),
                datetime.date(2026, 12, 31),
            ),
        ],
    )
    def test_default_dates_use_utc_instant(
        self,
        instant: datetime.datetime,
        local_timezone: datetime.tzinfo,
        expected_start: datetime.date,
        expected_end: datetime.date,
    ) -> None:
        """Default dates use UTC's calendar date, not the local date."""

        class _FakeDateTime(datetime.datetime):
            @classmethod
            def now(cls, tz: datetime.tzinfo | None = None) -> datetime.datetime:
                return instant.astimezone(tz) if tz is not None else instant.astimezone(local_timezone)

        fake_datetime = types.SimpleNamespace(
            datetime=_FakeDateTime,
            UTC=datetime.UTC,
            date=datetime.date,
        )
        with patch("tickerlake.config.datetime", fake_datetime):
            config = Config()

        assert config.start_date == expected_start
        assert config.end_date == expected_end

    def test_start_date_default(self) -> None:
        """start_date defaults to the same month and day 10 years before today.

        The Feb 28 fallback only matters on Feb 29, which is covered separately
        in ``test_start_date_default_on_leap_day``.
        """
        with (
            patch.dict(os.environ, {"MASSIVE_API_KEY": "test"}),
            patch("tickerlake.config.datetime", _config_datetime_on(datetime.date(2026, 10, 3))),
        ):
            config = Config()
        assert config.start_date == datetime.date(2016, 10, 3)
        assert isinstance(config.start_date, datetime.date)

    def test_start_date_default_on_leap_day(self) -> None:
        """Feb 29 falls back to Feb 28 when the target year is not a leap year."""
        with (
            patch.dict(os.environ, {"MASSIVE_API_KEY": "test"}),
            patch("tickerlake.config.datetime", _config_datetime_on(datetime.date(2024, 2, 29))),
        ):
            config = Config()
        assert config.start_date == datetime.date(2014, 2, 28)
        assert isinstance(config.start_date, datetime.date)

    def test_end_date_default(self) -> None:
        """end_date defaults to today."""
        with patch.dict(os.environ, {"MASSIVE_API_KEY": "test"}):
            config = Config()
            today = datetime.datetime.now(tz=datetime.UTC).date()
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


class TestDatabaseUrl:
    """Test optional PostgreSQL connection configuration."""

    def test_database_url_is_optional_and_loaded_from_environment(self) -> None:
        """The setting falls back to the default or loads DATABASE_URL when set."""
        with patch.dict(os.environ, {}, clear=True):
            assert Config().database_url == DATABASE_URL_DEFAULT
        with patch.dict(os.environ, {"DATABASE_URL": "postgresql://localhost/market"}):
            assert Config().database_url == "postgresql://localhost/market"

    def test_database_url_default_falls_back_when_env_unset(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Config falls back to the default URL when DATABASE_URL is unset."""
        monkeypatch.delenv("DATABASE_URL", raising=False)
        assert Config().database_url == DATABASE_URL_DEFAULT

    def test_database_url_env_overrides_default(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """A configured DATABASE_URL wins over the default."""
        monkeypatch.setenv("DATABASE_URL", "postgresql://example/db")
        assert Config().database_url == "postgresql://example/db"

    def test_explicit_database_url_overrides_environment(self) -> None:
        """An explicit constructor value takes precedence over the environment."""
        with patch.dict(os.environ, {"DATABASE_URL": "postgresql://env/market"}):
            config = Config(database_url="host=localhost dbname=market")
        assert config.database_url == "host=localhost dbname=market"

    @pytest.mark.parametrize("database_url", ["", " \t\n", "not a valid conninfo key=value extra"])
    def test_invalid_database_url_has_safe_error(self, database_url: str) -> None:
        """Invalid input fails without exposing connection credentials."""
        credential = "private-value"
        value = f"{database_url} password={credential}" if database_url.startswith("not") else database_url
        with pytest.raises(ValueError, match="DATABASE_URL") as error:
            Config(database_url=value)
        rendered = "".join(traceback.format_exception(error.type, error.value, error.tb))
        assert credential not in rendered
        if value:
            assert value not in repr(error.value)

    @pytest.mark.parametrize(
        "database_url",
        ["postgresql://user:password@localhost:5432/market", "host=localhost dbname=market user=reader"],
    )
    def test_valid_database_url_does_not_connect(self, database_url: str, monkeypatch: pytest.MonkeyPatch) -> None:
        """Valid connection strings are parsed without opening a connection."""

        def fail_connect(*args: object, **kwargs: object) -> None:
            pytest.fail("Config must not connect to PostgreSQL")

        monkeypatch.setattr(psycopg, "connect", fail_connect)
        assert Config(database_url=database_url).database_url == database_url

    def test_secrets_are_not_shown_in_config_repr(self) -> None:
        """API and database credentials are hidden from Config's string form."""
        credential = "private-value"
        with patch.dict(
            os.environ, {"MASSIVE_API_KEY": credential, "DATABASE_URL": f"postgresql://u:{credential}@localhost/db"}
        ):
            config = Config()
        assert credential not in repr(config)
        assert credential not in str(config)


class TestTickerTypes:
    """Test ticker types configuration."""

    def test_ticker_types_default(self) -> None:
        """ticker_types defaults to ["CS", "ETF", "ETV", "ETN", "ADRC"]."""
        with patch.dict(os.environ, {"MASSIVE_API_KEY": "test"}):
            config = Config()
            assert config.ticker_types == ["CS", "ETF", "ETV", "ETN", "ADRC"]

    def test_ticker_types_custom(self) -> None:
        """ticker_types can be overridden with canonical types."""
        custom_types = ["CS", "ETF"]
        with patch.dict(os.environ, {"MASSIVE_API_KEY": "test"}):
            config = Config(ticker_types=custom_types)
            assert config.ticker_types == custom_types

    def test_ticker_types_rejects_unknown(self) -> None:
        """ticker_types outside the canonical allowlist are rejected."""
        with (
            patch.dict(os.environ, {"MASSIVE_API_KEY": "test"}),
            pytest.raises(ValueError, match="unsupported ticker type"),
        ):
            Config(ticker_types=["CS", "FUND"])
