"""Pure validation helper unit tests (no database required)."""

import datetime as dt

import pytest

from tickerlake.postgres._validation import is_aware_datetime, is_date, require_unique_nonempty_strings
from tickerlake.postgres.connection import PostgresWriterError


class _NullOffsetTzinfo(dt.tzinfo):
    """A tzinfo whose offset cannot be resolved because utcoffset returns None."""

    def utcoffset(self, when: dt.datetime | None) -> dt.timedelta | None:
        """Report no UTC offset so awareness checks must fail."""
        return None

    def dst(self, when: dt.datetime | None) -> dt.timedelta | None:
        """Report no daylight-saving adjustment."""
        return None

    def tzname(self, when: dt.datetime | None) -> str | None:
        """Report no timezone name."""
        return None


# --- is_date ----------------------------------------------------------------


def test_is_date_accepts_plain_date() -> None:
    """A plain date is a date."""
    assert is_date(dt.date(2025, 1, 2))


def test_is_date_rejects_datetime() -> None:
    """A datetime is not a plain date even though it subclasses date."""
    assert not is_date(dt.datetime(2025, 1, 2, tzinfo=dt.UTC))


def test_is_date_rejects_none_and_non_dates() -> None:
    """None and non-date objects are rejected."""
    assert not is_date(None)
    assert not is_date("2025-01-02")
    assert not is_date(20250102)


# --- is_aware_datetime ------------------------------------------------------


def test_is_aware_datetime_accepts_aware_utc() -> None:
    """A UTC-aware datetime is aware."""
    assert is_aware_datetime(dt.datetime(2025, 1, 2, tzinfo=dt.UTC))


def test_is_aware_datetime_accepts_aware_offset() -> None:
    """A fixed-offset aware datetime is aware."""
    assert is_aware_datetime(dt.datetime(2025, 1, 2, tzinfo=dt.timezone(dt.timedelta(hours=-5))))


def test_is_aware_datetime_rejects_naive() -> None:
    """A datetime without tzinfo is naive."""
    assert not is_aware_datetime(dt.datetime(2025, 1, 2))  # noqa: DTZ001


def test_is_aware_datetime_rejects_date_and_string() -> None:
    """Dates, strings, and None are not datetimes."""
    assert not is_aware_datetime(dt.date(2025, 1, 2))
    assert not is_aware_datetime("2025-01-02T00:00:00Z")
    assert not is_aware_datetime(None)


def test_is_aware_datetime_rejects_tzinfo_without_utcoffset() -> None:
    """A tzinfo that cannot resolve an offset does not make a datetime aware."""
    bad = dt.datetime(2025, 1, 2, tzinfo=_NullOffsetTzinfo())
    assert not is_aware_datetime(bad)


# --- require_unique_nonempty_strings ----------------------------------------


def test_require_unique_nonempty_strings_accepts_unique_nonempty() -> None:
    """Unique non-empty strings are returned unchanged as a list."""
    assert require_unique_nonempty_strings(["a", "b"], field="letters") == ["a", "b"]


def test_require_unique_nonempty_strings_rejects_duplicates() -> None:
    """Repeated values are rejected."""
    with pytest.raises(PostgresWriterError, match="letters must be unique nonempty strings"):
        require_unique_nonempty_strings(["a", "a"], field="letters")


def test_require_unique_nonempty_strings_rejects_empty_string() -> None:
    """Blank strings are rejected."""
    with pytest.raises(PostgresWriterError, match="letters must be unique nonempty strings"):
        require_unique_nonempty_strings(["a", ""], field="letters")


def test_require_unique_nonempty_strings_rejects_string_input() -> None:
    """A bare string is not treated as a sequence of characters."""
    with pytest.raises(PostgresWriterError, match="letters must be unique nonempty strings"):
        require_unique_nonempty_strings("abc", field="letters")


def test_require_unique_nonempty_strings_rejects_empty_sequence() -> None:
    """An empty sequence is rejected."""
    with pytest.raises(PostgresWriterError, match="letters must be unique nonempty strings"):
        require_unique_nonempty_strings([], field="letters")


def test_require_unique_nonempty_strings_respects_custom_message() -> None:
    """A custom message replaces the default safe message."""
    with pytest.raises(PostgresWriterError, match="custom message"):
        require_unique_nonempty_strings([], field="x", message="custom message")
