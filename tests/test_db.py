"""Tests for PostgreSQL connection helpers."""

from unittest.mock import patch

import pytest

from tickerlake import db


class TestGetDsn:
    """Tests for get_dsn()."""

    def test_reads_uri_from_environment(self, monkeypatch) -> None:
        """The URI is loaded once from the environment."""
        monkeypatch.setenv("TICKERLAKE_DB_URI", "postgresql://db")
        monkeypatch.setattr(db, "_DSN", None)

        assert db.get_dsn() == "postgresql://db"
        monkeypatch.setenv("TICKERLAKE_DB_URI", "postgresql://other")
        assert db.get_dsn() == "postgresql://db"

    def test_missing_uri_raises_clear_error(self, monkeypatch) -> None:
        """A missing URI does not produce an unusable connection string."""
        monkeypatch.delenv("TICKERLAKE_DB_URI", raising=False)
        monkeypatch.setattr(db, "_DSN", None)

        with pytest.raises(ValueError, match="TICKERLAKE_DB_URI"):
            db.get_dsn()


def test_connect_uses_configured_dsn(monkeypatch) -> None:
    """connect() delegates the configured URI to the ADBC driver."""
    monkeypatch.setattr(db, "_DSN", "postgresql://db")
    with patch.object(db.pg, "connect") as connect:
        db.connect()

    connect.assert_called_once_with("postgresql://db")
