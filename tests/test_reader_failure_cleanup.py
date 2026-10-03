"""Reader failures must release DuckDB files and clean temporary exports."""

import subprocess
import sys
import tempfile
from typing import TYPE_CHECKING

import duckdb
import pytest

from tickerlake import load

if TYPE_CHECKING:
    from pathlib import Path


def test_raw_reader_failure_releases_database_and_temp_file(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify raw reader failures close the database and remove temp exports."""
    db_path = tmp_path / "raw.duckdb"
    temp_dir = tmp_path / "temp"
    temp_dir.mkdir()
    con = duckdb.connect(str(db_path))
    try:
        con.execute("CREATE TABLE raw_daily_bars AS SELECT DATE '2024-01-01' AS date")
    finally:
        con.close()
    monkeypatch.setattr(tempfile, "tempdir", str(temp_dir))

    with pytest.raises(duckdb.Error) as captured:
        load.read_raw_db(db_path)
    assert list(temp_dir.iterdir()) == []
    _assert_database_writable(db_path)
    assert captured.value is not None


def test_splits_reader_failure_releases_database_and_temp_file(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify splits reader failures close the database and remove temp exports."""
    db_path = tmp_path / "splits.duckdb"
    temp_dir = tmp_path / "temp"
    temp_dir.mkdir()
    con = duckdb.connect(str(db_path))
    try:
        con.execute("CREATE TABLE splits AS SELECT DATE '2024-01-01' AS execution_date")
    finally:
        con.close()
    monkeypatch.setattr(tempfile, "tempdir", str(temp_dir))

    with pytest.raises(duckdb.Error) as captured:
        load.read_splits(db_path)
    assert list(temp_dir.iterdir()) == []
    _assert_database_writable(db_path)
    assert captured.value is not None


def test_db_info_query_failure_releases_database(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify metadata query failures close the database connection."""
    db_path = tmp_path / "info.duckdb"
    with duckdb.connect(str(db_path)) as con:
        con.execute("CREATE TABLE sample (date DATE)")

    def fail_date_range(*_args: object, **_kwargs: object) -> None:
        raise duckdb.IOException

    monkeypatch.setattr(load, "_table_date_range", fail_date_range)
    with pytest.raises(duckdb.Error) as captured:
        load.get_db_info(db_path)
    _assert_database_writable(db_path)
    assert captured.value is not None


def _assert_database_writable(db_path: Path) -> None:
    script = (
        "import duckdb, sys; "
        "con = duckdb.connect(sys.argv[1]); "
        "con.execute(\"CREATE TABLE recovery_marker AS SELECT 'ok' AS value\"); "
        "con.close()"
    )
    # Run trusted interpreter argv against the isolated temp database to prove the lock is released.
    result = subprocess.run(  # noqa: S603
        [sys.executable, "-c", script, str(db_path)],
        capture_output=True,
        text=True,
        timeout=15,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        assert con.execute("SELECT value FROM recovery_marker").fetchone() == ("ok",)
    finally:
        con.close()
