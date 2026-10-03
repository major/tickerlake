"""Regression tests for writer cleanup after DuckDB rejects a write."""

import subprocess
import sys
from typing import TYPE_CHECKING

import duckdb
import pytest

from tickerlake import load

if TYPE_CHECKING:
    from pathlib import Path


def _assert_database_released(database: Path) -> None:
    script = (
        "import duckdb, sys; "
        "c=duckdb.connect(sys.argv[1]); "
        "c.execute('CREATE TABLE recovery_marker (value INTEGER)'); "
        "c.execute('INSERT INTO recovery_marker VALUES (1)'); c.close()"
    )
    subprocess.run([sys.executable, "-c", script, str(database)], check=True, timeout=15)  # noqa: S603

    con = duckdb.connect(str(database), read_only=True)
    try:
        assert con.execute("SELECT value FROM recovery_marker").fetchone() == (1,)
    finally:
        con.close()


def test_failed_append_releases_database_and_removes_parquet(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    sample_bars_df,
) -> None:
    """Failed appends release the database and remove temporary parquet files."""
    database = tmp_path / "raw.duckdb"
    con = duckdb.connect(str(database))
    try:
        con.execute("CREATE TABLE raw_daily_bars (wrong_column INTEGER)")
    finally:
        con.close()

    scratch = tmp_path / "scratch"
    scratch.mkdir()
    monkeypatch.setattr(load.tempfile, "tempdir", str(scratch))

    with pytest.raises(duckdb.Error) as captured:
        load.append_raw_db(sample_bars_df, database)

    assert captured.traceback is not None
    assert list(scratch.iterdir()) == []
    _assert_database_released(database)


@pytest.mark.parametrize(
    ("writer", "frame_fixture"),
    [
        (load.write_raw_db, "sample_bars_df"),
        (load.write_splits, "sample_splits_df"),
    ],
)
def test_failed_replacement_write_releases_database_and_removes_parquet(
    writer: object,
    frame_fixture: str,
    request: pytest.FixtureRequest,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Failed replacement writes release the database and remove parquet files."""
    database = tmp_path / ("splits.duckdb" if writer is load.write_splits else "raw.duckdb")
    con = duckdb.connect(str(database))
    con.close()

    scratch = tmp_path / "scratch"
    scratch.mkdir()
    monkeypatch.setattr(load.tempfile, "tempdir", str(scratch))
    monkeypatch.setattr(load, "_read_parquet_sql", lambda _order_by="": "SELECT * FROM missing_table")

    with pytest.raises(duckdb.Error) as captured:
        writer(request.getfixturevalue(frame_fixture), database)

    assert captured.traceback is not None
    assert list(scratch.iterdir()) == []
    _assert_database_released(database)
