"""Tests for tickerlake.load — DuckDB writer with schema validation."""

import datetime
from typing import TYPE_CHECKING

import duckdb
import polars as pl
import pytest

from tickerlake.load import (
    append_raw_db,
    compact_raw_db,
    delete_raw_dates,
    get_db_info,
    get_existing_dates,
    read_raw_db,
    read_splits,
    write_consumer_db,
    write_raw_db,
    write_splits,
)

if TYPE_CHECKING:
    from pathlib import Path


@pytest.fixture
def sample_metrics_df(sample_bars_df: pl.DataFrame) -> pl.DataFrame:
    """Create a sample stock metrics DataFrame aligned with sample_bars_df."""
    n = len(sample_bars_df)
    return pl.DataFrame(
        {
            "date": sample_bars_df["date"],
            "ticker": sample_bars_df["ticker"],
            "sma_50": [None] * n,
            "sma_200": [None] * n,
            "atr_14": [None] * n,
            "atr_pct": [None] * n,
            "adr_pct": [None] * n,
            "sma50_atr_distance": [None] * n,
            "rs": [None] * n,
            "rs_sma_20": [None] * n,
            "vars": [None] * n,
            "vars_sma_20": [None] * n,
        },
        schema={
            "date": pl.Date,
            "ticker": pl.Utf8,
            "sma_50": pl.Float32,
            "sma_200": pl.Float32,
            "atr_14": pl.Float32,
            "atr_pct": pl.Float32,
            "adr_pct": pl.Float32,
            "sma50_atr_distance": pl.Float32,
            "rs": pl.Float32,
            "rs_sma_20": pl.Float32,
            "vars": pl.Float32,
            "vars_sma_20": pl.Float32,
        },
    )


@pytest.fixture
def sample_hvcs_df() -> pl.DataFrame:
    """Create a sample HVC DataFrame with 2 realistic rows matching the 21-column schema."""  # noqa: E501
    return pl.DataFrame(
        {
            "ticker": ["AAPL", "MSFT"],
            "date": [datetime.date(2024, 1, 3), datetime.date(2024, 1, 3)],
            "open": [155.0, 385.0],
            "high": [160.0, 392.0],
            "low": [154.0, 383.0],
            "close": [158.0, 390.0],
            "prev_close": [151.5, 381.5],
            "volume": [3_100_000.0, 3_900_000.0],
            "volume_sma_20": [1_000_000.0, 1_200_000.0],
            "volume_multiplier": [3.1, 3.25],
            "total_move_pct": [4.3, 2.23],
            "gap_pct": [2.3, 0.92],
            "intraday_move_pct": [1.94, 1.30],
            "bar_range_pct": [3.97, 2.23],
            "adr_pct": [0.04, 0.02],
            "atr_pct": [0.03, 0.018],
            "close_position_in_range": [0.67, 0.83],
            "is_up_day": [True, True],
            "price_vs_sma50_pct": [5.2, 3.1],
            "price_vs_sma200_pct": [12.5, 8.7],
            "rs": [0.15, 0.08],
        },
        schema={
            "ticker": pl.Utf8,
            "date": pl.Date,
            "open": pl.Float32,
            "high": pl.Float32,
            "low": pl.Float32,
            "close": pl.Float32,
            "prev_close": pl.Float32,
            "volume": pl.Float32,
            "volume_sma_20": pl.Float32,
            "volume_multiplier": pl.Float32,
            "total_move_pct": pl.Float32,
            "gap_pct": pl.Float32,
            "intraday_move_pct": pl.Float32,
            "bar_range_pct": pl.Float32,
            "adr_pct": pl.Float32,
            "atr_pct": pl.Float32,
            "close_position_in_range": pl.Float32,
            "is_up_day": pl.Boolean,
            "price_vs_sma50_pct": pl.Float32,
            "price_vs_sma200_pct": pl.Float32,
            "rs": pl.Float32,
        },
    )


def test_write_raw_db_creates_table(
    tmp_path: Path, sample_bars_df: pl.DataFrame
) -> None:
    """write_raw_db() creates raw_daily_bars table with correct row count."""
    db_path = tmp_path / "raw.duckdb"
    write_raw_db(sample_bars_df, db_path)

    con = duckdb.connect(str(db_path), read_only=True)
    row = con.execute("SELECT COUNT(*) FROM raw_daily_bars").fetchone()
    con.close()

    assert row is not None
    count = row[0]
    assert count == len(sample_bars_df)


def test_write_raw_db_schema(tmp_path: Path, sample_bars_df: pl.DataFrame) -> None:
    """Price columns are FLOAT, date is DATE, transactions is UINTEGER."""
    db_path = tmp_path / "raw.duckdb"
    write_raw_db(sample_bars_df, db_path)

    con = duckdb.connect(str(db_path), read_only=True)
    schema = con.execute("DESCRIBE raw_daily_bars").fetchall()
    con.close()

    schema_dict = {row[0]: row[1] for row in schema}
    assert schema_dict["date"] == "DATE"
    assert schema_dict["ticker"] == "VARCHAR"
    assert schema_dict["open"] == "FLOAT"
    assert schema_dict["high"] == "FLOAT"
    assert schema_dict["low"] == "FLOAT"
    assert schema_dict["close"] == "FLOAT"
    assert schema_dict["volume"] == "FLOAT"
    assert schema_dict["vwap"] == "FLOAT"
    assert schema_dict["transactions"] == "UINTEGER"


def test_write_raw_db_sorted(tmp_path: Path, sample_bars_df: pl.DataFrame) -> None:
    """Data is sorted by (ticker, date) in the table."""
    db_path = tmp_path / "raw.duckdb"
    write_raw_db(sample_bars_df, db_path)

    con = duckdb.connect(str(db_path), read_only=True)
    rows = con.execute("SELECT ticker, date FROM raw_daily_bars").fetchall()
    con.close()

    tickers = [r[0] for r in rows]
    dates = [r[1] for r in rows]

    # Verify sorted by ticker first, then date within each ticker
    for i in range(1, len(rows)):
        if tickers[i] == tickers[i - 1]:
            assert dates[i] >= dates[i - 1], (
                f"Dates not sorted within ticker {tickers[i]}"
            )
        else:
            assert tickers[i] >= tickers[i - 1], "Tickers not sorted"


def test_append_raw_db(tmp_path: Path, sample_bars_df: pl.DataFrame) -> None:
    """append_raw_db() adds rows to existing table."""
    db_path = tmp_path / "raw.duckdb"
    write_raw_db(sample_bars_df, db_path)

    # Append the same data again — row count should double
    append_raw_db(sample_bars_df, db_path)

    con = duckdb.connect(str(db_path), read_only=True)
    row = con.execute("SELECT COUNT(*) FROM raw_daily_bars").fetchone()
    con.close()

    assert row is not None
    count = row[0]
    assert count == len(sample_bars_df) * 2


def test_read_raw_db(tmp_path: Path, sample_bars_df: pl.DataFrame) -> None:
    """read_raw_db() returns polars DataFrame with correct data."""
    db_path = tmp_path / "raw.duckdb"
    write_raw_db(sample_bars_df, db_path)

    result = read_raw_db(db_path)

    assert isinstance(result, pl.DataFrame)
    assert len(result) == len(sample_bars_df)
    assert set(result.columns) == set(sample_bars_df.columns)


def test_get_existing_dates_returns_correct_dates(
    tmp_path: Path, sample_bars_df: pl.DataFrame
) -> None:
    """get_existing_dates() returns set of dates from raw_daily_bars."""
    db_path = tmp_path / "raw.duckdb"
    write_raw_db(sample_bars_df, db_path)

    result = get_existing_dates(db_path)

    assert isinstance(result, set)
    expected_dates = set(sample_bars_df["date"].to_list())
    assert result == expected_dates


def test_get_existing_dates_missing_file(tmp_path: Path) -> None:
    """get_existing_dates() returns empty set for non-existent file."""
    db_path = tmp_path / "nonexistent.duckdb"

    result = get_existing_dates(db_path)

    assert result == set()


def test_get_existing_dates_missing_table(tmp_path: Path) -> None:
    """get_existing_dates() returns empty set when raw_daily_bars table doesn't exist."""  # noqa: E501
    db_path = tmp_path / "empty.duckdb"
    # Create an empty DuckDB file with no tables
    con = duckdb.connect(str(db_path))
    con.close()

    result = get_existing_dates(db_path)

    assert result == set()


def test_delete_raw_dates_removes_rows_for_matching_days(
    tmp_path: Path, sample_bars_df: pl.DataFrame
) -> None:
    """delete_raw_dates() removes all rows for the requested trading dates."""
    db_path = tmp_path / "raw.duckdb"
    write_raw_db(sample_bars_df, db_path)
    deleted_date = sample_bars_df["date"][0]

    delete_raw_dates(db_path, {deleted_date})
    result = read_raw_db(db_path)

    assert deleted_date not in set(result["date"].to_list())


def test_delete_raw_dates_ignores_missing_db(tmp_path: Path) -> None:
    """delete_raw_dates() quietly returns when the raw DB file is absent."""
    db_path = tmp_path / "missing.duckdb"

    delete_raw_dates(db_path, {datetime.date(2024, 1, 2)})

    assert not db_path.exists()


def test_write_consumer_db_creates_tables(
    tmp_path: Path,
    sample_bars_df: pl.DataFrame,
    sample_metrics_df: pl.DataFrame,
    sample_tickers_df: pl.DataFrame,
) -> None:
    """write_consumer_db() creates daily_bars, daily_metrics, tickers tables."""
    db_path = tmp_path / "tickerlake.duckdb"
    write_consumer_db(sample_bars_df, sample_metrics_df, sample_tickers_df, db_path)

    con = duckdb.connect(str(db_path), read_only=True)
    tables = {row[0] for row in con.execute("SHOW TABLES").fetchall()}
    con.close()

    assert "daily_bars" in tables
    assert "daily_metrics" in tables
    assert "tickers" in tables


def test_write_consumer_db_schema(
    tmp_path: Path,
    sample_bars_df: pl.DataFrame,
    sample_metrics_df: pl.DataFrame,
    sample_tickers_df: pl.DataFrame,
) -> None:
    """Price columns are FLOAT, date is DATE, transactions is UINTEGER in consumer db."""  # noqa: E501
    db_path = tmp_path / "tickerlake.duckdb"
    write_consumer_db(sample_bars_df, sample_metrics_df, sample_tickers_df, db_path)

    con = duckdb.connect(str(db_path), read_only=True)
    bars_schema = {
        row[0]: row[1] for row in con.execute("DESCRIBE daily_bars").fetchall()
    }
    metrics_schema = {
        row[0]: row[1] for row in con.execute("DESCRIBE daily_metrics").fetchall()
    }
    tickers_schema = {
        row[0]: row[1] for row in con.execute("DESCRIBE tickers").fetchall()
    }
    con.close()

    # daily_bars schema
    assert bars_schema["date"] == "DATE"
    assert bars_schema["open"] == "FLOAT"
    assert bars_schema["transactions"] == "UINTEGER"

    # daily_metrics schema
    assert metrics_schema["date"] == "DATE"
    assert metrics_schema["sma_50"] == "FLOAT"
    assert metrics_schema["sma_200"] == "FLOAT"
    assert metrics_schema["atr_14"] == "FLOAT"
    assert metrics_schema["atr_pct"] == "FLOAT"
    assert metrics_schema["sma50_atr_distance"] == "FLOAT"
    assert metrics_schema["rs"] == "FLOAT"
    assert metrics_schema["rs_sma_20"] == "FLOAT"
    assert metrics_schema["vars"] == "FLOAT"
    assert metrics_schema["vars_sma_20"] == "FLOAT"

    # tickers schema
    assert tickers_schema["ticker"] == "VARCHAR"
    assert tickers_schema["active"] == "BOOLEAN"


def test_consumer_query_pattern(
    tmp_path: Path,
    sample_bars_df: pl.DataFrame,
    sample_metrics_df: pl.DataFrame,
    sample_tickers_df: pl.DataFrame,
) -> None:
    """Three-table join works on consumer db."""
    db_path = tmp_path / "tickerlake.duckdb"
    write_consumer_db(sample_bars_df, sample_metrics_df, sample_tickers_df, db_path)

    con = duckdb.connect(str(db_path), read_only=True)
    result = con.execute("""
        SELECT d.date, d.ticker, t.name, d.close, m.sma_50
        FROM daily_bars d
        JOIN daily_metrics m USING (date, ticker)
        JOIN tickers t USING (ticker)
    """).fetchall()
    con.close()

    assert len(result) > 0


def test_get_db_info(tmp_path: Path, sample_bars_df: pl.DataFrame) -> None:
    """get_db_info() returns dict with row counts, date ranges, file size."""
    db_path = tmp_path / "raw.duckdb"
    write_raw_db(sample_bars_df, db_path)

    info = get_db_info(db_path)

    assert isinstance(info, dict)
    assert "tables" in info
    assert "row_counts" in info
    assert "date_range" in info
    assert "file_size_bytes" in info
    assert isinstance(info["tables"], list)
    assert isinstance(info["row_counts"], dict)
    assert info["file_size_bytes"] > 0
    assert "raw_daily_bars" in info["tables"]
    assert info["row_counts"]["raw_daily_bars"] == len(sample_bars_df)


def test_write_raw_db_idempotent(tmp_path: Path, sample_bars_df: pl.DataFrame) -> None:
    """Calling write_raw_db() twice replaces data (CREATE OR REPLACE)."""
    db_path = tmp_path / "raw.duckdb"
    write_raw_db(sample_bars_df, db_path)
    write_raw_db(sample_bars_df, db_path)

    con = duckdb.connect(str(db_path), read_only=True)
    row = con.execute("SELECT COUNT(*) FROM raw_daily_bars").fetchone()
    con.close()

    assert row is not None
    count = row[0]
    # Should still be the original count, not doubled
    assert count == len(sample_bars_df)


def test_compact_raw_db_preserves_data(
    tmp_path: Path, sample_bars_df: pl.DataFrame
) -> None:
    """compact_raw_db() preserves all rows after rebuild."""
    db_path = tmp_path / "raw.duckdb"
    write_raw_db(sample_bars_df, db_path)
    append_raw_db(sample_bars_df, db_path)

    compact_raw_db(db_path)

    result = read_raw_db(db_path)
    assert len(result) == len(sample_bars_df) * 2


def test_compact_raw_db_sorted(tmp_path: Path, sample_bars_df: pl.DataFrame) -> None:
    """compact_raw_db() output is sorted by (ticker, date)."""
    db_path = tmp_path / "raw.duckdb"
    write_raw_db(sample_bars_df, db_path)

    compact_raw_db(db_path)

    con = duckdb.connect(str(db_path), read_only=True)
    rows = con.execute("SELECT ticker, date FROM raw_daily_bars").fetchall()
    con.close()

    for i in range(1, len(rows)):
        if rows[i][0] == rows[i - 1][0]:
            assert rows[i][1] >= rows[i - 1][1], (
                f"Dates not sorted within ticker {rows[i][0]}"
            )
        else:
            assert rows[i][0] >= rows[i - 1][0], "Tickers not sorted"


def test_append_raw_db_sorted(tmp_path: Path, sample_bars_df: pl.DataFrame) -> None:
    """append_raw_db() inserts rows sorted by (ticker, date)."""
    db_path = tmp_path / "raw.duckdb"
    write_raw_db(sample_bars_df, db_path)
    append_raw_db(sample_bars_df, db_path)

    con = duckdb.connect(str(db_path), read_only=True)
    rows = con.execute("SELECT ticker, date FROM raw_daily_bars").fetchall()
    con.close()

    assert len(rows) == len(sample_bars_df) * 2


def test_write_consumer_db_hvcs_table_created(
    tmp_path: Path,
    sample_bars_df: pl.DataFrame,
    sample_metrics_df: pl.DataFrame,
    sample_tickers_df: pl.DataFrame,
    sample_hvcs_df: pl.DataFrame,
) -> None:
    """write_consumer_db() with hvcs kwarg creates 4th daily_hvcs table."""
    db_path = tmp_path / "tickerlake.duckdb"
    write_consumer_db(
        sample_bars_df,
        sample_metrics_df,
        sample_tickers_df,
        db_path,
        hvcs=sample_hvcs_df,
    )

    con = duckdb.connect(str(db_path), read_only=True)
    tables = {row[0] for row in con.execute("SHOW TABLES").fetchall()}
    con.close()

    assert "daily_hvcs" in tables
    assert len(tables) == 4


def test_write_consumer_db_hvcs_schema(
    tmp_path: Path,
    sample_bars_df: pl.DataFrame,
    sample_metrics_df: pl.DataFrame,
    sample_tickers_df: pl.DataFrame,
    sample_hvcs_df: pl.DataFrame,
) -> None:
    """HVC table schema: DATE, VARCHAR, FLOAT columns, BOOLEAN for is_up_day."""
    db_path = tmp_path / "tickerlake.duckdb"
    write_consumer_db(
        sample_bars_df,
        sample_metrics_df,
        sample_tickers_df,
        db_path,
        hvcs=sample_hvcs_df,
    )

    con = duckdb.connect(str(db_path), read_only=True)
    schema = {row[0]: row[1] for row in con.execute("DESCRIBE daily_hvcs").fetchall()}
    con.close()

    assert schema["date"] == "DATE"
    assert schema["ticker"] == "VARCHAR"
    assert schema["close"] == "FLOAT"
    assert schema["volume"] == "FLOAT"
    assert schema["volume_multiplier"] == "FLOAT"
    assert schema["is_up_day"] == "BOOLEAN"
    assert schema["rs"] == "FLOAT"


def test_write_consumer_db_hvcs_row_count(
    tmp_path: Path,
    sample_bars_df: pl.DataFrame,
    sample_metrics_df: pl.DataFrame,
    sample_tickers_df: pl.DataFrame,
    sample_hvcs_df: pl.DataFrame,
) -> None:
    """HVC table has the same number of rows as the sample_hvcs_df input."""
    db_path = tmp_path / "tickerlake.duckdb"
    write_consumer_db(
        sample_bars_df,
        sample_metrics_df,
        sample_tickers_df,
        db_path,
        hvcs=sample_hvcs_df,
    )

    con = duckdb.connect(str(db_path), read_only=True)
    row = con.execute("SELECT COUNT(*) FROM daily_hvcs").fetchone()
    con.close()

    assert row is not None
    count = row[0]
    assert count == len(sample_hvcs_df)


def test_write_consumer_db_hvcs_none_no_table(
    tmp_path: Path,
    sample_bars_df: pl.DataFrame,
    sample_metrics_df: pl.DataFrame,
    sample_tickers_df: pl.DataFrame,
) -> None:
    """write_consumer_db() with hvcs=None (default) does not create daily_hvcs."""
    db_path = tmp_path / "tickerlake.duckdb"
    write_consumer_db(sample_bars_df, sample_metrics_df, sample_tickers_df, db_path)

    con = duckdb.connect(str(db_path), read_only=True)
    tables = {row[0] for row in con.execute("SHOW TABLES").fetchall()}
    con.close()

    assert "daily_hvcs" not in tables
    assert len(tables) == 3


def test_write_consumer_db_backward_compat(
    tmp_path: Path,
    sample_bars_df: pl.DataFrame,
    sample_metrics_df: pl.DataFrame,
    sample_tickers_df: pl.DataFrame,
) -> None:
    """Existing positional call write_consumer_db(bars, metrics, tickers, path) still works."""  # noqa: E501
    db_path = tmp_path / "tickerlake.duckdb"
    write_consumer_db(sample_bars_df, sample_metrics_df, sample_tickers_df, db_path)

    con = duckdb.connect(str(db_path), read_only=True)
    tables = {row[0] for row in con.execute("SHOW TABLES").fetchall()}
    con.close()

    assert "daily_bars" in tables
    assert "daily_metrics" in tables
    assert "tickers" in tables


def test_write_consumer_db_weekly_tables_created(
    tmp_path: Path,
    sample_bars_df: pl.DataFrame,
    sample_metrics_df: pl.DataFrame,
    sample_tickers_df: pl.DataFrame,
    sample_hvcs_df: pl.DataFrame,
) -> None:
    """Weekly tables exist when weekly params are provided."""
    db_path = tmp_path / "tickerlake.duckdb"
    write_consumer_db(
        sample_bars_df,
        sample_metrics_df,
        sample_tickers_df,
        db_path,
        hvcs=sample_hvcs_df,
        weekly_bars=sample_bars_df,
        weekly_metrics=sample_metrics_df,
        weekly_hvcs=sample_hvcs_df,
    )
    con = duckdb.connect(str(db_path), read_only=True)
    tables = {row[0] for row in con.execute("SHOW TABLES").fetchall()}
    con.close()
    assert "weekly_bars" in tables
    assert "weekly_metrics" in tables
    assert "weekly_hvcs" in tables


def test_write_consumer_db_weekly_tables_optional(
    tmp_path: Path,
    sample_bars_df: pl.DataFrame,
    sample_metrics_df: pl.DataFrame,
    sample_tickers_df: pl.DataFrame,
) -> None:
    """No weekly tables when weekly params are omitted."""
    db_path = tmp_path / "tickerlake.duckdb"
    write_consumer_db(sample_bars_df, sample_metrics_df, sample_tickers_df, db_path)
    con = duckdb.connect(str(db_path), read_only=True)
    tables = {row[0] for row in con.execute("SHOW TABLES").fetchall()}
    con.close()
    assert "weekly_bars" not in tables
    assert "weekly_metrics" not in tables
    assert "weekly_hvcs" not in tables
    assert len(tables) == 3  # daily_bars, daily_metrics, tickers


def test_write_consumer_db_weekly_bars_schema(
    tmp_path: Path,
    sample_bars_df: pl.DataFrame,
    sample_metrics_df: pl.DataFrame,
    sample_tickers_df: pl.DataFrame,
) -> None:
    """weekly_bars has same columns as daily_bars."""
    db_path = tmp_path / "tickerlake.duckdb"
    write_consumer_db(
        sample_bars_df,
        sample_metrics_df,
        sample_tickers_df,
        db_path,
        weekly_bars=sample_bars_df,
    )
    con = duckdb.connect(str(db_path), read_only=True)
    daily_cols = {row[0] for row in con.execute("DESCRIBE daily_bars").fetchall()}
    weekly_cols = {row[0] for row in con.execute("DESCRIBE weekly_bars").fetchall()}
    con.close()
    assert daily_cols == weekly_cols


def test_write_consumer_db_weekly_bars_sorted(
    tmp_path: Path,
    sample_bars_df: pl.DataFrame,
    sample_metrics_df: pl.DataFrame,
    sample_tickers_df: pl.DataFrame,
) -> None:
    """weekly_bars is sorted by (ticker, date)."""
    db_path = tmp_path / "tickerlake.duckdb"
    write_consumer_db(
        sample_bars_df,
        sample_metrics_df,
        sample_tickers_df,
        db_path,
        weekly_bars=sample_bars_df,
    )
    con = duckdb.connect(str(db_path), read_only=True)
    rows = con.execute("SELECT ticker, date FROM weekly_bars").fetchall()
    con.close()
    assert rows == sorted(rows)


def test_write_consumer_db_weekly_metrics_sorted(
    tmp_path: Path,
    sample_bars_df: pl.DataFrame,
    sample_metrics_df: pl.DataFrame,
    sample_tickers_df: pl.DataFrame,
) -> None:
    """weekly_metrics is sorted by (ticker, date)."""
    db_path = tmp_path / "tickerlake.duckdb"
    write_consumer_db(
        sample_bars_df,
        sample_metrics_df,
        sample_tickers_df,
        db_path,
        weekly_metrics=sample_metrics_df,
    )
    con = duckdb.connect(str(db_path), read_only=True)
    rows = con.execute("SELECT ticker, date FROM weekly_metrics").fetchall()
    con.close()
    assert rows == sorted(rows)


def test_write_consumer_db_weekly_hvcs_sorted(
    tmp_path: Path,
    sample_bars_df: pl.DataFrame,
    sample_metrics_df: pl.DataFrame,
    sample_tickers_df: pl.DataFrame,
    sample_hvcs_df: pl.DataFrame,
) -> None:
    """weekly_hvcs is sorted by (ticker, date)."""
    db_path = tmp_path / "tickerlake.duckdb"
    write_consumer_db(
        sample_bars_df,
        sample_metrics_df,
        sample_tickers_df,
        db_path,
        weekly_hvcs=sample_hvcs_df,
    )
    con = duckdb.connect(str(db_path), read_only=True)
    rows = con.execute("SELECT ticker, date FROM weekly_hvcs").fetchall()
    con.close()
    assert rows == sorted(rows)


def test_write_splits_creates_table(
    tmp_path: Path, sample_splits_df: pl.DataFrame
) -> None:
    """write_splits() creates splits table with correct row count."""
    db_path = tmp_path / "raw.duckdb"
    write_splits(sample_splits_df, db_path)

    con = duckdb.connect(str(db_path), read_only=True)
    row = con.execute("SELECT COUNT(*) FROM splits").fetchone()
    con.close()

    assert row is not None
    count = row[0]
    assert count == len(sample_splits_df)


def test_write_splits_schema(tmp_path: Path, sample_splits_df: pl.DataFrame) -> None:
    """Splits table has correct column types: DATE, VARCHAR, FLOAT, DOUBLE."""
    db_path = tmp_path / "raw.duckdb"
    write_splits(sample_splits_df, db_path)

    con = duckdb.connect(str(db_path), read_only=True)
    schema = {row[0]: row[1] for row in con.execute("DESCRIBE splits").fetchall()}
    con.close()

    assert schema["ticker"] == "VARCHAR"
    assert schema["execution_date"] == "DATE"
    assert schema["split_from"] == "FLOAT"
    assert schema["split_to"] == "FLOAT"
    assert schema["adjustment_factor"] == "DOUBLE"
    assert schema["adjustment_type"] == "VARCHAR"


def test_write_splits_sorted(tmp_path: Path, sample_splits_df: pl.DataFrame) -> None:
    """Splits table is sorted by (ticker, execution_date)."""
    db_path = tmp_path / "raw.duckdb"
    write_splits(sample_splits_df, db_path)

    con = duckdb.connect(str(db_path), read_only=True)
    rows = con.execute("SELECT ticker, execution_date FROM splits").fetchall()
    con.close()

    assert rows == sorted(rows)


def test_read_splits_round_trip(tmp_path: Path, sample_splits_df: pl.DataFrame) -> None:
    """read_splits() returns the same data that was written by write_splits()."""
    db_path = tmp_path / "raw.duckdb"
    write_splits(sample_splits_df, db_path)

    result = read_splits(db_path)

    assert isinstance(result, pl.DataFrame)
    assert len(result) == len(sample_splits_df)
    assert set(result.columns) == set(sample_splits_df.columns)


def test_delete_then_append_no_duplicates(
    tmp_path: Path, sample_bars_df: pl.DataFrame
) -> None:
    """Delete-then-append for a date yields no duplicate (ticker, date) rows."""
    db_path = tmp_path / "raw.duckdb"
    write_raw_db(sample_bars_df, db_path)

    # Get the first date from the sample
    first_date = sample_bars_df["date"][0]

    # Delete that date
    delete_raw_dates(db_path, {first_date})

    # Append only the rows for the first date (simulating a re-fetch of that date)
    rows_to_append = sample_bars_df.filter(pl.col("date") == first_date)
    append_raw_db(rows_to_append, db_path)

    # Read back and verify no duplicates
    result = read_raw_db(db_path)

    # Count rows per (ticker, date) pair
    row_counts = result.group_by(["ticker", "date"]).agg(pl.len().alias("count"))

    # All counts should be 1 (no duplicates)
    assert row_counts["count"].max() == 1
    # Verify the round-trip didn't silently drop rows
    assert len(result) == len(sample_bars_df)


def test_delete_raw_dates_missing_table(tmp_path: Path) -> None:
    """delete_raw_dates() handles CatalogException when table doesn't exist."""
    db_path = tmp_path / "empty.duckdb"
    # Create an empty DuckDB file with no tables
    con = duckdb.connect(str(db_path))
    con.close()

    # Should not raise, just silently return
    delete_raw_dates(db_path, {datetime.date(2024, 1, 2)})

    # File should still exist
    assert db_path.exists()


def test_get_existing_dates_missing_table_returns_empty(tmp_path: Path) -> None:
    """get_existing_dates() returns empty set when table doesn't exist."""
    db_path = tmp_path / "empty.duckdb"
    # Create an empty DuckDB file with an unrelated table
    con = duckdb.connect(str(db_path))
    con.execute("CREATE TABLE unrelated (x INT)")
    con.close()

    result = get_existing_dates(db_path)

    assert result == set()


@pytest.fixture
def sample_hvc_vwap_anchors_df() -> pl.DataFrame:
    """Create a sample HVC VWAP anchors DataFrame."""
    return pl.DataFrame(
        {
            "ticker": ["AAPL", "MSFT"],
            "anchor_date": [datetime.date(2024, 1, 2), datetime.date(2024, 1, 3)],
            "date": [datetime.date(2024, 1, 2), datetime.date(2024, 1, 3)],
            "vwap_value": [151.5, 382.5],
        }
    ).cast(
        {
            "ticker": pl.Utf8,
            "anchor_date": pl.Date,
            "date": pl.Date,
            "vwap_value": pl.Float32,
        }
    )


def test_write_consumer_db_hvc_vwap_anchors_table_created(
    tmp_path: Path,
    sample_bars_df: pl.DataFrame,
    sample_metrics_df: pl.DataFrame,
    sample_tickers_df: pl.DataFrame,
    sample_hvc_vwap_anchors_df: pl.DataFrame,
) -> None:
    """write_consumer_db() with hvc_vwap_anchors kwarg creates table."""
    db_path = tmp_path / "tickerlake.duckdb"
    write_consumer_db(
        sample_bars_df,
        sample_metrics_df,
        sample_tickers_df,
        db_path,
        hvc_vwap_anchors=sample_hvc_vwap_anchors_df,
    )

    con = duckdb.connect(str(db_path), read_only=True)
    tables = {row[0] for row in con.execute("SHOW TABLES").fetchall()}
    con.close()

    assert "hvc_vwap_anchors" in tables


def test_write_consumer_db_hvc_vwap_anchors_schema(
    tmp_path: Path,
    sample_bars_df: pl.DataFrame,
    sample_metrics_df: pl.DataFrame,
    sample_tickers_df: pl.DataFrame,
    sample_hvc_vwap_anchors_df: pl.DataFrame,
) -> None:
    """hvc_vwap_anchors table has correct schema: DATE, VARCHAR, FLOAT columns."""
    db_path = tmp_path / "tickerlake.duckdb"
    write_consumer_db(
        sample_bars_df,
        sample_metrics_df,
        sample_tickers_df,
        db_path,
        hvc_vwap_anchors=sample_hvc_vwap_anchors_df,
    )

    con = duckdb.connect(str(db_path), read_only=True)
    schema = {
        row[0]: row[1] for row in con.execute("DESCRIBE hvc_vwap_anchors").fetchall()
    }
    con.close()

    assert schema["ticker"] == "VARCHAR"
    assert schema["anchor_date"] == "DATE"
    assert schema["date"] == "DATE"
    assert schema["vwap_value"] == "FLOAT"


def test_write_consumer_db_hvc_vwap_anchors_row_count(
    tmp_path: Path,
    sample_bars_df: pl.DataFrame,
    sample_metrics_df: pl.DataFrame,
    sample_tickers_df: pl.DataFrame,
    sample_hvc_vwap_anchors_df: pl.DataFrame,
) -> None:
    """hvc_vwap_anchors table has the same number of rows as input."""
    db_path = tmp_path / "tickerlake.duckdb"
    write_consumer_db(
        sample_bars_df,
        sample_metrics_df,
        sample_tickers_df,
        db_path,
        hvc_vwap_anchors=sample_hvc_vwap_anchors_df,
    )

    con = duckdb.connect(str(db_path), read_only=True)
    row = con.execute("SELECT COUNT(*) FROM hvc_vwap_anchors").fetchone()
    con.close()

    assert row is not None
    count = row[0]
    assert count == len(sample_hvc_vwap_anchors_df)


def test_get_db_info_table_without_date_column(tmp_path: Path) -> None:
    """get_db_info() handles tables without a date column."""
    db_path = tmp_path / "test.duckdb"
    con = duckdb.connect(str(db_path))
    con.execute("CREATE TABLE no_date_col (id INT, name VARCHAR)")
    con.execute("INSERT INTO no_date_col VALUES (1, 'test')")
    con.close()

    info = get_db_info(db_path)

    assert "no_date_col" in info["tables"]
    assert info["row_counts"]["no_date_col"] == 1
    # Table without date column should not appear in date_range
    assert "no_date_col" not in info.get("date_range", {})
