"""Write polars DataFrames to DuckDB files with correct schema types."""

import tempfile
from contextlib import contextmanager
from pathlib import Path
from typing import TYPE_CHECKING

import duckdb
import polars as pl

from tickerlake.extract import DAILY_AGGS_SCHEMA, TICKERS_SCHEMA

if TYPE_CHECKING:
    import datetime


_METRICS_SCHEMA = {
    "date": pl.Date,
    "ticker": pl.Utf8,
    "sma_20": pl.Float32,
    "sma_50": pl.Float32,
    "sma_200": pl.Float32,
    "atr_14": pl.Float32,
    "atr_pct": pl.Float32,
    "adr_pct": pl.Float32,
    "volume_sma_20": pl.Float32,
}


def _validate_schema(
    table: str, df: pl.DataFrame, expected: dict[str, pl.DataType]
) -> None:
    """Validate a DataFrame schema before writing a DuckDB table."""
    mismatches = _schema_mismatches(dict(df.schema), expected)

    if mismatches:
        msg = f"{table} schema mismatch: {'; '.join(mismatches)}"
        raise ValueError(msg)


def _schema_mismatches(
    actual: dict[str, pl.DataType], expected: dict[str, pl.DataType]
) -> list[str]:
    missing = [col for col in expected if col not in actual]
    extra = [col for col in actual if col not in expected]
    wrong_dtype = [
        f"{col}: expected {dtype}, got {actual[col]}"
        for col, dtype in expected.items()
        if col in actual and actual[col] != dtype
    ]

    return [
        *([f"missing columns: {', '.join(missing)}"] if missing else []),
        *([f"unexpected columns: {', '.join(extra)}"] if extra else []),
        *wrong_dtype,
    ]


@contextmanager
def _tmp_parquet(df: pl.DataFrame):
    """Write df to a temp parquet file, yield its path, then delete it."""
    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as f:
        tmp = Path(f.name)
    try:
        df.write_parquet(tmp)
        yield tmp
    finally:
        tmp.unlink(missing_ok=True)


def _read_parquet_sql(tmp: Path, order_by: str = "") -> str:
    """Build a SELECT from read_parquet() with optional ORDER BY clause."""
    sql = f"SELECT * FROM read_parquet('{tmp}')"
    if order_by:
        sql += f" ORDER BY {order_by}"
    return sql


def write_raw_db(bars: pl.DataFrame, path: Path) -> None:
    """Write bars DataFrame to raw_daily_bars table, replacing any existing data."""
    with _tmp_parquet(bars) as tmp:
        con = duckdb.connect(str(path))
        con.execute(
            "CREATE OR REPLACE TABLE raw_daily_bars AS "
            f"{_read_parquet_sql(tmp, 'ticker, date')}"
        )
        con.execute("CHECKPOINT")
        con.close()


def append_raw_db(new_bars: pl.DataFrame, path: Path) -> None:
    """Append new_bars rows to existing raw_daily_bars table."""
    with _tmp_parquet(new_bars) as tmp:
        con = duckdb.connect(str(path))
        con.execute(
            f"INSERT INTO raw_daily_bars {_read_parquet_sql(tmp, 'ticker, date')}"
        )
        con.execute("CHECKPOINT")
        con.close()


def delete_raw_dates(path: Path, dates: set[datetime.date]) -> None:
    """Delete raw_daily_bars rows for the provided trading dates."""
    if not dates or not path.exists():
        return

    con = duckdb.connect(str(path))
    try:
        con.execute("DELETE FROM raw_daily_bars WHERE date IN ?", [sorted(dates)])
        con.execute("CHECKPOINT")
    except duckdb.CatalogException:
        pass
    finally:
        con.close()


def read_raw_db(path: Path) -> pl.DataFrame:
    """Read raw_daily_bars table from DuckDB file, sorted by (ticker, date)."""
    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as f:
        tmp = Path(f.name)
    try:
        con = duckdb.connect(str(path), read_only=True)
        con.execute(
            "COPY (SELECT * FROM raw_daily_bars ORDER BY ticker, date) "
            f"TO '{tmp}' (FORMAT PARQUET)"
        )
        con.close()
        return pl.read_parquet(tmp)
    finally:
        tmp.unlink(missing_ok=True)


def get_existing_dates(path: Path) -> set[datetime.date]:
    """Return the set of dates already stored in raw_daily_bars.

    Without loading all rows.
    """
    if not path.exists():
        return set()
    con = duckdb.connect(str(path), read_only=True)
    try:
        rows = con.execute("SELECT DISTINCT date FROM raw_daily_bars").fetchall()
        return {row[0] for row in rows}
    except duckdb.CatalogException:
        return set()
    finally:
        con.close()


def write_consumer_db(
    bars: pl.DataFrame,
    metrics: pl.DataFrame,
    tickers: pl.DataFrame,
    path: Path,
    *,
    weekly_bars: pl.DataFrame | None = None,
    weekly_metrics: pl.DataFrame | None = None,
    monthly_bars: pl.DataFrame | None = None,
    monthly_metrics: pl.DataFrame | None = None,
) -> None:
    """Write bars, metrics, tickers, and optional period DataFrames to consumer DuckDB file."""  # noqa: E501
    _validate_schema("daily_bars", bars, DAILY_AGGS_SCHEMA)
    _validate_schema("daily_metrics", metrics, _METRICS_SCHEMA)
    _validate_schema("tickers", tickers, TICKERS_SCHEMA)
    if weekly_bars is not None:
        _validate_schema("weekly_bars", weekly_bars, DAILY_AGGS_SCHEMA)
    if weekly_metrics is not None:
        _validate_schema("weekly_metrics", weekly_metrics, _METRICS_SCHEMA)
    if monthly_bars is not None:
        _validate_schema("monthly_bars", monthly_bars, DAILY_AGGS_SCHEMA)
    if monthly_metrics is not None:
        _validate_schema("monthly_metrics", monthly_metrics, _METRICS_SCHEMA)

    with (
        _tmp_parquet(bars) as bars_tmp,
        _tmp_parquet(metrics) as metrics_tmp,
        _tmp_parquet(tickers) as tickers_tmp,
    ):
        con = duckdb.connect(str(path))
        try:
            con.execute(
                "CREATE OR REPLACE TABLE daily_bars AS "
                f"{_read_parquet_sql(bars_tmp, 'ticker, date')}"
            )
            con.execute(
                "CREATE OR REPLACE TABLE daily_metrics AS "
                f"{_read_parquet_sql(metrics_tmp, 'ticker, date')}"
            )
            con.execute(
                "CREATE OR REPLACE TABLE tickers AS "
                f"{_read_parquet_sql(tickers_tmp, 'ticker')}"
            )
            if weekly_bars is not None:
                with _tmp_parquet(weekly_bars) as wb_tmp:
                    con.execute(
                        "CREATE OR REPLACE TABLE weekly_bars AS "
                        f"{_read_parquet_sql(wb_tmp, 'ticker, date')}"
                    )
            if weekly_metrics is not None:
                with _tmp_parquet(weekly_metrics) as wm_tmp:
                    con.execute(
                        "CREATE OR REPLACE TABLE weekly_metrics AS "
                        f"{_read_parquet_sql(wm_tmp, 'ticker, date')}"
                    )
            if monthly_bars is not None:
                with _tmp_parquet(monthly_bars) as mb_tmp:
                    con.execute(
                        "CREATE OR REPLACE TABLE monthly_bars AS "
                        f"{_read_parquet_sql(mb_tmp, 'ticker, date')}"
                    )
            if monthly_metrics is not None:
                with _tmp_parquet(monthly_metrics) as mm_tmp:
                    con.execute(
                        "CREATE OR REPLACE TABLE monthly_metrics AS "
                        f"{_read_parquet_sql(mm_tmp, 'ticker, date')}"
                    )
            con.execute("CHECKPOINT")
        finally:
            con.close()


def write_splits(splits: pl.DataFrame, path: Path) -> None:
    """Write splits DataFrame to splits table, replacing any existing data."""
    with _tmp_parquet(splits) as tmp:
        con = duckdb.connect(str(path))
        con.execute(
            "CREATE OR REPLACE TABLE splits AS "
            f"{_read_parquet_sql(tmp, 'ticker, execution_date')}"
        )
        con.execute("CHECKPOINT")
        con.close()


def read_splits(path: Path) -> pl.DataFrame:
    """Read splits table from DuckDB file, sorted by (ticker, execution_date)."""
    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as f:
        tmp = Path(f.name)
    try:
        con = duckdb.connect(str(path), read_only=True)
        con.execute(
            "COPY (SELECT * FROM splits ORDER BY ticker, execution_date) "
            f"TO '{tmp}' (FORMAT PARQUET)"
        )
        con.close()
        return pl.read_parquet(tmp)
    finally:
        tmp.unlink(missing_ok=True)


def compact_raw_db(path: Path) -> None:
    """Rebuild raw.duckdb by exporting and reimporting to reclaim fragmented space."""
    bars = read_raw_db(path)
    write_raw_db(bars, path)


def _table_date_range(con: duckdb.DuckDBPyConnection, table: str) -> dict | None:
    """Return min/max date for a table if it has a date column, else None."""
    cols = {row[0] for row in con.execute(f"DESCRIBE {table}").fetchall()}
    if "date" not in cols:
        return None
    row = con.execute(f"SELECT MIN(date), MAX(date) FROM {table}").fetchone()
    return {"min": row[0], "max": row[1]} if row else None


def get_db_info(path: Path) -> dict:
    """Return metadata about a DuckDB file: tables, row counts, date range, file size."""  # noqa: E501
    con = duckdb.connect(str(path), read_only=True)
    tables = [row[0] for row in con.execute("SHOW TABLES").fetchall()]
    row_counts = {
        t: (con.execute(f"SELECT COUNT(*) FROM {t}").fetchone() or (0,))[0]
        for t in tables
    }
    date_range = {
        t: dr for t in tables if (dr := _table_date_range(con, t)) is not None
    }
    con.close()
    return {
        "tables": tables,
        "row_counts": row_counts,
        "date_range": date_range,
        "file_size_bytes": path.stat().st_size,
    }
