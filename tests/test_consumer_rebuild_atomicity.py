"""Atomicity guarantees for consumer database rebuilds."""

from typing import TYPE_CHECKING

import duckdb
import polars as pl
import pytest

from tickerlake import load
from tickerlake.load import write_consumer_db
from tickerlake.transform import compute_metrics

if TYPE_CHECKING:
    from pathlib import Path

_MONTHLY_METRICS_QUERY_NUMBER = 6  # daily, weekly, then monthly bars and metrics


def _snapshot(path: Path) -> dict[str, tuple[list[tuple], list[tuple]]]:
    """Read every table's rows and schema from a consumer database."""
    with duckdb.connect(str(path), read_only=True) as connection:
        tables = [row[0] for row in connection.execute("SHOW TABLES").fetchall()]
        return {
            table: (
                connection.execute(f'SELECT * FROM "{table}" ORDER BY ALL').fetchall(),  # noqa: S608 -- tables come from SHOW TABLES
                connection.execute(f'DESCRIBE "{table}"').fetchall(),
            )
            for table in tables
        }


def test_failed_consumer_rebuild_preserves_all_tables_then_retry_succeeds(
    tmp_path: Path,
    sample_bars_df: pl.DataFrame,
    sample_tickers_df: pl.DataFrame,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A late table replacement failure leaves the complete prior database intact."""
    db_path = tmp_path / "consumer.duckdb"
    bars = sample_bars_df
    period_bars = bars.with_columns(pl.lit(False).alias("left_truncated"), pl.lit(False).alias("calendar_closed"))
    metrics = compute_metrics(bars)
    tickers = sample_tickers_df
    write_consumer_db(
        bars,
        metrics,
        tickers,
        db_path,
        weekly_bars=period_bars,
        weekly_metrics=metrics,
        monthly_bars=period_bars,
        monthly_metrics=metrics,
    )
    with duckdb.connect(str(db_path)) as connection:
        connection.execute("CREATE TABLE sentinel AS SELECT 'keep this value' AS value")

    before = _snapshot(db_path)
    changed_bars = bars.with_columns((pl.col("close") + 700).alias("close"))
    changed_period_bars = period_bars.with_columns((pl.col("close") + 700).alias("close"))
    changed_metrics = compute_metrics(changed_bars)
    changed_tickers = tickers.with_columns(pl.lit("Replacement company").alias("name"))
    original_read_parquet_sql = load._read_parquet_sql  # noqa: SLF001 -- explicit SQL fault injection

    def fail_monthly_metrics(order_by: str = "") -> str:
        if order_by == "ticker, date":
            # The final optional table has the same order clause as earlier tables.
            # Fail on its sixth occurrence, after all preceding replacements ran.
            fail_monthly_metrics.calls += 1
            if fail_monthly_metrics.calls == _MONTHLY_METRICS_QUERY_NUMBER:
                return "INVALID MONTHLY METRICS SQL"
        return original_read_parquet_sql(order_by)

    fail_monthly_metrics.calls = 0
    monkeypatch.setattr(load, "_read_parquet_sql", fail_monthly_metrics)
    with pytest.raises(duckdb.ParserException):
        write_consumer_db(
            changed_bars,
            changed_metrics,
            changed_tickers,
            db_path,
            weekly_bars=changed_period_bars,
            weekly_metrics=changed_metrics,
            monthly_bars=changed_period_bars,
            monthly_metrics=changed_metrics,
        )

    assert _snapshot(db_path) == before

    monkeypatch.setattr(load, "_read_parquet_sql", original_read_parquet_sql)
    write_consumer_db(
        changed_bars,
        changed_metrics,
        changed_tickers,
        db_path,
        weekly_bars=changed_period_bars,
        weekly_metrics=changed_metrics,
        monthly_bars=changed_period_bars,
        monthly_metrics=changed_metrics,
    )
    after = _snapshot(db_path)
    assert after.keys() == before.keys()
    assert after["sentinel"] == before["sentinel"]
    assert after["daily_bars"] != before["daily_bars"]
    assert after["tickers"] != before["tickers"]


def test_monthly_metrics_parquet_failure_preserves_tables_and_cleans_temporary_files(
    tmp_path: Path,
    sample_bars_df: pl.DataFrame,
    sample_tickers_df: pl.DataFrame,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A final optional frame serialization error rolls back preceding replacements."""
    db_path = tmp_path / "consumer.duckdb"
    bars = sample_bars_df
    period_bars = bars.with_columns(pl.lit(False).alias("left_truncated"), pl.lit(False).alias("calendar_closed"))
    metrics = compute_metrics(bars)
    tickers = sample_tickers_df
    monthly_metrics = metrics.with_columns((pl.col("sma_20") + 900).alias("sma_20"))
    write_consumer_db(
        bars,
        metrics,
        tickers,
        db_path,
        weekly_bars=period_bars,
        weekly_metrics=metrics,
        monthly_bars=period_bars,
        monthly_metrics=metrics,
    )
    with duckdb.connect(str(db_path)) as connection:
        connection.execute("CREATE TABLE sentinel AS SELECT 'keep this value' AS value")
    before = _snapshot(db_path)

    changed_bars = bars.with_columns((pl.col("close") + 800).alias("close"))
    changed_period_bars = period_bars.with_columns((pl.col("close") + 800).alias("close"))
    changed_metrics = compute_metrics(changed_bars)
    changed_tickers = tickers.with_columns(pl.lit("Serialization replacement").alias("name"))
    parquet_temp_dir = tmp_path / "parquet-temp"
    parquet_temp_dir.mkdir()
    monkeypatch.setattr(load.tempfile, "tempdir", str(parquet_temp_dir))
    original_write_parquet = pl.DataFrame.write_parquet

    def fail_final_frame(frame: pl.DataFrame, file: object, *args: object, **kwargs: object) -> object:
        if frame is monthly_metrics:
            raise OSError("failure")
        return original_write_parquet(frame, file, *args, **kwargs)

    monkeypatch.setattr(pl.DataFrame, "write_parquet", fail_final_frame)
    with pytest.raises(OSError, match="failure"):
        write_consumer_db(
            changed_bars,
            changed_metrics,
            changed_tickers,
            db_path,
            weekly_bars=changed_period_bars,
            weekly_metrics=changed_metrics,
            monthly_bars=changed_period_bars,
            monthly_metrics=monthly_metrics,
        )

    assert _snapshot(db_path) == before
    assert list(parquet_temp_dir.iterdir()) == []

    monkeypatch.setattr(pl.DataFrame, "write_parquet", original_write_parquet)
    write_consumer_db(
        changed_bars,
        changed_metrics,
        changed_tickers,
        db_path,
        weekly_bars=changed_period_bars,
        weekly_metrics=changed_metrics,
        monthly_bars=changed_period_bars,
        monthly_metrics=monthly_metrics,
    )
    after = _snapshot(db_path)
    assert after.keys() == before.keys()
    assert after["sentinel"] == before["sentinel"]
    assert after["daily_bars"] != before["daily_bars"]
    assert after["monthly_bars"] != before["monthly_bars"]
