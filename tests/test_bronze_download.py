"""Tests for the bronze layer parallel download workflow."""

from __future__ import annotations

from datetime import date as dt_date
from typing import List

import polars as pl
import pytest

from tickerlake.bronze import main as bronze_main


def test_download_parallel_stops_on_limit(monkeypatch, make_transformed_df) -> None:
    """Ensure we stop scheduling new downloads once the API limit is hit."""
    dates = ["2024-03-03", "2024-03-02", "2024-03-01"]

    frames = {
        "2024-03-03": make_transformed_df("2024-03-03"),
    }
    fetch_order: List[str] = []

    def fake_fetch_single(target_date: str):
        fetch_order.append(target_date)
        if target_date == "2024-03-02":
            return target_date, None, True, RuntimeError("403")
        frame = frames.get(target_date)
        if frame is None:
            return target_date, None, False, None
        return target_date, frame, False, None

    monkeypatch.setattr(bronze_main, "_fetch_single_date", fake_fetch_single)

    progress_updates: list[str] = []
    summary = bronze_main._download_grouped_daily_aggs_parallel(
        dates,
        max_workers=2,
        progress_callback=progress_updates.append,
    )

    assert summary.limit_reached is True
    assert len(summary.frames) == 1
    assert summary.frames[0]["date"][0] == dt_date.fromisoformat("2024-03-03")
    assert set(progress_updates) <= {"2024-03-01", "2024-03-02", "2024-03-03"}


def _no_existing(make_df):
    return None


def _with_existing(make_df):
    return make_df("2024-03-04", tickers=("TSLA",))


@pytest.mark.parametrize(
    "existing_factory,expected_count",
    [
        (_no_existing, 4),
        (_with_existing, 5),
    ],
)
def test_load_grouped_daily_aggs_combines_results(
    monkeypatch,
    make_transformed_df,
    existing_factory,
    expected_count,
) -> None:
    """`load_grouped_daily_aggs` writes a single combined DataFrame to storage."""
    frames = [
        make_transformed_df("2024-03-03"),
        make_transformed_df("2024-03-02", tickers=("GOOG", "AMZN")),
    ]

    summary = bronze_main.FetchSummary(frames=frames)

    existing_rows = existing_factory(make_transformed_df)

    monkeypatch.setattr(
        bronze_main,
        "_download_grouped_daily_aggs_parallel",
        lambda *_: summary,
    )
    monkeypatch.setattr(bronze_main, "get_table_path", lambda *_, **__: "bronze/stocks")
    monkeypatch.setattr(bronze_main, "settings", bronze_main.settings)
    monkeypatch.setattr(bronze_main.settings, "bronze_parallel_requests", 2, raising=False)

    write_calls: dict[str, pl.DataFrame] = {}

    def fake_write_table(
        table_path: str,
        df: pl.DataFrame,
        mode: str = "overwrite",
        partition_by: str | list[str] | None = None,
    ) -> None:
        write_calls["path"] = table_path
        write_calls["df"] = df
        write_calls["mode"] = mode
        write_calls["partition"] = partition_by

    monkeypatch.setattr(bronze_main, "write_table", fake_write_table)

    if existing_rows is None:
        monkeypatch.setattr(bronze_main, "table_exists", lambda *_: False)
    else:
        monkeypatch.setattr(bronze_main, "table_exists", lambda *_: True)
        monkeypatch.setattr(bronze_main, "read_table", lambda *_: existing_rows)

    bronze_main.load_grouped_daily_aggs(
        ["2024-03-03", "2024-03-02"],
    )

    assert write_calls["path"] == "bronze/stocks"
    assert write_calls["mode"] == "overwrite"
    assert write_calls["partition"] == "date"

    written_df = write_calls["df"]
    assert isinstance(written_df, pl.DataFrame)
    assert len(written_df) == expected_count
    assert set(written_df.columns) >= {"ticker", "date"}


def test_load_grouped_daily_aggs_skips_when_empty(monkeypatch) -> None:
    """No write should occur when download returns no data."""
    summary = bronze_main.FetchSummary(frames=[])

    monkeypatch.setattr(
        bronze_main,
        "_download_grouped_daily_aggs_parallel",
        lambda *_: summary,
    )
    monkeypatch.setattr(bronze_main, "get_table_path", lambda *_, **__: "bronze/stocks")
    monkeypatch.setattr(bronze_main, "table_exists", lambda *_: False)
    monkeypatch.setattr(bronze_main.settings, "bronze_parallel_requests", 1, raising=False)

    calls = {"write": 0}

    def fake_write_table(*_args, **_kwargs) -> None:
        calls["write"] += 1

    monkeypatch.setattr(bronze_main, "write_table", fake_write_table)

    bronze_main.load_grouped_daily_aggs(["2024-03-03"])

    assert calls["write"] == 0
