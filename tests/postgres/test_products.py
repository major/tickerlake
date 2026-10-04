"""Golden behavior for complete-history PostgreSQL product building."""

from __future__ import annotations

from datetime import date, timedelta

import polars as pl
import pytest

from tickerlake.extract import DAILY_AGGS_SCHEMA, SPLITS_SCHEMA
from tickerlake.postgres.products import build_products

START = date(2024, 1, 2)
TARGET = date(2024, 4, 30)
WEEKDAY_COUNT = 5


def _input() -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    sessions = [
        START + timedelta(days=index)
        for index in range(130)
        if (START + timedelta(days=index)).weekday() < WEEKDAY_COUNT
    ]
    rows = [
        {
            "date": day,
            "ticker": "AAA",
            "open": float(index + 100),
            "high": float(index + 103),
            "low": float(index + 99),
            "close": float(index + 102),
            "volume": float(index * 100 + 0.5),
            "transactions": 2**35 + index,
        }
        for index, day in enumerate(sessions)
    ]
    raw = pl.DataFrame(rows, schema=DAILY_AGGS_SCHEMA)
    splits = pl.DataFrame(
        [
            {
                "ticker": "AAA",
                "execution_date": date(2024, 2, 1),
                "split_from": 2.0,
                "split_to": 1.0,
                "adjustment_factor": 0.5,
                "adjustment_type": "split",
            }
        ],
        schema=SPLITS_SCHEMA,
    )
    identities = pl.DataFrame(
        {"ticker_id": [11], "symbol": ["AAA"]}, schema={"ticker_id": pl.Int32, "symbol": pl.String}
    )
    return raw, splits, identities


def test_build_products_matches_complete_history_golden_values() -> None:
    """Split adjustments preserve expected product arithmetic."""
    raw, splits, identities = _input()
    result = build_products(raw, splits, identities, collection_start=START, target=TARGET)

    assert result.daily.height == raw.height
    assert result.daily["ticker_id"].unique().to_list() == [11]
    assert result.daily["date"].to_list() == raw.sort("date")["date"].to_list()
    assert result.daily["open"][0] == pytest.approx(50.0)
    assert result.daily["volume"][0] == pytest.approx(1.0)
    assert result.daily["transactions"][0] == 2**35
    assert result.weekly.height > 0
    assert result.monthly.height > 0
    first_week = result.weekly.filter(pl.col("date") == date(2024, 1, 1)).row(0, named=True)
    assert first_week["volume"] == pytest.approx(1204.0)
    assert first_week["transactions"] == 4 * 2**35 + 6
    assert result.weekly["left_truncated"].to_list() == [False] * result.weekly.height
    assert result.monthly["left_truncated"].to_list() == [False] * result.monthly.height
    assert result.weekly.filter(pl.col("date") == date(2024, 4, 29))["calendar_closed"].to_list() == [False]
    assert result.monthly.filter(pl.col("date") == date(2024, 4, 30))["calendar_closed"].to_list() == [True]
    # target classifies calendar closure but does not trim a future observed bar.
    future_row = raw.tail(1).with_columns(
        pl.lit(date(2024, 6, 3)).alias("date"),
        (pl.col("open") + 1).alias("open"),
        (pl.col("high") + 1).alias("high"),
        (pl.col("low") + 1).alias("low"),
        (pl.col("close") + 1).alias("close"),
    )
    future = pl.concat([raw, future_row])
    future_result = build_products(future, splits, identities, collection_start=START, target=TARGET)
    assert future_result.daily.height == raw.height + 1
    assert future_result.monthly.filter(pl.col("date") == date(2024, 6, 3))["calendar_closed"].to_list() == [False]
    repeated = build_products(raw, splits, identities, collection_start=START, target=TARGET)
    for name in ("daily", "weekly", "monthly"):
        assert getattr(result, name).equals(getattr(repeated, name))


def test_build_products_rejects_conflicting_cumulative_split_factors() -> None:
    """Conflicting cumulative factors are rejected before calculations."""
    raw, splits, identities = _input()
    conflicting = pl.concat(
        [splits, splits.with_columns(pl.lit(0.25).alias("adjustment_factor"))],
        how="vertical",
    )
    with pytest.raises(ValueError, match="Conflict"):
        build_products(raw, conflicting, identities, collection_start=START, target=TARGET)


def test_build_products_allows_identical_split_duplicates_and_rejects_infinite_products() -> None:
    """Identical split duplicates are normalized and infinite output is rejected."""
    raw, splits, identities = _input()
    products = build_products(
        pl.concat([raw, raw.head(0)]), pl.concat([splits, splits]), identities, collection_start=START, target=TARGET
    )
    assert products.daily.height == raw.height
    bad = raw.with_columns(pl.lit(float("inf"), dtype=pl.Float32).alias("high"))
    with pytest.raises(ValueError, match="Nonfinite"):
        build_products(bad, splits, identities, collection_start=START, target=TARGET)


def test_build_products_returns_empty_products_for_identity_without_history() -> None:
    """An identity with no raw history produces no artificial bars."""
    raw, splits, identities = _input()
    products = build_products(raw.head(0), splits.head(0), identities, collection_start=START, target=TARGET)
    assert products.daily.is_empty()
    assert products.weekly.is_empty()
    assert products.monthly.is_empty()
