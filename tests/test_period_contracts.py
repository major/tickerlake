"""Regression coverage for numeric precision and period boundary contracts."""

import datetime

import polars as pl
import pytest

from tickerlake.transform import PERIOD_AGGS_SCHEMA, adjust_splits, aggregate_to_monthly, aggregate_to_weekly


def bars(rows: list[dict]) -> pl.DataFrame:
    """Build typed source bars for period-contract tests."""
    return pl.DataFrame(
        rows,
        schema={
            "date": pl.Date,
            "ticker": pl.Utf8,
            "open": pl.Float32,
            "high": pl.Float32,
            "low": pl.Float32,
            "close": pl.Float32,
            "volume": pl.UInt32,
        },
    )


def test_period_aggregation_widens_integer_sums() -> None:
    """Widen aggregation volume inputs to Float64."""
    template = {
        "ticker": "A",
        "open": 10.0,
        "high": 11.0,
        "low": 9.0,
        "close": 10.0,
        "volume": 2**32 - 1,
    }
    frame = bars([{**template, "date": datetime.date(2024, 1, 8)}, {**template, "date": datetime.date(2024, 1, 9)}])
    row = aggregate_to_weekly(frame, collection_start=datetime.date(2024, 1, 1), target=datetime.date(2024, 1, 12)).row(
        0, named=True
    )
    assert row["volume"] == 2 * (2**32 - 1)
    assert type(row["volume"]) is float


def test_empty_period_schema_and_calendar_flags() -> None:
    """Preserve typed empty output and derive flags from schedule bounds."""
    empty = pl.DataFrame(
        schema={
            "date": pl.Date,
            "ticker": pl.Utf8,
            "open": pl.Float32,
            "high": pl.Float32,
            "low": pl.Float32,
            "close": pl.Float32,
            "volume": pl.Float64,
        }
    )
    assert (
        aggregate_to_monthly(
            empty, collection_start=datetime.date(2024, 1, 1), target=datetime.date(2024, 1, 31)
        ).schema
        == PERIOD_AGGS_SCHEMA
    )
    current = bars(
        [
            {
                "date": datetime.date(2024, 1, 8),
                "ticker": "A",
                "open": 10,
                "high": 10,
                "low": 10,
                "close": 10,
                "volume": 1,
            }
        ]
    )
    result = aggregate_to_weekly(
        current, collection_start=datetime.date(2024, 1, 9), target=datetime.date(2024, 1, 10)
    ).row(0, named=True)
    assert result["left_truncated"] is True
    assert result["calendar_closed"] is False


@pytest.mark.parametrize(
    "case",
    [
        (
            aggregate_to_weekly,
            datetime.date(2024, 1, 4),
            datetime.date(2024, 1, 2),
            datetime.date(2024, 1, 5),
            datetime.date(2024, 1, 1),
        ),
        (
            aggregate_to_monthly,
            datetime.date(2024, 1, 4),
            datetime.date(2024, 1, 2),
            datetime.date(2024, 1, 31),
            datetime.date(2024, 1, 4),
        ),
    ],
    ids=["holiday-week-first-session-tuesday", "monthly-first-session-tuesday"],
)
@pytest.mark.parametrize("offset", [-1, 0, 1], ids=["before", "on", "after"])
def test_left_truncated_uses_collection_bound_not_first_ticker_bar(case, offset) -> None:
    """Left truncation depends on the collection bound, including IPO gaps."""
    aggregate, observed, first_session, last_session, period_label = case
    collection_start = first_session + datetime.timedelta(days=offset)
    source = bars(
        [
            {
                "date": observed,
                "ticker": "IPO",
                "open": 10,
                "high": 10,
                "low": 10,
                "close": 10,
                "volume": 1,
            }
        ]
    )
    row = aggregate(source, collection_start=collection_start, target=last_session).row(0, named=True)
    assert row["date"] == period_label
    assert row["left_truncated"] is (collection_start > first_session)


@pytest.mark.parametrize(
    ("aggregate", "observed", "last_session", "period_label"),
    [
        (aggregate_to_weekly, datetime.date(2024, 1, 2), datetime.date(2024, 1, 5), datetime.date(2024, 1, 1)),
        (aggregate_to_monthly, datetime.date(2024, 1, 2), datetime.date(2024, 1, 31), datetime.date(2024, 1, 2)),
    ],
    ids=["holiday-week-friday-close", "monthly-january-close"],
)
@pytest.mark.parametrize("offset", [-1, 0, 1], ids=["before", "on", "after"])
def test_calendar_closed_uses_scheduled_last_session(aggregate, observed, last_session, period_label, offset) -> None:
    """A period closes on its last scheduled session, not observed ticker data."""
    target = last_session + datetime.timedelta(days=offset)
    source = bars(
        [
            {
                "date": observed,
                "ticker": "IPO",
                "open": 10,
                "high": 10,
                "low": 10,
                "close": 10,
                "volume": 1,
            }
        ]
    )
    row = aggregate(source, collection_start=observed, target=target).row(0, named=True)
    assert row["date"] == period_label
    assert row["calendar_closed"] is (last_session <= target)


@pytest.mark.parametrize("aggregate", [aggregate_to_weekly, aggregate_to_monthly], ids=["weekly", "monthly"])
@pytest.mark.parametrize("is_empty", [False, True], ids=["populated", "empty"])
def test_invalid_collection_bounds_rejected_for_empty_and_populated(aggregate, is_empty: bool) -> None:
    """Reject collection bounds with collection_start later than target."""
    frame = (
        pl.DataFrame(schema=PERIOD_AGGS_SCHEMA)
        if is_empty
        else bars(
            [
                {
                    "date": datetime.date(2024, 1, 8),
                    "ticker": "A",
                    "open": 10,
                    "high": 10,
                    "low": 10,
                    "close": 10,
                    "volume": 1,
                }
            ]
        )
    )
    with pytest.raises(ValueError, match=r".*"):
        aggregate(frame, collection_start=datetime.date(2024, 1, 9), target=datetime.date(2024, 1, 8))


@pytest.mark.parametrize("aggregate", [aggregate_to_weekly, aggregate_to_monthly], ids=["weekly", "monthly"])
def test_period_schema_has_public_numeric_and_flag_types(aggregate) -> None:
    """Period outputs have the documented public schema types."""
    empty = pl.DataFrame(
        schema={
            "date": pl.Date,
            "ticker": pl.Utf8,
            "open": pl.Float32,
            "high": pl.Float32,
            "low": pl.Float32,
            "close": pl.Float32,
            "volume": pl.Float64,
        }
    )
    assert aggregate(empty, collection_start=datetime.date(2024, 1, 1), target=datetime.date(2024, 1, 31)).schema == {
        "date": pl.Date,
        "ticker": pl.Utf8,
        "open": pl.Float32,
        "high": pl.Float32,
        "low": pl.Float32,
        "close": pl.Float32,
        "volume": pl.Float64,
        "left_truncated": pl.Boolean,
        "calendar_closed": pl.Boolean,
    }


@pytest.mark.parametrize("aggregate", [aggregate_to_weekly, aggregate_to_monthly], ids=["weekly", "monthly"])
def test_period_rejects_nonfinite_float64_volume_total(aggregate) -> None:
    """Reject a period volume total that overflows Float64."""
    source = pl.DataFrame(
        {
            "date": [datetime.date(2024, 1, 8), datetime.date(2024, 1, 9)],
            "ticker": ["A", "A"],
            "open": [10.0, 10.0],
            "high": [10.0, 10.0],
            "low": [10.0, 10.0],
            "close": [10.0, 10.0],
            "volume": [1e308, 1e308],
        },
        schema_overrides={"volume": pl.Float64},
    )
    with pytest.raises(ValueError, match=r".*"):
        aggregate(source, collection_start=datetime.date(2024, 1, 1), target=datetime.date(2024, 1, 31))


def test_future_cached_periods_are_retained_and_marked_open() -> None:
    """Keep retained periods after target, with calendar_closed false."""
    future = bars(
        [
            {
                "date": datetime.date(2024, 1, 16),
                "ticker": "A",
                "open": 10,
                "high": 10,
                "low": 10,
                "close": 10,
                "volume": 1,
            }
        ]
    )
    result = aggregate_to_weekly(future, collection_start=datetime.date(2024, 1, 1), target=datetime.date(2024, 1, 12))
    assert result.height == 1
    assert result["date"].item() == datetime.date(2024, 1, 15)
    assert result["calendar_closed"].item() is False


def test_split_adjustment_widens_volume_even_without_splits() -> None:
    """Canonicalize volume to Float64 on the no-split path."""
    source = bars(
        [
            {
                "date": datetime.date(2024, 1, 8),
                "ticker": "A",
                "open": 10,
                "high": 10,
                "low": 10,
                "close": 10,
                "volume": 5,
            }
        ]
    )
    result = adjust_splits(
        source, pl.DataFrame(schema={"ticker": pl.Utf8, "execution_date": pl.Date, "adjustment_factor": pl.Float64})
    )
    assert result.schema["volume"] == pl.Float64


def test_split_adjustment_accepts_finite_fractional_volume() -> None:
    """Keep fractional adjusted volume at Float64 precision."""
    source = pl.DataFrame(
        {
            "date": [datetime.date(2024, 1, 8)],
            "ticker": ["A"],
            "open": [10.0],
            "high": [10.0],
            "low": [10.0],
            "close": [10.0],
            "volume": [0.1],
        },
        schema_overrides={"volume": pl.Float64},
    )
    split = pl.DataFrame({"ticker": ["A"], "execution_date": [datetime.date(2024, 1, 9)], "adjustment_factor": [2.0]})
    result = adjust_splits(source, split)
    assert result["volume"].item() == pytest.approx(0.05)


def test_split_adjustment_rejects_nonfinite_adjusted_volume() -> None:
    """Reject split adjustment that overflows the volume Float64 range."""
    source = pl.DataFrame(
        {
            "date": [datetime.date(2024, 1, 8)],
            "ticker": ["A"],
            "open": [10.0],
            "high": [10.0],
            "low": [10.0],
            "close": [10.0],
            "volume": [1e308],
        },
        schema_overrides={"volume": pl.Float64},
    )
    split = pl.DataFrame({"ticker": ["A"], "execution_date": [datetime.date(2024, 1, 9)], "adjustment_factor": [0.5]})
    with pytest.raises(ValueError, match=r".*"):
        adjust_splits(source, split)
