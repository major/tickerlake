"""Behavioral tests for bar transformations."""

import datetime
import importlib

import polars as pl
import pytest
from polars.testing import assert_frame_equal

transform = importlib.import_module("tickerlake.transform")
extract = importlib.import_module("tickerlake.extract")
adjust_splits = transform.adjust_splits
filter_tickers = transform.filter_tickers
aggregate_to_weekly = transform.aggregate_to_weekly
aggregate_to_monthly = transform.aggregate_to_monthly
DAILY_AGGS_SCHEMA = extract.DAILY_AGGS_SCHEMA


BARS_SCHEMA = {
    "date": pl.Date,
    "ticker": pl.Utf8,
    "open": pl.Float32,
    "high": pl.Float32,
    "low": pl.Float32,
    "close": pl.Float32,
    "volume": pl.Float32,
    "transactions": pl.UInt32,
}

EXPECTED_WEEKLY_ROWS = 6
EXPECTED_TRANSACTIONS = 60
EXPECTED_SINGLE_DAY_TRANSACTIONS = 10
EXPECTED_MONTH_TRANSACTIONS = 21

SPLITS_SCHEMA = {
    "ticker": pl.Utf8,
    "execution_date": pl.Date,
    "split_from": pl.Float32,
    "split_to": pl.Float32,
    "adjustment_factor": pl.Float64,
    "adjustment_type": pl.Utf8,
}


def make_bars(rows: list[dict]) -> pl.DataFrame:
    """Build a typed bars frame from row dictionaries."""
    return pl.DataFrame(rows, schema=BARS_SCHEMA)


def make_splits(rows: list[dict]) -> pl.DataFrame:
    """Build a typed splits frame from row dictionaries."""
    return pl.DataFrame(rows, schema=SPLITS_SCHEMA)


class TestAggregateToWeekly:
    """Weekly aggregation behavior and schema guarantees."""

    def test_basic_aggregation(self):
        """Aggregate daily bars by ticker and week."""
        rows = []
        for ticker, base in [("AAPL", 100.0), ("MSFT", 200.0)]:
            week_1_dates = [
                datetime.date(2024, 1, 8),
                datetime.date(2024, 1, 9),
                datetime.date(2024, 1, 10),
                datetime.date(2024, 1, 11),
                datetime.date(2024, 1, 12),
            ]
            week_2_dates = [
                datetime.date(2024, 1, 16),
                datetime.date(2024, 1, 17),
                datetime.date(2024, 1, 18),
                datetime.date(2024, 1, 19),
            ]
            week_3_dates = [
                datetime.date(2024, 1, 22),
                datetime.date(2024, 1, 23),
                datetime.date(2024, 1, 24),
                datetime.date(2024, 1, 25),
                datetime.date(2024, 1, 26),
            ]
            all_dates = week_1_dates + week_2_dates + week_3_dates
            for i, date in enumerate(all_dates):
                price = base + i
                rows.append(
                    {
                        "date": date,
                        "ticker": ticker,
                        "open": price,
                        "high": price + 1.0,
                        "low": price - 1.0,
                        "close": price + 0.5,
                        "volume": 1000.0 + i,
                        "transactions": 100 + i,
                    }
                )

        result = aggregate_to_weekly(
            make_bars(rows), collection_start=datetime.date(2024, 1, 1), target=datetime.date(2024, 12, 31)
        )

        assert len(result) == EXPECTED_WEEKLY_ROWS
        per_ticker_counts = result.group_by("ticker").len().sort("ticker")
        assert per_ticker_counts["len"].to_list() == [3, 3]

    def test_ohlcv_rollup_values(self):
        """Roll up OHLCV values and transaction totals."""
        bars = make_bars(
            [
                {
                    "date": datetime.date(2024, 1, 8),
                    "ticker": "AAPL",
                    "open": 100.0,
                    "high": 102.0,
                    "low": 99.0,
                    "close": 101.0,
                    "volume": 1000.0,
                    "transactions": 10,
                },
                {
                    "date": datetime.date(2024, 1, 9),
                    "ticker": "AAPL",
                    "open": 101.0,
                    "high": 105.0,
                    "low": 100.0,
                    "close": 104.0,
                    "volume": 1100.0,
                    "transactions": 11,
                },
                {
                    "date": datetime.date(2024, 1, 10),
                    "ticker": "AAPL",
                    "open": 104.0,
                    "high": 106.0,
                    "low": 98.0,
                    "close": 99.0,
                    "volume": 1200.0,
                    "transactions": 12,
                },
                {
                    "date": datetime.date(2024, 1, 11),
                    "ticker": "AAPL",
                    "open": 99.0,
                    "high": 103.0,
                    "low": 97.0,
                    "close": 102.0,
                    "volume": 1300.0,
                    "transactions": 13,
                },
                {
                    "date": datetime.date(2024, 1, 12),
                    "ticker": "AAPL",
                    "open": 102.0,
                    "high": 104.0,
                    "low": 100.0,
                    "close": 103.0,
                    "volume": 1400.0,
                    "transactions": 14,
                },
            ]
        )

        row = aggregate_to_weekly(
            bars, collection_start=datetime.date(2024, 1, 1), target=datetime.date(2024, 12, 31)
        ).row(0, named=True)

        assert row["open"] == pytest.approx(100.0)
        assert row["high"] == pytest.approx(106.0)
        assert row["low"] == pytest.approx(97.0)
        assert row["close"] == pytest.approx(103.0)
        assert row["volume"] == pytest.approx(6000.0)
        assert row["transactions"] == EXPECTED_TRANSACTIONS
        assert row["date"] == datetime.date(2024, 1, 8)

    def test_date_is_week_start_monday(self):
        """Label each weekly aggregate with Monday's date."""
        bars = make_bars(
            [
                {
                    "date": datetime.date(2024, 1, 8),
                    "ticker": "AAPL",
                    "open": 100.0,
                    "high": 101.0,
                    "low": 99.0,
                    "close": 100.5,
                    "volume": 1000.0,
                    "transactions": 10,
                },
                {
                    "date": datetime.date(2024, 1, 9),
                    "ticker": "AAPL",
                    "open": 101.0,
                    "high": 102.0,
                    "low": 100.0,
                    "close": 101.5,
                    "volume": 1000.0,
                    "transactions": 10,
                },
                {
                    "date": datetime.date(2024, 1, 10),
                    "ticker": "AAPL",
                    "open": 102.0,
                    "high": 103.0,
                    "low": 101.0,
                    "close": 102.5,
                    "volume": 1000.0,
                    "transactions": 10,
                },
                {
                    "date": datetime.date(2024, 1, 11),
                    "ticker": "AAPL",
                    "open": 103.0,
                    "high": 104.0,
                    "low": 102.0,
                    "close": 103.5,
                    "volume": 1000.0,
                    "transactions": 10,
                },
            ]
        )

        row = aggregate_to_weekly(
            bars, collection_start=datetime.date(2024, 1, 1), target=datetime.date(2024, 12, 31)
        ).row(0, named=True)

        assert row["date"] == datetime.date(2024, 1, 8)

    def test_partial_midweek_start_labeled_monday(self):
        """Label a partial week with its calendar Monday."""
        bars = make_bars(
            [
                {
                    "date": datetime.date(2024, 1, 16),
                    "ticker": "AAPL",
                    "open": 100.0,
                    "high": 101.0,
                    "low": 99.0,
                    "close": 100.5,
                    "volume": 1000.0,
                    "transactions": 10,
                },
                {
                    "date": datetime.date(2024, 1, 17),
                    "ticker": "AAPL",
                    "open": 101.0,
                    "high": 102.0,
                    "low": 100.0,
                    "close": 101.5,
                    "volume": 1100.0,
                    "transactions": 11,
                },
                {
                    "date": datetime.date(2024, 1, 18),
                    "ticker": "AAPL",
                    "open": 102.0,
                    "high": 103.0,
                    "low": 101.0,
                    "close": 102.5,
                    "volume": 1200.0,
                    "transactions": 12,
                },
            ]
        )

        row = aggregate_to_weekly(
            bars, collection_start=datetime.date(2024, 1, 1), target=datetime.date(2024, 12, 31)
        ).row(0, named=True)

        assert row["date"] == datetime.date(2024, 1, 15)
        assert row["volume"] == pytest.approx(3300.0)

    def test_single_day_week(self):
        """Preserve values for a week with one trading day."""
        bars = make_bars(
            [
                {
                    "date": datetime.date(2024, 1, 8),
                    "ticker": "AAPL",
                    "open": 100.0,
                    "high": 105.0,
                    "low": 99.0,
                    "close": 104.0,
                    "volume": 1000.0,
                    "transactions": 10,
                }
            ]
        )

        row = aggregate_to_weekly(
            bars, collection_start=datetime.date(2024, 1, 1), target=datetime.date(2024, 12, 31)
        ).row(0, named=True)

        assert row["open"] == pytest.approx(100.0)
        assert row["high"] == pytest.approx(105.0)
        assert row["low"] == pytest.approx(99.0)
        assert row["close"] == pytest.approx(104.0)
        assert row["volume"] == pytest.approx(1000.0)
        assert row["transactions"] == EXPECTED_SINGLE_DAY_TRANSACTIONS
        assert row["date"] == datetime.date(2024, 1, 8)

    def test_per_ticker_isolation(self):
        """Aggregate each ticker independently."""
        bars = make_bars(
            [
                {
                    "date": datetime.date(2024, 1, 8),
                    "ticker": "AAPL",
                    "open": 100.0,
                    "high": 101.0,
                    "low": 99.0,
                    "close": 100.5,
                    "volume": 1000.0,
                    "transactions": 10,
                },
                {
                    "date": datetime.date(2024, 1, 9),
                    "ticker": "AAPL",
                    "open": 101.0,
                    "high": 102.0,
                    "low": 100.0,
                    "close": 101.5,
                    "volume": 1100.0,
                    "transactions": 11,
                },
                {
                    "date": datetime.date(2024, 1, 8),
                    "ticker": "MSFT",
                    "open": 200.0,
                    "high": 203.0,
                    "low": 199.0,
                    "close": 202.5,
                    "volume": 2000.0,
                    "transactions": 20,
                },
                {
                    "date": datetime.date(2024, 1, 9),
                    "ticker": "MSFT",
                    "open": 202.0,
                    "high": 204.0,
                    "low": 201.0,
                    "close": 203.5,
                    "volume": 2100.0,
                    "transactions": 21,
                },
            ]
        )

        result = aggregate_to_weekly(
            bars, collection_start=datetime.date(2024, 1, 1), target=datetime.date(2024, 12, 31)
        )
        aapl_row = result.filter(pl.col("ticker") == "AAPL").row(0, named=True)
        msft_row = result.filter(pl.col("ticker") == "MSFT").row(0, named=True)

        assert aapl_row["open"] == pytest.approx(100.0)
        assert aapl_row["close"] == pytest.approx(101.5)
        assert aapl_row["volume"] == pytest.approx(2100.0)
        assert msft_row["open"] == pytest.approx(200.0)
        assert msft_row["close"] == pytest.approx(203.5)
        assert msft_row["volume"] == pytest.approx(4100.0)

    def test_output_schema_matches_daily(self):
        """Keep weekly output columns and types aligned with daily bars."""
        bars = make_bars(
            [
                {
                    "date": datetime.date(2024, 1, 8),
                    "ticker": "AAPL",
                    "open": 100.0,
                    "high": 101.0,
                    "low": 99.0,
                    "close": 100.5,
                    "volume": 1000.0,
                    "transactions": 10,
                },
                {
                    "date": datetime.date(2024, 1, 9),
                    "ticker": "AAPL",
                    "open": 101.0,
                    "high": 102.0,
                    "low": 100.0,
                    "close": 101.5,
                    "volume": 1100.0,
                    "transactions": 11,
                },
            ]
        )

        result = aggregate_to_weekly(
            bars, collection_start=datetime.date(2024, 1, 1), target=datetime.date(2024, 12, 31)
        )

        assert result.columns == list(transform.PERIOD_AGGS_SCHEMA.keys())
        assert result.dtypes == list(transform.PERIOD_AGGS_SCHEMA.values())


def test_aggregate_to_monthly_values_and_last_trading_day():
    """Aggregate monthly values and retain the last trading date."""
    bars = make_bars(
        [
            {
                "date": datetime.date(2024, 1, 30),
                "ticker": "AAPL",
                "open": 100.0,
                "high": 102.0,
                "low": 99.0,
                "close": 101.0,
                "volume": 1000.0,
                "transactions": 10,
            },
            {
                "date": datetime.date(2024, 1, 31),
                "ticker": "AAPL",
                "open": 101.0,
                "high": 105.0,
                "low": 98.0,
                "close": 104.0,
                "volume": 1100.0,
                "transactions": 11,
            },
            {
                "date": datetime.date(2024, 2, 1),
                "ticker": "AAPL",
                "open": 104.0,
                "high": 106.0,
                "low": 103.0,
                "close": 105.0,
                "volume": 1200.0,
                "transactions": 12,
            },
        ]
    )

    result = aggregate_to_monthly(bars, collection_start=datetime.date(2024, 1, 1), target=datetime.date(2024, 12, 31))
    january = result.row(0, named=True)

    assert result.columns == list(transform.PERIOD_AGGS_SCHEMA.keys())
    assert january["date"] == datetime.date(2024, 1, 31)
    assert january["open"] == pytest.approx(100.0)
    assert january["high"] == pytest.approx(105.0)
    assert january["low"] == pytest.approx(98.0)
    assert january["close"] == pytest.approx(104.0)
    assert january["volume"] == pytest.approx(2100.0)
    assert january["transactions"] == EXPECTED_MONTH_TRANSACTIONS


def test_aggregate_to_period_empty_input_weekly_and_monthly():
    """Empty bars short-circuit to DAILY_AGGS_SCHEMA for both weekly and monthly."""
    empty_bars = pl.DataFrame(schema=BARS_SCHEMA)

    weekly = aggregate_to_weekly(
        empty_bars, collection_start=datetime.date(2024, 1, 1), target=datetime.date(2024, 12, 31)
    )
    monthly = aggregate_to_monthly(
        empty_bars, collection_start=datetime.date(2024, 1, 1), target=datetime.date(2024, 12, 31)
    )

    for result in (weekly, monthly):
        assert result.is_empty()
        assert result.columns == list(transform.PERIOD_AGGS_SCHEMA.keys())
        assert result.dtypes == list(transform.PERIOD_AGGS_SCHEMA.values())


class TestAggregateToMonthly:
    """Monthly aggregation behavior and schema guarantees."""

    def test_groups_by_calendar_month_with_actual_last_trading_day(self):
        """Group by month and retain the final observed trading date."""
        bars = make_bars(
            [
                {
                    "date": datetime.date(2024, 1, 30),
                    "ticker": "AAPL",
                    "open": 100.0,
                    "high": 102.0,
                    "low": 99.0,
                    "close": 101.0,
                    "volume": 1000.0,
                    "transactions": 10,
                },
                {
                    "date": datetime.date(2024, 1, 31),
                    "ticker": "AAPL",
                    "open": 101.0,
                    "high": 105.0,
                    "low": 100.0,
                    "close": 104.0,
                    "volume": 1100.0,
                    "transactions": 11,
                },
                {
                    "date": datetime.date(2024, 2, 1),
                    "ticker": "AAPL",
                    "open": 104.0,
                    "high": 106.0,
                    "low": 98.0,
                    "close": 99.0,
                    "volume": 1200.0,
                    "transactions": 12,
                },
            ]
        )

        result = aggregate_to_monthly(
            bars, collection_start=datetime.date(2024, 1, 1), target=datetime.date(2024, 12, 31)
        )

        assert result["date"].to_list() == [
            datetime.date(2024, 1, 31),
            datetime.date(2024, 2, 1),
        ]
        assert result["volume"].to_list() == pytest.approx([2100.0, 1200.0])
        assert result.columns == list(transform.PERIOD_AGGS_SCHEMA.keys())
        assert result.dtypes == list(transform.PERIOD_AGGS_SCHEMA.values())

    def test_empty_input(self):
        """Return the expected schema for empty monthly input."""
        result = aggregate_to_monthly(
            pl.DataFrame(schema=BARS_SCHEMA),
            collection_start=datetime.date(2024, 1, 1),
            target=datetime.date(2024, 12, 31),
        )

        assert result.is_empty()
        assert result.columns == list(transform.PERIOD_AGGS_SCHEMA.keys())
        assert result.dtypes == list(transform.PERIOD_AGGS_SCHEMA.values())


def test_adjust_splits_basic(sample_bars_df: pl.DataFrame, sample_splits_df: pl.DataFrame):
    """Adjust historical OHLCV values for stock splits."""
    result = adjust_splits(sample_bars_df, sample_splits_df)

    aapl_row = result.filter((pl.col("ticker") == "AAPL") & (pl.col("date") == datetime.date(2024, 1, 1))).row(
        0, named=True
    )

    assert aapl_row["open"] == pytest.approx(300.0)
    assert aapl_row["close"] == pytest.approx(303.0)
    assert aapl_row["volume"] == pytest.approx(500000.0)


def test_adjust_splits_same_day_not_adjusted():
    """Leave bars on the split execution date unchanged."""
    bars = make_bars(
        [
            {
                "date": datetime.date(2024, 8, 30),
                "ticker": "AAPL",
                "open": 500.0,
                "high": 510.0,
                "low": 495.0,
                "close": 505.0,
                "volume": 1000.0,
                "transactions": 100,
            },
            {
                "date": datetime.date(2024, 8, 31),
                "ticker": "AAPL",
                "open": 125.0,
                "high": 127.0,
                "low": 123.0,
                "close": 126.0,
                "volume": 4000.0,
                "transactions": 400,
            },
        ]
    )
    splits = make_splits(
        [
            {
                "ticker": "AAPL",
                "execution_date": datetime.date(2024, 8, 31),
                "split_from": 4.0,
                "split_to": 1.0,
                "adjustment_factor": 0.25,
                "adjustment_type": "split",
            }
        ]
    )

    result = adjust_splits(bars, splits)

    pre_split_row = result.filter(pl.col("date") == datetime.date(2024, 8, 30)).row(0, named=True)
    split_day_row = result.filter(pl.col("date") == datetime.date(2024, 8, 31)).row(0, named=True)

    assert pre_split_row["close"] == pytest.approx(126.25)
    assert pre_split_row["volume"] == pytest.approx(4000.0)
    assert split_day_row["open"] == pytest.approx(125.0)
    assert split_day_row["close"] == pytest.approx(126.0)
    assert split_day_row["volume"] == pytest.approx(4000.0)


def test_adjust_splits_no_split_unchanged():
    """Leave bars unchanged when there are no applicable splits."""
    bars = make_bars(
        [
            {
                "date": datetime.date(2024, 1, 10),
                "ticker": "GOOG",
                "open": 140.0,
                "high": 142.0,
                "low": 139.0,
                "close": 141.0,
                "volume": 2500.0,
                "transactions": 150,
            }
        ]
    )
    splits = make_splits(
        [
            {
                "ticker": "AAPL",
                "execution_date": datetime.date(2024, 2, 1),
                "split_from": 2.0,
                "split_to": 1.0,
                "adjustment_factor": 0.5,
                "adjustment_type": "split",
            }
        ]
    )

    result = adjust_splits(bars, splits)

    assert_frame_equal(result, bars.with_columns(pl.col("volume").cast(pl.Float64)))


def test_adjust_splits_aapl_4to1():
    """Apply a four-for-one split to historical bars."""
    bars = make_bars(
        [
            {
                "date": datetime.date(2020, 8, 28),
                "ticker": "AAPL",
                "open": 500.0,
                "high": 505.0,
                "low": 495.0,
                "close": 500.0,
                "volume": 1000.0,
                "transactions": 100,
            }
        ]
    )
    splits = make_splits(
        [
            {
                "ticker": "AAPL",
                "execution_date": datetime.date(2020, 8, 31),
                "split_from": 4.0,
                "split_to": 1.0,
                "adjustment_factor": 0.25,
                "adjustment_type": "split",
            }
        ]
    )

    row = adjust_splits(bars, splits).row(0, named=True)

    assert row["close"] == pytest.approx(125.0)
    assert row["volume"] == pytest.approx(4000.0)


def test_adjust_splits_reverse_split():
    """Apply a reverse split to historical bars."""
    bars = make_bars(
        [
            {
                "date": datetime.date(2024, 5, 30),
                "ticker": "UVXY",
                "open": 50.0,
                "high": 52.0,
                "low": 49.0,
                "close": 50.0,
                "volume": 1000.0,
                "transactions": 50,
            }
        ]
    )
    splits = make_splits(
        [
            {
                "ticker": "UVXY",
                "execution_date": datetime.date(2024, 6, 1),
                "split_from": 1.0,
                "split_to": 2.0,
                "adjustment_factor": 2.0,
                "adjustment_type": "reverse_split",
            }
        ]
    )

    row = adjust_splits(bars, splits).row(0, named=True)

    assert row["close"] == pytest.approx(100.0)
    assert row["volume"] == pytest.approx(500.0)


def test_adjust_splits_multiple_tickers():
    """Apply each ticker's split only to its own bars."""
    bars = make_bars(
        [
            {
                "date": datetime.date(2024, 1, 10),
                "ticker": "AAPL",
                "open": 400.0,
                "high": 404.0,
                "low": 398.0,
                "close": 402.0,
                "volume": 1000.0,
                "transactions": 30,
            },
            {
                "date": datetime.date(2024, 1, 10),
                "ticker": "MSFT",
                "open": 50.0,
                "high": 51.0,
                "low": 49.0,
                "close": 50.0,
                "volume": 2000.0,
                "transactions": 40,
            },
        ]
    )
    splits = make_splits(
        [
            {
                "ticker": "AAPL",
                "execution_date": datetime.date(2024, 1, 11),
                "split_from": 4.0,
                "split_to": 1.0,
                "adjustment_factor": 0.25,
                "adjustment_type": "split",
            },
            {
                "ticker": "MSFT",
                "execution_date": datetime.date(2024, 1, 11),
                "split_from": 1.0,
                "split_to": 2.0,
                "adjustment_factor": 2.0,
                "adjustment_type": "reverse_split",
            },
        ]
    )

    result = adjust_splits(bars, splits)
    aapl_row = result.filter(pl.col("ticker") == "AAPL").row(0, named=True)
    msft_row = result.filter(pl.col("ticker") == "MSFT").row(0, named=True)

    assert aapl_row["close"] == pytest.approx(100.5)
    assert aapl_row["volume"] == pytest.approx(4000.0)
    assert msft_row["close"] == pytest.approx(100.0)
    assert msft_row["volume"] == pytest.approx(1000.0)


@pytest.mark.parametrize(
    ("ticker", "splits_data", "checks"),
    [
        pytest.param(
            "ANET",
            [
                (datetime.date(2021, 11, 18), 0.0625),
                (datetime.date(2024, 12, 4), 0.25),
            ],
            [
                (datetime.date(2021, 10, 15), 400.0, 1000.0, 25.0, 16000.0),
                (datetime.date(2021, 12, 17), 136.0, 2000.0, 34.0, 8000.0),
                (datetime.date(2025, 1, 10), 102.0, 3000.0, 102.0, 3000.0),
            ],
            id="ANET-two-4to1",
        ),
        pytest.param(
            "NFLX",
            [
                (datetime.date(2015, 7, 15), 1.0 / 70.0),
                (datetime.date(2025, 11, 17), 0.1),
            ],
            [
                (datetime.date(2015, 6, 1), 700.0, 7000.0, 10.0, 490000.0),
                (datetime.date(2020, 1, 10), 350.0, 5000.0, 35.0, 50000.0),
                (datetime.date(2026, 1, 10), 90.0, 10000.0, 90.0, 10000.0),
            ],
            id="NFLX-7to1-then-10to1",
        ),
        pytest.param(
            "NVDA",
            [
                (datetime.date(2021, 7, 20), 0.025),
                (datetime.date(2024, 6, 10), 0.1),
            ],
            [
                (datetime.date(2021, 6, 1), 800.0, 1000.0, 20.0, 40000.0),
                (datetime.date(2023, 1, 10), 150.0, 2000.0, 15.0, 20000.0),
                (datetime.date(2025, 1, 10), 140.0, 3000.0, 140.0, 3000.0),
            ],
            id="NVDA-4to1-then-10to1",
        ),
        pytest.param(
            "NOW",
            [
                (datetime.date(2025, 12, 18), 0.2),
            ],
            [
                (datetime.date(2025, 12, 1), 780.0, 1000.0, 156.0, 5000.0),
                (datetime.date(2026, 1, 10), 155.0, 2000.0, 155.0, 2000.0),
            ],
            id="NOW-5to1",
        ),
        pytest.param(
            "TPL",
            [
                (datetime.date(2024, 3, 27), 1.0 / 9.0),
                (datetime.date(2025, 12, 23), 1.0 / 3.0),
            ],
            [
                (datetime.date(2024, 1, 10), 900.0, 1000.0, 100.0, 9000.0),
                (datetime.date(2025, 6, 10), 450.0, 2000.0, 150.0, 6000.0),
                (datetime.date(2026, 1, 10), 290.0, 3000.0, 290.0, 3000.0),
            ],
            id="TPL-two-3to1",
        ),
    ],
)
def test_adjust_splits_multi_split_spot_check(ticker, splits_data, checks):
    """Spot check real tickers with multiple splits using cumulative API factors.

    The Massive API returns cumulative adjustment factors: the earliest split's
    factor already includes all later splits. Each case has bars before both
    splits, between splits, and after both splits. checks tuples are
    (date, raw_close, raw_volume, expected_close, expected_volume).
    """
    bars = make_bars(
        [
            {
                "date": date,
                "ticker": ticker,
                "open": close,
                "high": close + 5.0,
                "low": close - 5.0,
                "close": close,
                "volume": volume,
                "transactions": 100,
            }
            for date, close, volume, _, _ in checks
        ]
    )
    splits = make_splits(
        [
            {
                "ticker": ticker,
                "execution_date": exec_date,
                "split_from": 1.0,
                "split_to": 1.0,
                "adjustment_factor": factor,
                "adjustment_type": "split",
            }
            for exec_date, factor in splits_data
        ]
    )

    result = adjust_splits(bars, splits)

    for date, _, _, expected_close, expected_volume in checks:
        row = result.filter(pl.col("date") == date).row(0, named=True)
        assert row["close"] == pytest.approx(expected_close, rel=1e-4)
        assert row["volume"] == pytest.approx(expected_volume, rel=1e-4)


def test_adjust_splits_empty_splits(sample_bars_df: pl.DataFrame, sample_splits_df: pl.DataFrame):
    """Preserve bars when the splits frame is empty."""
    result = adjust_splits(sample_bars_df, sample_splits_df.head(0))

    assert_frame_equal(result, sample_bars_df.with_columns(pl.col("volume").cast(pl.Float64)))


def test_filter_tickers_keeps_matching(sample_bars_df: pl.DataFrame, sample_tickers_df: pl.DataFrame):
    """Keep bars for tickers present in metadata."""
    result = filter_tickers(sample_bars_df, sample_tickers_df)

    assert set(result["ticker"].unique()) == {"AAPL", "MSFT"}
    assert len(result) == len(sample_bars_df)


def test_filter_tickers_removes_unknown(sample_tickers_df: pl.DataFrame):
    """Remove bars for tickers absent from metadata."""
    bars = make_bars(
        [
            {
                "date": datetime.date(2024, 1, 1),
                "ticker": "AAPL",
                "open": 100.0,
                "high": 101.0,
                "low": 99.0,
                "close": 100.5,
                "volume": 1000.0,
                "transactions": 10,
            },
            {
                "date": datetime.date(2024, 1, 1),
                "ticker": "ZZZZ",
                "open": 10.0,
                "high": 11.0,
                "low": 9.0,
                "close": 10.5,
                "volume": 500.0,
                "transactions": 5,
            },
        ]
    )

    result = filter_tickers(bars, sample_tickers_df)

    assert result["ticker"].to_list() == ["AAPL"]
