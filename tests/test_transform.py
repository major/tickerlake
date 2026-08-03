import datetime
import importlib

import polars as pl
import pytest
from polars.testing import assert_frame_equal

transform = importlib.import_module("tickerlake.transform")
extract = importlib.import_module("tickerlake.extract")
adjust_splits = transform.adjust_splits
compute_metrics = transform.compute_metrics
filter_tickers = transform.filter_tickers
_compute_atr = transform._compute_atr
_compute_adr_pct = transform._compute_adr_pct
aggregate_to_weekly = transform.aggregate_to_weekly
aggregate_to_monthly = transform.aggregate_to_monthly
bars_for_timeframe = transform.bars_for_timeframe
find_pivots = transform.find_pivots
DAILY_AGGS_SCHEMA = extract.DAILY_AGGS_SCHEMA


BARS_SCHEMA = {
    "date": pl.Date,
    "ticker": pl.Utf8,
    "open": pl.Float32,
    "high": pl.Float32,
    "low": pl.Float32,
    "close": pl.Float32,
    "volume": pl.Float32,
    "vwap": pl.Float32,
    "transactions": pl.UInt32,
}

SPLITS_SCHEMA = {
    "ticker": pl.Utf8,
    "execution_date": pl.Date,
    "split_from": pl.Float32,
    "split_to": pl.Float32,
    "adjustment_factor": pl.Float64,
    "adjustment_type": pl.Utf8,
}


def make_bars(rows: list[dict]) -> pl.DataFrame:
    return pl.DataFrame(rows, schema=BARS_SCHEMA)


def make_splits(rows: list[dict]) -> pl.DataFrame:
    return pl.DataFrame(rows, schema=SPLITS_SCHEMA)


def make_metric_bars(
    ticker_to_closes: dict[str, list[float]],
    start_date: datetime.date = datetime.date(2024, 1, 1),
) -> pl.DataFrame:
    rows = []
    for ticker, closes in ticker_to_closes.items():
        for index, close in enumerate(closes):
            rows.append(
                {
                    "date": start_date + datetime.timedelta(days=index),
                    "ticker": ticker,
                    "open": float(close),
                    "high": float(close),
                    "low": float(close),
                    "close": float(close),
                    "volume": 1000.0,
                    "vwap": float(close),
                    "transactions": 100,
                }
            )
    return make_bars(rows)


class TestAggregateToWeekly:
    def test_basic_aggregation(self):
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
                        "vwap": price + 0.25,
                        "transactions": 100 + i,
                    }
                )

        result = aggregate_to_weekly(make_bars(rows))

        assert len(result) == 6
        per_ticker_counts = result.group_by("ticker").len().sort("ticker")
        assert per_ticker_counts["len"].to_list() == [3, 3]

    def test_ohlcv_rollup_values(self):
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
                    "vwap": 100.7,
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
                    "vwap": 103.1,
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
                    "vwap": 100.2,
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
                    "vwap": 101.5,
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
                    "vwap": 102.8,
                    "transactions": 14,
                },
            ]
        )

        row = aggregate_to_weekly(bars).row(0, named=True)

        assert row["open"] == pytest.approx(100.0)
        assert row["high"] == pytest.approx(106.0)
        assert row["low"] == pytest.approx(97.0)
        assert row["close"] == pytest.approx(103.0)
        assert row["volume"] == pytest.approx(6000.0)
        expected_vwap = (
            (100.7 * 1000.0)
            + (103.1 * 1100.0)
            + (100.2 * 1200.0)
            + (101.5 * 1300.0)
            + (102.8 * 1400.0)
        ) / 6000.0
        assert row["vwap"] == pytest.approx(expected_vwap)
        assert row["transactions"] == 60
        assert row["date"] == datetime.date(2024, 1, 12)

    def test_date_is_last_trading_day(self):
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
                    "vwap": 100.2,
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
                    "vwap": 101.2,
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
                    "vwap": 102.2,
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
                    "vwap": 103.2,
                    "transactions": 10,
                },
            ]
        )

        row = aggregate_to_weekly(bars).row(0, named=True)

        assert row["date"] == datetime.date(2024, 1, 11)

    def test_partial_week_included(self):
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
                    "vwap": 100.2,
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
                    "vwap": 101.2,
                    "transactions": 11,
                },
                {
                    "date": datetime.date(2024, 1, 10),
                    "ticker": "AAPL",
                    "open": 102.0,
                    "high": 103.0,
                    "low": 101.0,
                    "close": 102.5,
                    "volume": 1200.0,
                    "vwap": 102.2,
                    "transactions": 12,
                },
            ]
        )

        row = aggregate_to_weekly(bars).row(0, named=True)

        assert row["date"] == datetime.date(2024, 1, 10)
        assert row["volume"] == pytest.approx(3300.0)

    def test_single_day_week(self):
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
                    "vwap": 103.0,
                    "transactions": 10,
                }
            ]
        )

        row = aggregate_to_weekly(bars).row(0, named=True)

        assert row["open"] == pytest.approx(100.0)
        assert row["high"] == pytest.approx(105.0)
        assert row["low"] == pytest.approx(99.0)
        assert row["close"] == pytest.approx(104.0)
        assert row["volume"] == pytest.approx(1000.0)
        assert row["vwap"] == pytest.approx(103.0)
        assert row["transactions"] == 10
        assert row["date"] == datetime.date(2024, 1, 8)

    def test_per_ticker_isolation(self):
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
                    "vwap": 100.2,
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
                    "vwap": 101.2,
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
                    "vwap": 201.2,
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
                    "vwap": 202.2,
                    "transactions": 21,
                },
            ]
        )

        result = aggregate_to_weekly(bars)
        aapl_row = result.filter(pl.col("ticker") == "AAPL").row(0, named=True)
        msft_row = result.filter(pl.col("ticker") == "MSFT").row(0, named=True)

        assert aapl_row["open"] == pytest.approx(100.0)
        assert aapl_row["close"] == pytest.approx(101.5)
        assert aapl_row["volume"] == pytest.approx(2100.0)
        assert msft_row["open"] == pytest.approx(200.0)
        assert msft_row["close"] == pytest.approx(203.5)
        assert msft_row["volume"] == pytest.approx(4100.0)

    def test_output_schema_matches_daily(self):
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
                    "vwap": 100.2,
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
                    "vwap": 101.2,
                    "transactions": 11,
                },
            ]
        )

        result = aggregate_to_weekly(bars)

        assert result.columns == list(DAILY_AGGS_SCHEMA.keys())
        assert result.dtypes == list(DAILY_AGGS_SCHEMA.values())


def test_aggregate_to_monthly_values_and_last_trading_day():
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
                "vwap": 100.5,
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
                "vwap": 103.5,
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
                "vwap": 104.5,
                "transactions": 12,
            },
        ]
    )

    result = aggregate_to_monthly(bars)
    january = result.row(0, named=True)
    expected_vwap = (100.5 * 1000.0 + 103.5 * 1100.0) / 2100.0

    assert result.columns == list(DAILY_AGGS_SCHEMA.keys())
    assert january["date"] == datetime.date(2024, 1, 31)
    assert january["open"] == pytest.approx(100.0)
    assert january["high"] == pytest.approx(105.0)
    assert january["low"] == pytest.approx(98.0)
    assert january["close"] == pytest.approx(104.0)
    assert january["volume"] == pytest.approx(2100.0)
    assert january["vwap"] == pytest.approx(expected_vwap)
    assert january["transactions"] == 21


def test_aggregate_to_period_empty_input_weekly_and_monthly():
    """Empty bars short-circuit to DAILY_AGGS_SCHEMA for both weekly and monthly."""
    empty_bars = pl.DataFrame(schema=BARS_SCHEMA)

    weekly = aggregate_to_weekly(empty_bars)
    monthly = aggregate_to_monthly(empty_bars)

    for result in (weekly, monthly):
        assert result.is_empty()
        assert result.columns == list(DAILY_AGGS_SCHEMA.keys())
        assert result.dtypes == list(DAILY_AGGS_SCHEMA.values())


def test_bars_for_timeframe_daily_weekly_monthly():
    bars = make_metric_bars(
        {"AAPL": [1.0, 2.0, 3.0, 4.0, 5.0, 6.0]},
        datetime.date(2024, 1, 29),
    )

    daily = bars_for_timeframe(bars.reverse(), "daily")
    weekly = bars_for_timeframe(bars, "weekly")
    monthly = bars_for_timeframe(bars, "monthly")

    assert daily["date"].to_list() == sorted(bars["date"].to_list())
    assert weekly["date"].to_list() == [datetime.date(2024, 2, 3)]
    assert monthly["date"].to_list() == [
        datetime.date(2024, 1, 31),
        datetime.date(2024, 2, 3),
    ]


def test_bars_for_timeframe_invalid():
    with pytest.raises(ValueError, match="timeframe"):
        bars_for_timeframe(pl.DataFrame(schema=BARS_SCHEMA), "yearly")


def test_find_pivots_high_low_plateau_and_confirmation():
    bars = make_ohlc_bars(
        {
            "AAPL": [
                (10.0, 10.0, 5.0, 10.0),
                (11.0, 12.0, 4.0, 11.0),
                (12.0, 15.0, 6.0, 12.0),
                (12.0, 15.0, 7.0, 12.0),
                (11.0, 11.0, 3.0, 11.0),
                (10.0, 10.0, 4.0, 10.0),
                (9.0, 9.0, 5.0, 9.0),
            ]
        }
    )

    result = find_pivots(bars, k=1)

    assert result.to_dicts() == [
        {
            "date": datetime.date(2024, 1, 2),
            "ticker": "AAPL",
            "pivot_type": "low",
            "price": 4.0,
            "confirmed_at": datetime.date(2024, 1, 3),
        },
        {
            "date": datetime.date(2024, 1, 3),
            "ticker": "AAPL",
            "pivot_type": "high",
            "price": 15.0,
            "confirmed_at": datetime.date(2024, 1, 4),
        },
        {
            "date": datetime.date(2024, 1, 5),
            "ticker": "AAPL",
            "pivot_type": "low",
            "price": 3.0,
            "confirmed_at": datetime.date(2024, 1, 6),
        },
    ]


def test_find_pivots_edges_remain_unknown():
    bars = make_ohlc_bars(
        {
            "AAPL": [
                (1.0, 100.0, 0.0, 1.0),
                (1.0, 1.0, 1.0, 1.0),
                (1.0, 99.0, -1.0, 1.0),
            ]
        }
    )

    result = find_pivots(bars, k=1)

    assert result.is_empty()


def test_find_pivots_k2_first_and_last_two_bars_never_emitted():
    bars = make_ohlc_bars(
        {
            "AAPL": [
                (1.0, 100.0, 0.0, 1.0),
                (1.0, 9.0, 1.0, 1.0),
                (1.0, 5.0, 5.0, 1.0),
                (1.0, 20.0, 2.0, 1.0),
                (1.0, 6.0, 6.0, 1.0),
                (1.0, 8.0, -1.0, 1.0),
                (1.0, 101.0, -2.0, 1.0),
            ]
        }
    )

    result = find_pivots(bars, k=2)

    assert result["date"].to_list() == [datetime.date(2024, 1, 4)]
    assert result["confirmed_at"].null_count() == 0


def test_find_pivots_invalid_k():
    with pytest.raises(ValueError, match="k"):
        find_pivots(pl.DataFrame(schema=BARS_SCHEMA), k=0)


def test_find_pivots_empty_input():
    result = find_pivots(pl.DataFrame(schema=BARS_SCHEMA), k=2)

    assert result.is_empty()
    assert result.columns == ["date", "ticker", "pivot_type", "price", "confirmed_at"]
    assert result.dtypes == [pl.Date, pl.Utf8, pl.Utf8, pl.Float32, pl.Date]


class TestAggregateToMonthly:
    def test_groups_by_calendar_month_with_actual_last_trading_day(self):
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
                    "vwap": 100.7,
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
                    "vwap": 103.1,
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
                    "vwap": 100.2,
                    "transactions": 12,
                },
            ]
        )

        result = aggregate_to_monthly(bars)

        assert result["date"].to_list() == [
            datetime.date(2024, 1, 31),
            datetime.date(2024, 2, 1),
        ]
        assert result["volume"].to_list() == pytest.approx([2100.0, 1200.0])
        expected_january_vwap = ((100.7 * 1000.0) + (103.1 * 1100.0)) / 2100.0
        assert result["vwap"].to_list() == pytest.approx([expected_january_vwap, 100.2])
        assert result.columns == list(DAILY_AGGS_SCHEMA.keys())
        assert result.dtypes == list(DAILY_AGGS_SCHEMA.values())

    def test_empty_input(self):
        result = aggregate_to_monthly(pl.DataFrame(schema=BARS_SCHEMA))

        assert result.is_empty()
        assert result.columns == list(DAILY_AGGS_SCHEMA.keys())
        assert result.dtypes == list(DAILY_AGGS_SCHEMA.values())


def test_adjust_splits_basic(
    sample_bars_df: pl.DataFrame, sample_splits_df: pl.DataFrame
):
    result = adjust_splits(sample_bars_df, sample_splits_df)

    aapl_row = result.filter(
        (pl.col("ticker") == "AAPL") & (pl.col("date") == datetime.date(2024, 1, 1))
    ).row(0, named=True)

    assert aapl_row["open"] == pytest.approx(300.0)
    assert aapl_row["close"] == pytest.approx(303.0)
    assert aapl_row["vwap"] == pytest.approx(302.4)
    assert aapl_row["volume"] == pytest.approx(500000.0)


def test_adjust_splits_same_day_not_adjusted():
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
                "vwap": 502.0,
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
                "vwap": 125.5,
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

    pre_split_row = result.filter(pl.col("date") == datetime.date(2024, 8, 30)).row(
        0, named=True
    )
    split_day_row = result.filter(pl.col("date") == datetime.date(2024, 8, 31)).row(
        0, named=True
    )

    assert pre_split_row["close"] == pytest.approx(126.25)
    assert pre_split_row["volume"] == pytest.approx(4000.0)
    assert split_day_row["open"] == pytest.approx(125.0)
    assert split_day_row["close"] == pytest.approx(126.0)
    assert split_day_row["volume"] == pytest.approx(4000.0)
    assert split_day_row["vwap"] == pytest.approx(125.5)


def test_adjust_splits_no_split_unchanged():
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
                "vwap": 140.5,
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

    assert_frame_equal(result, bars)


def test_adjust_splits_aapl_4to1():
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
                "vwap": 499.0,
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
                "vwap": 50.5,
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


def test_adjust_splits_vwap_adjusted():
    bars = make_bars(
        [
            {
                "date": datetime.date(2024, 3, 14),
                "ticker": "AAPL",
                "open": 100.0,
                "high": 104.0,
                "low": 98.0,
                "close": 102.0,
                "volume": 1000.0,
                "vwap": 101.5,
                "transactions": 20,
            }
        ]
    )
    splits = make_splits(
        [
            {
                "ticker": "AAPL",
                "execution_date": datetime.date(2024, 3, 15),
                "split_from": 2.0,
                "split_to": 1.0,
                "adjustment_factor": 0.5,
                "adjustment_type": "split",
            }
        ]
    )

    row = adjust_splits(bars, splits).row(0, named=True)

    assert row["vwap"] == pytest.approx(50.75)


def test_adjust_splits_multiple_tickers():
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
                "vwap": 401.0,
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
                "vwap": 50.5,
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
                "vwap": close,
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


def test_adjust_splits_empty_splits(
    sample_bars_df: pl.DataFrame, sample_splits_df: pl.DataFrame
):
    result = adjust_splits(sample_bars_df, sample_splits_df.head(0))

    assert_frame_equal(result, sample_bars_df)


def test_filter_tickers_keeps_matching(
    sample_bars_df: pl.DataFrame, sample_tickers_df: pl.DataFrame
):
    result = filter_tickers(sample_bars_df, sample_tickers_df)

    assert set(result["ticker"].unique()) == {"AAPL", "MSFT"}
    assert len(result) == len(sample_bars_df)


def test_filter_tickers_removes_unknown(sample_tickers_df: pl.DataFrame):
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
                "vwap": 100.2,
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
                "vwap": 10.2,
                "transactions": 5,
            },
        ]
    )

    result = filter_tickers(bars, sample_tickers_df)

    assert result["ticker"].to_list() == ["AAPL"]


def test_compute_metrics_sma20_correct():
    bars = make_metric_bars({"AAPL": [float(i) for i in range(1, 31)]})

    result = compute_metrics(bars)
    row = result.filter(pl.col("date") == datetime.date(2024, 1, 20)).row(0, named=True)

    assert row["sma_20"] == pytest.approx(10.5)


def test_compute_metrics_sma50_correct():
    bars = make_metric_bars({"AAPL": [float(i) for i in range(1, 61)]})

    result = compute_metrics(bars)
    row = result.filter(pl.col("date") == datetime.date(2024, 2, 19)).row(0, named=True)

    assert row["sma_50"] == pytest.approx(25.5)


def test_compute_metrics_sma200_correct():
    bars = make_metric_bars({"AAPL": [float(i) for i in range(1, 251)]})

    result = compute_metrics(bars)
    row = result.filter(pl.col("date") == datetime.date(2024, 7, 18)).row(0, named=True)

    assert row["sma_200"] == pytest.approx(100.5)


def test_compute_metrics_sma20_null_count():
    bars = make_metric_bars(
        {
            "AAPL": [100.0] * 250,
            "MSFT": [200.0] * 250,
        }
    )

    result = compute_metrics(bars)
    null_counts = result.group_by("ticker").agg(
        pl.col("sma_20").null_count().alias("nulls")
    )

    assert null_counts.sort("ticker")["nulls"].to_list() == [19, 19]


def test_compute_metrics_sma50_null_count():
    bars = make_metric_bars(
        {
            "AAPL": [100.0] * 250,
            "MSFT": [200.0] * 250,
        }
    )

    result = compute_metrics(bars)
    null_counts = result.group_by("ticker").agg(
        pl.col("sma_50").null_count().alias("nulls")
    )

    assert null_counts.sort("ticker")["nulls"].to_list() == [49, 49]


def test_compute_metrics_sma200_null_count():
    bars = make_metric_bars(
        {
            "AAPL": [100.0] * 250,
            "MSFT": [200.0] * 250,
        }
    )

    result = compute_metrics(bars)
    null_counts = result.group_by("ticker").agg(
        pl.col("sma_200").null_count().alias("nulls")
    )

    assert null_counts.sort("ticker")["nulls"].to_list() == [199, 199]


def test_compute_metrics_per_ticker():
    bars = make_metric_bars(
        {
            "AAPL": [10.0] * 60,
            "MSFT": [20.0] * 60,
        }
    )

    result = compute_metrics(bars)
    aapl_row = result.filter(
        (pl.col("ticker") == "AAPL") & (pl.col("date") == datetime.date(2024, 2, 19))
    ).row(0, named=True)
    msft_row = result.filter(
        (pl.col("ticker") == "MSFT") & (pl.col("date") == datetime.date(2024, 2, 19))
    ).row(0, named=True)

    assert aapl_row["sma_20"] == pytest.approx(10.0)
    assert msft_row["sma_20"] == pytest.approx(20.0)
    assert aapl_row["sma_50"] == pytest.approx(10.0)
    assert msft_row["sma_50"] == pytest.approx(20.0)


def test_compute_metrics_output_columns():
    bars = make_metric_bars({"AAPL": [100.0] * 250})

    result = compute_metrics(bars)

    assert result.columns == [
        "date",
        "ticker",
        "sma_20",
        "sma_50",
        "sma_200",
        "atr_14",
        "atr_pct",
        "adr_pct",
        "volume_sma_20",
    ]


def make_ohlc_bars(
    ticker_to_ohlc: dict[str, list[tuple[float, float, float, float]]],
    start_date: datetime.date = datetime.date(2024, 1, 1),
) -> pl.DataFrame:
    """Build a bars DataFrame from per-ticker (open, high, low, close) tuples.

    Each tuple maps to one trading day. Volume is fixed at 1000.0, vwap equals
    close, and transactions is fixed at 100.
    """
    rows = []
    for ticker, ohlc_list in ticker_to_ohlc.items():
        for index, (open_, high, low, close) in enumerate(ohlc_list):
            rows.append(
                {
                    "date": start_date + datetime.timedelta(days=index),
                    "ticker": ticker,
                    "open": float(open_),
                    "high": float(high),
                    "low": float(low),
                    "close": float(close),
                    "volume": 1000.0,
                    "vwap": float(close),
                    "transactions": 100,
                }
            )
    return make_bars(rows)


def test_compute_atr_basic():
    """ATR(14) equals the simple mean of True Range over 14 bars.

    With constant high=102, low=98, close=100 (and first bar close=100 too),
    TR is always 4.0. After 14 bars the rolling mean is 4.0.
    """
    # 20 bars: first bar sets prev_close baseline, rest have TR=4 always
    ohlc = [(100.0, 102.0, 98.0, 100.0)] * 20
    bars = make_ohlc_bars({"AAPL": ohlc})

    result = _compute_atr(bars)

    # Row 13 (0-indexed) is the 14th bar — first non-null ATR
    # date = 2024-01-01 + 13 days = 2024-01-14
    row = result.filter(pl.col("date") == datetime.date(2024, 1, 14)).row(0, named=True)
    assert row["atr_14"] == pytest.approx(4.0, abs=1e-4)


def test_compute_atr_null_count():
    """ATR(14) produces exactly 13 nulls per ticker (rolling_mean needs 14 values)."""
    ohlc = [(100.0, 102.0, 98.0, 100.0)] * 30
    bars = make_ohlc_bars({"AAPL": ohlc, "MSFT": ohlc})

    result = _compute_atr(bars)
    null_counts = result.group_by("ticker").agg(
        pl.col("atr_14").null_count().alias("nulls")
    )

    assert null_counts.sort("ticker")["nulls"].to_list() == [13, 13]


def test_compute_atr_per_ticker_isolation():
    """ATR values for different tickers must not bleed into each other."""
    # AAPL: high-low range = 4, MSFT: high-low range = 2
    # Constant close so prev_close legs are zero — TR = high - low
    aapl_ohlc = [(100.0, 102.0, 98.0, 100.0)] * 20
    msft_ohlc = [(100.0, 101.0, 99.0, 100.0)] * 20
    bars = make_ohlc_bars({"AAPL": aapl_ohlc, "MSFT": msft_ohlc})

    result = _compute_atr(bars)

    aapl_row = result.filter(
        (pl.col("ticker") == "AAPL") & (pl.col("date") == datetime.date(2024, 1, 14))
    ).row(0, named=True)
    msft_row = result.filter(
        (pl.col("ticker") == "MSFT") & (pl.col("date") == datetime.date(2024, 1, 14))
    ).row(0, named=True)

    assert aapl_row["atr_14"] == pytest.approx(4.0, abs=1e-4)
    assert msft_row["atr_14"] == pytest.approx(2.0, abs=1e-4)


def test_compute_atr_flat_price():
    """ATR is 0.0 (not null) after warmup when all OHLC values are identical."""
    ohlc = [(100.0, 100.0, 100.0, 100.0)] * 20
    bars = make_ohlc_bars({"AAPL": ohlc})

    result = _compute_atr(bars)

    # After warmup, every ATR value should be 0.0
    non_null = result.filter(pl.col("atr_14").is_not_null())
    assert non_null["atr_14"].to_list() == pytest.approx([0.0] * 7, abs=1e-6)


def _make_compounding_ohlc(
    start_price: float, daily_pct: float, n_days: int
) -> list[tuple[float, float, float, float]]:
    """Build flat-candle OHLC tuples with compounding daily returns.

    Each day's close = prev_close * (1 + daily_pct). open=high=low=close.
    """
    closes = [start_price]
    for _ in range(n_days - 1):
        closes.append(closes[-1] * (1 + daily_pct))
    return [(c, c, c, c) for c in closes]


def _make_constant_ohlc(
    start_price: float, daily_change: float, spread: float, n_days: int
) -> list[tuple[float, float, float, float]]:
    """Build OHLC tuples with constant spread and linear daily price change.

    Each day: close = prev_close + daily_change, high = close + spread,
    low = close - spread. This gives a constant True Range of 2 * spread,
    so ATR(14) stabilizes at 2 * spread after warmup.
    """
    closes = [start_price]
    for _ in range(n_days - 1):
        closes.append(closes[-1] + daily_change)
    return [(c, c + spread, c - spread, c) for c in closes]


def test_compute_metrics_no_spy():
    """When SPY is absent, atr_14 is still computed (independent of SPY).

    atr_14 must be non-null after warmup — it doesn't depend on SPY.
    sma_20, sma_50 and sma_200 are also unaffected.
    """
    n = 100
    aapl_ohlc = _make_constant_ohlc(100.0, 1.0, 1.0, n)
    msft_ohlc = _make_constant_ohlc(200.0, 1.0, 1.0, n)
    bars = make_ohlc_bars({"AAPL": aapl_ohlc, "MSFT": msft_ohlc})

    result = compute_metrics(bars)

    # ATR must be computed (not all-null) — independent of SPY
    assert result["atr_14"].null_count() < len(result)

    # volume_sma_20 must be computed (not all-null) — independent of SPY
    assert result["volume_sma_20"].null_count() < len(result)

    # SMA columns still work
    aapl_late = result.filter(
        (pl.col("ticker") == "AAPL")
        & (pl.col("date") == datetime.date(2024, 1, 1) + datetime.timedelta(days=99))
    ).row(0, named=True)
    assert aapl_late["sma_20"] is not None
    assert aapl_late["sma_50"] is not None


def test_compute_metrics_atr_pct_correct():
    """atr_pct = atr_14 / close. With constant OHLC (100, 102, 98, 100), TR=4.0 always.

    After 14-bar ATR warmup: atr_14=4.0, close=100 → atr_pct = 4.0/100.0 = 0.04.
    """
    ohlc = [(100.0, 102.0, 98.0, 100.0)] * 20
    bars = make_ohlc_bars({"AAPL": ohlc})

    result = compute_metrics(bars)

    # Row 13 (0-indexed) = 14th bar = first non-null ATR = date 2024-01-14
    row = result.filter(pl.col("date") == datetime.date(2024, 1, 14)).row(0, named=True)
    assert row["atr_pct"] == pytest.approx(0.04, abs=1e-4)


def test_compute_metrics_atr_pct_null_count():
    """atr_pct inherits ATR(14)'s 13-row warmup — exactly 13 leading nulls per ticker."""  # noqa: E501
    ohlc = [(100.0, 102.0, 98.0, 100.0)] * 30
    bars = make_ohlc_bars({"AAPL": ohlc, "MSFT": ohlc})

    result = compute_metrics(bars)
    null_counts = result.group_by("ticker").agg(
        pl.col("atr_pct").null_count().alias("nulls")
    )

    assert null_counts.sort("ticker")["nulls"].to_list() == [13, 13]


def test_compute_metrics_atr_pct_no_spy():
    """atr_pct is computed even when SPY is absent (independent of SPY)."""
    n = 30
    aapl_ohlc = _make_constant_ohlc(100.0, 1.0, 1.0, n)
    bars = make_ohlc_bars({"AAPL": aapl_ohlc})

    result = compute_metrics(bars)

    # After ATR warmup (row 13+), atr_pct must be non-null
    after_warmup = result.filter(
        (pl.col("ticker") == "AAPL") & pl.col("atr_14").is_not_null()
    )
    assert len(after_warmup) > 0
    assert after_warmup["atr_pct"].null_count() == 0


def test_compute_metrics_atr_pct_per_ticker():
    """atr_pct is computed independently per ticker.

    Different spreads yield different values.

    AAPL: spread=2, close=100 → TR=4 → atr_14=4 → atr_pct=4/100=0.04.
    MSFT: spread=1, close=100 → TR=2 → atr_14=2 → atr_pct=2/100=0.02.
    AAPL's atr_pct should be approximately 2x MSFT's.
    """
    aapl_ohlc = [(100.0, 102.0, 98.0, 100.0)] * 20
    msft_ohlc = [(100.0, 101.0, 99.0, 100.0)] * 20
    bars = make_ohlc_bars({"AAPL": aapl_ohlc, "MSFT": msft_ohlc})

    result = compute_metrics(bars)

    # Row 13 = first non-null ATR = date 2024-01-14
    target_date = datetime.date(2024, 1, 14)
    aapl_row = result.filter(
        (pl.col("ticker") == "AAPL") & (pl.col("date") == target_date)
    ).row(0, named=True)
    msft_row = result.filter(
        (pl.col("ticker") == "MSFT") & (pl.col("date") == target_date)
    ).row(0, named=True)

    assert aapl_row["atr_pct"] == pytest.approx(0.04, abs=1e-4)
    assert msft_row["atr_pct"] == pytest.approx(0.02, abs=1e-4)
    assert aapl_row["atr_pct"] == pytest.approx(2 * msft_row["atr_pct"], abs=1e-4)


def test_compute_metrics_volume_sma20_correct():
    """volume_sma_20 = rolling_mean(volume, 20).

    With increasing volumes, SMA is correct.

    Volumes: 1.0, 2.0, ..., 30.0. At row 19 (20th bar), SMA(20) = mean(1..20) = 10.5.
    """
    bars = make_metric_bars({"AAPL": [float(i) for i in range(1, 31)]})
    bars = bars.with_columns(
        pl.Series("volume", [float(i) for i in range(1, 31)]).cast(pl.Float32)
    )

    result = compute_metrics(bars)

    target_date = datetime.date(2024, 1, 1) + datetime.timedelta(days=19)
    row = result.filter(pl.col("date") == target_date).row(0, named=True)

    assert row["volume_sma_20"] == pytest.approx(10.5)


def test_compute_metrics_volume_sma20_null_count():
    """volume_sma_20 has exactly 19 leading nulls per ticker (rolling_mean(20) warmup)."""  # noqa: E501
    bars = make_metric_bars(
        {
            "AAPL": [100.0] * 250,
            "MSFT": [200.0] * 250,
        }
    )

    result = compute_metrics(bars)
    null_counts = result.group_by("ticker").agg(
        pl.col("volume_sma_20").null_count().alias("nulls")
    )

    assert null_counts.sort("ticker")["nulls"].to_list() == [19, 19]


def test_compute_metrics_volume_sma20_per_ticker():
    """volume_sma_20 is computed independently per ticker.

    Different volumes yield different values.

    AAPL: volume=1000.0 (default) → volume_sma_20=1000.0.
    MSFT: volume=2000.0 (overridden) → volume_sma_20=2000.0.
    """
    bars = make_metric_bars(
        {
            "AAPL": [10.0] * 60,
            "MSFT": [20.0] * 60,
        }
    )
    bars = bars.with_columns(
        pl.when(pl.col("ticker") == "MSFT")
        .then(pl.lit(2000.0).cast(pl.Float32))
        .otherwise(pl.col("volume"))
        .alias("volume")
    )

    result = compute_metrics(bars)

    target_date = datetime.date(2024, 1, 1) + datetime.timedelta(days=59)
    aapl_row = result.filter(
        (pl.col("ticker") == "AAPL") & (pl.col("date") == target_date)
    ).row(0, named=True)
    msft_row = result.filter(
        (pl.col("ticker") == "MSFT") & (pl.col("date") == target_date)
    ).row(0, named=True)

    assert aapl_row["volume_sma_20"] == pytest.approx(1000.0)
    assert msft_row["volume_sma_20"] == pytest.approx(2000.0)


def test_compute_adr_pct_basic():
    """ADR%(20) equals SMA20((high-low)/close). With constant spread=4, close=100: ADR%=0.04."""  # noqa: E501
    ohlc = [(100.0, 102.0, 98.0, 100.0)] * 25
    bars = make_ohlc_bars({"AAPL": ohlc})
    result = _compute_adr_pct(bars)
    # Row 19 (0-indexed) = 20th bar = first non-null ADR%
    row = result.filter(pl.col("date") == datetime.date(2024, 1, 20)).row(0, named=True)
    assert row["adr_pct"] == pytest.approx(0.04, abs=1e-4)


def test_compute_adr_pct_warmup_nulls():
    """ADR%(20) has exactly 19 leading nulls per ticker (rolling_mean(20) needs 20 values)."""  # noqa: E501
    ohlc = [(100.0, 102.0, 98.0, 100.0)] * 30
    bars = make_ohlc_bars({"AAPL": ohlc, "MSFT": ohlc})
    result = _compute_adr_pct(bars)
    null_counts = result.group_by("ticker").agg(
        pl.col("adr_pct").null_count().alias("nulls")
    )
    assert null_counts.sort("ticker")["nulls"].to_list() == [19, 19]


def test_compute_adr_pct_per_ticker_isolation():
    """Different tickers with different spreads produce different ADR% values."""
    aapl_ohlc = [(100.0, 102.0, 98.0, 100.0)] * 25  # spread=4 → ADR%=0.04
    msft_ohlc = [(100.0, 105.0, 95.0, 100.0)] * 25  # spread=10 → ADR%=0.10
    bars = make_ohlc_bars({"AAPL": aapl_ohlc, "MSFT": msft_ohlc})
    result = _compute_adr_pct(bars)
    target_date = datetime.date(2024, 1, 20)
    aapl_row = result.filter(
        (pl.col("ticker") == "AAPL") & (pl.col("date") == target_date)
    ).row(0, named=True)
    msft_row = result.filter(
        (pl.col("ticker") == "MSFT") & (pl.col("date") == target_date)
    ).row(0, named=True)
    assert aapl_row["adr_pct"] == pytest.approx(0.04, abs=1e-4)
    assert msft_row["adr_pct"] == pytest.approx(0.10, abs=1e-4)


def test_compute_adr_pct_flat_price():
    """When high == low, daily range = 0, so ADR% = 0.0 after warmup (not null)."""
    ohlc = [(100.0, 100.0, 100.0, 100.0)] * 25
    bars = make_ohlc_bars({"AAPL": ohlc})
    result = _compute_adr_pct(bars)
    non_null = result.filter(pl.col("adr_pct").is_not_null())
    assert len(non_null) > 0
    assert non_null["adr_pct"].to_list() == pytest.approx(
        [0.0] * len(non_null), abs=1e-6
    )


def test_compute_adr_pct_output_columns():
    """Result has exactly 3 columns: [date, ticker, adr_pct]."""
    ohlc = [(100.0, 102.0, 98.0, 100.0)] * 25
    bars = make_ohlc_bars({"AAPL": ohlc})
    result = _compute_adr_pct(bars)
    assert result.columns == ["date", "ticker", "adr_pct"]


def test_compute_metrics_adr_pct_value():
    """adr_pct in compute_metrics output equals SMA20((high-low)/close).

    With constant spread (high=102, low=98, close=100): daily range = 4/100 = 0.04.
    After 20-bar warmup, adr_pct should equal 0.04.
    """
    ohlc = [(100.0, 102.0, 98.0, 100.0)] * 25
    bars = make_ohlc_bars({"AAPL": ohlc})
    result = compute_metrics(bars)
    # Row 19 (0-indexed) = 20th bar = first non-null ADR%
    row = result.filter(pl.col("date") == datetime.date(2024, 1, 20)).row(0, named=True)
    assert row["adr_pct"] == pytest.approx(0.04, abs=1e-4)


def test_compute_metrics_adr_pct_null_count():
    """adr_pct has exactly 19 leading nulls per ticker (rolling_mean(20) warmup)."""
    ohlc = [(100.0, 102.0, 98.0, 100.0)] * 30
    bars = make_ohlc_bars({"AAPL": ohlc, "MSFT": ohlc})
    result = compute_metrics(bars)
    null_counts = result.group_by("ticker").agg(
        pl.col("adr_pct").null_count().alias("nulls")
    )
    assert null_counts.sort("ticker")["nulls"].to_list() == [19, 19]


def test_compute_metrics_adr_pct_independent_of_spy():
    """adr_pct is non-null after warmup even when SPY is absent from bars."""
    ohlc = [(100.0, 102.0, 98.0, 100.0)] * 25
    bars = make_ohlc_bars({"AAPL": ohlc})  # No SPY
    result = compute_metrics(bars)
    after_warmup = result.filter(
        (pl.col("ticker") == "AAPL") & pl.col("adr_pct").is_not_null()
    )
    assert len(after_warmup) > 0
    assert after_warmup["adr_pct"].null_count() == 0
