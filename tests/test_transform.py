import datetime
import importlib

import polars as pl
import pytest
from polars.testing import assert_frame_equal

transform = importlib.import_module("tickerlake.transform")
adjust_splits = transform.adjust_splits
compute_metrics = transform.compute_metrics
filter_tickers = transform.filter_tickers


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

    assert aapl_row["sma_50"] == pytest.approx(10.0)
    assert msft_row["sma_50"] == pytest.approx(20.0)


def test_compute_metrics_output_columns():
    bars = make_metric_bars({"AAPL": [100.0] * 250})

    result = compute_metrics(bars)

    assert result.columns == ["date", "ticker", "sma_50", "sma_200"]
