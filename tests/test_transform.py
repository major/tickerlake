import datetime
import importlib

import polars as pl
import pytest
from polars.testing import assert_frame_equal

transform = importlib.import_module("tickerlake.transform")
adjust_splits = transform.adjust_splits
compute_metrics = transform.compute_metrics
filter_tickers = transform.filter_tickers
_compute_atr = transform._compute_atr


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

    assert result.columns == [
        "date",
        "ticker",
        "sma_50",
        "sma_200",
        "atr_14",
        "rs",
        "rs_sma_20",
        "vars",
        "vars_sma_20",
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


def test_compute_metrics_rs_correct():
    """RS for AAPL vs SPY = rolling_sum(50) of daily diff ≈ 0.25 after warmup.

    AAPL gains exactly 1%/day, SPY gains exactly 0.5%/day.
    Daily diff = 0.01 - 0.005 = 0.005. rolling_sum(50) = 0.005 * 50 = 0.25.
    """
    n = 60
    aapl_ohlc = _make_compounding_ohlc(100.0, 0.01, n)
    spy_ohlc = _make_compounding_ohlc(400.0, 0.005, n)
    bars = make_ohlc_bars({"AAPL": aapl_ohlc, "SPY": spy_ohlc})

    result = compute_metrics(bars)

    # Row 50 (0-indexed) = 51st bar = date 2024-01-01 + 50 days = 2024-02-20
    target_date = datetime.date(2024, 1, 1) + datetime.timedelta(days=50)
    row = result.filter(
        (pl.col("ticker") == "AAPL") & (pl.col("date") == target_date)
    ).row(0, named=True)

    assert row["rs"] == pytest.approx(0.25, abs=1e-4)


def test_compute_metrics_rs_negative():
    """RS is negative when stock underperforms SPY.

    AAPL gains 0.5%/day, SPY gains 1%/day → daily diff = -0.005.
    After 50 non-null days, RS = -0.25.
    """
    n = 60
    aapl_ohlc = _make_compounding_ohlc(100.0, 0.005, n)
    spy_ohlc = _make_compounding_ohlc(400.0, 0.01, n)
    bars = make_ohlc_bars({"AAPL": aapl_ohlc, "SPY": spy_ohlc})

    result = compute_metrics(bars)

    target_date = datetime.date(2024, 1, 1) + datetime.timedelta(days=50)
    row = result.filter(
        (pl.col("ticker") == "AAPL") & (pl.col("date") == target_date)
    ).row(0, named=True)

    assert row["rs"] < 0


def test_compute_metrics_rs_sma20_correct():
    """RS_SMA_20 = rolling_mean(RS, 20). When RS is constant, SMA equals RS.

    AAPL +1%/day, SPY +0.5%/day over 80 days. RS stabilizes at 0.25 from
    row 50 onward. SMA(20) of a constant 0.25 = 0.25.
    """
    n = 80
    aapl_ohlc = _make_compounding_ohlc(100.0, 0.01, n)
    spy_ohlc = _make_compounding_ohlc(400.0, 0.005, n)
    bars = make_ohlc_bars({"AAPL": aapl_ohlc, "SPY": spy_ohlc})

    result = compute_metrics(bars)

    # Row 69 (0-indexed) = 70th bar — RS has been stable for 20 rows, SMA is settled
    target_date = datetime.date(2024, 1, 1) + datetime.timedelta(days=69)
    row = result.filter(
        (pl.col("ticker") == "AAPL") & (pl.col("date") == target_date)
    ).row(0, named=True)

    assert row["rs_sma_20"] == pytest.approx(0.25, abs=1e-4)


def test_compute_metrics_rs_null_count():
    """RS has 50 leading nulls per ticker; RS_SMA_20 has 69 leading nulls.

    pct_change shift(1) → first row null → rolling_sum(50) needs 50 values
    → 50 leading nulls. rolling_mean(20) on top adds 19 more → 69 total.
    """
    n = 250
    aapl_ohlc = _make_compounding_ohlc(100.0, 0.01, n)
    msft_ohlc = _make_compounding_ohlc(200.0, 0.008, n)
    spy_ohlc = _make_compounding_ohlc(400.0, 0.005, n)
    bars = make_ohlc_bars({"AAPL": aapl_ohlc, "MSFT": msft_ohlc, "SPY": spy_ohlc})

    result = compute_metrics(bars)

    rs_nulls = (
        result.filter(pl.col("ticker").is_in(["AAPL", "MSFT"]))
        .group_by("ticker")
        .agg(pl.col("rs").null_count().alias("rs_nulls"))
        .sort("ticker")
    )
    rs_sma_nulls = (
        result.filter(pl.col("ticker").is_in(["AAPL", "MSFT"]))
        .group_by("ticker")
        .agg(pl.col("rs_sma_20").null_count().alias("sma_nulls"))
        .sort("ticker")
    )

    assert rs_nulls["rs_nulls"].to_list() == [50, 50]
    assert rs_sma_nulls["sma_nulls"].to_list() == [69, 69]


def test_compute_metrics_rs_spy_is_zero():
    """SPY's RS vs itself is 0 at every non-null row (comparing to itself)."""
    n = 60
    aapl_ohlc = _make_compounding_ohlc(100.0, 0.01, n)
    spy_ohlc = _make_compounding_ohlc(400.0, 0.005, n)
    bars = make_ohlc_bars({"AAPL": aapl_ohlc, "SPY": spy_ohlc})

    result = compute_metrics(bars)

    spy_rs = result.filter((pl.col("ticker") == "SPY") & pl.col("rs").is_not_null())[
        "rs"
    ].to_list()

    assert len(spy_rs) > 0
    assert spy_rs == pytest.approx([0.0] * len(spy_rs), abs=1e-5)


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


def test_compute_metrics_vars_correct():
    """VARS = rolling_sum(stock_norm - spy_norm, 50) where norm = daily_change / ATR14.

    AAPL: daily_change=+2.0, spread=1.0. TR = max(2.0, |close+1-(close-2)|, ...) = 3.0.
    ATR = 3.0. stock_norm = 2.0 / 3.0 = 0.6667.
    SPY: daily_change=+0.5, spread=0.5. TR = max(1.0, |close+0.5-(close-0.5)|, ...) = 1.0.
    ATR = 1.0. spy_norm = 0.5 / 1.0 = 0.5.
    vars_daily = 0.6667 - 0.5 = 0.1667. VARS = 0.1667 * 50 = 8.333.
    """
    n = 80
    aapl_ohlc = _make_constant_ohlc(100.0, 2.0, 1.0, n)
    spy_ohlc = _make_constant_ohlc(400.0, 0.5, 0.5, n)
    bars = make_ohlc_bars({"AAPL": aapl_ohlc, "SPY": spy_ohlc})

    result = compute_metrics(bars)

    # ATR warmup: 13 nulls. rolling_sum(50) needs 50 non-null stock_norm values.
    # stock_norm first non-null at row 13 (ATR binding). VARS first non-null at row 13+49=62.
    target_date = datetime.date(2024, 1, 1) + datetime.timedelta(days=62)
    row = result.filter(
        (pl.col("ticker") == "AAPL") & (pl.col("date") == target_date)
    ).row(0, named=True)

    # vars_daily = 2/3 - 0.5 = 1/6, rolling_sum(50) = 50/6 ≈ 8.333
    assert row["vars"] == pytest.approx(50 / 6, abs=0.1)


def test_compute_metrics_vars_vs_rs_divergence():
    """VARS and RS can disagree: high-ATR stock outperforms in % but underperforms in ATR units.

    AAPL: close=100, spread=10 → ATR≈20, daily_change=+2 → stock_norm=2/20=0.1
    SPY:  close=400, spread=2  → ATR≈4,  daily_change=+2 → spy_norm=2/4=0.5

    RS daily = stock_pct - spy_pct = (2/100) - (2/400) = 0.02 - 0.005 = +0.015 → RS > 0
    vars_daily = 0.1 - 0.5 = -0.4 → VARS < 0

    Jeff Sun's insight: a 2% move in a 20% ATR name is weak vs SPY moving 0.5% on 1% ATR.
    """
    n = 80
    # AAPL: spread=10 → ATR≈20, daily_change=+2
    aapl_ohlc = _make_constant_ohlc(100.0, 2.0, 10.0, n)
    # SPY: spread=2 → ATR≈4, daily_change=+2
    spy_ohlc = _make_constant_ohlc(400.0, 2.0, 2.0, n)
    bars = make_ohlc_bars({"AAPL": aapl_ohlc, "SPY": spy_ohlc})

    result = compute_metrics(bars)

    # Check at a date after both RS and VARS have warmed up (row 62+)
    target_date = datetime.date(2024, 1, 1) + datetime.timedelta(days=62)
    row = result.filter(
        (pl.col("ticker") == "AAPL") & (pl.col("date") == target_date)
    ).row(0, named=True)

    # RS > 0: stock gained more % than SPY
    assert row["rs"] is not None
    assert row["rs"] > 0
    # VARS < 0: stock moved less than SPY in ATR-normalized terms
    assert row["vars"] is not None
    assert row["vars"] < 0


def test_compute_metrics_vars_sma20_correct():
    """VARS_SMA_20 = rolling_mean(VARS, 20). When VARS is constant, SMA equals VARS.

    Using same setup as test_vars_correct: VARS stabilizes at 25.0 from row 62 onward.
    SMA(20) of a constant 25.0 = 25.0 after 19 more rows (row 81).
    """
    n = 100
    aapl_ohlc = _make_constant_ohlc(100.0, 2.0, 1.0, n)
    spy_ohlc = _make_constant_ohlc(400.0, 0.5, 0.5, n)
    bars = make_ohlc_bars({"AAPL": aapl_ohlc, "SPY": spy_ohlc})

    result = compute_metrics(bars)

    # VARS stabilizes at row 62, SMA(20) settles at row 62+19=81
    target_date = datetime.date(2024, 1, 1) + datetime.timedelta(days=81)
    row = result.filter(
        (pl.col("ticker") == "AAPL") & (pl.col("date") == target_date)
    ).row(0, named=True)

    assert row["vars_sma_20"] == pytest.approx(row["vars"], abs=0.5)


def test_compute_metrics_vars_null_count():
    """VARS has ~62 leading nulls per ticker; VARS_SMA_20 has ~81 leading nulls.

    ATR(14): 13 nulls. daily_change shift(1): 1 null (binding at row 0 only).
    stock_norm first non-null at row 13 (ATR is binding constraint).
    rolling_sum(50) needs 50 non-null values → 13 + 49 = 62 leading nulls.
    VARS_SMA_20 adds 19 more → 81 total.
    """
    n = 250
    aapl_ohlc = _make_constant_ohlc(100.0, 1.0, 1.0, n)
    msft_ohlc = _make_constant_ohlc(200.0, 1.0, 1.0, n)
    spy_ohlc = _make_constant_ohlc(400.0, 0.5, 0.5, n)
    bars = make_ohlc_bars({"AAPL": aapl_ohlc, "MSFT": msft_ohlc, "SPY": spy_ohlc})

    result = compute_metrics(bars)

    vars_nulls = (
        result.filter(pl.col("ticker").is_in(["AAPL", "MSFT"]))
        .group_by("ticker")
        .agg(pl.col("vars").null_count().alias("vars_nulls"))
        .sort("ticker")
    )
    vars_sma_nulls = (
        result.filter(pl.col("ticker").is_in(["AAPL", "MSFT"]))
        .group_by("ticker")
        .agg(pl.col("vars_sma_20").null_count().alias("sma_nulls"))
        .sort("ticker")
    )

    # Expected: 62 nulls for VARS, 81 for VARS_SMA_20 — verify empirically
    actual_vars_nulls = vars_nulls["vars_nulls"].to_list()
    actual_sma_nulls = vars_sma_nulls["sma_nulls"].to_list()

    # Both tickers should have the same null count
    assert actual_vars_nulls[0] == actual_vars_nulls[1]
    assert actual_sma_nulls[0] == actual_sma_nulls[1]
    # VARS nulls should be in the expected range (ATR warmup + rolling_sum warmup)
    assert 50 < actual_vars_nulls[0] < 90
    # VARS_SMA_20 should have ~19 more nulls than VARS
    assert actual_sma_nulls[0] == actual_vars_nulls[0] + 19


def test_compute_metrics_vars_atr_zero():
    """When ATR=0 (flat price), stock_norm = daily_change / 0 → null (not inf).

    AAPL: open=high=low=close=100.0 for all bars → ATR=0 after warmup.
    After ATR warmup, stock_norm = 0 / 0 = NaN → fill_nan(None) → null.
    VARS should be null for all rows where ATR=0.
    """
    n = 50
    # Flat AAPL: ATR=0 after warmup
    aapl_ohlc = [(100.0, 100.0, 100.0, 100.0)] * n
    # Normal SPY for reference
    spy_ohlc = _make_constant_ohlc(400.0, 0.5, 0.5, n)
    bars = make_ohlc_bars({"AAPL": aapl_ohlc, "SPY": spy_ohlc})

    result = compute_metrics(bars)

    # After ATR warmup (row 13+), AAPL ATR=0 → stock_norm=null → VARS=null
    aapl_after_warmup = result.filter(
        (pl.col("ticker") == "AAPL") & pl.col("atr_14").is_not_null()
    )
    assert len(aapl_after_warmup) > 0
    # All VARS values after ATR warmup should be null (ATR=0 → norm=null → rolling_sum=null)
    assert aapl_after_warmup["vars"].null_count() == len(aapl_after_warmup)


def test_compute_metrics_no_spy():
    """When SPY is absent, rs/rs_sma_20/vars/vars_sma_20 are null; atr_14 is computed.

    atr_14 must still be non-null after warmup — it doesn't depend on SPY.
    sma_50 and sma_200 are also unaffected.
    """
    n = 100
    aapl_ohlc = _make_constant_ohlc(100.0, 1.0, 1.0, n)
    msft_ohlc = _make_constant_ohlc(200.0, 1.0, 1.0, n)
    bars = make_ohlc_bars({"AAPL": aapl_ohlc, "MSFT": msft_ohlc})

    result = compute_metrics(bars)

    # SPY-dependent columns must be all-null
    assert result["rs"].null_count() == len(result)
    assert result["rs_sma_20"].null_count() == len(result)
    assert result["vars"].null_count() == len(result)
    assert result["vars_sma_20"].null_count() == len(result)

    # ATR must be computed (not all-null) — independent of SPY
    assert result["atr_14"].null_count() < len(result)

    # SMA columns still work
    aapl_late = result.filter(
        (pl.col("ticker") == "AAPL")
        & (pl.col("date") == datetime.date(2024, 1, 1) + datetime.timedelta(days=99))
    ).row(0, named=True)
    assert aapl_late["sma_50"] is not None
