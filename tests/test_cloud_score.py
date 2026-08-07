"""Tests for tickerlake.cloud_score — Ciovacco cloud-score data layer."""

import datetime
import re
from typing import TYPE_CHECKING
from unittest.mock import MagicMock, patch

import duckdb
import polars as pl
import pytest
from polars.testing import assert_frame_equal

from tickerlake import cloud_score
from tickerlake.cloud_score import (
    CLOUD_BARS_SCHEMA,
    CLOUD_SCORE_SCHEMA,
    ICHIMOKU_SCHEMA,
    MA_SCORE_SCHEMA,
    MA_SLOPE_SCHEMA,
    MA_VALUES_SCHEMA,
    TIMEFRAME_ICHIMOKU_PERIODS,
    _above_ratio_expression,
    _compute_ma_scores,
    _slope_ratio_expression,
    aggregate_daily_to_period,
    compute_cloud_scores,
    compute_ichimoku,
    compute_ma_and_slope,
    read_daily_bars,
    render_cloud_scorecard,
    score_ichimoku,
    score_ma,
)

if TYPE_CHECKING:
    from pathlib import Path


class FakeConnection:
    """DuckDB stand-in: records SQL calls and writes a parquet result."""

    def __init__(self, result: pl.DataFrame) -> None:
        self.result = result
        self.calls: list[tuple[str, list]] = []
        self.closed = False

    def execute(self, sql: str, params: list | None = None):
        self.calls.append((sql, list(params or [])))
        match = re.search(r"TO '([^']+)'", sql)
        if match:
            self.result.write_parquet(match.group(1))
        return self

    def close(self) -> None:
        self.closed = True


def _make_ohlc(closes: dict[str, list[float]]) -> pl.DataFrame:
    """Build a CLOUD_BARS_SCHEMA frame from per-ticker close series.

    Open = previous close (or close on the first bar), high = close*1.001,
    low = close*0.999. Uses consecutive calendar days as dates.
    """
    rows = []
    for ticker, series in closes.items():
        for index, close in enumerate(series):
            prev = series[index - 1] if index else close
            rows.append(
                {
                    "date": datetime.date(2024, 1, 1) + datetime.timedelta(days=index),
                    "ticker": ticker,
                    "open": prev,
                    "high": close * 1.001,
                    "low": close * 0.999,
                    "close": close,
                    "volume": 1_000_000.0,
                    "vwap": close,
                    "transactions": 5000,
                }
            )
    return pl.DataFrame(rows, schema=CLOUD_BARS_SCHEMA)


def _ichimoku_bars(n_bars: int) -> pl.DataFrame:
    """Build n bars with high == low == close == 100 + i for hand-calc."""
    return pl.DataFrame(
        {
            "date": [
                datetime.date(2024, 1, 1) + datetime.timedelta(days=i)
                for i in range(n_bars)
            ],
            "ticker": ["X"] * n_bars,
            "high": [100.0 + i for i in range(n_bars)],
            "low": [100.0 + i for i in range(n_bars)],
            "close": [100.0 + i for i in range(n_bars)],
        }
    )


def _rich_text(table) -> str:
    """Render a Rich Table to plain text."""
    from rich.console import Console

    console = Console(record=True, width=200, force_terminal=False)
    console.print(table)
    return console.export_text()


def _rich_ansi(table) -> str:
    """Render a Rich Table to ANSI text so style assertions can inspect it."""
    from rich.console import Console

    console = Console(record=True, width=200, force_terminal=False)
    console.print(table)
    return console.export_text(styles=True)


# ---- schemas and constants -------------------------------------------------


def test_cloud_bars_schema_defined():
    assert set(cloud_score.CLOUD_BARS_SCHEMA) == {
        "date",
        "ticker",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "vwap",
        "transactions",
    }
    assert cloud_score.CLOUD_BARS_SCHEMA["date"] == pl.Date
    assert cloud_score.CLOUD_BARS_SCHEMA["close"] == pl.Float32


def test_cloud_score_schema_defined():
    assert set(cloud_score.CLOUD_SCORE_SCHEMA) == {
        "ticker",
        "score_weekly_cloud",
        "score_2wk_cloud",
        "score_3wk_cloud",
        "score_monthly_cloud",
        "score_2mo_cloud",
        "score_200wk_ma",
        "score_200wk_ma_slope",
        "score_300wk_ma",
        "score_300wk_ma_slope",
        "total",
    }
    for column, dtype in cloud_score.CLOUD_SCORE_SCHEMA.items():
        if column == "ticker":
            continue
        if column.startswith("score_") and column.endswith("_cloud"):
            assert dtype == pl.Float32  # 0.0-1.0 decimals
        elif column == "total":
            assert dtype == pl.Float32  # 0.0-9.0 decimals
        else:
            assert dtype == pl.Int64  # 0/1 MA conditions


def test_timeframe_ichimoku_periods_are_typed_and_locked():
    assert TIMEFRAME_ICHIMOKU_PERIODS == {
        "weekly": (9, 26, 52),
        "2wk": (9, 26, 52),
        "3wk": (9, 26, 52),
        "monthly": (9, 26, 52),
        "2mo": (9, 26, 52),
    }
    assert all(
        isinstance(value, tuple) and len(value) == 3
        for value in TIMEFRAME_ICHIMOKU_PERIODS.values()
    )


# ---- read_daily_bars ------------------------------------------------------


def test_read_daily_bars_filters_by_ticker_and_date(tmp_path: Path):
    consumer = tmp_path / "tickerlake.duckdb"
    consumer.touch()
    today = datetime.date.today()  # noqa: DTZ011
    start = today - datetime.timedelta(days=30)
    expected = _make_ohlc({"AAPL": [100.0, 110.0, 120.0]}).sort(["ticker", "date"])

    fake = FakeConnection(expected)
    with patch("tickerlake.cloud_score.duckdb.connect", return_value=fake):
        result = read_daily_bars(consumer, tickers=["aapl"], lookback_days=30)

    assert len(fake.calls) == 1
    sql, params = fake.calls[0]
    assert "FROM daily_bars" in sql
    assert "WHERE ticker IN (?) AND date >= ?" in sql
    assert "ORDER BY ticker, date" in sql
    assert params == ["AAPL", start]
    assert fake.closed
    assert result.columns == list(CLOUD_BARS_SCHEMA)
    assert result.dtypes == list(CLOUD_BARS_SCHEMA.values())
    assert_frame_equal(result, expected)


def test_read_daily_bars_multiple_tickers_uppercased(tmp_path: Path):
    consumer = tmp_path / "tickerlake.duckdb"
    consumer.touch()
    fake = FakeConnection(pl.DataFrame(schema=CLOUD_BARS_SCHEMA))
    with patch("tickerlake.cloud_score.duckdb.connect", return_value=fake):
        read_daily_bars(consumer, tickers=["cibr", "igv"], lookback_days=100)
    sql, params = fake.calls[0]
    assert "ticker IN (?, ?)" in sql
    assert params[:2] == ["CIBR", "IGV"]


def test_read_daily_bars_validates_tickers_empty(tmp_path: Path):
    with pytest.raises(ValueError, match="tickers"):
        read_daily_bars(tmp_path / "missing.duckdb", tickers=[], lookback_days=30)


@pytest.mark.parametrize("lookback_days", [0, -1])
def test_read_daily_bars_validates_lookback(tmp_path: Path, lookback_days: int):
    with pytest.raises(ValueError, match="lookback"):
        read_daily_bars(
            tmp_path / "missing.duckdb",
            tickers=["AAPL"],
            lookback_days=lookback_days,
        )


def test_read_daily_bars_validates_path_exists(tmp_path: Path):
    with pytest.raises(ValueError, match="Consumer DB not found"):
        read_daily_bars(tmp_path / "missing.duckdb", tickers=["AAPL"], lookback_days=30)


def test_read_daily_bars_missing_table_raises_valueerror(tmp_path: Path):
    consumer = tmp_path / "tickerlake.duckdb"
    consumer.touch()
    fake = MagicMock()
    fake.execute.side_effect = duckdb.CatalogException("no such table: daily_bars")

    with (
        patch("tickerlake.cloud_score.duckdb.connect", return_value=fake),
        pytest.raises(ValueError, match="daily_bars"),
    ):
        read_daily_bars(consumer, tickers=["CIBR"], lookback_days=30)

    fake.close.assert_called_once()


# ---- aggregate_daily_to_period ---------------------------------------------


def test_aggregate_daily_to_period_weekly_labels_monday_and_last_close():
    bars = _make_ohlc({"X": [100.0 + i for i in range(15)]})
    result = aggregate_daily_to_period(bars, every="1w")
    weekly = result.filter(pl.col("ticker") == "X")
    # 2024-01-01 is a Monday; the window labels are the Monday that starts
    # each calendar week and the close is the last close in that week.
    assert weekly["date"].to_list() == [
        datetime.date(2024, 1, 1),
        datetime.date(2024, 1, 8),
        datetime.date(2024, 1, 15),
    ]
    assert weekly["close"].to_list() == pytest.approx([106.0, 113.0, 114.0])
    # open is the previous day's close, so the week-2 open is close[6] = 106.
    assert weekly["open"].to_list() == pytest.approx([100.0, 106.0, 113.0])


def test_aggregate_daily_to_period_monthly_labels_last_trading_day():
    bars = _make_ohlc({"X": [100.0 + i for i in range(60)]})
    result = aggregate_daily_to_period(bars, every="1mo")
    monthly = result.filter(pl.col("ticker") == "X")
    # Month-based bars are labeled with the last trading day present in the
    # period (2024 is a leap year).
    assert monthly.height == 2
    assert monthly["date"].to_list()[0] == datetime.date(2024, 1, 31)
    assert monthly["date"].to_list()[1] == datetime.date(2024, 2, 29)
    assert monthly["close"].to_list() == pytest.approx([130.0, 159.0])


@pytest.mark.parametrize("every", ["2w", "3w", "2mo"])
def test_aggregate_daily_to_period_supports_custom_periods(every: str):
    bars = _make_ohlc({"X": [100.0] * 400})
    result = aggregate_daily_to_period(bars, every=every)
    assert not result.is_empty()
    assert result.columns == list(CLOUD_BARS_SCHEMA)


def test_aggregate_daily_to_period_invalid_every():
    with pytest.raises(ValueError, match="every"):
        aggregate_daily_to_period(_make_ohlc({"X": [1.0]}), every="1d")


def test_aggregate_daily_to_period_empty_input():
    empty = pl.DataFrame(schema=CLOUD_BARS_SCHEMA)
    result = aggregate_daily_to_period(empty, every="1w")
    assert result.is_empty()
    assert result.columns == list(CLOUD_BARS_SCHEMA)


# ---- compute_ichimoku ------------------------------------------------------


def test_compute_ichimoku_hand_calculated_values():
    """Verify all four outputs against hand-computed Ichimoku math."""
    bars = _ichimoku_bars(100)  # close == high == low == 100 + i
    result = compute_ichimoku(
        bars, tenkan_period=9, kijun_period=26, senkou_b_period=52
    )
    last = result.filter(pl.col("ticker") == "X").sort("date").row(-1, named=True)

    # At bar 99 (close 199):
    # tenkan = (max high[91..99] + min low[91..99]) / 2 = (199 + 191) / 2
    assert last["tenkan"] == pytest.approx(195.0)
    # kijun = (max high[74..99] + min low[74..99]) / 2 = (199 + 174) / 2
    assert last["kijun"] == pytest.approx(186.5)
    # senkou_a_at_current = senkou_a 26 bars ago: tenkan[73]=(173+165)/2=169,
    # kijun[73]=(173+148)/2=160.5, so senkou_a[73]=(169+160.5)/2=164.75
    assert last["senkou_a_at_current"] == pytest.approx(164.75)
    # senkou_b_at_current = senkou_b 26 bars ago: max high[22..73]=173,
    # min low[22..73]=122 → (173+122)/2=147.5
    assert last["senkou_b_at_current"] == pytest.approx(147.5)
    # Chikou is not part of the cloud-line output.
    assert "chikou_ok" not in result.columns


def test_compute_ichimoku_per_ticker_independence():
    """Two tickers with different paths are computed independently."""
    bars = _ichimoku_bars(100).vstack(
        _ichimoku_bars(100).with_columns(pl.lit("Y").alias("ticker"))
    )
    result = compute_ichimoku(
        bars, tenkan_period=9, kijun_period=26, senkou_b_period=52
    )
    assert result["ticker"].n_unique() == 2
    for ticker in ("X", "Y"):
        last = (
            result.filter(pl.col("ticker") == ticker).sort("date").row(-1, named=True)
        )
        assert last["tenkan"] == pytest.approx(195.0)


@pytest.mark.parametrize(
    ("kwargs", "match"),
    [
        ({"tenkan_period": 0}, "tenkan_period"),
        ({"kijun_period": 0}, "kijun_period"),
        ({"senkou_b_period": 0}, "senkou_b_period"),
    ],
)
def test_compute_ichimoku_validates_periods(kwargs: dict, match: str):
    params = {"tenkan_period": 9, "kijun_period": 26, "senkou_b_period": 52}
    params.update(kwargs)
    with pytest.raises(ValueError, match=match):
        compute_ichimoku(_ichimoku_bars(10), **params)


def test_compute_ichimoku_empty_input():
    empty = pl.DataFrame(schema=CLOUD_BARS_SCHEMA)
    result = compute_ichimoku(
        empty, tenkan_period=9, kijun_period=26, senkou_b_period=52
    )
    assert result.is_empty()
    assert result.columns == list(ICHIMOKU_SCHEMA)


# ---- score_ichimoku --------------------------------------------------------


def test_score_ichimoku_weights_four_cloud_lines(sample_ichimoku_df):
    """Each of the four cloud lines contributes 0.25; score lands in 0-1."""
    result = score_ichimoku(
        sample_ichimoku_df,
        tickers=["T1", "T2", "T3", "T4", "T5", "T6", "SPY"],
        benchmark="SPY",
    )
    assert result["score"].dtype == pl.Float32
    by_ticker = {row["ticker"]: row["score"] for row in result.iter_rows(named=True)}
    assert by_ticker == {
        "T1": pytest.approx(1.0),
        "T2": pytest.approx(0.75),
        "T3": pytest.approx(0.5),
        "T4": pytest.approx(0.25),
        "T5": pytest.approx(0.0),
        "T6": None,
    }


def test_score_ichimoku_inside_the_cloud_scores_half():
    """Exactly on Senkou A and Senkou B → 2 of 4 above → 0.5.

    This is the canonical Ciovacco "inside the cloud" case: Tenkan and Kijun
    are above while the close sits exactly on both Senkou lines, which count
    as NOT above (strict `>`).
    """
    ichimoku = pl.DataFrame(
        {
            "date": [datetime.date(2024, 1, 1), datetime.date(2024, 1, 2)],
            "ticker": ["INSIDE", "INSIDE"],
            "close": [10.0, 10.0],
            "tenkan": [None, 9.0],
            "kijun": [None, 9.0],
            "senkou_a_at_current": [None, 10.0],
            "senkou_b_at_current": [None, 10.0],
        },
        schema=ICHIMOKU_SCHEMA,
    )
    result = score_ichimoku(ichimoku, tickers=["INSIDE"], benchmark="SPY")
    assert result["score"][0] == pytest.approx(0.5)


def test_score_ichimoku_benchmark_never_scored(sample_ichimoku_df):
    result = score_ichimoku(sample_ichimoku_df, tickers=["SPY", "T1"], benchmark="SPY")
    assert result["ticker"].to_list() == ["T1"]
    assert result.height == 1


def test_score_ichimoku_insufficient_history_is_null():
    """A ticker with an undefined element scores null, not a partial sum."""
    bars = _ichimoku_bars(5)  # too short for any rolling window
    result = score_ichimoku(
        compute_ichimoku(bars, tenkan_period=9, kijun_period=26, senkou_b_period=52),
        tickers=["X"],
        benchmark="SPY",
    )
    assert result.height == 1
    assert result["score"][0] is None


def test_score_ichimoku_empty_input():
    empty = pl.DataFrame(schema=ICHIMOKU_SCHEMA)
    result = score_ichimoku(empty, tickers=["X"], benchmark="SPY")
    assert result.is_empty()
    assert result.columns == ["ticker", "score"]
    assert result["score"].dtype == pl.Float32


def test_score_ichimoku_empty_tickers(sample_ichimoku_df):
    result = score_ichimoku(sample_ichimoku_df, tickers=[], benchmark="SPY")
    assert result.is_empty()


def test_score_ichimoku_no_scored_tickers_present(sample_ichimoku_df):
    """When none of the requested tickers appear in the frame, return empty."""
    result = score_ichimoku(sample_ichimoku_df, tickers=["ZZZ"], benchmark="SPY")
    assert result.is_empty()
    assert result.columns == ["ticker", "score"]


def test_score_ichimoku_benchmark_excluded_when_only_ticker(sample_ichimoku_df):
    """A benchmark-only ticker list scores nothing."""
    result = score_ichimoku(sample_ichimoku_df, tickers=["SPY"], benchmark="SPY")
    assert result.is_empty()


# ---- compute_ma_and_slope ---------------------------------------------------


def test_compute_ma_and_slope_rising_series():
    bars = _make_ohlc({"X": [100.0 + i for i in range(11)]})
    result = compute_ma_and_slope(bars, period=3, slope_lookback=2)
    row = result.row(0, named=True)
    # Last close 110: ma = (108+109+110)/3 = 109; ma 2 bars earlier at index 8
    # = (106+107+108)/3 = 107; slope = 2.
    assert row["ticker"] == "X"
    assert row["ma_value"] == pytest.approx(109.0)
    assert row["ma_slope"] == pytest.approx(2.0)


def test_compute_ma_and_slope_falling_series():
    bars = _make_ohlc({"X": [110.0 - i for i in range(11)]})
    result = compute_ma_and_slope(bars, period=3, slope_lookback=2)
    row = result.row(0, named=True)
    assert row["ma_value"] == pytest.approx(101.0)  # (100+101+102)/3
    assert row["ma_slope"] == pytest.approx(-2.0)


def test_compute_ma_and_slope_null_slope_at_series_start():
    """With insufficient history the slope (and early MAs) are null."""
    bars = _make_ohlc({"X": [100.0, 101.0, 102.0, 103.0]})
    result = compute_ma_and_slope(bars, period=3, slope_lookback=2)
    row = result.row(0, named=True)
    # ma at last bar = (101+102+103)/3 = 102, but the ma 2 bars earlier is
    # undefined (only 2 values at that point) → slope is null.
    assert row["ma_value"] == pytest.approx(102.0)
    assert row["ma_slope"] is None


def test_compute_ma_and_slope_multiple_tickers():
    bars = _make_ohlc(
        {"UP": [100.0 + i for i in range(11)], "DOWN": [110.0 - i for i in range(11)]}
    )
    result = compute_ma_and_slope(bars, period=3, slope_lookback=2).sort("ticker")
    by_ticker = {row["ticker"]: row["ma_slope"] for row in result.iter_rows(named=True)}
    assert by_ticker["UP"] > 0
    assert by_ticker["DOWN"] < 0


def test_compute_ma_and_slope_validates_args():
    bars = _make_ohlc({"X": [100.0] * 10})
    with pytest.raises(ValueError, match="period"):
        compute_ma_and_slope(bars, period=0)
    with pytest.raises(ValueError, match="slope_lookback"):
        compute_ma_and_slope(bars, period=3, slope_lookback=0)


def test_compute_ma_and_slope_empty_input():
    empty = pl.DataFrame(schema=CLOUD_BARS_SCHEMA)
    result = compute_ma_and_slope(empty, period=3)
    assert result.is_empty()
    assert result.columns == list(MA_SLOPE_SCHEMA)


# ---- score_ma ---------------------------------------------------------------


def _ma_values(
    rows: list[dict[str, object]],
) -> pl.DataFrame:
    return pl.DataFrame(rows, schema=MA_VALUES_SCHEMA)


def _latest_ratio(rows: list[dict[str, object]]) -> pl.DataFrame:
    """Build a ``(ticker, ratio)`` frame for the ``score_ma`` ratio argument."""
    return pl.DataFrame(rows, schema={"ticker": pl.Utf8, "ratio": pl.Float32})


def test_score_ma_above_below_and_null():
    ma_ratio = _ma_values(
        [
            {
                "ticker": "ABOVE",
                "ma_200": 100.0,
                "slope_200": 1.0,
                "ma_300": 100.0,
                "slope_300": 1.0,
            },
            {
                "ticker": "BELOW",
                "ma_200": 100.0,
                "slope_200": -1.0,
                "ma_300": 100.0,
                "slope_300": -1.0,
            },
            {
                "ticker": "NOHIST",
                "ma_200": None,
                "slope_200": None,
                "ma_300": None,
                "slope_300": None,
            },
        ]
    )
    latest = _latest_ratio(
        [
            {"ticker": "ABOVE", "ratio": 150.0},
            {"ticker": "BELOW", "ratio": 50.0},
            {"ticker": "NOHIST", "ratio": 50.0},
        ]
    )
    result = score_ma(ma_ratio, latest_ratio=latest)
    by_ticker = {row["ticker"]: row for row in result.iter_rows(named=True)}
    assert by_ticker["ABOVE"]["score_200wk_ma"] == 1
    assert by_ticker["BELOW"]["score_200wk_ma"] == 0
    assert by_ticker["NOHIST"]["score_200wk_ma"] is None
    assert by_ticker["ABOVE"]["score_200wk_ma_slope"] == 1
    assert by_ticker["BELOW"]["score_200wk_ma_slope"] == 0
    assert by_ticker["NOHIST"]["score_200wk_ma_slope"] is None


def test_score_ma_slope_reflects_ratio_ma_trend():
    """Slope checks score 1 when the ratio's own MA is rising (slope > 0)."""
    ma_ratio = _ma_values(
        [
            # Ratio MA rising → 1
            {
                "ticker": "RISING",
                "ma_200": 150.0,
                "slope_200": 2.0,
                "ma_300": 150.0,
                "slope_300": 2.0,
            },
            # Ratio MA falling → 0
            {
                "ticker": "FALLING",
                "ma_200": 90.0,
                "slope_200": -3.0,
                "ma_300": 90.0,
                "slope_300": -3.0,
            },
            # Ratio MA flat → 0 (strict `>`)
            {
                "ticker": "FLAT",
                "ma_200": 100.0,
                "slope_200": 0.0,
                "ma_300": 100.0,
                "slope_300": 0.0,
            },
        ]
    )
    latest = _latest_ratio(
        [
            {"ticker": "RISING", "ratio": 200.0},
            {"ticker": "FALLING", "ratio": 80.0},
            {"ticker": "FLAT", "ratio": 100.0},
        ]
    )
    result = score_ma(ma_ratio, latest_ratio=latest)
    by_ticker = {row["ticker"]: row for row in result.iter_rows(named=True)}
    assert by_ticker["RISING"]["score_200wk_ma_slope"] == 1
    assert by_ticker["FALLING"]["score_200wk_ma_slope"] == 0
    assert by_ticker["FLAT"]["score_200wk_ma_slope"] == 0
    assert by_ticker["RISING"]["score_300wk_ma_slope"] == 1


def test_score_ma_empty_ma_ratio_frame():
    empty = pl.DataFrame(schema=MA_VALUES_SCHEMA)
    latest = _latest_ratio([{"ticker": "X", "ratio": 1.0}])
    result = score_ma(empty, latest_ratio=latest)
    assert result.is_empty()
    assert result.columns == list(MA_SCORE_SCHEMA)


def test_score_ma_missing_latest_ratio_yields_null_scores():
    ma_ratio = _ma_values(
        [
            {
                "ticker": "X",
                "ma_200": 150.0,
                "slope_200": 2.0,
                "ma_300": 150.0,
                "slope_300": 2.0,
            }
        ]
    )
    result = score_ma(
        ma_ratio,
        latest_ratio=pl.DataFrame(schema={"ticker": pl.Utf8, "ratio": pl.Float32}),
    )
    row = result.row(0, named=True)
    assert row["ticker"] == "X"
    assert row["score_200wk_ma"] is None
    assert row["score_300wk_ma_slope"] is None


# ---- relative-ratio score expressions ---------------------------------------


def test_above_ratio_expression_scores_ratio_against_own_ma():
    """The above-MA check compares the ratio to its own MA (strict >)."""
    frame = pl.DataFrame(
        {
            "ratio": [1.2, 0.8, None, 1.0],
            "ma_200": [1.0, 1.0, 1.0, 1.0],
        }
    )
    scores = frame.select(_above_ratio_expression("ratio", "ma_200", "score"))[
        "score"
    ].to_list()
    # Strict `>`: a ratio exactly on its MA scores 0, null ratio stays null.
    assert scores == [1, 0, None, 0]


def test_slope_ratio_expression_scores_positive_ratio_ma_slope():
    """The slope check scores 1 only for a strictly rising ratio MA."""
    frame = pl.DataFrame(
        {
            "ma_200": [1.0, 1.0, 1.0, None],
            "slope_200": [0.5, -0.5, 0.0, 0.5],
        }
    )
    scores = frame.select(_slope_ratio_expression("ma_200", "slope_200", "score"))[
        "score"
    ].to_list()
    # Strict `>`: a flat MA scores 0, undefined MA stays null.
    assert scores == [1, 0, 0, None]


# ---- _compute_ma_scores -----------------------------------------------------


def test_compute_ma_scores_rising_ratio_scores_one_falling_scores_zero():
    """The four MA conditions are scored on the ETF/SPY ratio, not raw prices.

    RISING climbs faster than SPY (ratio rising → close above its own MAs and
    rising slopes → all four conditions 1). FALLING climbs slower than SPY
    (ratio falling → close below its own MAs and falling slopes → all 0).
    """
    bars = _make_ohlc(
        {
            "RISING": [100.0 + 2.0 * i for i in range(2500)],
            "FALLING": [100.0 + 0.5 * i for i in range(2500)],
            "SPY": [100.0 + i for i in range(2500)],
        }
    )
    result = _compute_ma_scores(bars, benchmark="SPY", slope_lookback=5).sort("ticker")
    by_ticker = {row["ticker"]: row for row in result.iter_rows(named=True)}
    for column in (
        "score_200wk_ma",
        "score_200wk_ma_slope",
        "score_300wk_ma",
        "score_300wk_ma_slope",
    ):
        assert by_ticker["RISING"][column] == 1
        assert by_ticker["FALLING"][column] == 0


def test_compute_ma_scores_empty_ratio_returns_empty():
    """Benchmark-only bars yield no ratio series → empty MA scores."""
    spy_only = _make_ohlc({"SPY": [100.0 + i for i in range(2500)]})
    result = _compute_ma_scores(spy_only, benchmark="SPY", slope_lookback=5)
    assert result.is_empty()
    assert result.columns == list(MA_SCORE_SCHEMA)


# ---- compute_cloud_scores ---------------------------------------------------


def test_compute_cloud_scores_end_to_end(sample_daily_bars_df):
    """3 ETFs + SPY across all 5 timeframes produce a full scorecard."""
    result = compute_cloud_scores(
        sample_daily_bars_df, tickers=["UP", "DOWN", "FLAT"], benchmark="SPY"
    )
    assert result.columns == list(CLOUD_SCORE_SCHEMA)
    assert result.dtypes == list(CLOUD_SCORE_SCHEMA.values())
    assert result.height == 3
    assert "SPY" not in result["ticker"].to_list()

    by_ticker = {row["ticker"]: row for row in result.iter_rows(named=True)}
    # UP is above all four cloud lines on all five timeframes → 1.0; DOWN is
    # below everything → 0.0. The clouds are scored on the ETF/SPY ratio, so
    # UP's rising ratio beats its own lines and DOWN's falling ratio loses to
    # them on every timeframe. The 2mo column may be null because the 8y
    # fixture only has ~67 2-month bars and the standard (9, 26, 52) Ichimoku
    # needs 78 2-month bars; accept null for the 2mo column.
    for column in (
        "score_weekly_cloud",
        "score_2wk_cloud",
        "score_3wk_cloud",
        "score_monthly_cloud",
    ):
        assert by_ticker["UP"][column] == pytest.approx(1.0)
        assert by_ticker["DOWN"][column] == pytest.approx(0.0)
    # 2mo: the 8y fixture may not have enough 2-month bars for the standard
    # periods; accept either 1.0/0.0 or null.
    assert by_ticker["UP"]["score_2mo_cloud"] in (pytest.approx(1.0), None)
    assert by_ticker["DOWN"]["score_2mo_cloud"] in (pytest.approx(0.0), None)

    # The four MA conditions are self-comparisons on the ratio: UP's ratio
    # rises (above its own MAs, rising slopes → all 1); DOWN's ratio falls
    # and FLAT's ratio falls (both below their own MAs, falling slopes → 0).
    for column in (
        "score_200wk_ma",
        "score_200wk_ma_slope",
        "score_300wk_ma",
        "score_300wk_ma_slope",
    ):
        assert by_ticker["UP"][column] == 1
        assert by_ticker["DOWN"][column] == 0
        assert by_ticker["FLAT"][column] == 0

    # The total is the sum of all nine columns (nulls count as 0), max 9.0.
    for ticker in ("UP", "DOWN", "FLAT"):
        row = by_ticker[ticker]
        expected_total = sum(
            row[column] or 0
            for column in CLOUD_SCORE_SCHEMA
            if column not in {"ticker", "total"}
        )
        assert row["total"] == pytest.approx(expected_total)
    assert by_ticker["UP"]["total"] <= 9.0
    assert result["total"].dtype == pl.Float32


def test_compute_cloud_scores_benchmark_missing_raises(sample_daily_bars_df):
    bars = sample_daily_bars_df.filter(pl.col("ticker") != "SPY")
    with pytest.raises(ValueError, match="benchmark ticker"):
        compute_cloud_scores(bars, tickers=["UP"], benchmark="SPY")


def test_compute_cloud_scores_empty_bars():
    empty = pl.DataFrame(schema=CLOUD_BARS_SCHEMA)
    result = compute_cloud_scores(empty, tickers=["UP"], benchmark="SPY")
    assert result.is_empty()
    assert result.columns == list(CLOUD_SCORE_SCHEMA)


def test_compute_cloud_scores_empty_tickers_raises(sample_daily_bars_df):
    with pytest.raises(ValueError, match="tickers"):
        compute_cloud_scores(sample_daily_bars_df, tickers=[], benchmark="SPY")


def test_compute_cloud_scores_only_benchmark_returns_empty(sample_daily_bars_df):
    result = compute_cloud_scores(
        sample_daily_bars_df, tickers=["SPY"], benchmark="SPY"
    )
    assert result.is_empty()
    assert result.columns == list(CLOUD_SCORE_SCHEMA)


def test_compute_cloud_scores_no_scored_tickers_in_bars_returns_empty():
    """When the bars only hold the benchmark, no ticker is scored."""
    spy_only = _make_ohlc({"SPY": [200.0 + i * 0.1 for i in range(1000)]})
    result = compute_cloud_scores(spy_only, tickers=["UP"], benchmark="SPY")
    assert result.is_empty()
    assert result.columns == list(CLOUD_SCORE_SCHEMA)


def test_compute_cloud_scores_insufficient_history_is_null_and_counts_zero():
    """A short-history ticker gets null cells; nulls count as 0 in total."""
    long_bars = _make_ohlc({"LONG": [100.0 + i * 0.1 for i in range(1000)]})
    short_bars = _make_ohlc({"SHORT": [50.0 + i * 0.1 for i in range(60)]})
    spy_bars = _make_ohlc({"SPY": [200.0 + i * 0.1 for i in range(1000)]})
    bars = pl.concat([long_bars, short_bars, spy_bars])

    result = compute_cloud_scores(bars, tickers=["LONG", "SHORT"], benchmark="SPY")
    by_ticker = {row["ticker"]: row for row in result.iter_rows(named=True)}

    # SHORT has ~12 weeks of data: far too little for the 78-week weekly cloud.
    assert by_ticker["SHORT"]["score_weekly_cloud"] is None
    assert by_ticker["SHORT"]["score_200wk_ma"] is None
    assert by_ticker["SHORT"]["total"] == pytest.approx(0.0)
    # LONG has ~200 weeks: enough for the weekly cloud but not the 300-WK MA.
    assert by_ticker["LONG"]["score_weekly_cloud"] is not None
    assert by_ticker["LONG"]["score_300wk_ma"] is None
    # Both tickers appear in the scorecard.
    assert set(by_ticker) == {"LONG", "SHORT"}


def test_compute_cloud_scores_clouds_scored_on_ratio_not_raw_prices():
    """The cloud score reflects the ratio's position, not the raw price level.

    HIGH is priced far above SPY on every bar (raw price 1000+ vs ~200), but
    its ETF/SPY ratio is strictly falling. Scored on the ratio, its close is
    below the ratio's own Ichimoku lines and MAs → every cell is 0 (or null
    for the deeper timeframes whose history the 2500-day fixture can't
    support with the standard 9/26/52 Ichimoku periods). Under the old
    raw-price methodology HIGH's rising raw close would have scored 1.0 on
    the clouds, so this proves the relative-pricing rewrite.
    """
    bars = _make_ohlc(
        {
            "HIGH": [1000.0 + 0.5 * i for i in range(2500)],
            "SPY": [200.0 + i for i in range(2500)],
        }
    )
    result = compute_cloud_scores(bars, tickers=["HIGH"], benchmark="SPY")
    row = result.row(0, named=True)
    for column in (
        "score_weekly_cloud",
        "score_2wk_cloud",
        "score_3wk_cloud",
        "score_monthly_cloud",
        "score_2mo_cloud",
    ):
        # Either 0.0 (ratio below Ichimoku lines) or None (insufficient
        # history for the deeper timeframe with the standard periods).
        assert row[column] in (pytest.approx(0.0), None)
    for column in (
        "score_200wk_ma",
        "score_200wk_ma_slope",
        "score_300wk_ma",
        "score_300wk_ma_slope",
    ):
        assert row[column] == 0
    assert row["total"] == pytest.approx(0.0)


# ---- render_cloud_scorecard -------------------------------------------------


def _scorecard_frame() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "ticker": ["AAA", "BBB", "CCC"],
            "score_weekly_cloud": [1.0, 0.75, 0.25],
            "score_2wk_cloud": [1.0, 0.75, 0.25],
            "score_3wk_cloud": [1.0, 0.75, 0.25],
            "score_monthly_cloud": [1.0, 0.75, 0.25],
            "score_2mo_cloud": [1.0, 0.75, 0.25],
            "score_200wk_ma": [1, 1, 0],
            "score_200wk_ma_slope": [1, 1, 0],
            "score_300wk_ma": [1, 0, None],
            "score_300wk_ma_slope": [1, 0, None],
            "total": [9.0, 5.75, 1.25],
        },
        schema=CLOUD_SCORE_SCHEMA,
    )


def test_render_cloud_scorecard_returns_table_with_expected_columns():
    table = render_cloud_scorecard(_scorecard_frame(), benchmark="SPY")
    assert table.title == "Ciovacco cloud scorecard vs SPY"
    assert len(table.columns) == 11
    labels = [column.header for column in table.columns]
    assert labels == [
        "Ticker",
        "W-Cloud",
        "2W-Cloud",
        "3W-Cloud",
        "Mo-Cloud",
        "2Mo-Cloud",
        "200W MA",
        "200W slope",
        "300W MA",
        "300W slope",
        "Total",
    ]


def test_render_cloud_scorecard_sorts_by_total_descending():
    table = render_cloud_scorecard(_scorecard_frame(), benchmark="SPY")
    text = _rich_text(table)
    lines = [
        line
        for line in text.split("\n")
        if any(t in line for t in ["AAA", "BBB", "CCC"])
    ]
    assert len(lines) == 3
    assert "AAA" in lines[0]  # total 9.0
    assert "BBB" in lines[1]  # total 5.75
    assert "CCC" in lines[2]  # total 1.25


def test_render_cloud_scorecard_displays_two_decimal_clouds():
    """Cloud cells render as 2-decimal values (1.00, 0.75, ...)."""
    table = render_cloud_scorecard(_scorecard_frame(), benchmark="SPY")
    text = _rich_text(table)
    aaa_line = next(line for line in text.split("\n") if "AAA" in line)
    assert "1.00" in aaa_line
    bbb_line = next(line for line in text.split("\n") if "BBB" in line)
    assert "0.75" in bbb_line
    ccc_line = next(line for line in text.split("\n") if "CCC" in line)
    assert "0.25" in ccc_line


def test_render_cloud_scorecard_caps_rows():
    table = render_cloud_scorecard(_scorecard_frame(), benchmark="SPY", max_etfs=2)
    assert len(table.rows) == 2
    text = _rich_text(table)
    assert "AAA" in text
    assert "BBB" in text
    assert "CCC" not in text


def test_render_cloud_scorecard_null_cells_show_na():
    table = render_cloud_scorecard(_scorecard_frame(), benchmark="SPY")
    text = _rich_text(table)
    ccc_line = next(line for line in text.split("\n") if "CCC" in line)
    assert "n/a" in ccc_line


def test_render_cloud_scorecard_null_cloud_cell_renders_na_dim():
    """A null cloud cell renders as 'n/a' in dim style."""
    frame = _scorecard_frame().with_columns(
        pl.when(pl.col("ticker") == "CCC")
        .then(None)
        .otherwise(pl.col("score_weekly_cloud"))
        .alias("score_weekly_cloud")
    )
    text = _rich_text(render_cloud_scorecard(frame, benchmark="SPY"))
    ccc_line = next(line for line in text.split("\n") if "CCC" in line)
    assert "n/a" in ccc_line
    assert cloud_score._cloud_style(None) == "dim"


def test_render_cloud_scorecard_empty_input():
    empty = pl.DataFrame(schema=CLOUD_SCORE_SCHEMA)
    table = render_cloud_scorecard(empty, benchmark="SPY")
    assert len(table.rows) == 0
    text = _rich_text(table)
    assert "Ciovacco cloud scorecard vs SPY" in text
    assert "Ticker" in text


def test_total_style_buckets():
    """Total cell colors (0-9 scale): >= 7 green, 4-6.99 yellow, < 4 red."""
    assert cloud_score._total_style(7.0) == "green"
    assert cloud_score._total_style(9.0) == "green"
    assert cloud_score._total_style(4.0) == "yellow"
    assert cloud_score._total_style(6.99) == "yellow"
    assert cloud_score._total_style(3.99) == "red"
    assert cloud_score._total_style(0.0) == "red"
    assert cloud_score._total_style(None) == "dim"


def test_render_cloud_scorecard_total_colors():
    ansi = _rich_ansi(render_cloud_scorecard(_scorecard_frame(), benchmark="SPY"))
    red = "\x1b[31m"
    green = "\x1b[32m"
    yellow = "\x1b[33m"
    # Isolate each row's total cell: take everything between the last column
    # separator and the trailing total value in the row line.
    lines = ansi.split("\n")
    aaa_line = next(line for line in lines if "AAA" in line)
    bbb_line = next(line for line in lines if "BBB" in line)
    ccc_line = next(line for line in lines if "CCC" in line)
    aaa_pos = aaa_line.rfind("9.00")
    bbb_pos = bbb_line.rfind("5.75")
    ccc_pos = ccc_line.rfind("1.25")
    aaa_cell = aaa_line[aaa_line.rfind("│", 0, aaa_pos) : aaa_pos]
    bbb_cell = bbb_line[bbb_line.rfind("│", 0, bbb_pos) : bbb_pos]
    ccc_cell = ccc_line[ccc_line.rfind("│", 0, ccc_pos) : ccc_pos]
    assert green in aaa_cell  # total 9.0 → green
    assert yellow in bbb_cell  # total 5.75 → yellow
    assert red not in bbb_cell
    assert red in ccc_cell  # total 1.25 → red
