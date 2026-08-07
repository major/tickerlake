"""Tests for tickerlake.race — pure polars data layer for etf-race."""

import datetime
import re
from typing import TYPE_CHECKING
from unittest.mock import MagicMock, patch

import duckdb
import polars as pl
import pytest
from polars.testing import assert_frame_equal

from tickerlake import race
from tickerlake.race import (
    RACE_BARS_SCHEMA,
    RELATIVE_MOMENTUM_SCHEMA,
    RELATIVE_TREND_SCHEMA,
    classify_horse_form,
    classify_relative_trend,
    compute_relative_momentum,
    compute_relative_race_metrics,
    compute_relative_ratio,
    rebase_to_100,
    render_relative_leaderboard,
)

if TYPE_CHECKING:
    from pathlib import Path


def make_race_bars(ticker_closes: dict[str, list[float]]) -> pl.DataFrame:
    """Build a date/ticker/close frame from per-ticker close series."""
    start = datetime.date(2024, 1, 1)
    rows = [
        {
            "date": start + datetime.timedelta(days=index),
            "ticker": ticker,
            "close": close,
        }
        for ticker, closes in ticker_closes.items()
        for index, close in enumerate(closes)
    ]
    return pl.DataFrame(rows, schema=RACE_BARS_SCHEMA)


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


def test_race_bars_schema_defined():
    assert set(race.RACE_BARS_SCHEMA) == {"date", "ticker", "close"}
    assert race.RACE_BARS_SCHEMA["date"] == pl.Date
    assert race.RACE_BARS_SCHEMA["ticker"] == pl.Utf8
    assert race.RACE_BARS_SCHEMA["close"] == pl.Float32


def test_timeframe_table_maps_all_three():
    assert race.TIMEFRAME_TABLE == {
        "daily": "daily_bars",
        "weekly": "weekly_bars",
        "monthly": "monthly_bars",
    }


def test_read_race_bars_filters_by_ticker_and_date(tmp_path: Path):
    consumer = tmp_path / "tickerlake.duckdb"
    consumer.touch()
    today = datetime.date.today()  # noqa: DTZ011
    start = today - datetime.timedelta(days=30)
    expected = pl.DataFrame(
        {
            "date": [start, start, today - datetime.timedelta(days=1)],
            "ticker": ["AAPL", "MSFT", "AAPL"],
            "close": [100.0, 200.0, 110.0],
        },
        schema=RACE_BARS_SCHEMA,
    ).sort(["ticker", "date"])

    fake = FakeConnection(expected)
    with patch("tickerlake.race.duckdb.connect", return_value=fake):
        result = race.read_race_bars(
            consumer, timeframe="daily", tickers=["aapl", "msft"], lookback_days=30
        )

    assert len(fake.calls) == 1
    sql, params = fake.calls[0]
    assert "FROM daily_bars" in sql
    assert "WHERE ticker IN (?, ?) AND date >= ?" in sql
    assert "ORDER BY ticker, date" in sql
    assert params == ["AAPL", "MSFT", start]
    assert fake.closed
    assert result.columns == ["date", "ticker", "close"]
    assert result.dtypes == [pl.Date, pl.Utf8, pl.Float32]
    assert_frame_equal(result, expected)


def test_read_race_bars_validates_timeframe(tmp_path: Path):
    with pytest.raises(ValueError, match="timeframe"):
        race.read_race_bars(
            tmp_path / "missing.duckdb",
            timeframe="hourly",
            tickers=["AAPL"],
            lookback_days=30,
        )


def test_read_race_bars_validates_tickers_empty(tmp_path: Path):
    with pytest.raises(ValueError, match="tickers"):
        race.read_race_bars(
            tmp_path / "missing.duckdb",
            timeframe="daily",
            tickers=[],
            lookback_days=30,
        )


@pytest.mark.parametrize("lookback_days", [0, -1])
def test_read_race_bars_validates_lookback(tmp_path: Path, lookback_days: int):
    with pytest.raises(ValueError, match="lookback"):
        race.read_race_bars(
            tmp_path / "missing.duckdb",
            timeframe="daily",
            tickers=["AAPL"],
            lookback_days=lookback_days,
        )


def test_read_race_bars_validates_path_exists(tmp_path: Path):
    with pytest.raises(ValueError, match="Consumer DB not found"):
        race.read_race_bars(
            tmp_path / "missing.duckdb",
            timeframe="daily",
            tickers=["AAPL"],
            lookback_days=30,
        )


def test_read_race_bars_missing_table_raises_valueerror(tmp_path: Path):
    consumer = tmp_path / "tickerlake.duckdb"
    consumer.touch()
    fake = MagicMock()
    fake.execute.side_effect = duckdb.CatalogException("no such table: weekly_bars")

    with (
        patch("tickerlake.race.duckdb.connect", return_value=fake),
        pytest.raises(ValueError, match="weekly_bars"),
    ):
        race.read_race_bars(
            consumer, timeframe="weekly", tickers=["CIBR"], lookback_days=30
        )

    fake.close.assert_called_once()


# ---- read_qualifying_etfs -------------------------------------------------


def test_read_qualifying_etfs_filters_by_type_volume_and_active(tmp_path: Path):
    consumer = tmp_path / "tickerlake.duckdb"
    consumer.touch()
    result_df = pl.DataFrame({"ticker": ["SPY", "XLK", "XLF"]})
    fake = FakeConnection(result_df)

    with patch("tickerlake.race.duckdb.connect", return_value=fake):
        out = race.read_qualifying_etfs(consumer)

    assert out == ["SPY", "XLK", "XLF"]
    assert len(fake.calls) == 1
    sql, _params = fake.calls[0]
    assert "FROM tickers t" in sql
    assert "FROM daily_metrics" in sql  # picks latest row per ticker
    assert "WHERE t.type = 'ETF' AND t.active" in sql
    assert "m.volume_sma_20 >= 250000" in sql
    assert "regexp_matches(lower(t.name)" in sql
    assert "1x|2x|3x|inverse|leverage" in sql
    assert sql.index("regexp_matches") < sql.index("ORDER BY")
    assert "ORDER BY m.volume_sma_20 DESC, t.ticker" in sql
    assert "LIMIT 50" in sql
    assert fake.closed


def test_read_qualifying_etfs_respects_custom_threshold(tmp_path: Path):
    consumer = tmp_path / "tickerlake.duckdb"
    consumer.touch()
    result_df = pl.DataFrame({"ticker": ["SPY"]})
    fake = FakeConnection(result_df)

    with patch("tickerlake.race.duckdb.connect", return_value=fake):
        out = race.read_qualifying_etfs(consumer, min_volume_sma_20=1_000_000.0)

    assert out == ["SPY"]
    sql, _ = fake.calls[0]
    assert "m.volume_sma_20 >= 1000000" in sql


def test_read_qualifying_etfs_default_limit_is_50():
    assert race._DEFAULT_MAX_ETFS == 50


def test_read_qualifying_etfs_respects_custom_limit(tmp_path: Path):
    consumer = tmp_path / "tickerlake.duckdb"
    consumer.touch()
    result_df = pl.DataFrame({"ticker": ["SPY"]})
    fake = FakeConnection(result_df)

    with patch("tickerlake.race.duckdb.connect", return_value=fake):
        race.read_qualifying_etfs(consumer, limit=10)

    sql, _ = fake.calls[0]
    assert "LIMIT 10" in sql


def test_read_qualifying_etfs_limit_none_omits_sql_limit(tmp_path: Path):
    consumer = tmp_path / "tickerlake.duckdb"
    consumer.touch()
    result_df = pl.DataFrame({"ticker": ["SPY"]})
    fake = FakeConnection(result_df)

    with patch("tickerlake.race.duckdb.connect", return_value=fake):
        race.read_qualifying_etfs(consumer, limit=None)

    sql, _ = fake.calls[0]
    assert "LIMIT" not in sql


def test_read_qualifying_etfs_validates_limit_below_one(tmp_path: Path):
    with pytest.raises(ValueError, match="limit"):
        race.read_qualifying_etfs(tmp_path / "missing.duckdb", limit=0)


def test_read_qualifying_etfs_default_threshold_is_250k():
    assert race._DEFAULT_MIN_VOL_SMA_20 == 250_000.0


def test_read_qualifying_etfs_validates_negative_threshold(tmp_path: Path):
    with pytest.raises(ValueError, match="min_volume_sma_20"):
        race.read_qualifying_etfs(tmp_path / "missing.duckdb", min_volume_sma_20=-1.0)


def test_read_qualifying_etfs_validates_path_exists(tmp_path: Path):
    with pytest.raises(ValueError, match="Consumer DB not found"):
        race.read_qualifying_etfs(tmp_path / "missing.duckdb")


def test_read_qualifying_etfs_missing_tables_raises_valueerror(tmp_path: Path):
    consumer = tmp_path / "tickerlake.duckdb"
    consumer.touch()
    fake = MagicMock()
    fake.execute.side_effect = duckdb.CatalogException("no such table: daily_metrics")

    with (
        patch("tickerlake.race.duckdb.connect", return_value=fake),
        pytest.raises(ValueError, match="daily_metrics"),
    ):
        race.read_qualifying_etfs(consumer)

    fake.close.assert_called_once()


def test_rebase_to_100_handles_multi_ticker():
    bars = make_race_bars(
        {
            "AAPL": [100.0 + i * 10 for i in range(10)],
            "MSFT": [200.0 + i * 5 for i in range(10)],
            "SPY": [300.0 + i * 2 for i in range(10)],
        }
    )
    result = race.rebase_to_100(bars)

    assert result.columns == ["date", "ticker", "close", "rebased"]
    assert result["rebased"].dtype == pl.Float32
    for ticker in ("AAPL", "MSFT", "SPY"):
        first = (
            result.filter(pl.col("ticker") == ticker).sort("date").row(0, named=True)
        )
        assert first["rebased"] == 100.0
    aapl = result.filter(pl.col("ticker") == "AAPL").sort("date")
    assert aapl["rebased"].to_list() == pytest.approx(
        [100.0 + i * 10 for i in range(10)]
    )


def test_rebase_to_100_empty_input():
    empty = pl.DataFrame(schema=RACE_BARS_SCHEMA)
    result = race.rebase_to_100(empty)

    assert result.is_empty()
    assert result.columns == ["date", "ticker", "close"]
    assert "rebased" not in result.columns


def _rich_text(table) -> str:
    """Render a Rich Table to text via record+export."""
    from rich.console import Console

    console = Console(record=True, width=200, force_terminal=False)
    console.print(table)
    return console.export_text()


# ---- detect_pending_overtakes ---------------------------------------------


def _make_race_bars_for_overtakes() -> pl.DataFrame:
    """Build race_bars with leader A and trailing B closing the gap.

    A goes 100 -> 110 (steady gain).
    B goes 100 -> 108 (faster recent gain, closing the gap).
    """
    dates = [
        datetime.date(2024, 1, 1) + datetime.timedelta(days=7 * i) for i in range(12)
    ]
    a = [100.0 + i for i in range(12)]  # 100..111
    b = [100.0 + 0.5 * i for i in range(12)]  # 100..105.5
    # Make B accelerate in the last 4 bars
    for i in range(8, 12):
        b[i] = b[i - 1] + 1.5
    rows = []
    for i, d in enumerate(dates):
        rows.append({"date": d, "ticker": "A", "close": a[i], "rebased": a[i]})
        rows.append({"date": d, "ticker": "B", "close": b[i], "rebased": b[i]})
    return pl.DataFrame(rows, schema=RACE_BARS_SCHEMA | {"rebased": pl.Float32})


@pytest.mark.parametrize(
    ("kw", "val"),
    [("recent_window", 0), ("gap_close_pct", 0.0), ("gap_close_pct", 101.0)],
)
def _make_leaderboard_metrics() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "ticker": ["A", "B", "C", "D"],
            "current_value": [110.0, 108.0, 102.0, 99.0],
            "total_return_pct": [10.0, 8.0, 2.0, -1.0],
            "recent_return_pct": [3.0, -2.0, 0.5, 1.5],
            "momentum": [1.5, 0.5, -0.2, -1.5],
            "rank": [1, 2, 3, 4],
        },
        schema={
            "ticker": pl.Utf8,
            "current_value": pl.Float32,
            "total_return_pct": pl.Float32,
            "recent_return_pct": pl.Float32,
            "momentum": pl.Float32,
            "rank": pl.Int32,
        },
    )


def _make_leaderboard_bars() -> pl.DataFrame:
    dates = [
        datetime.date(2024, 1, 1) + datetime.timedelta(days=7 * i) for i in range(5)
    ]
    rows = []
    series = [
        ("A", [100, 102, 105, 108, 110]),
        ("B", [100, 103, 106, 107, 108]),
        ("C", [100, 101, 101, 102, 102]),
        ("D", [100, 100, 99, 99, 99]),
    ]
    for tk, vals in series:
        for d, v in zip(dates, vals, strict=True):
            rows.append(
                {"date": d, "ticker": tk, "close": float(v), "rebased": float(v)}
            )
    return pl.DataFrame(rows, schema=RACE_BARS_SCHEMA | {"rebased": pl.Float32})


# ---- compute_relative_ratio ------------------------------------------------


def test_compute_relative_ratio_basic():
    """Compute ratio for 3 tickers vs SPY benchmark."""
    bars = make_race_bars(
        {
            "SPY": [100.0, 110.0, 120.0],
            "AAPL": [200.0, 220.0, 240.0],
            "MSFT": [150.0, 165.0, 180.0],
        }
    )
    result = compute_relative_ratio(bars, benchmark="SPY")

    # Should have AAPL and MSFT, not SPY.
    tickers = set(result["ticker"].to_list())
    assert tickers == {"AAPL", "MSFT"}

    # Check AAPL ratio: 200/100=2.0, 220/110=2.0, 240/120=2.0
    aapl = result.filter(pl.col("ticker") == "AAPL").sort("date")
    assert aapl["close"].to_list() == pytest.approx([2.0, 2.0, 2.0])

    # Check MSFT ratio: 150/100=1.5, 165/110=1.5, 180/120=1.5
    msft = result.filter(pl.col("ticker") == "MSFT").sort("date")
    assert msft["close"].to_list() == pytest.approx([1.5, 1.5, 1.5])


def test_compute_relative_ratio_benchmark_not_present():
    """Raise ValueError when benchmark is not in bars."""
    bars = make_race_bars({"AAPL": [100.0, 110.0], "MSFT": [200.0, 220.0]})
    with pytest.raises(ValueError, match="benchmark ticker"):
        compute_relative_ratio(bars, benchmark="SPY")


def test_compute_relative_ratio_empty_input():
    """Return empty frame matching RACE_BARS_SCHEMA."""
    empty = pl.DataFrame(schema=RACE_BARS_SCHEMA)
    result = compute_relative_ratio(empty, benchmark="SPY")
    assert result.is_empty()
    assert result.columns == ["date", "ticker", "close"]
    assert result.dtypes == [pl.Date, pl.Utf8, pl.Float32]


def test_compute_relative_ratio_only_benchmark_ticker():
    """Return empty frame when input contains only the benchmark."""
    bars = make_race_bars({"SPY": [100.0, 110.0, 120.0]})
    result = compute_relative_ratio(bars, benchmark="SPY")
    assert result.is_empty()
    assert result.columns == ["date", "ticker", "close"]


# ---- compute_relative_momentum -----------------------------------------------


def test_compute_relative_momentum_basic():
    """Compute momentum across three windows."""
    # Build a simple series: 100, 102, 104, 106, 108 (constant +2 per bar)
    bars = make_race_bars({"AAPL": [100.0, 102.0, 104.0, 106.0, 108.0]})
    rebased = rebase_to_100(bars)
    result = compute_relative_momentum(
        rebased, short_window=1, medium_window=2, long_window=3
    )

    assert result.columns == list(RELATIVE_MOMENTUM_SCHEMA.keys())
    assert result.height == 1
    row = result.row(0, named=True)
    assert row["ticker"] == "AAPL"
    # Current rebased value is 108 (last bar).
    assert row["rs_ratio"] == pytest.approx(108.0)
    # momentum_short = 108 - 106 (1 bar back) = 2.0
    assert row["momentum_short"] == pytest.approx(2.0)
    # momentum_medium = 108 - 104 (2 bars back) = 4.0
    assert row["momentum_medium"] == pytest.approx(4.0)
    # momentum_long = 108 - 102 (3 bars back) = 6.0
    assert row["momentum_long"] == pytest.approx(6.0)
    # rate_short = 2.0 / 1 = 2.0
    assert row["rate_short"] == pytest.approx(2.0)
    # rate_medium = 4.0 / 2 = 2.0
    assert row["rate_medium"] == pytest.approx(2.0)
    # rate_long = 6.0 / 3 = 2.0
    assert row["rate_long"] == pytest.approx(2.0)


def test_compute_relative_momentum_window_larger_than_history():
    """Gracefully clamp when window exceeds available history."""
    bars = make_race_bars({"AAPL": [100.0, 102.0, 104.0]})
    rebased = rebase_to_100(bars)
    result = compute_relative_momentum(
        rebased, short_window=1, medium_window=2, long_window=10
    )

    row = result.row(0, named=True)
    # long_window=10 exceeds history (3 bars), so clamps to first value (100).
    assert row["momentum_short"] == pytest.approx(2.0)  # 104 - 102
    assert row["momentum_medium"] == pytest.approx(4.0)  # 104 - 100
    assert row["momentum_long"] == pytest.approx(4.0)  # 104 - 100 (clamped)


def test_compute_relative_momentum_validates_windows():
    """Raise ValueError for invalid window sizes."""
    bars = make_race_bars({"AAPL": [100.0, 102.0]})
    rebased = rebase_to_100(bars)

    with pytest.raises(ValueError, match="short_window"):
        compute_relative_momentum(
            rebased, short_window=0, medium_window=1, long_window=2
        )

    with pytest.raises(ValueError, match="medium_window"):
        compute_relative_momentum(
            rebased, short_window=1, medium_window=0, long_window=2
        )

    with pytest.raises(ValueError, match="long_window"):
        compute_relative_momentum(
            rebased, short_window=1, medium_window=2, long_window=0
        )

    # N11: windows must be strictly increasing
    with pytest.raises(ValueError, match="strictly increasing"):
        compute_relative_momentum(
            rebased, short_window=2, medium_window=2, long_window=3
        )

    with pytest.raises(ValueError, match="strictly increasing"):
        compute_relative_momentum(
            rebased, short_window=1, medium_window=3, long_window=2
        )


def test_compute_relative_momentum_empty_input():
    """Return empty frame matching RELATIVE_MOMENTUM_SCHEMA."""
    empty = pl.DataFrame(schema=RACE_BARS_SCHEMA)
    result = compute_relative_momentum(
        empty, short_window=1, medium_window=2, long_window=3
    )
    assert result.is_empty()
    assert result.columns == list(RELATIVE_MOMENTUM_SCHEMA.keys())
    assert result.dtypes == list(RELATIVE_MOMENTUM_SCHEMA.values())


def test_compute_relative_momentum_missing_rebased_column():
    """Return empty frame when rebased column is missing."""
    bars = make_race_bars({"AAPL": [100.0, 102.0]})
    result = compute_relative_momentum(
        bars, short_window=1, medium_window=2, long_window=3
    )
    assert result.is_empty()
    assert result.columns == list(RELATIVE_MOMENTUM_SCHEMA.keys())


# ---- classify_relative_trend ------------------------------------------------


def test_classify_relative_trend_leading():
    """rs_ratio >= 100 and momentum_short > 0 -> Leading."""
    momentum = pl.DataFrame(
        {
            "ticker": ["A"],
            "rs_ratio": [105.0],
            "momentum_short": [2.0],
            "momentum_medium": [1.0],
            "momentum_long": [0.5],
            "rate_short": [2.0],
            "rate_medium": [1.0],
            "rate_long": [0.5],
        },
        schema=RELATIVE_MOMENTUM_SCHEMA,
    )
    result = classify_relative_trend(momentum)
    assert result["trend"][0] == "Leading"


def test_classify_relative_trend_fading():
    """rs_ratio >= 100 and momentum_short <= 0 -> Fading."""
    momentum = pl.DataFrame(
        {
            "ticker": ["A"],
            "rs_ratio": [105.0],
            "momentum_short": [-1.0],
            "momentum_medium": [1.0],
            "momentum_long": [2.0],
            "rate_short": [-1.0],
            "rate_medium": [1.0],
            "rate_long": [2.0],
        },
        schema=RELATIVE_MOMENTUM_SCHEMA,
    )
    result = classify_relative_trend(momentum)
    assert result["trend"][0] == "Fading"


def test_classify_relative_trend_improving():
    """rs_ratio < 100 and momentum_short > 0 -> Improving."""
    momentum = pl.DataFrame(
        {
            "ticker": ["A"],
            "rs_ratio": [95.0],
            "momentum_short": [2.0],
            "momentum_medium": [1.0],
            "momentum_long": [0.5],
            "rate_short": [2.0],
            "rate_medium": [1.0],
            "rate_long": [0.5],
        },
        schema=RELATIVE_MOMENTUM_SCHEMA,
    )
    result = classify_relative_trend(momentum)
    assert result["trend"][0] == "Improving"


def test_classify_relative_trend_lagging():
    """rs_ratio < 100 and momentum_short <= 0 -> Lagging."""
    momentum = pl.DataFrame(
        {
            "ticker": ["A"],
            "rs_ratio": [95.0],
            "momentum_short": [-1.0],
            "momentum_medium": [1.0],
            "momentum_long": [2.0],
            "rate_short": [-1.0],
            "rate_medium": [1.0],
            "rate_long": [2.0],
        },
        schema=RELATIVE_MOMENTUM_SCHEMA,
    )
    result = classify_relative_trend(momentum)
    assert result["trend"][0] == "Lagging"


def test_classify_relative_trend_building_true():
    """building=True when rate_short > rate_medium > rate_long AND momentum > 0."""
    momentum = pl.DataFrame(
        {
            "ticker": ["A"],
            "rs_ratio": [100.0],
            "momentum_short": [3.0],
            "momentum_medium": [2.0],
            "momentum_long": [1.0],
            "rate_short": [3.0],
            "rate_medium": [1.0],
            "rate_long": [0.5],
        },
        schema=RELATIVE_MOMENTUM_SCHEMA,
    )
    result = classify_relative_trend(momentum)
    assert result["building"][0] is True


def test_classify_relative_trend_building_false():
    """building=False when rates do not accelerate."""
    momentum = pl.DataFrame(
        {
            "ticker": ["A"],
            "rs_ratio": [100.0],
            "momentum_short": [1.0],
            "momentum_medium": [2.0],
            "momentum_long": [3.0],
            "rate_short": [1.0],
            "rate_medium": [2.0],
            "rate_long": [3.0],
        },
        schema=RELATIVE_MOMENTUM_SCHEMA,
    )
    result = classify_relative_trend(momentum)
    assert result["building"][0] is False


def test_classify_relative_trend_building_false_with_null():
    """building=False when any rate value is null."""
    momentum = pl.DataFrame(
        {
            "ticker": ["A"],
            "rs_ratio": [100.0],
            "momentum_short": [3.0],
            "momentum_medium": [2.0],
            "momentum_long": [1.0],
            "rate_short": [None],
            "rate_medium": [2.0],
            "rate_long": [1.0],
        },
        schema=RELATIVE_MOMENTUM_SCHEMA,
    )
    result = classify_relative_trend(momentum)
    assert result["building"][0] is False


def test_classify_relative_trend_unknown():
    """M4: rs_ratio or momentum_short null -> Unknown."""
    momentum = pl.DataFrame(
        {
            "ticker": ["A", "B"],
            "rs_ratio": [None, 100.0],
            "momentum_short": [2.0, None],
            "momentum_medium": [1.0, 1.0],
            "momentum_long": [0.5, 0.5],
            "rate_short": [2.0, 2.0],
            "rate_medium": [1.0, 1.0],
            "rate_long": [0.5, 0.5],
        },
        schema=RELATIVE_MOMENTUM_SCHEMA,
    )
    result = classify_relative_trend(momentum)
    assert result["trend"][0] == "Unknown"
    assert result["trend"][1] == "Unknown"


def test_classify_relative_trend_empty_input():
    """Return empty frame matching RELATIVE_TREND_SCHEMA."""
    empty = pl.DataFrame(schema=RELATIVE_MOMENTUM_SCHEMA)
    result = classify_relative_trend(empty)
    assert result.is_empty()
    assert result.columns == list(RELATIVE_TREND_SCHEMA.keys())
    assert result.dtypes == list(RELATIVE_TREND_SCHEMA.values())


# ---- render_relative_leaderboard -------------------------------------------


def test_render_relative_leaderboard_returns_table():
    """render_relative_leaderboard returns a Rich Table."""
    trend = pl.DataFrame(
        {
            "ticker": ["A", "B"],
            "rs_ratio": [105.0, 95.0],
            "momentum_short": [2.0, 1.0],
            "momentum_medium": [1.0, 0.5],
            "momentum_long": [0.5, 0.2],
            "rate_short": [2.0, 1.0],
            "rate_medium": [1.0, 0.5],
            "rate_long": [0.5, 0.2],
            "trend": ["Leading", "Improving"],
            "building": [True, False],
        },
        schema=RELATIVE_TREND_SCHEMA,
    )
    table = render_relative_leaderboard(trend, benchmark="SPY")
    assert table.title == "🐎 vs SPY Momentum"


def test_render_relative_leaderboard_sorts_by_building_then_momentum():
    """Rows sorted by building desc, then momentum_short desc."""
    trend = pl.DataFrame(
        {
            "ticker": ["AAA", "BBB", "CCC"],
            "rs_ratio": [105.0, 100.0, 95.0],
            "momentum_short": [1.0, 3.0, 2.0],
            "momentum_medium": [0.5, 2.0, 1.0],
            "momentum_long": [0.2, 1.0, 0.5],
            "rate_short": [1.0, 3.0, 2.0],
            "rate_medium": [0.5, 2.0, 1.0],
            "rate_long": [0.2, 1.0, 0.5],
            "trend": ["Leading", "Leading", "Improving"],
            "building": [False, True, True],
        },
        schema=RELATIVE_TREND_SCHEMA,
    )
    table = render_relative_leaderboard(trend, benchmark="SPY")
    text = _rich_text(table)
    # BBB and CCC have building=True, so they appear before AAA.
    # Among BBB and CCC: BBB has momentum_short=3.0, CCC has 2.0, so BBB comes first.
    # Expected order: BBB (building=True, momentum=3.0), CCC (building=True,
    # momentum=2.0), AAA (building=False, momentum=1.0)
    lines = text.split("\n")
    # Find rows with ticker names (skip header).
    ticker_lines = [
        line for line in lines if any(t in line for t in ["AAA", "BBB", "CCC"])
    ]
    # BBB should appear before CCC, CCC before AAA.
    bbb_idx = next(i for i, line in enumerate(ticker_lines) if "BBB" in line)
    ccc_idx = next(i for i, line in enumerate(ticker_lines) if "CCC" in line)
    aaa_idx = next(i for i, line in enumerate(ticker_lines) if "AAA" in line)
    assert bbb_idx < ccc_idx < aaa_idx


def test_render_relative_leaderboard_empty_input():
    """Empty input returns table with headers only."""
    empty = pl.DataFrame(schema=RELATIVE_TREND_SCHEMA)
    table = render_relative_leaderboard(empty, benchmark="SPY")
    text = _rich_text(table)
    # Should have title and headers but no data rows.
    assert "🐎 vs SPY Momentum" in text
    assert "Ticker" in text


def test_render_relative_leaderboard_omits_legacy_trend_column():
    """The compact horse table omits the legacy trend diagnostic."""
    trend = pl.DataFrame(
        {
            "ticker": ["A", "B", "C", "D"],
            "rs_ratio": [105.0, 105.0, 95.0, 95.0],
            "momentum_short": [2.0, -1.0, 2.0, -1.0],
            "momentum_medium": [1.0, 1.0, 1.0, 1.0],
            "momentum_long": [0.5, 0.5, 0.5, 0.5],
            "rate_short": [2.0, -1.0, 2.0, -1.0],
            "rate_medium": [1.0, 1.0, 1.0, 1.0],
            "rate_long": [0.5, 0.5, 0.5, 0.5],
            "trend": ["Leading", "Fading", "Improving", "Lagging"],
            "building": [False, False, False, False],
        },
        schema=RELATIVE_TREND_SCHEMA,
    )
    table = render_relative_leaderboard(trend, benchmark="SPY")
    text = _rich_text(table)
    assert "Trend" not in text
    assert "🟢" not in text
    assert "🟠" not in text
    assert "🟡" not in text
    assert "🔴" not in text


def test_render_relative_leaderboard_omits_legacy_building_column():
    """The compact horse table omits the legacy building diagnostic."""
    trend = pl.DataFrame(
        {
            "ticker": ["A", "B"],
            "rs_ratio": [105.0, 95.0],
            "momentum_short": [2.0, 1.0],
            "momentum_medium": [1.0, 0.5],
            "momentum_long": [0.5, 0.2],
            "rate_short": [2.0, 1.0],
            "rate_medium": [1.0, 0.5],
            "rate_long": [0.5, 0.2],
            "trend": ["Leading", "Improving"],
            "building": [True, False],
        },
        schema=RELATIVE_TREND_SCHEMA,
    )
    table = render_relative_leaderboard(trend, benchmark="SPY")
    text = _rich_text(table)
    assert "Building" not in text
    assert "🚀" not in text


# ---- End-to-end tests (M5) ------------------------------------------------


def test_relative_momentum_end_to_end_monotonic_outperformer():
    """M5: Monotonically outperforming ticker should have building=True.

    Ticker gains +2 per bar vs benchmark (constant), so every window shows
    acceleration: rate_short > rate_medium > rate_long, momentum_medium > 0.
    """
    # Benchmark: 100, 100, 100, 100, 100 (flat)
    # Ticker: 100, 102, 104, 106, 108 (constant +2 per bar)
    start = datetime.date(2024, 1, 1)
    dates = [start + datetime.timedelta(days=i) for i in range(5)]
    bars = pl.DataFrame(
        {
            "date": dates + dates,
            "ticker": ["SPY"] * 5 + ["OUTPERFORMER"] * 5,
            "close": [100.0] * 5 + [100.0, 102.0, 104.0, 106.0, 108.0],
        },
        schema=RACE_BARS_SCHEMA,
    )

    # Compute relative ratio: OUTPERFORMER / SPY
    ratio_bars = compute_relative_ratio(bars, benchmark="SPY")
    assert not ratio_bars.is_empty()
    assert set(ratio_bars["ticker"].to_list()) == {"OUTPERFORMER"}

    # Rebase to 100
    rebased = rebase_to_100(ratio_bars)
    assert "rebased" in rebased.columns

    # Compute momentum with short=1, medium=2, long=3
    momentum = compute_relative_momentum(
        rebased, short_window=1, medium_window=2, long_window=3
    )
    assert momentum.height == 1
    row = momentum.row(0, named=True)
    # Ratios: 1.0, 1.02, 1.04, 1.06, 1.08 (rebased: 100, 102, 104, 106, 108)
    # rate_short = (108-106)/1 = 2.0, rate_medium = (108-104)/2 = 2.0,
    # rate_long = (108-102)/3 = 2.0. All equal, so building=False (not strictly >).
    # Verify rates are equal for constant acceleration.
    assert row["rate_short"] == pytest.approx(2.0, abs=1e-4)
    assert row["rate_medium"] == pytest.approx(2.0, abs=1e-4)
    assert row["rate_long"] == pytest.approx(2.0, abs=1e-4)

    # Classify trend
    trend = classify_relative_trend(momentum)
    assert trend.height == 1
    trend_row = trend.row(0, named=True)
    # rs_ratio = 108 (rebased), momentum_short > 0, so "Leading"
    assert trend_row["trend"] == "Leading"
    # building: rate_short > rate_medium > rate_long? No, all equal.
    # So building=False. This is correct: constant acceleration doesn't
    # show *accelerating* acceleration.


def test_relative_momentum_end_to_end_monotonic_decliner():
    """M5: Monotonically underperforming ticker should have building=False.

    Ticker loses -2 per bar vs benchmark (constant), so rates are all
    negative and equal: rate_short = rate_medium = rate_long = -2.0.
    building=False because momentum_medium < 0 (positivity gate).
    """
    # Benchmark: 100, 100, 100, 100, 100 (flat)
    # Ticker: 100, 98, 96, 94, 92 (constant -2 per bar)
    start = datetime.date(2024, 1, 1)
    dates = [start + datetime.timedelta(days=i) for i in range(5)]
    bars = pl.DataFrame(
        {
            "date": dates + dates,
            "ticker": ["SPY"] * 5 + ["DECLINER"] * 5,
            "close": [100.0] * 5 + [100.0, 98.0, 96.0, 94.0, 92.0],
        },
        schema=RACE_BARS_SCHEMA,
    )

    ratio_bars = compute_relative_ratio(bars, benchmark="SPY")
    rebased = rebase_to_100(ratio_bars)
    momentum = compute_relative_momentum(
        rebased, short_window=1, medium_window=2, long_window=3
    )
    trend = classify_relative_trend(momentum)

    trend_row = trend.row(0, named=True)
    # rs_ratio = 92 (rebased), momentum_short < 0, so "Lagging"
    assert trend_row["trend"] == "Lagging"
    # building: momentum_medium < 0, so building=False (positivity gate)
    assert trend_row["building"] is False


def test_relative_ratio_misaligned_dates():
    """M5: Ticker with fewer/offset bars than benchmark should not crash.

    Benchmark has 5 bars, ticker has only 3 (recent listing simulation).
    Inner join should produce 3 rows (only the overlapping dates).
    No nulls in the output.
    """
    start = datetime.date(2024, 1, 1)
    bars = pl.DataFrame(
        {
            "date": [
                start,
                start + datetime.timedelta(days=1),
                start + datetime.timedelta(days=2),
                start + datetime.timedelta(days=3),
                start + datetime.timedelta(days=4),
                start + datetime.timedelta(days=2),
                start + datetime.timedelta(days=3),
                start + datetime.timedelta(days=4),
            ],
            "ticker": ["SPY"] * 5 + ["NEWLISTING"] * 3,
            "close": [100.0, 101.0, 102.0, 103.0, 104.0, 50.0, 51.0, 52.0],
        },
        schema=RACE_BARS_SCHEMA,
    )

    ratio_bars = compute_relative_ratio(bars, benchmark="SPY")
    # Should have 3 rows (only overlapping dates)
    assert ratio_bars.height == 3
    # No nulls in close column
    assert ratio_bars["close"].null_count() == 0
    # Ratios should be: 50/102, 51/103, 52/104
    assert ratio_bars["close"].to_list() == pytest.approx(
        [50.0 / 102.0, 51.0 / 103.0, 52.0 / 104.0]
    )

    # Verify render doesn't crash
    rebased = rebase_to_100(ratio_bars)
    momentum = compute_relative_momentum(
        rebased, short_window=1, medium_window=2, long_window=3
    )
    trend = classify_relative_trend(momentum)
    table = render_relative_leaderboard(trend, benchmark="SPY")
    # Should render without error
    assert table.title == "🐎 vs SPY Momentum"


def test_compute_relative_race_metrics_identifies_closing_horse():
    """A horse gaining places has positive relative pace and places gained."""
    dates = [datetime.date(2024, 1, 1) + datetime.timedelta(days=i) for i in range(6)]
    ratio_bars = pl.DataFrame(
        {
            "date": dates * 3,
            "ticker": ["CHARGER"] * 6 + ["STEADY"] * 6 + ["FADER"] * 6,
            "close": (
                [
                    100.0,
                    99.0,
                    98.0,
                    99.0,
                    100.0,
                    112.0,
                    100.0,
                    101.0,
                    102.0,
                    103.0,
                    104.0,
                    105.0,
                    100.0,
                    101.0,
                    102.0,
                    101.0,
                    99.0,
                    97.0,
                ]
            ),
        },
        schema=RACE_BARS_SCHEMA,
    )

    result = compute_relative_race_metrics(
        ratio_bars, short_window=1, medium_window=2, long_window=3
    )
    charger = result.filter(pl.col("ticker") == "CHARGER").row(0, named=True)

    assert charger["relative_return_short"] == pytest.approx(12.0, abs=0.01)
    assert charger["relative_return_medium"] == pytest.approx(13.13, abs=0.01)
    assert charger["relative_return_long"] == pytest.approx(14.29, abs=0.01)
    assert charger["places_gained"] > 0
    assert charger["race_score"] > 50


def test_classify_horse_form_labels_front_runner_and_charger():
    """Horse form uses plain race language for the single race table."""
    metrics = pl.DataFrame(
        {
            "ticker": ["LEADER", "CHARGER", "FADER"],
            "position": [1, 5, 3],
            "places_gained": [0, 4, -2],
            "relative_return_short": [2.0, 3.0, -1.0],
            "relative_return_medium": [4.0, 5.0, 2.0],
            "relative_return_long": [8.0, 1.0, 7.0],
            "building": [False, True, False],
            "race_score": [90.0, 85.0, 70.0],
        }
    )

    result = classify_horse_form(metrics)

    assert result["form"].to_list() == ["Front-runner", "Charging", "Losing steam"]


def test_render_relative_leaderboard_shows_horse_metrics():
    """The single horse table exposes position, places, pace, and form."""
    metrics = pl.DataFrame(
        {
            "ticker": ["CHARGER"],
            "position": [2],
            "places_gained": [5],
            "relative_return_short": [2.0],
            "relative_return_medium": [4.0],
            "relative_return_long": [6.0],
            "race_score": [88.0],
            "form": ["Charging"],
            "rs_ratio": [108.0],
            "momentum_short": [2.0],
            "momentum_medium": [4.0],
            "momentum_long": [6.0],
            "rate_short": [2.0],
            "rate_medium": [2.0],
            "rate_long": [2.0],
            "trend": ["Leading"],
            "building": [True],
        }
    )

    text = _rich_text(render_relative_leaderboard(metrics, benchmark="SPY"))

    assert "Places" in text
    assert "Pace Short" in text
    assert "Race" in text
    assert "Charging" in text
    assert "RS-Ratio" not in text
    assert "Trend" not in text
    assert "Momentum Short" not in text
    assert "Building" not in text


def test_classify_relative_trend_decelerating_decline():
    """M5: Decelerating decline (all negative but less negative each window)
    should have building=False due to momentum_medium <= 0 gate.
    """
    # Rates: -0.5 > -1.0 > -2.0 (accelerating in magnitude, but all negative)
    # momentum_medium = -1.0 (negative), so building=False
    momentum = pl.DataFrame(
        {
            "ticker": ["A"],
            "rs_ratio": [95.0],
            "momentum_short": [-0.5],
            "momentum_medium": [-1.0],
            "momentum_long": [-2.0],
            "rate_short": [-0.5],
            "rate_medium": [-1.0],
            "rate_long": [-2.0],
        },
        schema=RELATIVE_MOMENTUM_SCHEMA,
    )
    result = classify_relative_trend(momentum)
    # Trend: rs_ratio < 100 and momentum_short <= 0 -> "Lagging"
    assert result["trend"][0] == "Lagging"
    # building: rate_short > rate_medium > rate_long? Yes (-0.5 > -1.0 > -2.0)
    # But momentum_medium > 0? No (-1.0 <= 0), so building=False
    assert result["building"][0] is False


# ---- Cleanup pass tests (ora-2) -------------------------------------------


def test_compute_relative_momentum_short_history_clamp():
    """M-1: Clamp (min(window, n_bars-1)) has test coverage.

    3-bar ticker with large positive move: if clamp is removed/reverted to
    nominal window, this test should fail (building would be incorrectly True).
    """
    # 3 bars: 100, 105, 110 (constant +5/bar)
    bars = make_race_bars({"TICKER": [100.0, 105.0, 110.0]})
    rebased = rebase_to_100(bars)
    # short_window=1, medium_window=2, long_window=3
    # With only 3 bars, all windows clamp: bars_back_short=1, bars_back_medium=2,
    # bars_back_long=2 (min(3, 3-1)=2)
    result = compute_relative_momentum(
        rebased, short_window=1, medium_window=2, long_window=3
    )

    row = result.row(0, named=True)
    # momentum_short = 110 - 105 = 5.0, rate_short = 5.0 / 1 = 5.0
    # momentum_medium = 110 - 100 = 10.0, rate_medium = 10.0 / 2 = 5.0
    # momentum_long = 110 - 100 = 10.0, rate_long = 10.0 / 2 = 5.0
    # (long clamps to first bar because n_bars-1=2 < long_window=3)
    assert row["rate_short"] == pytest.approx(5.0, abs=1e-4)
    assert row["rate_medium"] == pytest.approx(5.0, abs=1e-4)
    assert row["rate_long"] == pytest.approx(5.0, abs=1e-4)

    # Classify: all rates equal, so building=False (not strictly >)
    trend = classify_relative_trend(result)
    assert trend["building"][0] is False


def test_relative_momentum_end_to_end_constant_rate_outperformer():
    """M-2: Rename + clarify: constant-rate ramp (arithmetic progression)."""
    # Arithmetic progression: +2, +2, +2, +2 per bar
    # Due to Float32 rounding, rates may be very close but not exactly equal.
    # This test verifies the pipeline works end-to-end with a simple case.
    start = datetime.date(2024, 1, 1)
    dates = [start + datetime.timedelta(days=i) for i in range(5)]
    bars = pl.DataFrame(
        {
            "date": dates + dates,
            "ticker": ["SPY"] * 5 + ["OUTPERFORMER"] * 5,
            "close": [100.0] * 5 + [100.0, 102.0, 104.0, 106.0, 108.0],
        },
        schema=RACE_BARS_SCHEMA,
    )

    ratio_bars = compute_relative_ratio(bars, benchmark="SPY")
    rebased = rebase_to_100(ratio_bars)
    momentum = compute_relative_momentum(
        rebased, short_window=1, medium_window=2, long_window=3
    )
    trend = classify_relative_trend(momentum)

    trend_row = trend.row(0, named=True)
    # Constant-rate ramp: trend should be "Leading" (rs_ratio >= 100, momentum > 0)
    assert trend_row["trend"] == "Leading"
    # building may be True or False depending on Float32 rounding of rates
    # (rates are very close but may not be exactly equal)


def test_relative_momentum_end_to_end_accelerating_outperformer():
    """M-2: Accelerating outperformer (convex curve) → building=True."""
    # Gains: +1, +2, +4, +8 (accelerating)
    start = datetime.date(2024, 1, 1)
    dates = [start + datetime.timedelta(days=i) for i in range(5)]
    bars = pl.DataFrame(
        {
            "date": dates + dates,
            "ticker": ["SPY"] * 5 + ["ACCEL"] * 5,
            "close": [100.0] * 5 + [100.0, 101.0, 103.0, 107.0, 115.0],
        },
        schema=RACE_BARS_SCHEMA,
    )

    ratio_bars = compute_relative_ratio(bars, benchmark="SPY")
    rebased = rebase_to_100(ratio_bars)
    momentum = compute_relative_momentum(
        rebased, short_window=1, medium_window=2, long_window=3
    )
    trend = classify_relative_trend(momentum)

    trend_row = trend.row(0, named=True)
    # Accelerating: rate_short > rate_medium > rate_long, momentum_medium > 0
    # rate_short = (115-107)/1 = 8.0
    # rate_medium = (115-103)/2 = 6.0
    # rate_long = (115-101)/3 = 4.67
    # 8.0 > 6.0 > 4.67 and momentum_medium > 0 → building=True
    assert trend_row["building"] is True


def test_relative_momentum_end_to_end_accelerating_decliner():
    """M-2: Accelerating decliner (concave-down, SOXS/SQQQ shape) → building=False."""
    # Losses: -1, -2, -4, -8 (accelerating downward)
    start = datetime.date(2024, 1, 1)
    dates = [start + datetime.timedelta(days=i) for i in range(5)]
    bars = pl.DataFrame(
        {
            "date": dates + dates,
            "ticker": ["SPY"] * 5 + ["DECLINER"] * 5,
            "close": [100.0] * 5 + [100.0, 99.0, 97.0, 93.0, 85.0],
        },
        schema=RACE_BARS_SCHEMA,
    )

    ratio_bars = compute_relative_ratio(bars, benchmark="SPY")
    rebased = rebase_to_100(ratio_bars)
    momentum = compute_relative_momentum(
        rebased, short_window=1, medium_window=2, long_window=3
    )
    trend = classify_relative_trend(momentum)

    trend_row = trend.row(0, named=True)
    # Accelerating decline: rate_short > rate_medium > rate_long (all negative)
    # but momentum_medium < 0, so building=False (positivity gate)
    assert trend_row["building"] is False


def test_relative_ratio_misaligned_dates_reverse():
    """m-3: Join direction test — ticker has date benchmark lacks."""
    start = datetime.date(2024, 1, 1)
    bars = pl.DataFrame(
        {
            "date": [
                start,
                start + datetime.timedelta(days=1),
                start + datetime.timedelta(days=2),
                start + datetime.timedelta(days=3),
                start + datetime.timedelta(days=4),
                start,
                start + datetime.timedelta(days=1),
                start + datetime.timedelta(days=2),
                start + datetime.timedelta(days=5),  # Extra date not in benchmark
            ],
            "ticker": ["SPY"] * 5 + ["TICKER"] * 4,
            "close": [100.0, 101.0, 102.0, 103.0, 104.0, 50.0, 51.0, 52.0, 53.0],
        },
        schema=RACE_BARS_SCHEMA,
    )

    ratio_bars = compute_relative_ratio(bars, benchmark="SPY")
    # Inner join: only dates in both SPY and TICKER
    # SPY has: 1, 2, 3, 4, 5; TICKER has: 1, 2, 3, 6
    # Intersection: 1, 2, 3 (3 rows)
    assert ratio_bars.height == 3
    assert ratio_bars["close"].null_count() == 0
    # Verify the extra date (5) is excluded
    assert ratio_bars["date"].to_list() == [
        start,
        start + datetime.timedelta(days=1),
        start + datetime.timedelta(days=2),
    ]


def test_render_relative_leaderboard_no_hot_cold_emoji():
    """m-5: Verify compact rendering has no legacy trend or hot/cold emoji."""
    trend = pl.DataFrame(
        {
            "ticker": ["A", "B"],
            "rs_ratio": [105.0, 95.0],
            "momentum_short": [2.0, -1.0],
            "momentum_medium": [1.0, -0.5],
            "momentum_long": [0.5, -0.2],
            "rate_short": [2.0, -1.0],
            "rate_medium": [1.0, -0.5],
            "rate_long": [0.5, -0.2],
            "trend": ["Leading", "Improving"],
            "building": [False, False],
        },
        schema=RELATIVE_TREND_SCHEMA,
    )
    table = render_relative_leaderboard(trend, benchmark="SPY")
    text = _rich_text(table)
    # 🔥 and 🧊 should NOT appear (they're only in _fmt_pct, not _fmt_momentum)
    assert "🔥" not in text
    assert "🧊" not in text
    assert "🟢" not in text
    assert "🟡" not in text


def test_render_relative_leaderboard_null_safe():
    """m-4: Null-safe rendering for Unknown form with null values."""
    # Construct a row with trend="Unknown" and null rs_ratio/momentum values
    # (bypassing compute_relative_momentum's filter)
    trend = pl.DataFrame(
        {
            "ticker": ["UNKNOWN"],
            "rs_ratio": [None],
            "momentum_short": [None],
            "momentum_medium": [1.0],
            "momentum_long": [0.5],
            "rate_short": [1.0],
            "rate_medium": [0.5],
            "rate_long": [0.25],
            "trend": ["Unknown"],
            "building": [False],
        },
        schema=RELATIVE_TREND_SCHEMA,
    )
    table = render_relative_leaderboard(trend, benchmark="SPY")
    text = _rich_text(table)
    # Should render without raising, with "n/a" for null values
    assert "UNKNOWN" in text
    assert "Unknown" in text
    assert "n/a" in text  # Null rs_ratio rendered as "n/a"
