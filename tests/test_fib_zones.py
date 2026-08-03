"""Tests for tickerlake.fib_zones — weekly Fibonacci-retracement IBZ/SMZ zones."""

import datetime

import polars as pl

from tickerlake.fib_zones import (
    WEEKLY_FIB_ZONES_SCHEMA,
    _classify_status,
    _classify_zone,
    _find_most_recent_unswept_leg,
    _levels_and_zones,
    compute_fib_zones_for_ticker,
    compute_weekly_fib_zones_all,
)


def _make_bars(
    prices_high: list[float],
    prices_low: list[float],
    start: datetime.date,
) -> pl.DataFrame:
    """Build a polars DataFrame of weekly OHLCV bars from high/low lists."""
    n = len(prices_high)
    assert len(prices_low) == n
    dates = [start + datetime.timedelta(weeks=i) for i in range(n)]
    closes: list[float] = [
        (hi + lo) / 2 for hi, lo in zip(prices_high, prices_low, strict=True)
    ]
    return pl.DataFrame(
        {
            "date": dates,
            "open": closes,
            "high": prices_high,
            "low": prices_low,
            "close": closes,
            "volume": [1_000_000.0] * n,
        },
        schema_overrides={"date": pl.Date},
    )


def test_classify_zone_bands() -> None:
    """Zone boundaries: above_ibz, in_ibz, in_smz, below_smz."""
    assert _classify_zone(100.0, 50.0, 70.0, 45.0) == "above_ibz"
    assert _classify_zone(70.0, 50.0, 70.0, 45.0) == "in_ibz"
    assert _classify_zone(50.0, 50.0, 70.0, 45.0) == "in_ibz"
    assert _classify_zone(45.0, 50.0, 70.0, 45.0) == "in_smz"
    assert _classify_zone(44.9, 50.0, 70.0, 45.0) == "below_smz"


def test_classify_status_live_deep_void() -> None:
    """Status: live / deep / void based on min_low_after_high vs swing_low/smz_low."""
    assert _classify_status(None, 10.0, 15.0) == "live"
    assert _classify_status(20.0, 10.0, 15.0) == "live"
    assert _classify_status(14.9, 10.0, 15.0) == "deep"
    assert _classify_status(10.0, 10.0, 15.0) == "void"
    assert _classify_status(5.0, 10.0, 15.0) == "void"


def test_levels_and_zones_celh_golden() -> None:
    """CELH golden: swing_low=21.10, swing_high=66.74 → known IBZ/SMZ bands."""
    data = _levels_and_zones(21.10, 66.74)
    assert abs(data["ibz_low"] - 30.87) < 0.05
    assert abs(data["ibz_high"] - 38.53) < 0.05
    assert abs(data["smz_low"] - 29.04) < 0.05
    assert abs(data["smz_high"] - 30.87) < 0.05
    assert data["range"] == 45.64


def test_compute_fib_zones_empty_bars() -> None:
    """Empty bars → None."""
    assert compute_fib_zones_for_ticker(pl.DataFrame()) is None
    assert compute_fib_zones_for_ticker(None) is None  # type: ignore[arg-type]


def test_compute_fib_zones_v_shape() -> None:
    """Clear V-shape: down to 8, up to 30, retrace to 14 → finds 8→30 leg."""
    highs = [
        20.0,
        18.0,
        15.0,
        12.0,
        11.0,
        10.0,
        10.0,
        14.0,
        20.0,
        25.0,
        30.0,
        28.0,
        24.0,
        20.0,
        14.0,
    ]
    lows = [
        18.0,
        15.0,
        12.0,
        10.0,
        9.0,
        8.0,
        8.0,
        12.0,
        18.0,
        22.0,
        28.0,
        25.0,
        21.0,
        18.0,
        12.0,
    ]
    bars = _make_bars(highs, lows, datetime.date(2024, 1, 1))
    result = compute_fib_zones_for_ticker(
        bars, k=3, min_leg_pct=0.20, min_bars_between_pivots=2
    )
    assert result is not None
    assert result["swing_low"] == 8.0
    assert result["swing_high"] == 30.0


def test_compute_fib_zones_min_leg_pct_too_strict() -> None:
    """With min_leg_pct=0.99, no leg passes and the algorithm returns None."""
    highs = [
        20.0,
        18.0,
        15.0,
        12.0,
        11.0,
        10.0,
        10.0,
        14.0,
        20.0,
        25.0,
        30.0,
        28.0,
        24.0,
        20.0,
        14.0,
    ]
    lows = [
        18.0,
        15.0,
        12.0,
        10.0,
        9.0,
        8.0,
        8.0,
        12.0,
        18.0,
        22.0,
        28.0,
        25.0,
        21.0,
        18.0,
        12.0,
    ]
    bars = _make_bars(highs, lows, datetime.date(2024, 1, 1))
    result = compute_fib_zones_for_ticker(
        bars, k=3, min_leg_pct=0.99, min_bars_between_pivots=2
    )
    assert result is None


def test_compute_fib_zones_min_bars_between_pivots_too_strict() -> None:
    """min_bars_between_pivots=1000 → no leg has that gap → None."""
    highs = [
        20.0,
        18.0,
        15.0,
        12.0,
        11.0,
        10.0,
        10.0,
        14.0,
        20.0,
        25.0,
        30.0,
        28.0,
        24.0,
        20.0,
        14.0,
    ]
    lows = [
        18.0,
        15.0,
        12.0,
        10.0,
        9.0,
        8.0,
        8.0,
        12.0,
        18.0,
        22.0,
        28.0,
        25.0,
        21.0,
        18.0,
        12.0,
    ]
    bars = _make_bars(highs, lows, datetime.date(2024, 1, 1))
    result = compute_fib_zones_for_ticker(
        bars, k=3, min_leg_pct=0.20, min_bars_between_pivots=1000
    )
    assert result is None


def test_compute_fib_zones_min_bars_between_pivots_relaxed() -> None:
    """min_bars_between_pivots=1 → 8→30 leg accepted."""
    highs = [
        20.0,
        18.0,
        15.0,
        12.0,
        11.0,
        10.0,
        10.0,
        14.0,
        20.0,
        25.0,
        30.0,
        28.0,
        24.0,
        20.0,
        14.0,
    ]
    lows = [
        18.0,
        15.0,
        12.0,
        10.0,
        9.0,
        8.0,
        8.0,
        12.0,
        18.0,
        22.0,
        28.0,
        25.0,
        21.0,
        18.0,
        12.0,
    ]
    bars = _make_bars(highs, lows, datetime.date(2024, 1, 1))
    result = compute_fib_zones_for_ticker(
        bars, k=3, min_leg_pct=0.20, min_bars_between_pivots=1
    )
    assert result is not None
    assert result["swing_low"] == 8.0
    assert result["swing_high"] == 30.0


def test_compute_fib_zones_highest_high_not_most_recent() -> None:
    """Anchors on most-recent pivot low, pairs with highest high after it."""
    highs = [
        20.0,
        18.0,
        15.0,
        12.0,
        11.0,
        10.0,
        10.0,
        14.0,
        20.0,
        25.0,
        30.0,
        28.0,
        25.0,
        22.0,
        20.0,
        22.0,
        30.0,
        40.0,
        50.0,
        60.0,
        58.0,
        55.0,
        50.0,
        45.0,
        40.0,
    ]
    lows = [
        18.0,
        15.0,
        12.0,
        10.0,
        9.0,
        8.0,
        8.0,
        12.0,
        18.0,
        22.0,
        28.0,
        25.0,
        22.0,
        20.0,
        18.0,
        20.0,
        28.0,
        38.0,
        48.0,
        58.0,
        55.0,
        52.0,
        48.0,
        42.0,
        38.0,
    ]
    bars = _make_bars(highs, lows, datetime.date(2024, 1, 1))
    result = compute_fib_zones_for_ticker(
        bars, k=3, min_leg_pct=0.20, min_bars_between_pivots=3
    )
    assert result is not None
    assert result["swing_low"] == 18.0
    assert result["swing_high"] == 60.0


def test_compute_fib_zones_schema_compliance() -> None:
    """Returned dict has all WEEKLY_FIB_ZONES_SCHEMA keys."""
    highs = [
        20.0,
        18.0,
        15.0,
        12.0,
        11.0,
        10.0,
        10.0,
        14.0,
        20.0,
        25.0,
        30.0,
        28.0,
        24.0,
        20.0,
        14.0,
    ]
    lows = [
        18.0,
        15.0,
        12.0,
        10.0,
        9.0,
        8.0,
        8.0,
        12.0,
        18.0,
        22.0,
        28.0,
        25.0,
        21.0,
        18.0,
        12.0,
    ]
    bars = _make_bars(highs, lows, datetime.date(2024, 1, 1))
    result = compute_fib_zones_for_ticker(
        bars, k=3, min_leg_pct=0.20, min_bars_between_pivots=2
    )
    assert result is not None
    for key in WEEKLY_FIB_ZONES_SCHEMA:
        assert key in result, f"missing key: {key}"


def test_compute_weekly_fib_zones_all_empty_input() -> None:
    """Empty bars or empty eligible set returns empty schema-correct DataFrame."""
    result = compute_weekly_fib_zones_all(pl.DataFrame(), eligible_tickers={"AAPL"})
    assert result.is_empty()
    for col in WEEKLY_FIB_ZONES_SCHEMA:
        assert col in result.columns

    result = compute_weekly_fib_zones_all(
        pl.DataFrame({"ticker": [], "date": [], "high": [], "low": [], "close": []}),
        eligible_tickers=set(),
    )
    assert result.is_empty()


def test_compute_weekly_fib_zones_all_filters_eligible() -> None:
    """Only eligible tickers are processed."""
    bars_rows = [
        {
            "ticker": ticker,
            "date": datetime.date(2024, 1, 1) + datetime.timedelta(weeks=i),
            "high": 20.0 + i,
            "low": 18.0 + i,
            "close": 19.0 + i,
            "volume": 1_000_000.0,
        }
        for ticker in ("AAA", "BBB", "CCC")
        for i in range(30)
    ]
    df = pl.DataFrame(bars_rows, schema_overrides={"date": pl.Date})
    result = compute_weekly_fib_zones_all(df, eligible_tickers={"AAA", "CCC"})
    tickers = set(result["ticker"].to_list()) if not result.is_empty() else set()
    assert "BBB" not in tickers
    assert tickers.issubset({"AAA", "CCC"})


def test_find_most_recent_unswept_leg_empty_pivots() -> None:
    """Empty pivots → None."""
    pivots = pl.DataFrame(
        schema={"date": pl.Date, "pivot_type": pl.Utf8, "price": pl.Float64}
    )
    assert _find_most_recent_unswept_leg(pivots, [], 0.20) is None
