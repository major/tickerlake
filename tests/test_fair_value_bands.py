"""Tests for tickerlake.fair_value_bands across all supported timeframes."""

import datetime

import polars as pl
import pytest

from tickerlake.fair_value_bands import (
    FAIR_VALUE_BANDS_SCHEMA,
    SMA_PERIOD,
    ZONE_ABOVE_UPPER,
    ZONE_BELOW_LOWER,
    ZONE_IN_BAND,
    _compute_fair_value,
    _compute_ohlc4,
    _per_ticker_band_widths,
    _zone_expression,
    align_fair_value_bands,
    compute_fair_value_bands,
)


def _make_bars(
    rows: list[dict],
    *,
    sort: bool = True,
) -> pl.DataFrame:
    """Build a polars DataFrame of monthly bars from a list of row dicts.

    Each row dict must have: date (datetime.date), ticker (str), open, high,
    low, close (numbers). Volume is filled with 1M. Casts dtypes to match
    the project convention (Float32 / Date / Utf8).
    """
    if not rows:
        return pl.DataFrame(schema=FAIR_VALUE_BANDS_SCHEMA)
    df = pl.DataFrame(
        {
            "date": [r["date"] for r in rows],
            "ticker": [r["ticker"] for r in rows],
            "open": [r["open"] for r in rows],
            "high": [r["high"] for r in rows],
            "low": [r["low"] for r in rows],
            "close": [r["close"] for r in rows],
            "volume": [1_000_000.0] * len(rows),
        }
    ).with_columns(
        pl.col("date").cast(pl.Date),
        pl.col("ticker").cast(pl.Utf8),
        pl.col("open").cast(pl.Float32),
        pl.col("high").cast(pl.Float32),
        pl.col("low").cast(pl.Float32),
        pl.col("close").cast(pl.Float32),
        pl.col("volume").cast(pl.Float32),
    )
    return df.sort(["ticker", "date"]) if sort else df


def _dates(start: datetime.date, n: int) -> list[datetime.date]:
    """Generate n ordered observation dates starting at `start`."""
    out: list[datetime.date] = []
    for i in range(n):
        month_index = start.month + i - 1
        year = start.year + month_index // 12
        month = month_index % 12 + 1
        out.append(datetime.date(year, month, 1))
    return out


def test_constants_locked() -> None:
    """SMA_PERIOD must match the THT Fair Value Bands default."""
    assert SMA_PERIOD == 33


def test_empty_input_returns_empty_schema() -> None:
    """Empty and None inputs return an empty DataFrame matching the schema."""
    empty = compute_fair_value_bands(pl.DataFrame())
    assert empty.is_empty()
    assert dict(empty.schema) == dict(FAIR_VALUE_BANDS_SCHEMA)
    assert compute_fair_value_bands(None) is not None  # type: ignore[arg-type]
    assert compute_fair_value_bands(None).is_empty()  # type: ignore[arg-type]


def test_compute_ohlc4_basic() -> None:
    """OHLC4 = (open + high + low + close) / 4."""
    bars = _make_bars(
        [
            {
                "date": datetime.date(2024, 1, 31),
                "ticker": "A",
                "open": 100.0,
                "high": 110.0,
                "low": 90.0,
                "close": 100.0,
            }
        ]
    )
    out = _compute_ohlc4(bars)
    assert out["ohlc4"][0] == pytest.approx(100.0, abs=1e-5)
    assert out.schema["ohlc4"] == pl.Float32


def test_compute_fair_value_sma33_warmup() -> None:
    """First 32 OHLC4 values per ticker are null; 33rd is the first non-null."""
    dates = _dates(datetime.date(2020, 1, 1), 40)
    rows = [
        {
            "date": dates[i],
            "ticker": "A",
            "open": 100.0,
            "high": 102.0,
            "low": 98.0,
            "close": 100.0,
        }
        for i in range(40)
    ]
    bars = _compute_ohlc4(_make_bars(rows))
    fv = _compute_fair_value(bars)
    fv_list = fv["fair_value"].to_list()
    # First 32 rows are null.
    assert all(v is None for v in fv_list[:32])
    # 33rd row is non-null and equals the OHLC4 mean (100.0).
    assert fv_list[32] == pytest.approx(100.0, abs=1e-4)
    # All subsequent rows are non-null.
    assert all(v is not None for v in fv_list[33:])


def test_per_ticker_band_widths_drops_no_straddles() -> None:
    """Widths stay undefined for tickers whose bars never straddle fv."""
    dates = _dates(datetime.date(2020, 1, 1), 40)
    # Ticker A: constant bars at 110 — OHLC4=110, fv=110, low=high=110, never straddles.
    # Ticker B: bars straddle fv=100 — open=close=100, low=95, high=105.
    rows_a = [
        {
            "date": dates[i],
            "ticker": "A",
            "open": 110.0,
            "high": 110.0,
            "low": 110.0,
            "close": 110.0,
        }
        for i in range(40)
    ]
    rows_b = [
        {
            "date": dates[i],
            "ticker": "B",
            "open": 100.0,
            "high": 105.0,
            "low": 95.0,
            "close": 100.0,
        }
        for i in range(40)
    ]
    bars = _compute_fair_value(_compute_ohlc4(_make_bars(rows_a + rows_b)))
    widths = _per_ticker_band_widths(bars)
    assert widths["ticker"].n_unique() == 2
    assert widths.filter(pl.col("ticker") == "A")["upper_dev"].null_count() == 40
    b_widths = widths.filter(pl.col("ticker") == "B")
    assert b_widths["n_straddling_bars"].to_list()[:32] == [0] * 32
    assert b_widths["n_straddling_bars"].to_list()[-1] == 8
    assert b_widths["upper_dev"].tail(1)[0] == pytest.approx(0.05, abs=1e-4)
    assert b_widths["lower_dev"].tail(1)[0] == pytest.approx(0.05, abs=1e-4)


def test_per_ticker_band_widths_median_known_value() -> None:
    """Median of a known set of deviations matches polars' median.

    Build 33 post-warmup bars with d=1,2,...,33 → upper_dev = d/100.
    Median of 33 values (1..33)/100 = 17/100 = 0.17.
    """
    dates = _dates(datetime.date(2020, 1, 1), 65)
    zero_row = {
        "ticker": "A",
        "open": 100.0,
        "high": 100.0,
        "low": 100.0,
        "close": 100.0,
    }
    rows = [{**zero_row, "date": dates[i]} for i in range(32)]
    rows.extend(
        {
            "date": dates[32 + j],
            "ticker": "A",
            "open": 100.0,
            "high": 100.0 + float(d),
            "low": 100.0 - float(d),
            "close": 100.0,
        }
        for j, d in enumerate(range(1, 34))
    )
    bars = _compute_fair_value(_compute_ohlc4(_make_bars(rows)))
    widths = _per_ticker_band_widths(bars)
    rows = widths.filter(pl.col("ticker") == "A")
    # The first valid SMA row sees only its own deviation, while the final
    # row sees the complete historical set.
    assert rows["n_straddling_bars"].to_list()[32:] == list(range(1, 34))
    assert rows["upper_dev"][32] == pytest.approx(0.01, abs=1e-3)
    assert rows["upper_dev"].tail(1)[0] == pytest.approx(0.17, abs=1e-3)
    assert rows["lower_dev"].tail(1)[0] == pytest.approx(0.17, abs=1e-3)


def test_zone_expression_classification() -> None:
    """Vectorized zone classification: below / in / above the bands.

    Boundary semantics: a close exactly equal to lower_band or upper_band
    is treated as in_band (the threshold is exclusive).
    """
    df = pl.DataFrame(
        {
            "current_close": [9.0, 10.0, 11.0, 10.0, 13.0],
            "lower_band": [10.0, 10.0, 10.0, 9.0, 11.0],
            "upper_band": [11.0, 11.0, 11.0, 11.0, 12.0],
        }
    )
    zones = df.with_columns(_zone_expression().alias("zone"))["zone"].to_list()
    # close 9 < lower 10 → below_lower
    assert zones[0] == ZONE_BELOW_LOWER
    # close 10 == lower 10 → boundary is in_band (exclusive)
    assert zones[1] == ZONE_IN_BAND
    # close 11 == upper 11 → boundary is in_band
    assert zones[2] == ZONE_IN_BAND
    # close 10 with bands 9..11 → in_band
    assert zones[3] == ZONE_IN_BAND
    # close 13 > upper 12 → above_upper
    assert zones[4] == ZONE_ABOVE_UPPER


def _source_bands_df() -> pl.DataFrame:
    """Build source bands with distinct values for as-of alignment tests."""
    return pl.DataFrame(
        {
            "ticker": ["A", "A"],
            "as_of_date": [datetime.date(2024, 1, 5), datetime.date(2024, 2, 10)],
            "fair_value": [100.0, 200.0],
            "upper_band": [110.0, 220.0],
            "lower_band": [90.0, 180.0],
            "current_close": [1.0, 1.0],
            "upper_dev": [0.10, 0.20],
            "lower_dev": [0.10, 0.20],
            "n_straddling_bars": [3, 7],
            "zone": [ZONE_BELOW_LOWER, ZONE_ABOVE_UPPER],
            "bar_count": [40, 50],
        },
        schema=FAIR_VALUE_BANDS_SCHEMA,
    )


def test_align_fair_value_bands_weekly_display_uses_confirmed_monthly_source() -> None:
    """Weekly bars use monthly bands from the following calendar month."""
    display_bars = _make_bars(
        [
            {
                "date": datetime.date(2024, 1, 31),
                "ticker": "A",
                "open": 100.0,
                "high": 101.0,
                "low": 99.0,
                "close": 85.0,
            },
            {
                "date": datetime.date(2024, 2, 1),
                "ticker": "A",
                "open": 100.0,
                "high": 101.0,
                "low": 99.0,
                "close": 115.0,
            },
            {
                "date": datetime.date(2024, 2, 2),
                "ticker": "A",
                "open": 100.0,
                "high": 101.0,
                "low": 99.0,
                "close": 105.0,
            },
            {
                "date": datetime.date(2024, 2, 29),
                "ticker": "A",
                "open": 100.0,
                "high": 101.0,
                "low": 99.0,
                "close": 85.0,
            },
            {
                "date": datetime.date(2024, 3, 1),
                "ticker": "A",
                "open": 100.0,
                "high": 101.0,
                "low": 99.0,
                "close": 230.0,
            },
        ]
    )

    result = align_fair_value_bands(_source_bands_df(), display_bars, "monthly")

    assert dict(result.schema) == dict(FAIR_VALUE_BANDS_SCHEMA)
    assert result["as_of_date"].to_list() == [
        datetime.date(2024, 2, 1),
        datetime.date(2024, 2, 2),
        datetime.date(2024, 2, 29),
        datetime.date(2024, 3, 1),
    ]
    assert result["fair_value"].to_list() == [100.0, 100.0, 100.0, 200.0]
    assert result["current_close"].to_list() == [115.0, 105.0, 85.0, 230.0]
    assert result["zone"].to_list() == [
        ZONE_ABOVE_UPPER,
        ZONE_IN_BAND,
        ZONE_BELOW_LOWER,
        ZONE_ABOVE_UPPER,
    ]
    assert result["n_straddling_bars"].to_list() == [3, 3, 3, 7]
    assert result["bar_count"].to_list() == [40, 40, 40, 50]


def test_align_fair_value_bands_daily_display_uses_confirmed_weekly_source() -> None:
    """Daily closes drive zones after the following weekly bar begins."""
    source = _source_bands_df().with_columns(
        pl.Series(
            "as_of_date",
            [datetime.date(2024, 1, 3), datetime.date(2024, 1, 10)],
        )
    )
    display_bars = _make_bars(
        [
            {
                "date": datetime.date(2024, 1, 9),
                "ticker": "A",
                "open": 100.0,
                "high": 101.0,
                "low": 99.0,
                "close": 95.0,
            },
            {
                "date": datetime.date(2024, 1, 10),
                "ticker": "A",
                "open": 100.0,
                "high": 101.0,
                "low": 99.0,
                "close": 115.0,
            },
            {
                "date": datetime.date(2024, 1, 16),
                "ticker": "A",
                "open": 100.0,
                "high": 101.0,
                "low": 99.0,
                "close": 105.0,
            },
            {
                "date": datetime.date(2024, 1, 17),
                "ticker": "A",
                "open": 100.0,
                "high": 101.0,
                "low": 99.0,
                "close": 230.0,
            },
        ]
    )

    result = align_fair_value_bands(source, display_bars, "weekly")

    assert result["as_of_date"].to_list() == [
        datetime.date(2024, 1, 10),
        datetime.date(2024, 1, 16),
        datetime.date(2024, 1, 17),
    ]
    assert result["fair_value"].to_list() == [100.0, 100.0, 200.0]
    assert result["current_close"].to_list() == [115.0, 105.0, 230.0]
    assert result["zone"].to_list() == [
        ZONE_ABOVE_UPPER,
        ZONE_IN_BAND,
        ZONE_ABOVE_UPPER,
    ]


def test_align_fair_value_bands_rejects_daily_source_timeframe() -> None:
    """Only weekly and monthly native bands can be used as overlay sources."""
    with pytest.raises(ValueError, match="source_timeframe must be one of"):
        align_fair_value_bands(pl.DataFrame(), pl.DataFrame(), "daily")


def test_compute_fair_value_bands_end_to_end() -> None:
    """End-to-end: a ticker with a known median deviation produces known bands.

    All bars have OHLC4 = 100 exactly (open=close=100, high=100+d,
    low=100-d). fv = 100. All bars straddle fv. The upper_dev is d/100.

    We use 65 bars: the first 32 are warmup (fv=null, not counted); the
    remaining 33 are post-warmup and all straddle. d ∈ {1, 2, ..., 33},
    so upper_dev = d/100 with median = 17/100 = 0.17.
    """
    dates = _dates(datetime.date(2020, 1, 1), 65)
    zero_row = {
        "ticker": "X",
        "open": 100.0,
        "high": 100.0,
        "low": 100.0,
        "close": 100.0,
    }
    rows = [{**zero_row, "date": dates[i]} for i in range(32)]
    rows.extend(
        {
            "date": dates[32 + j],
            "ticker": "X",
            "open": 100.0,
            "high": 100.0 + float(d),
            "low": 100.0 - float(d),
            "close": 100.0,
        }
        for j, d in enumerate(range(1, 34))
    )
    bars = _make_bars(rows)
    result = compute_fair_value_bands(bars)
    assert result.height == 33
    first = result.row(0, named=True)
    last = result.row(-1, named=True)
    assert first["as_of_date"] == dates[32]
    assert first["bar_count"] == 33
    assert first["fair_value"] == pytest.approx(100.0, abs=0.01)
    assert first["upper_dev"] == pytest.approx(0.01, abs=1e-3)
    assert first["upper_band"] == pytest.approx(101.0, abs=0.05)
    assert first["n_straddling_bars"] == 1
    # The final row's expanding median sees all 33 deviations.
    assert last["upper_dev"] == pytest.approx(0.17, abs=1e-3)
    assert last["upper_band"] == pytest.approx(117.0, abs=0.05)
    assert last["lower_dev"] == pytest.approx(0.17, abs=1e-3)
    assert last["lower_band"] == pytest.approx(83.0, abs=0.05)
    assert last["zone"] == ZONE_IN_BAND
    assert last["bar_count"] == 65
    # All 33 post-warmup bars straddle; the first 32 had fv=null.
    assert last["n_straddling_bars"] == 33


def test_compute_fair_value_bands_does_not_use_future_deviations() -> None:
    """An earlier band's width is unaffected by a later extreme month."""
    dates = _dates(datetime.date(2020, 1, 1), 34)
    rows = [
        {
            "date": dates[i],
            "ticker": "NOLEAK",
            "open": 100.0,
            "high": 101.0,
            "low": 99.0,
            "close": 100.0,
        }
        for i in range(33)
    ]
    rows.append(
        {
            "date": dates[-1],
            "ticker": "NOLEAK",
            "open": 100.0,
            "high": 200.0,
            "low": 0.0,
            "close": 100.0,
        }
    )

    result = compute_fair_value_bands(_make_bars(rows))
    first = result.row(0, named=True)
    later = result.row(-1, named=True)
    assert first["as_of_date"] == dates[32]
    assert first["upper_dev"] == pytest.approx(0.01, abs=1e-4)
    assert first["upper_band"] == pytest.approx(101.0, abs=0.01)
    assert later["upper_dev"] == pytest.approx(0.505, abs=1e-3)


def test_compute_fair_value_bands_drops_under_warmup() -> None:
    """Tickers with fewer than 33 monthly bars are excluded (SMA warmup)."""
    dates = _dates(datetime.date(2024, 1, 1), 10)  # only 10 observations
    rows = [
        {
            "date": dates[i],
            "ticker": "SHORT",
            "open": 100.0,
            "high": 105.0,
            "low": 95.0,
            "close": 100.0,
        }
        for i in range(10)
    ]
    result = compute_fair_value_bands(_make_bars(rows))
    assert result.is_empty()


def test_compute_fair_value_bands_drops_no_straddles() -> None:
    """Tickers whose bars never straddle the fair value are excluded.

    Constant bars at 110 → OHLC4=110, fv=110, low=high=110 → no straddle.
    """
    dates = _dates(datetime.date(2020, 1, 1), 40)
    rows = [
        {
            "date": dates[i],
            "ticker": "CONST",
            "open": 110.0,
            "high": 110.0,
            "low": 110.0,
            "close": 110.0,
        }
        for i in range(40)
    ]
    result = compute_fair_value_bands(_make_bars(rows))
    assert result.is_empty()


def test_compute_fair_value_bands_zone_discount() -> None:
    """A close below the lower band is classified as below_lower (discount)."""
    dates = _dates(datetime.date(2020, 1, 1), 40)
    # Build a ticker with 1% upper_dev, 1% lower_dev median. The last month
    # has a very low close (95) so the close is below lower_band = 100 * 0.99 = 99.
    base_row = {
        "ticker": "D",
        "open": 100.0,
        "high": 101.0,
        "low": 99.0,
        "close": 100.0,
    }
    rows = [{**base_row, "date": dates[i]} for i in range(39)]
    # Last month: a deep discount close.
    rows.append(
        {
            "date": dates[39],
            "ticker": "D",
            "open": 95.0,
            "high": 96.0,
            "low": 90.0,
            "close": 95.0,
        }
    )
    result = compute_fair_value_bands(_make_bars(rows))
    assert result.height == 8
    assert result.row(-1, named=True)["zone"] == ZONE_BELOW_LOWER


def test_compute_fair_value_bands_zone_premium() -> None:
    """A close above the upper band is classified as above_upper (premium)."""
    dates = _dates(datetime.date(2020, 1, 1), 40)
    base_row = {
        "ticker": "P",
        "open": 100.0,
        "high": 101.0,
        "low": 99.0,
        "close": 100.0,
    }
    rows = [{**base_row, "date": dates[i]} for i in range(39)]
    # Last month: a deep premium close.
    rows.append(
        {
            "date": dates[39],
            "ticker": "P",
            "open": 105.0,
            "high": 110.0,
            "low": 104.0,
            "close": 105.0,
        }
    )
    result = compute_fair_value_bands(_make_bars(rows))
    assert result.height == 8
    assert result.row(-1, named=True)["zone"] == ZONE_ABOVE_UPPER


def test_compute_fair_value_bands_exactly_sma_period_bars() -> None:
    """A ticker with exactly SMA_PERIOD monthly bars is included (warmup boundary)."""
    dates = _dates(datetime.date(2020, 1, 1), SMA_PERIOD)
    rows = [
        {
            "date": dates[i],
            "ticker": "B",
            "open": 100.0,
            "high": 100.0 + float(i + 1),
            "low": 100.0 - float(i + 1),
            "close": 100.0,
        }
        for i in range(SMA_PERIOD)
    ]
    result = compute_fair_value_bands(_make_bars(rows))
    # bar_count == SMA_PERIOD → eligible (>= SMA_PERIOD, off-by-one boundary).
    assert result.height == 1
    assert result.row(0, named=True)["bar_count"] == SMA_PERIOD
    # With exactly 33 bars, fair_value is non-null only at the last row
    # (the first 32 are warmup). So only 1 straddle is possible.
    assert result.row(0, named=True)["n_straddling_bars"] == 1


def test_compute_fair_value_bands_drops_fair_value_zero() -> None:
    """A ticker whose fair value collapses to 0 is excluded (no div-by-zero)."""
    dates = _dates(datetime.date(2020, 1, 1), 40)
    # All bars are at price 0 → OHLC4 = 0 → SMA = 0 → fair_value = 0.
    # The straddle filter (fair_value > 0) excludes all bars; the ticker
    # is then dropped from the deviations table → final output is empty.
    rows = [
        {
            "date": dates[i],
            "ticker": "ZERO",
            "open": 0.0,
            "high": 0.0,
            "low": 0.0,
            "close": 0.0,
        }
        for i in range(40)
    ]
    result = compute_fair_value_bands(_make_bars(rows))
    assert result.is_empty()


def test_compute_fair_value_bands_handles_unsorted_input() -> None:
    """Unsorted input is sorted internally; output is identical to sorted input."""
    dates = _dates(datetime.date(2020, 1, 1), 40)
    base_row = {
        "ticker": "U",
        "open": 100.0,
        "high": 100.0 + 5.0,
        "low": 100.0 - 5.0,
        "close": 100.0,
    }
    rows_sorted = [{**base_row, "date": dates[i]} for i in range(40)]
    # Shuffle deterministically using a fixed seed.
    # ruff: noqa: S311  # random is fine for shuffling test data
    import random

    rng = random.Random(42)
    rows_shuffled = rows_sorted.copy()
    rng.shuffle(rows_shuffled)
    sorted_result = compute_fair_value_bands(_make_bars(rows_sorted))
    shuffled_result = compute_fair_value_bands(_make_bars(rows_shuffled, sort=False))
    assert sorted_result.height == shuffled_result.height == 8
    assert sorted_result.equals(shuffled_result)


def test_compute_fair_value_bands_multi_ticker() -> None:
    """Historical rows are sorted by ticker and then date."""
    dates = _dates(datetime.date(2020, 1, 1), 40)
    rows = [
        {
            "date": dates[i],
            "ticker": ticker,
            "open": 100.0,
            "high": 101.0,
            "low": 99.0,
            "close": 100.0,
        }
        for ticker in ("AAA", "BBB", "CCC")
        for i in range(40)
    ]
    result = compute_fair_value_bands(_make_bars(rows))
    assert result.height == 24
    assert result["ticker"].to_list() == [
        ticker for ticker in ("AAA", "BBB", "CCC") for _ in range(8)
    ]
    assert (
        result.filter(pl.col("ticker") == "BBB")["as_of_date"].to_list() == dates[32:]
    )
    pairs = list(
        zip(result["ticker"].to_list(), result["as_of_date"].to_list(), strict=True)
    )
    assert pairs == sorted(pairs)


def test_compute_fair_value_bands_schema_match() -> None:
    """All supported timeframes produce the shared schema and same values."""
    dates = _dates(datetime.date(2020, 1, 1), 40)
    rows = [
        {
            "date": dates[i],
            "ticker": "S",
            "open": 100.0,
            "high": 101.0,
            "low": 99.0,
            "close": 100.0,
        }
        for i in range(40)
    ]
    bars = _make_bars(rows)
    results = [
        compute_fair_value_bands(bars, timeframe)
        for timeframe in ("daily", "weekly", "monthly")
    ]
    assert all(
        dict(result.schema) == dict(FAIR_VALUE_BANDS_SCHEMA) for result in results
    )
    assert results[0].equals(results[1])
    assert results[1].equals(results[2])


def test_compute_fair_value_bands_rejects_invalid_timeframe() -> None:
    """The algorithm rejects unsupported timeframe names deterministically."""
    with pytest.raises(ValueError, match="timeframe must be one of"):
        compute_fair_value_bands(pl.DataFrame(), "yearly")


def test_as_of_date_is_last_monthly_bar() -> None:
    """The final historical row uses the most recent monthly date and close."""
    dates = _dates(datetime.date(2020, 1, 1), 40)
    rows = [
        {
            "date": dates[i],
            "ticker": "A",
            "open": 100.0,
            "high": 101.0,
            "low": 99.0,
            "close": 100.0,
        }
        for i in range(40)
    ]
    result = compute_fair_value_bands(_make_bars(rows))
    assert result.row(-1, named=True)["as_of_date"] == dates[-1]
    assert result.row(-1, named=True)["current_close"] == pytest.approx(100.0, abs=0.01)
