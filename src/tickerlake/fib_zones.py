"""Weekly Fibonacci-retracement IBZ/SMZ zone computation.

Finds the most recent unswept major swing low → swing high leg on weekly
bars using k-bar fractal pivots (see `tickerlake.transform.find_pivots`),
then computes the Institutional Buying Zone (IBZ, 0.618-0.786 retracement)
and Smart Money Zone (SMZ, 0.786-0.826 retracement) for that leg.

Algorithm:
    0. Restrict bars to the most recent max_lookback_years (default 2
       years) — legs anchored on older pivot lows are ignored.
    1. Detect k-bar fractal pivots on weekly bars (a bar is a pivot high
       if it's higher than k bars on each side; similarly for pivot low).
    2. Walk pivot lows from most recent to oldest.
    3. For each pivot low, find the HIGHEST pivot high that came at
       least min_bars_between_pivots bars later.
    4. Verify the leg is "major" — range >= min_leg_pct of the swing high.
    5. Verify the swing low hasn't been swept — no bar after the swing
       high has traded below the swing low.
    6. Return the first valid (most recent) leg found.
    7. Compute the IBZ/SMZ bands and classify the current price.

Public API:
    WEEKLY_FIB_ZONES_SCHEMA: schema dict for the persisted table.
    compute_fib_zones_for_ticker(bars): per-ticker dict for the most
        recent unswept major leg, or None.
    compute_weekly_fib_zones_all(weekly_bars, *, eligible_tickers):
        bulk version over a polars DataFrame of weekly bars.
"""

from __future__ import annotations

import datetime

import polars as pl

from tickerlake.transform import find_pivots

DEFAULT_MAX_LOOKBACK_YEARS: int = 2

# Public schema — used by load.py.
WEEKLY_FIB_ZONES_SCHEMA: dict = {
    "ticker": pl.Utf8,
    "as_of_date": pl.Date,
    "swing_low": pl.Float32,
    "swing_high": pl.Float32,
    "range": pl.Float32,
    "swing_low_date": pl.Date,
    "swing_high_date": pl.Date,
    "bars_since_swing_high": pl.UInt32,
    "ibz_low": pl.Float32,
    "ibz_high": pl.Float32,
    "smz_low": pl.Float32,
    "smz_high": pl.Float32,
    "current_price": pl.Float32,
    "pct_retracement": pl.Float32,
    "zone": pl.Utf8,
    "primary_degree": pl.UInt32,
    "primary_status": pl.Utf8,
    "still_making_new_highs": pl.Boolean,
    "zigzag_pct": pl.Float32,
    "bar_count": pl.UInt32,
}

RATIOS: tuple[float, ...] = (0.0, 0.236, 0.382, 0.5, 0.618, 0.786, 0.826, 1.0)
LABELS: tuple[str, ...] = (
    "Swing High",
    "23.6%",
    "38.2%",
    "50%",
    "IBZ Start (61.8%)",
    "IBZ End / SMZ Start (78.6%)",
    "SMZ End (82.6%)",
    "Swing Low",
)


def _round2(x: float) -> float:
    """Round to 2 decimal places, returning a float."""
    return round(float(x), 2)


def _empty_bars() -> pl.DataFrame:
    return pl.DataFrame(schema=WEEKLY_FIB_ZONES_SCHEMA)


def _classify_zone(
    current_price: float, ibz_low: float, ibz_high: float, smz_low: float
) -> str:
    """Classify current_price against the IBZ/SMZ bands."""
    if current_price > ibz_high:
        return "above_ibz"
    if current_price >= ibz_low and current_price <= ibz_high:
        return "in_ibz"
    if current_price >= smz_low and current_price < ibz_low:
        return "in_smz"
    return "below_smz"


def _classify_status(
    min_low_after_high: float | None, swing_low: float, smz_low: float
) -> str:
    """Determine degree status: live (untouched below SMZ), deep
    (touched but held above swing low), or void (broke the swing low).
    """
    if min_low_after_high is None:
        return "live"
    if min_low_after_high <= swing_low:
        return "void"
    if min_low_after_high < smz_low:
        return "deep"
    return "live"


def _levels_and_zones(swing_low: float, swing_high: float) -> dict:
    """Compute 8 fib levels and the IBZ/SMZ bands."""
    rng = swing_high - swing_low
    levels = [
        {
            "ratio": ratio,
            "label": label,
            "price": _round2(swing_high - rng * ratio),
        }
        for ratio, label in zip(RATIOS, LABELS, strict=True)
    ]
    return {
        "range": _round2(rng),
        "levels": levels,
        "ibz_low": levels[5]["price"],
        "ibz_high": levels[4]["price"],
        "smz_low": levels[6]["price"],
        "smz_high": levels[5]["price"],
    }


def _find_bar_index(bars: list[dict], date) -> int | None:
    """Return the index of the bar with the given date, or None if not found."""
    for j, bar in enumerate(bars):
        if bar["date"] == date:
            return j
    return None


def _is_swept(bars: list[dict], start_idx: int, threshold: float) -> bool:
    """Return True if any bar after start_idx has a low below threshold."""
    for j in range(start_idx + 1, len(bars)):
        if float(bars[j]["low"]) < threshold:
            return True
    return False


def _try_leg_for_low(
    low_row: dict,
    pivots: pl.DataFrame,
    bars: list[dict],
    min_leg_pct: float,
    min_bars_between_pivots: int,
) -> dict | None:
    """Try to build a valid leg for a single pivot low. Returns the leg
    dict if valid, else None. A valid leg pairs the pivot low with the
    highest pivot high that came at least min_bars_between_pivots bars
    later, passes the "major" leg-size filter, and hasn't been swept.
    """
    low_date = low_row["date"]
    low_price = float(low_row["price"])
    low_idx = _find_bar_index(bars, low_date)
    if low_idx is None:
        return None

    candidate_highs = pivots.filter(
        (pl.col("pivot_type") == "high") & (pl.col("date") > low_date)
    )
    eligible_highs = [
        h
        for h in candidate_highs.iter_rows(named=True)
        if (h_idx := _find_bar_index(bars, h["date"])) is not None
        and h_idx - low_idx >= min_bars_between_pivots
    ]
    if not eligible_highs:
        return None

    highest = max(eligible_highs, key=lambda r: float(r["price"]))
    high_price = float(highest["price"])
    high_idx = _find_bar_index(bars, highest["date"])
    if high_idx is None:
        return None

    rng = high_price - low_price
    too_small = high_price == 0 or rng / high_price < min_leg_pct
    if too_small or _is_swept(bars, high_idx, low_price):
        return None

    return {
        "swing_low": low_price,
        "swing_low_idx": low_idx,
        "swing_high": high_price,
        "swing_high_idx": high_idx,
    }


def _find_most_recent_unswept_leg(
    pivots: pl.DataFrame,
    bars: list[dict],
    min_leg_pct: float,
    min_bars_between_pivots: int = 5,
) -> dict | None:
    """Find the most recent unswept major leg using k-bar fractal pivots.

    The algorithm anchors on the most recent pivot low and pairs it with
    the HIGHEST pivot high that came after it (not just the most recent
    pivot high). This matches the "draw the fib from the most recent
    major swing low to the major swing high" mental model — the swing low
    is the starting point, and the swing high is the highest peak that
    followed.

    A leg is valid if (a) the range is >= min_leg_pct of the swing high
    (the "major" qualifier), and (b) the swing low hasn't been swept by
    any bar after the swing high.

    Returns a dict with swing_low, swing_low_idx, swing_high, swing_high_idx,
    or None if no valid leg exists.
    """
    if pivots.is_empty():
        return None

    low_pivots = pivots.filter(pl.col("pivot_type") == "low").sort(
        "date", descending=True
    )
    if low_pivots.is_empty():
        return None

    for low_row in low_pivots.iter_rows(named=True):
        leg = _try_leg_for_low(
            low_row, pivots, bars, min_leg_pct, min_bars_between_pivots
        )
        if leg is not None:
            return leg

    return None


def compute_fib_zones_for_ticker(
    bars: pl.DataFrame,
    *,
    k: int = 4,
    min_leg_pct: float = 0.20,
    min_bars_between_pivots: int = 5,
    max_lookback_years: int | None = DEFAULT_MAX_LOOKBACK_YEARS,
) -> dict | None:
    """Compute fib zones for the most recent unswept major leg on weekly bars.

    Uses k-bar fractal pivots to identify swing highs and lows, then anchors
    on the most recent pivot low and pairs it with the highest pivot high
    that came at least `min_bars_between_pivots` weeks later.

    Bars are first restricted to the most recent `max_lookback_years` (default
    2 years; pass None to disable the cap), so the leg's swing low cannot be
    older than that window.

    Returns a flat dict matching WEEKLY_FIB_ZONES_SCHEMA keys, or None
    when no valid unswept leg exists.

    Args:
        bars: polars DataFrame of weekly OHLCV bars for one ticker, sorted
            by date (or unsorted — sorted internally).
        k: number of bars on each side required to confirm a fractal pivot.
            Default 4 (matches the project's `find_pivots` default).
        min_leg_pct: minimum leg size as a fraction of swing high. Default
            0.20 (20%) — legs below this are filtered as not "major".
        min_bars_between_pivots: minimum number of weekly bars between
            the swing low and the swing high. Default 5 — filters out
            legs where the pivots are too close together in time.
        max_lookback_years: maximum age of bars considered, capped at this
            many years before the last bar. Default 2.
    """
    if bars is None or bars.is_empty():
        return None

    sorted_df = bars.sort("date")
    if max_lookback_years is not None:
        cutoff = datetime.timedelta(days=365 * max_lookback_years)
        sorted_df = sorted_df.filter(pl.col("date") >= pl.col("date").max() - cutoff)
    bar_dicts = sorted_df.select(["date", "high", "low", "close"]).to_dicts()
    if not bar_dicts:
        return None

    n = len(bar_dicts)
    as_of_date = bar_dicts[-1]["date"]
    current_price = float(bar_dicts[-1]["close"])

    # find_pivots needs date, ticker, high, low columns. Add a dummy ticker.
    bars_with_ticker = sorted_df.with_columns(pl.lit("T").alias("ticker"))
    pivots = find_pivots(bars_with_ticker, k=k)

    leg = _find_most_recent_unswept_leg(
        pivots, bar_dicts, min_leg_pct, min_bars_between_pivots
    )
    if leg is None:
        return None

    data = _levels_and_zones(leg["swing_low"], leg["swing_high"])
    zone = _classify_zone(
        current_price, data["ibz_low"], data["ibz_high"], data["smz_low"]
    )

    # min_low_after_high: lowest low after the swing high (for status).
    min_low_after_high: float | None = None
    if leg["swing_high_idx"] < n - 1:
        min_low_after_high = float(bar_dicts[leg["swing_high_idx"] + 1]["low"])
        for j in range(leg["swing_high_idx"] + 1, n):
            min_low_after_high = min(min_low_after_high, float(bar_dicts[j]["low"]))

    status = _classify_status(min_low_after_high, leg["swing_low"], data["smz_low"])

    rng = leg["swing_high"] - leg["swing_low"]
    pct_retracement = (
        round((leg["swing_high"] - current_price) / rng * 100, 2) if rng > 0 else None
    )

    return {
        "ticker": "",
        "as_of_date": as_of_date,
        "swing_low": _round2(leg["swing_low"]),
        "swing_high": _round2(leg["swing_high"]),
        "range": data["range"],
        "swing_low_date": bar_dicts[leg["swing_low_idx"]].get("date"),
        "swing_high_date": bar_dicts[leg["swing_high_idx"]].get("date"),
        "bars_since_swing_high": n - 1 - leg["swing_high_idx"],
        "ibz_low": data["ibz_low"],
        "ibz_high": data["ibz_high"],
        "smz_low": data["smz_low"],
        "smz_high": data["smz_high"],
        "current_price": _round2(current_price),
        "pct_retracement": pct_retracement,
        "zone": zone,
        "primary_degree": 1,
        "primary_status": status,
        "still_making_new_highs": leg["swing_high_idx"] >= n - 2,
        "zigzag_pct": 0.0,  # Not applicable — uses k-bar fractals
        "bar_count": n,
    }


def compute_weekly_fib_zones_all(
    weekly_bars: pl.DataFrame,
    *,
    eligible_tickers: set[str],
    k: int = 4,
    min_leg_pct: float = 0.20,
) -> pl.DataFrame:
    """Filter weekly_bars to eligible_tickers, compute zones per ticker.

    Returns a single DataFrame matching WEEKLY_FIB_ZONES_SCHEMA. Tickers
    that produce no valid unswept leg are silently skipped.
    """
    if weekly_bars is None or weekly_bars.is_empty() or not eligible_tickers:
        return _empty_bars()

    filtered = weekly_bars.filter(pl.col("ticker").is_in(list(eligible_tickers)))
    if filtered.is_empty():
        return _empty_bars()

    rows: list[dict] = []
    for ticker in sorted(eligible_tickers):
        sub = filtered.filter(pl.col("ticker") == ticker).sort("date")
        if sub.is_empty():
            continue
        row = compute_fib_zones_for_ticker(sub, k=k, min_leg_pct=min_leg_pct)
        if row is None:
            continue
        row["ticker"] = ticker
        rows.append(row)

    if not rows:
        return _empty_bars()
    return pl.DataFrame(rows).cast(WEEKLY_FIB_ZONES_SCHEMA, strict=False)
