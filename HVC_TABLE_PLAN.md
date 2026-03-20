# HVC (High Volume Catalyst) Table Plan

## Goal

Add a `high_volume_catalysts` table to `tickerlake.duckdb` that captures days where
volume >= 3x the 20-day moving average, along with context that makes each HVC
immediately queryable without joining back to `daily_bars` or `stock_metrics`.

## Why a Separate Table

HVCs are sparse events (most days are NOT HVCs). A dedicated table means:
- Direct `SELECT * FROM high_volume_catalysts` gives you everything
- No filtering through millions of non-HVC rows
- Pre-computed derived fields eliminate repeated query-time math

## Detection Criteria

A row qualifies as an HVC when:
- `volume >= 3.0 * volume_sma_20` (from `stock_metrics`)
- `volume_sma_20 IS NOT NULL` (need 20 days of history, excludes warmup period)

## Proposed Schema

### Identification

| Column | Type | Description |
|--------|------|-------------|
| `ticker` | Utf8 | Stock symbol |
| `date` | Date | Trading date of the HVC |

### Price Action (from the HVC bar itself)

| Column | Type | Description |
|--------|------|-------------|
| `open` | Float32 | Opening price |
| `high` | Float32 | High of the HVC bar |
| `low` | Float32 | Low of the HVC bar |
| `close` | Float32 | Closing price |
| `prev_close` | Float32 | Previous day's close (needed for gap/move calcs, avoids joins) |

### Volume

| Column | Type | Description |
|--------|------|-------------|
| `volume` | Float32 | Actual volume on the HVC day |
| `volume_sma_20` | Float32 | 20-day average volume at time of HVC |
| `volume_multiplier` | Float32 | `volume / volume_sma_20` (e.g., 4.2 = 4.2x average) |

### Percent Moves

| Column | Type | Description |
|--------|------|-------------|
| `total_move_pct` | Float32 | `(close - prev_close) / prev_close * 100` (close-to-close, includes gaps) |
| `gap_pct` | Float32 | `(open - prev_close) / prev_close * 100` (overnight gap only) |
| `intraday_move_pct` | Float32 | `(close - open) / open * 100` (open-to-close, intraday only) |
| `bar_range_pct` | Float32 | `(high - low) / prev_close * 100` (full bar range as % of prev close) |

### Volatility

| Column | Type | Description |
|--------|------|-------------|
| `adr_pct` | Float32 | 20-day average of `(high - low) / close` expressed as % (volatility baseline) |
| `atr_pct` | Float32 | ATR(14) / close from `stock_metrics` (includes gaps in range calc) |

### Bar Quality Indicators

| Column | Type | Description |
|--------|------|-------------|
| `close_position_in_range` | Float32 | `(close - low) / (high - low)` (0.0 = closed at low, 1.0 = closed at high) |
| `is_up_day` | Boolean | `close > prev_close` |

### Trend Context (snapshot at time of HVC)

| Column | Type | Description |
|--------|------|-------------|
| `price_vs_sma50_pct` | Float32 | `(close - sma_50) / sma_50 * 100` (how far above/below 50-day MA) |
| `price_vs_sma200_pct` | Float32 | `(close - sma_200) / sma_200 * 100` (how far above/below 200-day MA) |
| `rs` | Float32 | Relative strength vs SPY at time of HVC (from `stock_metrics`) |

## Fields Considered but Excluded

| Field | Why Excluded |
|-------|-------------|
| Float rotation | No float/shares outstanding data available from API |
| Short interest % | No short interest data available |
| News/earnings catalyst type | No fundamental event data available |
| Pocket pivot signal (vol vs 10d max down-vol) | Interesting but complex to compute correctly, could add later |
| VCP / base breakout detection | Pattern recognition is out of scope for a single table, better as a separate analysis |
| Next-day / 5-day follow-through | Forward-looking metrics computed at HVC time would be stale until the future data arrives. Better queried ad-hoc by joining `daily_bars` at query time. |

## Open Questions

1. **ADR% calculation**: Use `(high - low) / close` (simpler) or true ADR which averages
   the high and low separately: `(SMA20(high) - SMA20(low)) / close`? The simpler version
   is more standard in the Minervini/O'Neil world.

2. **Minimum price filter**: Should we exclude penny stocks (e.g., close < $5) from HVC
   detection? High-volume days on sub-$1 stocks are usually noise.

3. **Volume multiplier threshold**: You said 3x. Should the table store ALL 3x+ events,
   or should we also add a tier column (3x, 5x, 10x) for easy filtering?

4. **Transactions column**: Include `transactions` count from the bar? Could be useful for
   distinguishing institutional (fewer large transactions) vs retail (many small ones),
   though the signal is weak with daily aggregates.

## Implementation Plan

### 1. transform.py: Add `compute_adr_pct()`

New helper function following the `_compute_atr()` pattern:

```python
def _compute_adr_pct(bars: pl.DataFrame, period: int = 20) -> pl.DataFrame:
    """Compute Average Daily Range as a percentage of close price.

    ADR% = SMA(period) of ((high - low) / close).
    Warmup: first (period - 1) rows per ticker are null.
    """
```

Returns: `DataFrame[date, ticker, adr_pct_20]`

### 2. transform.py: Add `detect_hvcs()`

New function that takes bars + metrics, computes derived fields, filters to HVC rows:

```python
def detect_hvcs(bars: pl.DataFrame, metrics: pl.DataFrame) -> pl.DataFrame:
    """Detect High Volume Catalyst days and compute HVC-specific metrics.

    An HVC occurs when volume >= 3x the 20-day SMA of volume.
    Returns only rows that qualify as HVCs with all derived fields.
    """
```

Steps:
1. Join bars with metrics on `(ticker, date)`
2. Compute `prev_close` via `pl.col("close").shift(1).over("ticker")`
3. Compute ADR% (or pull from a new `adr_pct` column in metrics)
4. Compute all derived fields (gap_pct, total_move_pct, etc.)
5. Filter to `volume >= 3.0 * volume_sma_20`
6. Select final columns per schema above

### 3. load.py: Update `write_consumer_db()`

Add `hvcs` parameter to `write_consumer_db()`:

```python
def write_consumer_db(
    bars: pl.DataFrame,
    metrics: pl.DataFrame,
    tickers: pl.DataFrame,
    hvcs: pl.DataFrame,       # NEW
    path: Path,
) -> None:
```

Write as 4th table: `CREATE OR REPLACE TABLE high_volume_catalysts AS ...`
Ordered by: `ticker, date`

### 4. pipeline.py: Wire it up

In both `_run_backfill()` and `update()`, after `compute_metrics()`:

```python
metrics = compute_metrics(bars)
hvcs = detect_hvcs(bars, metrics)    # NEW
write_consumer_db(bars, metrics, tickers, hvcs, consumer_path)
```

### 5. Tests

- `test_transform.py`: Test `detect_hvcs()` with synthetic data where some days hit 3x volume
- `test_load.py`: Verify `high_volume_catalysts` table is created in consumer DB
- `test_pipeline.py`: Verify HVC DataFrame flows through backfill and update

## Sort Order

Table sorted by `(ticker, date)` to match existing convention and optimize for
per-ticker time-series queries.
