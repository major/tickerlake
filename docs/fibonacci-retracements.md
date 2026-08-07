# Weekly Fibonacci Retracement Zones

> This is a research and screening framework, not investment advice.

## Purpose and scope

This document describes the existing weekly Fibonacci retracement implementation in `tickerlake`.
The framework identifies a recent unswept major swing-low-to-swing-high leg and calculates Institutional Buying Zone (IBZ) and Smart Money Zone (SMZ) price bands.
The output is intended for research and screening rather than trade execution or investment recommendations.
The implementation produces one current snapshot row per eligible ticker with a valid leg.

## Data flow and weekly defaults

`backfill` and `update` split-adjust and ticker-filter daily bars before building weekly bars, weekly metrics, and fib zones.
Weekly aggregation groups each ticker by Monday-start calendar weeks.
The weekly row uses the first open, highest high, lowest low, last close, summed volume, volume-weighted VWAP, and summed transactions for that week.
The weekly output date is the Monday that starts the week.
`compute_weekly_fib_zones` reads `weekly_bars` and `weekly_metrics` from `tickerlake.duckdb`.
Each ticker uses the close of its last available weekly bar as `current_price` and that bar date as `as_of_date`.
All available weekly bars are considered without a lookback cap.

The per-ticker function is `compute_fib_zones_for_ticker`.
The bulk function is `compute_weekly_fib_zones_all`.
The default fractal width is `k=4` weekly bars on each side of a pivot.
The default minimum major-leg size is `min_leg_pct=0.20`, or 20 percent of the swing high.
The default minimum distance between the swing low and swing high is `min_bars_between_pivots=5` weekly bar positions.
The bulk function accepts `k` and `min_leg_pct`, while its internal minimum pivot gap remains 5 unless the per-ticker function is called directly.

## Weekly fractal and leg calculation

A pivot high is strictly higher than each of the prior `k` high values and greater than or equal to each of the next `k` high values.
A pivot low is strictly lower than each of the prior `k` low values and less than or equal to each of the next `k` low values.
The first and last `k` weekly rows cannot supply a complete fractal window and are not emitted as pivots.
The pivot detector sorts bars by ticker and date before applying these comparisons.

The leg search walks pivot lows from most recent to oldest.
For each pivot low, it considers pivot highs dated later than the low and at least five bar positions later by default.
It pairs the low with the highest eligible pivot high rather than simply the most recent high.
The major-leg test requires `(swing_high - swing_low) / swing_high >= min_leg_pct` and rejects a zero swing high.
The swing low is considered unswept only when no bar after the swing high has a low strictly below the swing low.
The first candidate satisfying the distance, major-size, and unswept tests is returned.
If no candidate passes, that ticker is omitted from the persisted zone table.

## Formula and ratios

Let `L` be `swing_low`, `H` be `swing_high`, and `R = H - L` be the leg range.
The implementation calculates each retracement level as `level(r) = H - (R * r)`.
The 0.0 ratio is at the swing high and the 1.0 ratio is at the swing low.
Prices, range, and percentage retracement values are rounded to two decimal places before persistence.

| Ratio | Level label | Formula position |
| ---: | --- | --- |
| 0.000 | Swing High | `H` |
| 0.236 | 23.6% | `H - 0.236R` |
| 0.382 | 38.2% | `H - 0.382R` |
| 0.500 | 50% | `H - 0.500R` |
| 0.618 | IBZ Start | `H - 0.618R` |
| 0.786 | IBZ End and SMZ Start | `H - 0.786R` |
| 0.826 | SMZ End | `H - 0.826R` |
| 1.000 | Swing Low | `L` |

The IBZ is bounded by `ibz_high = level(0.618)` and `ibz_low = level(0.786)`.
The SMZ is bounded by `smz_high = level(0.786)` and `smz_low = level(0.826)`.
The 78.6% level is therefore both the IBZ lower boundary and the SMZ upper boundary.

## Zone classification and primary status

The current price is classified against the rounded persisted band boundaries.

| Zone | Condition | Boundary behavior |
| --- | --- | --- |
| `above_ibz` | `current_price > ibz_high` | The upper IBZ boundary is excluded |
| `in_ibz` | `ibz_low <= current_price <= ibz_high` | Both IBZ boundaries are included |
| `in_smz` | `smz_low <= current_price < ibz_low` | SMZ low is included and the shared 78.6% boundary is excluded here |
| `below_smz` | Otherwise | With normally ordered bands this is `current_price < smz_low` |

The exact shared 78.6% value is classified as `in_ibz` because the IBZ test runs before the SMZ test.
The exact 82.6% value is classified as `in_smz`.

`min_low_after_high` is the lowest weekly low strictly after the selected swing high, or `None` when no later weekly bar exists.

| Primary status | Rule | Boundary behavior |
| --- | --- | --- |
| `live` | No later bar exists or `min_low_after_high >= smz_low` | Touching SMZ low is still live |
| `deep` | `swing_low < min_low_after_high < smz_low` | The low is below SMZ low but remains above swing low |
| `void` | `min_low_after_high <= swing_low` | Touching or breaking swing low is void |

The candidate-leg sweep test uses a strict low below the swing low, while primary status treats an equal swing-low retest as `void`.
`primary_degree` is currently always `1` because this implementation computes one primary fractal leg rather than multiple degrees.
`still_making_new_highs` is true when the selected swing high is the last or penultimate weekly bar.
`zigzag_pct` is currently always `0.0` because the implementation uses k-bar fractals rather than a zigzag detector.

## Eligibility, liquidity, and CLI commands

The compute pipeline qualifies a ticker only when its latest `weekly_metrics` row has `volume_sma_20 >= 1,000,000` shares per week.
The latest row is selected by the maximum weekly metrics date for each ticker.
A stale older liquid row does not qualify a ticker whose latest row is below the threshold.
Tickers that fail the liquidity gate or have no valid unswept major leg do not produce `weekly_fib_zones` rows.

The `backfill` command builds the consumer database and refreshes weekly fib zones after writing the weekly tables.
The `update` command performs the same refresh after rebuilding the consumer database.
The standalone `fib-zones compute` command reads the existing consumer database and replaces its fib-zone snapshot without fetching market data.
The standalone compute command requires an existing `tickerlake.duckdb` with `weekly_bars` and `weekly_metrics`.
The standalone screen command requires an existing `tickerlake.duckdb` with `weekly_fib_zones`.
The output directory defaults to the current working directory and contains `tickerlake.duckdb`.
The current `fib-zones` CLI does not expose overrides for the fractal width, minimum leg size, or minimum pivot gap.

```bash
tickerlake backfill --output-dir ./data
tickerlake update --output-dir ./data
tickerlake fib-zones compute --output-dir ./data
tickerlake fib-zones screen --output-dir ./data
```

`backfill` and `update` require `MASSIVE_API_KEY`, while standalone computation and screening operate on the existing database.
The screen command accepts `--zone` values `in_ibz`, `in_smz`, `below_smz`, `above_ibz`, and `all`.
The default `--zone all` selects `in_ibz`, `in_smz`, and `below_smz` while excluding rows with `primary_status = 'void'`.
A specific zone selection includes that zone regardless of primary status.
The default `--min-swing-low 5.0` excludes rows with a swing low below five dollars.
Use `--min-swing-low 0` to disable the intended five-dollar floor.
The optional positive `--limit` caps displayed rows but does not change the logged total match count.
Screen results are sorted by ticker.

## Persisted `weekly_fib_zones` schema

The table is stored in `tickerlake.duckdb` and is replaced on every fib-zone computation.
The table is therefore a current snapshot rather than a historical series of zone calculations.
The logical Polars types map to DuckDB `VARCHAR`, `DATE`, `FLOAT`, `UINTEGER`, and `BOOLEAN` types.

| Column | Type | Meaning |
| --- | --- | --- |
| `ticker` | `VARCHAR` | Ticker symbol |
| `as_of_date` | `DATE` | Date of the last weekly bar used |
| `swing_low` | `FLOAT` | Selected swing-low price |
| `swing_high` | `FLOAT` | Selected swing-high price |
| `range` | `FLOAT` | Rounded `swing_high - swing_low` |
| `swing_low_date` | `DATE` | Date of the selected swing low |
| `swing_high_date` | `DATE` | Date of the selected swing high |
| `bars_since_swing_high` | `UINTEGER` | Weekly bars after the selected swing high |
| `ibz_low` | `FLOAT` | 78.6% retracement boundary |
| `ibz_high` | `FLOAT` | 61.8% retracement boundary |
| `smz_low` | `FLOAT` | 82.6% retracement boundary |
| `smz_high` | `FLOAT` | 78.6% retracement boundary |
| `current_price` | `FLOAT` | Close of the last weekly bar |
| `pct_retracement` | `FLOAT` | `100 * (swing_high - current_price) / range` |
| `zone` | `VARCHAR` | One of `above_ibz`, `in_ibz`, `in_smz`, or `below_smz` |
| `primary_degree` | `UINTEGER` | Current primary degree, always `1` |
| `primary_status` | `VARCHAR` | One of `live`, `deep`, or `void` |
| `still_making_new_highs` | `BOOLEAN` | Whether the high is in the last two weekly bars |
| `zigzag_pct` | `FLOAT` | Current zigzag field, always `0.0` |
| `bar_count` | `UINTEGER` | Number of weekly bars processed for the ticker |

## DuckDB queries

Run these queries against the consumer database, such as with `duckdb ./data/tickerlake.duckdb`.

### Actionable screen

This query mirrors the default CLI screen, including its actionable zones, non-void rule, and five-dollar swing-low floor.

```sql
SELECT
    ticker,
    as_of_date,
    current_price,
    zone,
    primary_status,
    pct_retracement,
    swing_low,
    swing_high,
    ibz_low,
    ibz_high,
    smz_low,
    smz_high
FROM weekly_fib_zones
WHERE zone IN ('in_ibz', 'in_smz', 'below_smz')
  AND primary_status <> 'void'
  AND swing_low >= 5.0
ORDER BY ticker;
```

### IBZ candidates with latest weekly liquidity

This query finds non-void tickers currently inside the IBZ and displays the latest `volume_sma_20` used by the eligibility gate.

```sql
WITH latest_weekly_metrics AS (
    SELECT m.ticker, m.volume_sma_20
    FROM weekly_metrics AS m
    JOIN (
        SELECT ticker, MAX(date) AS date
        FROM weekly_metrics
        GROUP BY ticker
    ) AS latest USING (ticker, date)
)
SELECT
    z.ticker,
    z.as_of_date,
    z.current_price,
    z.pct_retracement,
    z.ibz_low,
    z.ibz_high,
    z.primary_status,
    m.volume_sma_20
FROM weekly_fib_zones AS z
JOIN latest_weekly_metrics AS m USING (ticker)
WHERE z.zone = 'in_ibz'
  AND z.primary_status <> 'void'
  AND z.swing_low >= 5.0
ORDER BY z.pct_retracement DESC, z.ticker;
```

### Underlying weekly ticker history

The fib-zone table stores only the current snapshot, so use `weekly_bars` and `weekly_metrics` for a ticker's weekly history.

```sql
SELECT
    b.date,
    b.ticker,
    b.open,
    b.high,
    b.low,
    b.close,
    b.volume,
    m.sma_50,
    m.sma_200,
    m.volume_sma_20
FROM weekly_bars AS b
LEFT JOIN weekly_metrics AS m USING (ticker, date)
WHERE b.ticker = 'AAPL'
ORDER BY b.date;
```

The current fib snapshot for one ticker can be inspected separately with the following query.

```sql
SELECT *
FROM weekly_fib_zones
WHERE ticker = 'AAPL';
```

### Zone and status counts

This query counts the current snapshot by zone and primary status.

```sql
SELECT
    zone,
    primary_status,
    COUNT(*) AS ticker_count
FROM weekly_fib_zones
GROUP BY zone, primary_status
ORDER BY zone, primary_status;
```
