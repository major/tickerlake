# Ciovacco Cloud-Score Report

Every condition in the `ciovacco` report is computed on the per-ETF relative
(ETF/SPY) ratio series. The relative chart makes the "vs SPY" comparison
intrinsic — there is no separate "ETF vs SPY" comparison step. The command
scores each ETF against a benchmark (default: SPY) on 9 conditions: five
Ichimoku "cloud" timeframes (0.0-1.0 each, in 0.25 steps) plus four weekly
moving-average conditions (0/1 each). The result is a scorecard where a
strong, broadly-confirmed uptrend scores up to 9.0.

## Usage

```bash
# Default: dynamic liquid-ETF list (every qualifying ETF analyzed, top 50 displayed)
uv run tickerlake ciovacco

# Score a positional ticker list
uv run tickerlake ciovacco CIBR IGV XLK

# Custom benchmark and display cap
uv run tickerlake ciovacco --benchmark QQQ --max-etfs 0

# Write the full scorecard to CSV (un-capped; Rich table still prints)
uv run tickerlake ciovacco --csv ciovacco.csv
```

When called with no arguments, `ciovacco` builds its ticker list the same way
`etf-race` does: every active, non-leveraged ETF (`type='ETF'`, `active=true`)
whose latest `daily_metrics` row has `volume_sma_20 >= 250,000` is fetched and
scored. The displayed scorecard is capped at the top 50 ETFs by `total`
(`--max-etfs`, pass `0` for unlimited). `--min-vol-sma-20` tunes the
eligibility threshold. Dynamic discovery excludes leveraged names; explicit
positional ticker arguments are not filtered.

The command defaults to a **2555-day (~7 year) lookback** (`--lookback-days`)
so every cloud timeframe and the 300-week moving average have enough daily
bars to resolve. It is read-only and does not require `MASSIVE_API_KEY`.

## Output

The command prints a single Rich scorecard, one row per ETF:

```
┌────────┬──────────┬─────────┬──────────┬──────────┬─────────┬────────┬───────────┬────────┬───────────┬───────┐
│ Ticker ┆ 1D-Cloud ┆ W-Cloud ┆ 2W-Cloud ┆ 3W-Cloud ┆ Mo-Cloud ┆ 200W MA ┆ 200W slope ┆ 300W MA ┆ 300W slope ┆ Total │
├────────┼──────────┼─────────┼──────────┼──────────┼─────────┼────────┼───────────┼────────┼───────────┼───────┤
│ XLK    ┆   0.75   ┆  0.75   ┆   0.75   ┆   0.75   ┆  1.00   ┆   1    ┆     1      ┆   1    ┆     1      ┆ 8.25  │
│ CIBR   ┆   1.00   ┆  1.00   ┆   1.00   ┆   1.00   ┆  1.00   ┆   1    ┆     0      ┆   1    ┆     1      ┆ 8.00  │
│ IGV    ┆   0.50   ┆  0.50   ┆   0.50   ┆   0.50   ┆  0.00   ┆   0    ┆     0      ┆   0    ┆     0      ┆ 1.75  │
└────────┴──────────┴─────────┴──────────┴──────────┴─────────┴────────┴───────────┴────────┴───────────┴───────┘
```

Columns:

- **Ticker** — the ETF.
- **1D-Cloud / W-Cloud / 2W-Cloud / 3W-Cloud / Mo-Cloud** — 0.0-1.0 in 0.25
  steps: the count of the four Ichimoku lines the ETF/SPY ratio's close is
  above on daily, weekly, 2-week, 3-week, and monthly bars, divided by 4.
  "1.00" means the ratio is above all four lines; "0.50" means above two of
  four (the classic "inside the cloud" reading); "0.00" means below all four.
- **200W MA / 300W MA** — 1 when the ratio's close is above the ratio's own
  weekly 200/300-week simple moving average; 0 otherwise.
- **200W slope / 300W slope** — 1 when the ratio's 200/300-week MA is rising
  (the MA at the current week is above the MA 26 weeks earlier); 0 otherwise.
- **Total** — the sum of all 9 columns (max 9.0). Cells shown as `n/a`
  (insufficient history) count as 0 toward the total.

### Reading the table

- **Cloud cells** are tinted by score: 1.00 green, 0.75 dim green, 0.50
  yellow, 0.25 dim yellow, 0.00 red. `n/a` cells are dim.
- **MA cells** are green for 1, red for 0, dim for `n/a`.
- **Total** is green at >= 7, yellow 4-6.99, red below.

Colors are ANSI and are disabled automatically by Rich when the output is not
a terminal.

### CSV output

Pass `--csv PATH` to write the full scorecard to a CSV file alongside the
Rich table:

```bash
uv run tickerlake ciovacco --csv ciovacco.csv
```

The CSV is un-capped by `--max-etfs` (which only constrains the Rich table)
and includes every scored ticker sorted by `total` descending (nulls last).
Columns:

- `ticker` — the ETF
- `benchmark` — the benchmark passed to `--benchmark` (default: SPY)
- `score_1d_cloud` / `score_weekly_cloud` / `score_2wk_cloud` /
  `score_3wk_cloud` / `score_monthly_cloud` — the 0.0-1.0 cloud scores
- `score_200wk_ma` / `score_200wk_ma_slope` / `score_300wk_ma` /
  `score_300wk_ma_slope` — the 0/1 MA scores
- `total` — the 0.0-9.0 sum

The parent directory is created if it does not already exist. Null cells
(`n/a` in the Rich table) are written as empty strings.

## Methodology

Every condition is computed on the per-ETF relative (ETF/SPY) ratio series.
The relative chart makes the "vs SPY" comparison intrinsic — there is no
separate "ETF vs SPY" comparison step.

### Why relative pricing?

Comparing raw-dollar moving averages across tickers is meaningless because
tickers trade at different price levels — a $30 ETF with a strongly rising
200-week MA would always "lose" to SPY at $500 on absolute levels, no matter
how much it was outperforming on a percentage basis. The ETF/SPY ratio
normalizes price level: a ratio above its own rising MA means the ETF is
genuinely outperforming its benchmark. Computing every condition on the ratio
series makes the report comparable across tickers and captures relative
strength directly.

### The 4 Ichimoku lines per cloud

On each timeframe, the ETF's daily bars are aggregated to that timeframe's
bars (1d, 1w, 2w, 3w, 1mo) for both the ETF and SPY, then reduced to the
per-ETF ratio series: `ratio_close = etf_close / spy_close`, date-aligned via
an inner join. The cloud lines are computed on the ratio series itself, and
the ratio's close is compared to them, each weighted 0.25:

- **Tenkan-sen**: `tenkan_above = 1 if close > tenkan else 0`, where
  `tenkan = (highest high + lowest low) / 2` over the last `tenkan_period` bars.
- **Kijun-sen**: `kijun_above = 1 if close > kijun else 0`, where
  `kijun = (highest high + lowest low) / 2` over the last `kijun_period` bars.
- **Senkou Span A**: `senkou_a_above = 1 if close > senkou_a_at_current else 0`.
  Senkou A (`(tenkan + kijun) / 2`) is plotted 26 periods ahead, so the value
  shown at the current bar is the one computed 26 bars ago.
- **Senkou Span B**: `senkou_b_above = 1 if close > senkou_b_at_current else 0`.
  Senkou B (`(highest high + lowest low) / 2` over the last
  `senkou_b_period` bars) uses the same 26-bar displacement.

The cloud score is the count of "above" lines divided by 4:

```
cloud_score = tenkan_above + kijun_above + senkou_a_above + senkou_b_above
```

which always lands in {0.00, 0.25, 0.50, 0.75, 1.00}. A close exactly on a
line counts as NOT above (strict `>`), so a ticker sitting inside the cloud
between Senkou A and B scores 0.50. If any line is undefined (insufficient
history for its window, including the 26-bar displacement), the cell is null
rather than a partial weight.

The Chikou Span (`close` plotted 26 periods behind; bullish when the current
close exceeds the close 26 periods earlier) is a standard Ichimoku element
but is **not** part of the Ciovacco per-cloud score — the "above cloud"
check is the four lines above. It is not computed in the report.

The 0.25 increments are the canonical scoring. The occasional "0.33" / "0.4"
cells seen in Ciovacco's own spreadsheets come from partial weighting (e.g.
"2 of 6") on real data; this report uses the uniform 0.25 weighting.

### Timeframe periods

The Ichimoku periods live in one place as a typed dict,
`TIMEFRAME_ICHIMOKU_PERIODS` (tenkan, kijun, senkou_b):

| Timeframe | tenkan | kijun | senkou_b | History needed |
|-----------|--------|-------|----------|----------------|
| daily (1d) | 9 | 26 | 52 | ~4 months |
| weekly (1w) | 9 | 26 | 52 | ~1.5y |
| 2-week (2w) | 9 | 26 | 52 | ~3y |
| 3-week (3w) | 9 | 26 | 52 | ~4.5y |
| monthly (1mo) | 9 | 26 | 52 | ~6.5y |

All five timeframes use the standard 9/26/52 Ichimoku periods — only the bar
timeframe changes between columns. The deeper timeframes need more history
(daily needs 78 daily bars ≈ 4 months; monthly needs 78 monthly bars ≈ 6.5y).
All five resolve within the 10-year backfill.

### MA + slope conditions

The four MA conditions are self-comparisons on the ETF/SPY ratio's own weekly
moving averages:

- **ABOVE N-WK MA**: 1 when `ratio_close > ratio_ma_N`, 0 otherwise — the
  ratio's close is above the ratio's own N-week simple moving average.
- **N-WK MA SLOPE**: 1 when `ratio_ma_slope_N > 0`, where
  `ratio_ma_slope_N = ratio_ma_N - ratio_ma_N_26_bars_ago`. In short: the
  relative MA is rising. The cell is null when any component is undefined
  (the 300-week MA needs ~6.25 years of weekly bars).

There is no comparison to SPY's own MA or slope — the ratio series already
embeds the benchmark, so "above the ratio's MA" is "outperforming the
benchmark's trend".

### Total

`total = sum of all 9 score columns`, with null cells counted as 0. Maximum
possible score is 9.0 (1.00 + 1.00 + 1.00 + 1.00 + 1.00 for the clouds plus
1 + 1 + 1 + 1 for the MAs).

## Caveats

- **Backfill depth matters.** The deeper conditions need a deep backfill: the
  monthly cloud includes the 26-bar Senkou displacement (needing ~6.5 years of
  daily bars) and the 300-WK MA needs ~6.25 years of weekly bars. With only
  ~5 years of daily bars those columns render `n/a` (the daily/weekly/2wk/3wk
  clouds and the 200-WK MA resolve fine). Re-run the backfill for a deeper
  range to fill them in:

  ```bash
  uv run tickerlake backfill --start-date 2015-01-01
  ```

- **The `ciovacco` command itself defaults to a 2555-day (7y) lookback** so
  its data window covers every timeframe once the backfill is deep enough.
- **Relative, not absolute, pricing.** All conditions are computed on the
  ETF/SPY ratio, never on raw dollar levels. An ETF priced below SPY in
  dollars can still score 1 on the MA columns when its ratio is above and
  rising — that is the point of the relative chart.
- **Senkou displacement.** Senkou Span A/B use the standard 26-period
  displacement, so a timeframe's score needs the period window plus 26 more
  bars of history before its first non-null value.
