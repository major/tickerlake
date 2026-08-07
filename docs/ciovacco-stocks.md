# Ciovacco Cloud-Score Report for Common Stocks

`ciovacco-stocks` is the common-stock twin of the [`ciovacco`](ciovacco.md)
report. It applies the exact same 9-condition Ichimoku cloud + moving-average
scorecard to the common-stock universe (`type='CS'`) instead of ETFs, against
the same benchmark (default: SPY). Every condition is computed on the per-stock
relative (stock/SPY) ratio series, so the scorecard means the same thing here
as it does for ETFs — a strong, broadly-confirmed uptrend scores up to 9.0.

## Usage

```bash
# Default: dynamic liquid common-stock list (every qualifying stock analyzed, top 50 displayed)
uv run tickerlake ciovacco-stocks

# Score a positional ticker list
uv run tickerlake ciovacco-stocks AAPL MSFT NVDA

# Custom benchmark and display cap
uv run tickerlake ciovacco-stocks --benchmark QQQ --max-stocks 0

# Write the full scorecard to CSV (un-capped; Rich table still prints)
uv run tickerlake ciovacco-stocks --csv ciovacco-stocks.csv
```

When called with no arguments, `ciovacco-stocks` builds its ticker list via
`read_qualifying_stocks` in `race.py`: every active common stock
(`type='CS'`, `active=true`) whose latest `daily_metrics` row has
`volume_sma_20 >= 250,000` is fetched and scored. The displayed scorecard is
capped at the top 50 stocks by `total` (`--max-stocks`, pass `0` for
unlimited). `--min-vol-sma-20` tunes the eligibility threshold. Unlike
`etf-race` and `ciovacco`, no leverage-name regex is applied — common stocks
are already plain `type='CS'`, so there is nothing to filter out.

The command defaults to a **3650-day (10 year) lookback** (`--lookback-days`)
so every cloud timeframe and the 300-week moving average have enough daily
bars to resolve. It is read-only and does not require `MASSIVE_API_KEY`.

## Output

The command prints a single Rich scorecard, one row per stock — identical in
layout to the `ciovacco` scorecard (see `docs/ciovacco.md` for the annotated
example). Columns:

- **Ticker** — the common stock.
- **1D-Cloud / W-Cloud / 2W-Cloud / 3W-Cloud / Mo-Cloud** — 0.0-1.0 in 0.25
  steps: the count of the four Ichimoku lines the stock/SPY ratio's close is
  above on daily, weekly, 2-week, 3-week, and monthly bars, divided by 4.
- **200W MA / 300W MA** — 1 when the ratio's close is above the ratio's own
  weekly 200/300-week simple moving average; 0 otherwise.
- **200W slope / 300W slope** — 1 when the ratio's 200/300-week MA is rising;
  0 otherwise.
- **Total** — the sum of all 9 columns (max 9.0). Cells shown as `n/a`
  (insufficient history) count as 0 toward the total.
- **Name** — the stock's full description from the consumer-DB `tickers`
  table, dim-styled, right of Total, truncated to 50 characters with an
  ellipsis.

### CSV output

Pass `--csv PATH` to write the full scorecard to a CSV file alongside the
Rich table:

```bash
uv run tickerlake ciovacco-stocks --csv ciovacco-stocks.csv
```

The CSV is un-capped by `--max-stocks` (which only constrains the Rich table)
and includes every scored stock sorted by `total` descending (nulls last).
Columns: `ticker`, `benchmark`, the five `score_*_cloud` columns, the four
`score_*_ma` columns, and `total`. The parent directory is created if it does
not already exist. Null cells are written as empty strings.

## Methodology

Identical to the `ciovacco` report: every condition is computed on the
per-stock relative (stock/benchmark) ratio series, the ratio's cloud lines
use the standard 9/26/52 Ichimoku periods per timeframe, and the four MA
conditions are self-comparisons on the ratio's own weekly moving averages.
See [docs/ciovacco.md](ciovacco.md) for the full methodology (why relative
pricing, the four Ichimoku lines per cloud, timeframe periods, MA + slope
conditions, and the total).

The only differences from `ciovacco` are the universe and the eligibility
rule:

- **Universe**: `type='CS'` common stocks instead of `type='ETF'`.
- **Eligibility**: `active` + `volume_sma_20 >= 250,000`; no leverage-name
  regex, because common stocks have no leverage-naming convention to screen.

## Caveats

- **Backfill depth matters**, exactly as for `ciovacco`: the monthly cloud
  includes the 26-bar Senkou displacement (needing ~6.5 years of daily bars)
  and the 300-WK MA needs ~6.25 years of weekly bars. With only ~5 years of
  daily bars those columns render `n/a`. Re-run the backfill for a deeper
  range to fill them in:

  ```bash
  uv run tickerlake backfill --start-date 2015-01-01
  ```

- **The `ciovacco-stocks` command itself defaults to a 3650-day (10y)
  lookback** so its data window covers every timeframe once the backfill is
  deep enough.
- **Relative, not absolute, pricing.** All conditions are computed on the
  stock/SPY ratio, never on raw dollar levels.
