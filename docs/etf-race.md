# ETF Race Report

The `etf-race` command compares ETFs by their relative strength vs a
benchmark (default: SPY), showing which are outperforming and which are
lagging across multiple timeframes.

## Usage

```bash
# Default: dynamic liquid-ETF list (top 50 by 20-day volume, min 250k shares/day)
uv run tickerlake etf-race

# vs-benchmark momentum table: 3+ tickers
uv run tickerlake etf-race CIBR IGV XLK

# vs-benchmark momentum table with custom benchmark (default: SPY)
uv run tickerlake etf-race CIBR IGV XLK --benchmark QQQ

# Custom time window
uv run tickerlake etf-race CIBR IGV --timeframe daily --lookback-days 90
```

When called with no arguments, `etf-race` builds its ticker list
dynamically from the consumer DB: every active ETF (`type='ETF'`,
`active=true`) whose latest `daily_metrics` row has
`volume_sma_20 >= 250,000` qualifies. The list is then ranked by
`volume_sma_20` descending and capped at the 50 most-liquid names, so
the leaderboard is always a market-wide snapshot, not a hand-picked
one. Tune the threshold with `--min-vol-sma-20` and the cap with
`--max-etfs` (pass `0` for unlimited).

## Output

The command prints a vs-benchmark momentum leaderboard.

### vs-Benchmark momentum view

A "🐎 vs {benchmark} Momentum" table (default
benchmark: SPY, configurable via `--benchmark`) shows relative strength
(RS) momentum for each ticker compared to the chosen benchmark. The
benchmark ticker itself is excluded from this table (it still appears in
the main leaderboard if raced explicitly). Tickers with at most the
long momentum window bars are filtered out to avoid degenerate values:

- **Ticker** — the asset.
- **RS-Ratio** — current ratio of `ticker_close / benchmark_close`,
  rebased to 100 at each ticker's first available bar (per-ticker rebasing,
  not shared window start; matches TradingView's relative-strength charts,
  not RRG-style z-score normalization).
- **Trend** — one of:
  - **Leading** — RS ≥ 100 and short-term momentum > 0 (outperforming and rising).
  - **Fading** — RS ≥ 100 and short-term momentum ≤ 0 (outperforming but declining).
  - **Improving** — RS < 100 and short-term momentum > 0 (underperforming but rising).
  - **Lagging** — RS < 100 and short-term momentum ≤ 0 (underperforming and declining).
- **Momentum Short/Medium/Long** — point-change on the rebased RS ratio
  over short (4 bars, default ~1 month for weekly), medium (13 bars,
  default ~1 quarter), and long (26 bars, default ~6 months) windows.
  Gracefully clamped to available history for recently-listed tickers.
- **Building** — 🚀 when outperformance vs benchmark is accelerating
  (short-term rate of change > medium > long, and medium-term momentum
  already positive). Indicates sustained momentum building, not early-mover
  fade.

## Methodology

### vs-Benchmark Momentum

The vs-benchmark momentum table uses the **plain ratio convention**:
`RS-Ratio(t) = ticker_close(t) / benchmark_close(t)`, rebased to 100 at
each ticker's first available bar (per-ticker rebasing, not shared window
start; matching TradingView's relative-strength charts, not RRG-style
z-score normalization — confirmed against actual TradingView CSV exports).

Momentum columns are **cumulative point-deltas** on the rebased ratio over
each window (gracefully clamped to available history for recently-listed
tickers):

- `momentum_short = rs_ratio_current − rs_ratio_4_bars_ago` (default 4-bar window)
- `momentum_medium = rs_ratio_current − rs_ratio_13_bars_ago` (default 13-bar window)
- `momentum_long = rs_ratio_current − rs_ratio_26_bars_ago` (default 26-bar window)

The **building indicator** (🚀) uses **rate-normalized** momentum
(`momentum / actual_bars_back`, not raw cumulative deltas), with a
positivity gate:

- `building = (rate_short > rate_medium > rate_long) AND (momentum_medium > 0)`
- **Why rates, not raw momenta?** Comparing raw cumulative point-deltas
  across unequal windows is misleading — a ticker declining −1 per bar
  for 3 bars shows `short < medium < long` cumulatively (−1 < −2 < −3)
  despite *constant* deceleration. Rates normalize for window length,
  surfacing *accelerating* outperformers (rates increasing as windows
  shrink) vs decelerating decliners (rates decreasing even while
  cumulatively negative).
- **Positivity gate** (`momentum_medium > 0`) excludes "decelerating
  declines" (e.g. losing −0.5 pts per bar instead of −1.0, accelerating
  per rate-math but still underperforming) from sharing the 🚀 marker
  with genuine accelerating outperformers.

### Caveats

- **Short-history tickers in vs-benchmark momentum**: tickers with at most
  the long momentum window bars (default 26) are filtered out of the
  vs-benchmark table entirely to avoid degenerate/misleading momentum values.
  Such tickers still appear in the main leaderboard.
- **Daily bars**: the median-gap date projection underestimates
  calendar days by ~40% for daily bars (median gap = 1 trading day,
  but each week has one 3-day gap). Weekly and monthly projections
  are exact. This is a projection, not a promise.
- **Float32 rounding**: the consumer DB stores prices as Float32. The
  sub-0.1% max relative error on the last bar is Float32 rounding
  in the weekly close column.
- **Backfill depth**: the report reads whatever is in the consumer
  DB. The default backfill is 5 years. Run `tickerlake backfill
  --start-date 2015-01-01` to see the full CIBR/IGV history
  available in the TradingView CSV.
