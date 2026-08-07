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

The command prints one vs-benchmark momentum table: the field running against
the chosen benchmark.

### vs-Benchmark momentum view

A "🐎 vs {benchmark} Momentum" table (default
benchmark: SPY, configurable via `--benchmark`) shows relative strength
(RS) momentum for each ticker compared to the chosen benchmark. The
benchmark ticker itself is excluded from this table. Tickers with at most the
long momentum window bars are
filtered out to avoid degenerate values:

- **Ticker** — the asset.
- **Pos** — current position in the field, ranked by long-term relative pace.
- **Places** — places gained or lost over the medium window. Positive values
  mean the horse is moving toward the front.
- **Pace Short/Medium/Long** — percentage performance of the ETF/benchmark
  ratio over the short (4 bars), medium (13 bars), and long (26 bars) windows.
  These are comparable across ETFs and are the primary pace measurements.
- **Race** — a 0–100 score combining current momentum, places gained, and
  staying power. Higher is better.
- **Form** — horse-racing interpretation of the current race:
  `Charging`, `Closing ground`, `Front-runner`, `Losing steam`, `Fading`,
  `Back of field`, or `Steady`.

The table intentionally omits the underlying RS-Ratio, raw momentum, trend,
and building diagnostics to keep the race readable. They remain available to
the calculation layer that derives pace, scores, and form.

The race score is descriptive rather than predictive. It separates horses
already in front from horses closing ground: front-runners score well on
leadership and staying power, while chargers score well on places gained and
recent pace.

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

The horse-table pace columns use directly comparable relative returns instead:

- `pace_N = ((ratio_current / ratio_N_bars_ago) - 1) × 100`

The field is ranked by long-window pace. `places_gained` is the prior
medium-window position minus the current position, so a positive number means
the horse moved toward the front. The 0–100 race score combines momentum
ranking (45%), places gained (35%), and the percentage of available
medium-window observations with positive pace, called staying power (20%).

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
