# tickerlake

ETL pipeline for US equity market data. Fetches daily OHLCV bars, stock splits, and ticker metadata from the [Massive API](https://github.com/Massive-Algo/massive), adjusts prices for splits, computes technical indicators, and stores everything in PostgreSQL.

## Pipeline

```mermaid
flowchart LR
    API["Massive API<br/>(Polygon-compatible)"]
    Calendar["NYSE trading days<br/>(exchange_calendars)"]

    subgraph EX["Extract (extract.py + client.py)"]
        Client["client.py<br/>MassiveClient"]
        Bars["OHLCV bars"]
        Splits["stock splits"]
        Tickers["ticker metadata"]
    end

    subgraph TR["Transform (transform.py)"]
        Adjust["adjust_splits<br/>(split-adjust bars)"]
        Metrics["compute_metrics<br/>SMA-20 / 50 / 200<br/>ATR-14, ATR%, ADR%<br/>volume_sma_20"]
    end

    subgraph LD["Load (postgres/)"]
        Raw[("ingest.raw_daily<br/>+ split_event<br/>+ run + cache_state")]
        Consumer[("market.adjusted_daily / weekly / monthly<br/>+ ticker + latest_daily<br/>+ publication_state")]
    end

    API --> Client
    Calendar -.-> Client
    Client --> Bars
    Client --> Splits
    Client --> Tickers

    Bars --> Raw
    Bars --> Adjust
    Splits --> Adjust
    Adjust --> Metrics
    Adjust --> Consumer
    Metrics --> Consumer
    Tickers --> Consumer
```

## Setup

Requires Python 3.14 and a `MASSIVE_API_KEY` environment variable.

```bash
uv sync
export MASSIVE_API_KEY=your_key_here
```

## Usage

```bash
export DATABASE_URL=postgresql://user:pass@host:5432/tickerlake
uv run tickerlake backfill                          # Full 5-year historical load
uv run tickerlake backfill --start-date 2023-01-01  # Custom start date
uv run tickerlake update                            # Append new trading days
```

The `info` subcommand is temporarily removed; the postgres-backed version will land in a follow-up.

## Output

A single PostgreSQL database, addressed by `DATABASE_URL`:

- **`ingest.raw_daily`** -- unadjusted daily bars, plus private tables (`split_event`), run ledger, and cache state
- **`market.adjusted_daily` / `_weekly` / `_monthly`** -- split-adjusted bars joined with technical indicators (`sma_*`, `atr_*`, `adr_*`, `volume_sma_20`)
- **`market.ticker`**, **`market.latest_daily`**, **`market.publication_state`** -- published reference and latest-session projection

The public `market` schema is published atomically per run; readers should observe one generation per transaction.

## Stack

Python 3.14, [Polars](https://pola.rs/) for data processing, [PostgreSQL](https://www.postgresql.org/) 18 for storage, [psycopg](https://www.psycopg.org/) 3 as the driver, [exchange_calendars](https://github.com/gerrymanoim/exchange_calendars) for NYSE trading day resolution.