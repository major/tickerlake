# PROJECT KNOWLEDGE BASE

**Generated:** 2026-04-06
**Branch:** main

> **Maintenance note:** Keep this file updated when adding new tables, transforms, schemas, or pipeline stages.

## OVERVIEW

US equity market ETL pipeline. Pulls OHLCV bars, splits, and ticker metadata from Massive API (Polygon.io-compatible), adjusts for splits, computes technical indicators (SMA-50/200, ATR, ADR%), and stores in DuckDB. Python 3.14 + Polars + DuckDB.

## STRUCTURE

```
tickerlake/
├── src/tickerlake/
│   ├── __init__.py     # CLI entry point (argparse, 3 subcommands)
│   ├── config.py       # Config dataclass, env var validation
│   ├── client.py       # Thin wrapper around massive.RESTClient
│   ├── calendar.py     # NYSE trading day calendar via exchange_calendars
│   ├── extract.py      # API objects -> polars DataFrames (schema-driven)
│   ├── transform.py    # Split adjustment, ticker filtering, SMA/ATR metrics
│   ├── load.py         # DuckDB I/O via temporary parquet intermediaries
│   └── pipeline.py     # Orchestrates E->T->L for backfill/update/info
├── tests/              # 1:1 test files per module + conftest fixtures
├── .github/workflows/
│   └── ci.yml          # Lint, format, complexity, test-cov on push to main + PRs
├── pyproject.toml      # uv_build backend, deps, dev tools
└── uv.lock
```

## WHERE TO LOOK

| Task | Location | Notes |
|------|----------|-------|
| Add CLI subcommand | `__init__.py` | `_build_parser()` + dispatch in `main()` |
| Add new data source | `extract.py` | Define schema dict, write `_*_to_row()` + `extract_*()` |
| New transformation | `transform.py` | Pure functions: `pl.DataFrame` in, `pl.DataFrame` out |
| New DuckDB table | `load.py` | Add to `write_consumer_db()`, use `_tmp_parquet` context manager |
| Change API calls | `client.py` | Wraps `massive.RESTClient` methods |
| Trading day logic | `calendar.py` | Uses `exchange_calendars` XNYS calendar |
| Config defaults | `config.py` | Dataclass fields + `__post_init__` validation |
| Pipeline flow | `pipeline.py` | `_run_backfill()` has the full E->T->L sequence |
| Shared test data | `tests/conftest.py` | 3 fixtures: `sample_bars_df`, `sample_splits_df`, `sample_tickers_df` |
| CI/deployment | `.github/workflows/ci.yml` | Runs `make check` (lint, format-check, complexity+xenon, test-cov) on push to main + PRs |

## CONVENTIONS

- **Polars only** -- no pandas for data processing (pandas imported only for `pd.Timestamp` in calendar.py)
- **Schema dicts** -- define column types as `{"col": pl.Type}` dicts, cast with `.cast(schema)`
- **Float32 for prices** -- saves space, Float64 only for adjustment factors
- **Dataclasses over Pydantic** -- `Config` uses `@dataclass` with `__post_init__` validation
- **`pathlib.Path` everywhere** -- never raw strings for file paths
- **Logging via `logging` module** -- `logger = logging.getLogger(__name__)` in pipeline.py
- **Temp parquet for DuckDB I/O** -- write polars -> parquet -> DuckDB SQL, cleanup in `finally`
- **Functional ETL** -- pure functions, no base classes or inheritance
- **Test fixtures** -- pytest fixtures (not helper functions) for reusable test data
- **Mock-first tests** -- all external deps mocked; `patch.multiple()` for pipeline tests
- **Dev tools**: ruff (lint/format), ty (type check), radon (complexity), vulture (dead code), pytest-cov, pytest-randomly, xenon
- **Coverage gate**: 95% minimum enforced via `pytest-cov --cov-fail-under=95` in CI
- **Complexity gate**: A/B-only (per-function, per-module, and average) enforced via `xenon --max-absolute B --max-modules B --max-average A` in CI. Ruff's `mccabe`/`C901` rule (part of `select = ["ALL"]`, default `max-complexity = 10`) already flags per-function complexity above B at lint time as a first signal; `xenon` remains the authoritative radon-based gate because it also covers module-level and average complexity, which ruff does not.

## ANTI-PATTERNS

- **NEVER pass `datetime.timezone.utc` to exchange_calendars** -- causes `AttributeError: 'datetime.timezone' object has no attribute 'key'`. Use tz-naive `pd.Timestamp` objects instead. See `calendar.py:31-32`.
- **NEVER use pandas DataFrames** for pipeline data -- Polars only. Pandas is a transitive dep of exchange_calendars.
- **NEVER leave DuckDB connections open** -- always `con.close()` (no context manager on duckdb.connect).
- **NEVER use `delete=True` with `NamedTemporaryFile`** for parquet intermediaries -- need the path after closing. Use `delete=False` + manual `unlink(missing_ok=True)` in `finally`.

## COMMANDS

```bash
uv sync                                          # Install deps
uv run pytest                                    # Run tests
uv run tickerlake backfill                       # Full 5-year backfill
uv run tickerlake backfill --start-date 2023-01-01  # Custom start
uv run tickerlake update                         # Incremental (appends new days)
uv run tickerlake info                           # Show DB metadata
uv run ruff check src/ tests/                    # Lint
uv run ty check src/                             # Type check
```

## NOTES

- **MASSIVE_API_KEY** env var is required -- `Config.__post_init__` raises `ValueError` if missing
- **Two DuckDB files**: `raw.duckdb` (raw bars) and `tickerlake.duckdb` (adjusted bars + metrics + tickers)
- **Consumer DB tables**: `daily_bars`, `daily_metrics`, `tickers`, `weekly_bars`, `weekly_metrics`, `monthly_bars`, `monthly_metrics`
- **Update is incremental and revision-aware**: delegates to the backfill sequence (`_run_backfill(config, bars_start=...)`) with the bars-fetch start narrowed to a trailing `_REVISION_WINDOW_DAYS`-day window of already-cached dates (fetch-then-swap: fetch first, then delete+replace only the dates Massive actually returned data for in that window), so revisions Massive makes to already-published bars (up to ~5 trading days back) get picked up on the next run. Splits/tickers extraction always covers the full `config.start_date`-`config.end_date` range regardless. Consumer db is always fully rebuilt from raw.duckdb.
- **Python 3.14 only**: `requires-python = ">=3.14,<3.15"` -- uses modern syntax throughout
