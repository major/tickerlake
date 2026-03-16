# PROJECT KNOWLEDGE BASE

**Generated:** 2026-03-16
**Commit:** 5f55e22
**Branch:** main

## OVERVIEW

US equity market ETL pipeline. Pulls OHLCV bars, splits, and ticker metadata from Massive API (Polygon.io-compatible), adjusts for splits, computes technical indicators (SMA-50/200), and stores in DuckDB. Python 3.14 + Polars + DuckDB.

## STRUCTURE

```
tickerlake/
├── src/tickerlake/
│   ├── __init__.py     # CLI entry point (argparse, 3 subcommands)
│   ├── config.py       # Config dataclass, env var validation
│   ├── client.py       # Thin wrapper around massive.RESTClient
│   ├── calendar.py     # NYSE trading day calendar via exchange_calendars
│   ├── extract.py      # API objects -> polars DataFrames (schema-driven)
│   ├── transform.py    # Split adjustment, ticker filtering, SMA metrics
│   ├── load.py         # DuckDB I/O via temporary parquet intermediaries
│   └── pipeline.py     # Orchestrates E->T->L for backfill/update/info
├── tests/              # 1:1 test files per module + conftest fixtures
├── .github/workflows/
│   ├── daily.yml       # Cron 22:00 UTC weekdays, incremental update
│   └── backfill.yml    # Manual dispatch, full historical load
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
| CI/deployment | `.github/workflows/` | DuckDB files uploaded to GitHub "latest" release |

## CONVENTIONS

- **Polars only** -- no pandas for data processing (pandas imported only for `pd.Timestamp` in calendar.py)
- **Schema dicts** -- define column types as `{"col": pl.Type}` dicts, cast with `.cast(schema)`
- **Float32 for prices** -- saves space, Float64 only for adjustment factors
- **Dataclasses over Pydantic** -- `Config` uses `@dataclass` with `__post_init__` validation
- **`pathlib.Path` everywhere** -- never raw strings for file paths
- **Print for user feedback** -- no logging framework, plain `print()` statements
- **Temp parquet for DuckDB I/O** -- write polars -> parquet -> DuckDB SQL, cleanup in `finally`
- **Functional ETL** -- pure functions, no base classes or inheritance
- **Test fixtures** -- pytest fixtures (not helper functions) for reusable test data
- **Mock-first tests** -- all external deps mocked; `patch.multiple()` for pipeline tests
- **Dev tools**: ruff (lint/format), ty (type check), radon (complexity), vulture (dead code), pytest-cov, pytest-randomly

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
- **Update is incremental**: reads max date from `raw.duckdb`, fetches only newer trading days, appends, then rebuilds consumer DB from scratch
- **No CI test step**: GitHub Actions only runs the data pipeline, not tests/linting
- **Downstream notifications**: daily workflow can trigger `repository_dispatch` to a downstream repo via `DOWNSTREAM_PAT` secret
- **Python 3.14 only**: `requires-python = ">=3.14,<3.15"` -- uses modern syntax throughout
