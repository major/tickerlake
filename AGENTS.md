# Agent Instructions

## Project
- Tickerlake is a US equity market ETL using Massive, Polars, and DuckDB.
- Python support is only `>=3.14,<3.15`.
- CLI entry point and commands are in `src/tickerlake/__init__.py`: `backfill`, `update`, `info`, `compact`.
- `pipeline.py` shares the backfill flow for full and incremental loads. Update refreshes a trailing five cached-date revision window, then rebuilds the consumer database from raw data.
- Processing uses Polars. Pandas is only for timestamps used with `exchange_calendars`; pass tz-naive timestamps, not `datetime.timezone.utc`.
- Always close DuckDB connections. Temporary parquet files must be removed in `finally` after use; do not use `NamedTemporaryFile(delete=True)` for these files.

## Setup and configuration
- Install dependencies with `uv sync` (or CI's locked form: `uv sync --locked`).
- `MASSIVE_API_KEY` is required for `backfill` and `update`; `info` and `compact` do not require it.
- Database output defaults to the current working directory. Use `--output-dir DIR` to isolate output.

## Make targets
| Target | Action |
| --- | --- |
| `make test` | `uv run pytest tests/ -x --tb=short` |
| `make test-cov` | pytest with branch coverage and HTML/XML/terminal reports |
| `make lint` | Ruff check on `src/` and `tests/` |
| `make format` | Format `src/` and `tests/` with Ruff |
| `make format-check` | Check Ruff formatting without changes |
| `make typecheck` | Type check `src/` with ty |
| `make check` | lint, format-check, typecheck, complexity, test-cov |
| `make sync` | Runs `tickerlake sync --verbose`; unsupported by current CLI |

`make sync` is not dependency setup. Current CLI has no `sync` subcommand.
CI runs lint, format-check, typecheck, complexity, and test-cov separately. There is no configured coverage minimum.

## Validation
- Pipeline tests use real extraction, transforms, calendar, and DuckDB files under `tmp_path`. Fake the Massive API at `pipeline.MassiveClient`. Assert persisted state and observable behavior, not internal calls. Patch internal functions only for explicit fault injection.
- Focused test example: `uv run pytest tests/test_transform.py -k test_name` (use the corresponding test file and selector).
- `uv run ty check src/` is the project type checker. It runs via `make typecheck` and is part of `make check` and CI.
