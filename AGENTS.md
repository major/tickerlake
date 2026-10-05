# Agent Instructions

## Project
- Tickerlake is a US equity market ETL using Massive, Polars, and PostgreSQL.
- Python support is only `>=3.14,<3.15`.
- CLI entry point and commands are in `src/tickerlake/__init__.py`: `backfill`, `update`. The `info` and `compact` subcommands were removed in the postgres cutover.
- `pipeline.py` is now a thin adapter that builds `BackfillRequest` / `UpdateRequest` and delegates to `src/tickerlake/postgres/backfill.py`. Update refreshes a trailing five cached-date revision window, then rebuilds the consumer database from raw data.
- Processing uses Polars. Pandas is only for timestamps used with `exchange_calendars`; pass tz-naive timestamps, not `datetime.timezone.utc`.
- Always close psycopg connections (`writer_connection` is a context manager). The legacy Polars -> temp parquet -> ingest path is gone; do not reintroduce it.

## Setup and configuration
- Install dependencies with `uv sync` (or CI's locked form: `uv sync --locked`).
- `MASSIVE_API_KEY` is required for `backfill` and `update`.
- `DATABASE_URL` is required for `backfill` and `update`; the postgres package validates the DSN via `psycopg.conninfo.conninfo_to_dict` at config time. No working-directory or `--output-dir` flag exists anymore.

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
- Pipeline tests use real extraction, transforms, calendar, and PostgreSQL state. Fake the Massive API by passing a fake `MassiveClient` (the Protocol from `tickerlake.client`) to `backfill()` or `update()` via the `client=` kwarg. The default is `SdkMassiveClient(config)`. Assert persisted state and observable behavior, not internal calls. Patch internal functions only for explicit fault injection. Postgres integration tests in `tests/postgres/` are gated by `TICKERLAKE_TEST_POSTGRES=1` and require `pg_ctl` / `initdb` on `PATH` (the CI job installs them via `ankane/setup-postgres`).
- Focused test example: `uv run pytest tests/test_transform.py -k test_name` (use the corresponding test file and selector).
- `uv run ty check src/` is the project type checker. It runs via `make typecheck` and is part of `make check` and CI.
