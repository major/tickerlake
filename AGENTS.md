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
| `make check` | lint, format-check, complexity, test-cov |
| `make mutation-focused SCOPE="..."` | Run scoped mutation testing for the specified code area |
| `make sync` | Runs `tickerlake sync --verbose`; unsupported by current CLI |

`make sync` is not dependency setup. Current CLI has no `sync` subcommand.
CI runs lint, format-check, complexity, and test-cov separately. There is no configured coverage minimum.

### Mutation testing
- For meaningful behavior changes and bug fixes, first run the relevant ordinary tests and confirm they pass, then run scoped mutation testing for affected functions. Documentation, formatting, and behavior-preserving changes may skip it with a short reason.
- Use `make mutation-focused SCOPE='tickerlake.pipeline.x__require_api_key*'` as the scope form. Scope must name an explicit top-level generated function with the `x_` prefix and trailing `*`; it is not a module-wide wildcard. The target runs in its own Podman container, so do not wrap it in another container.
- Generation and the initial baseline may cover the whole package or test suite even for a scoped run. Global counters can include unselected mutants. Report outcomes for selected mutants only, and do not treat the global denominator as the executed count.
- Report the exact scope, selected mutant counts for killed, surviving, timed-out, and no-test outcomes, and whether the run was incomplete. Investigate survivors, but do not blindly chase equivalent mutants. A zero-match or incomplete run is not successful. Do not run the full suite routinely; weekly CI remains the full run. Never apply mutants automatically to source files.

## Validation
- Run tests and other Makefile commands in temporary Podman containers using the full non-slim official Python 3.14 image (`docker.io/library/python:3.14`). Do not run them on the host or use slim images. The `mutation-focused` target is a container launcher: invoke it directly, since it runs mutation testing inside its own temporary container.
- Pipeline tests use real extraction, transforms, calendar, and DuckDB files under `tmp_path`. Fake the Massive API at `pipeline.MassiveClient`. Assert persisted state and observable behavior, not internal calls. Patch internal functions only for explicit fault injection.
- Tests, including `make test`, `make test-cov`, and `make check`, must run in a temporary Podman container per user policy. Do not run them directly on the host.
- Focused test example: `uv run pytest tests/test_transform.py -k test_name` (use the corresponding test file and selector).
- `uv run ty check src/` is available as a separate check; it is not part of `make check` or CI's listed steps.
