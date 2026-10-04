# PostgreSQL backfill contract

## Scope

Chunk 5 adds one orchestration entry point:

```python
backfill(config: Config, request: BackfillRequest, *, now: datetime, batch_size: int = 100) -> PublicationResult
```

It fetches Massive history and corrections, stores them in the PostgreSQL ingest
relations through the existing storage primitives, then calls the chunk 4
`rebuild_cache` once to rebuild and publish. It adds no CLI command, no update
command, no migrations, no DuckDB dependency, and no new package dependency. It
does not call `finish_run`: publication sets the run state.

The run uses one `writer_connection(config.database_url)`. That connection holds
the shared writer advisory lock for the whole run. There is no outer
transaction. Each storage primitive opens its own short transaction. Network
calls happen while the connection is idle and commit-free. If the connection is
lost, the lock is released, the run is left `running`, and the run is never
resumed or reconnected in place.

## Frozen inputs

```python
@dataclass(frozen=True, slots=True, kw_only=True)
class BackfillRequest:
    code_version: str
    schema_version: str
    transform_version: str
    target: date | None = None
    correction_range: tuple[date, date] | None = None
```

`target` is a frozen upper bound, not an as-of publication stamp. `correction_range`
selects only the explicitly requested closed sessions. It never changes the resolved target
and never adds the target to the requested run.

The entry point also depends on these agreed APIs from other lanes:

| Symbol | Module | Contract |
|---|---|---|
| `get_closed_sessions(start, end, *, now)` | `tickerlake.calendar` | Ascending closed XNYS sessions in an inclusive range. |
| `resolve_closed_target(target, *, now)` | `tickerlake.calendar` | Latest closed XNYS session at or before `target`. |
| `read_split_range(connection, start, end)` | `tickerlake.postgres.reading` | Canonical `SPLITS_SCHEMA` rows in an inclusive range. |
| `read_split_bounds(connection)` | `tickerlake.postgres.reading` | `(min, max)` stored split dates or `(None, None)`. |

`MassiveClient` is imported as a module-level symbol from `tickerlake.client` so
tests can patch `tickerlake.postgres.backfill.MassiveClient`. It is constructed
with the exact existing constructor, `MassiveClient(config)`.

## Validation before any write

Validation runs before the writer connection is opened and before any network
call:

- Credentials: `config.api_key` must be nonblank, otherwise `ValueError` with the
  existing `MASSIVE_API_KEY environment variable is required` message.
- Database URL: `config.database_url` must be a nonblank string.
- Configured bounds: `config.start_date` and `config.end_date` must be ordered
  plain dates. This is checked before every run, including correction-only runs.
- Ticker types: `config.ticker_types` must contain 1 to 20 unique, nonblank types.
- Run versions: `code_version`, `schema_version`, and `transform_version` must be
  nonempty strings.
- Dates: `target` is `None` or a plain `date`; `correction_range` is `None` or a
  two-tuple of ordered plain dates.
- `batch_size` is an `int` from 1 through 1000.
- `now` is a timezone-aware `datetime`.

Any failure above raises `BackfillError` (or `ValueError` for credentials) and
does not start a run.

## Target resolution

The base target is `request.target` when present, otherwise `config.end_date`.
`resolve_closed_target(base, now=now)` resolves the latest closed XNYS session at
or before that base, independently of the correction range, the cache state, and
current freshness. The result is stored in `ingest.run.target_date`. Target
resolution is separate from session selection.

## Fetch scope

Session selection is exactly one of two modes:

- Full mode, when `request.correction_range` is `None`: every closed session from
  `config.start_date` through the resolved target. Cached sessions are still
  fetched.
- Correction mode, when `request.correction_range` is present: only the closed
  sessions inside `request.correction_range`, inclusive at both bounds. Cached
  sessions are still fetched. This is not the trailing five-date update window,
  and it never adds the configured history or the resolved target.

A correction run does not widen its range or the run's requested target. If a
supplied correction range contains no closed session, the run is rejected even
when the configured full range has sessions. An empty selected set is rejected
with `BackfillError` before any run starts.

`RunSpec.target` is the resolved target. `RunSpec.requested_start` and
`requested_end` are the first and last selected session.

## Daily fetch and completeness

For each selected session, in order:

1. `read_raw_date(connection, day)` reads the prior canonical date for
   shrink detection.
2. `extract_daily_aggs(client, [day], previous=prior)` fetches and validates.
3. `store_daily_outcome` replaces that date's raw rows atomically and records the
   manifest.
4. Frames are released before the next date.

Every selected session is processed and recorded even when an earlier session is
rejected. After all sessions are stored, if any outcome is not `populated`, the
run is marked failed with `incomplete_fetch` when that is safe, and
`BackfillIncompleteError` is raised. This blocks publication even when the
resolved target itself has accepted cached evidence. Accepted daily writes remain
committed even when another date is rejected. Rejected dates retain their previous
accepted contents, and the previous public generation remains unchanged.

## Ticker metadata

`read_ticker_reference(connection, config.ticker_types)` reads the prior scope,
then `extract_tickers(client, types, previous=prior)` fetches it. The outcome is
stored through `store_ticker_outcome` using normal validation. A result that is
not `populated` marks the run failed and raises `BackfillError`.

## Split coverage

The coverage range is the union of `config.start_date`, `config.end_date`, the
resolved target, the correction bounds (when present), the current retained raw
bounds from `ingest.cache_state`, and the stored split bounds from
`read_split_bounds`. The range is split into non-overlapping inclusive
calendar-year windows.

For each window, independently:

1. `read_split_range(connection, window_start, window_end)` reads the prior
   window scope.
2. `extract_splits(client, window_start, window_end, previous=prior)` fetches and
   validates it. This step already quarantines destructive empty responses when
   previous events existed.
3. `store_split_outcome` replaces that window atomically and records the manifest.
4. Frames are released before the next window.

`populated` and `successful_empty` are accepted. `successful_empty` is only
reachable when no previous events existed, because `extract_splits` quarantines
destructive empties. Any `failed` or `quarantined` outcome marks the run failed
and raises `BackfillError`, blocking publication. Raw history is never filtered
by metadata, and split windows are never concatenated into a universe-wide frame.

## Publication and failure handling

After daily, metadata, and split storage, `rebuild_cache(connection, run_id,
ticker_types=config.ticker_types, batch_size=batch_size)` runs once. It captures
the input revision and publishes atomically, setting the run state to
`published`. `finish_run` is never called.

- `PublicationOutcomeUnknownError` is caught before generic writer errors and
  re-raised unchanged with no `fail_run`. Resolve it later with
  `resolve_publication`.
- Other `PostgresWriterError` values are treated as known failures. The run is
  marked failed with the safe `validation_error` code only when the original
  connection is still open, idle, and verified to hold the writer lock. If the
  connection is lost, the run is left `running`. No resolver logic is copied and
  no reconnect or resume is attempted.
- Daily completeness uses `incomplete_fetch`. Both `validation_error` and
  `incomplete_fetch` are members of the existing allowed run failure code set in
  `tickerlake.postgres.state`; `fail_run` rejects any other code and that
  rejection is contained so the original error is preserved.
- If `fail_run` cannot write, the original error is preserved.

Errors and messages never contain credentials, DSNs, API keys, or source records.

## Documented source validation limits

- A first fetch cannot prove completeness. Calendar closure and row counts do not
  establish that the vendor returned every expected record.
- Adjusted products use latest-known split contents, not historical as-of
  contents.
- An active-catalog shrink is quarantined by extraction rather than silently
  deleting prior state.
- Source adjudication for anomalous but decodable responses is deferred to
  operator policy; this lane records safe evidence and blocks publication.

## Out of scope

No CLI wiring, no routine update command, no automatic migrations, no DuckDB
path, no live Massive calls in tests, and no dependency or lockfile changes.
Integration validation runs with host Python pytest and a disposable PostgreSQL
18 Testcontainers container. Database-free tests do not require Docker. Never
use a local or live PostgreSQL service for tests.
