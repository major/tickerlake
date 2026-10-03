# PostgreSQL publication contract

## Scope

Chunk 4 provides a cache-only rebuild API through
`rebuild_cache(connection, run_id, *, ticker_types, batch_size=100)`. The
caller must pass an existing writer connection that holds the shared writer
lock. This API does not fetch data or bootstrap a DuckDB database. It does not
add a CLI command. CLI integration and the remaining work in chunks 5 through
7 are deferred.

The caller must supply a frozen closed XNYS session and validated split
coverage for the complete retained history. Fetching those inputs and resolving
the target session belong to chunks 5 and 6.

## Inputs and products

The rebuild pages durable ticker identities by `ticker_id`. Each page contains
at most `batch_size` identities, with columns `ticker_id` (`Int32`) and
`symbol` (`String`). Raw bars and split history are read in full for those
identities, then transformed and staged before the next page is processed.
Memory use is bounded by a page and its complete retained history, not by a
concatenation of the whole ticker universe.

Each page produces a `ProductBatch` with daily, weekly, and monthly frames.
The frames map symbols to durable ticker IDs and contain all retained history
for each identity. Inactive and unknown identities are not excluded. Cached
bars after the target session are retained. The target session controls period
closure flags and the exact shared `latest_daily` selection; it is not a bar
filter or a truncation date.

## Evidence and revisions

Publication requires accepted populated raw-session evidence for the exact
target session. A migration or rebuild must not invent accepted raw-session
markers for dates that lack that evidence.

An accepted date's revision can be older than the global cache revision after
other dates or references change. Rebuild captures its inputs once, before
reading batches. The run's captured input revision must match the current cache
revision when publication is prepared. A revision mismatch prevents publishing
a build from stale inputs.

## Staging and publication

Staging is disposable and run-scoped. It is not a resume point. Publication
replaces the adjusted daily, weekly, and monthly products as one atomic unit,
including metadata, history, `latest_daily`, publication state, and the run's
terminal state. Data that no longer appears in the rebuilt output, including
obsolete monthly rows, is removed as part of that replacement.

If the client loses the connection while `COMMIT` is in progress, the outcome
is uncertain. Resolve it with `resolve_publication` after acquiring a fresh
shared writer lock, and inspect the durable publication marker. The marker may
show that the run published even if a later run has already superseded it.
Lock contention or connection loss that leaves the outcome unresolved is not
proof of failure. Do not mark the run failed until the publication outcome is
known.

## Validation

PostgreSQL integration validation uses only the disposable isolated harness in
`scripts/test-postgres.sh`. Do not use a host test database or a live service.
