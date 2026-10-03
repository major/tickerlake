# PostgreSQL storage migration plan

## Purpose and boundaries

This document is an implementation plan, not authorization to change the database service or perform a migration. PostgreSQL 18 is already running locally under Podman Quadlet. Leave that service and its files unchanged. Do not connect to it during implementation planning or tests. The intended storage change is to PostgreSQL while preserving Massive as the source and Polars as the transformation engine. DuckDB is not a required compatibility backend.

The initial PostgreSQL database is populated by a fresh full backfill from Massive. This is explicitly intended; Massive usage is unlimited. Do not build a DuckDB-to-PostgreSQL data migration, require local DuckDB files, or make cutover depend on them. Existing DuckDB files may be used optionally for characterization or golden comparisons, but are not an input or prerequisite. The initial deployment should be designed for a single machine and one tickerlake writer, with future Django reads. Approximately 99% of searches are against adjusted data, primarily a latest-session stock screener; this is not a claim that 99% of database activity is reads.

### In scope

- Replace durable raw and consumer storage with PostgreSQL.
- Retain the current raw, split-adjusted, weekly and monthly products, plus a purpose-built latest-session screening table.
- Preserve Polars transformations as functional, testable computation.
- Provide idempotent initial loading, repeatable updates, schema migrations, validation and recovery procedures.
- Make the market-data schema consumable by a future Django application without making Django the owner of tickerlake's schema.

### Explicit non-goals

- Do not modify, recreate, or reconfigure the running Quadlet service as part of this project.
- Do not require DuckDB data migration or compatibility storage. A fresh full Massive backfill is the intended PostgreSQL bootstrap.
- Do not introduce TimescaleDB, extensions, table partitioning, a connection pool, a web service, or a Django application in the first migration.
- Do not move market-data calculations into SQL or replace Polars with ORM code.
- Do not claim PostgreSQL will use less disk or RAM than DuckDB without measurements.
- Do not discard inactive-symbol history merely to make today's screener smaller.
- Do not promise a complete survivorship-free historical universe: the existing source metadata and filters do not establish that guarantee.

## Defaults and decisions still needed

The choices below are proposed defaults. Product-owner decisions are listed at the end; those decisions must not block implementing safe defaults but must be resolved before relying on historical screens/backtests.

| Topic | Recommended default | Status |
|---|---|---|
| Database | Existing local PostgreSQL 18; connect using a DSN from environment | Agreed context |
| SQL ownership | Tickerlake-owned versioned SQL migrations; Django uses unmanaged models for market tables | Recommended |
| Data model | Raw daily cache, adjusted daily/weekly/monthly history, ticker dimension, splits, latest-session projection, ingest state | Recommended |
| Latest semantics | One row per eligible ticker for one shared published market session, not each ticker's most recent non-null row | Recommended |
| Transformations | Polars remains the canonical calculation layer | Agreed context |
| Types | PostgreSQL `real` for existing Float32 prices and metrics; `double precision` for split factors; widen counts and volumes | Recommended; numeric acceptance needs confirmation |
| Publication | Load and validate staging data, then publish atomically in one transaction | Recommended |
| Indexing | Primary/unique keys first; add query-specific indexes only after representative benchmarks | Recommended |
| Partitions/extensions | None initially | Recommended |
| Raw storage | PostgreSQL is the durable cache; bootstrap it with a fresh full Massive backfill | Recommended storage choice; fresh backfill is user requirement |

## Current behavior to preserve or consciously change

Source inspection confirms:

- `src/tickerlake/client.py` fetches grouped daily aggregates with `adjusted=False`, excludes OTC, and requests active ticker metadata only.
- `src/tickerlake/extract.py` turns per-date API errors into warnings and skips those dates; an empty response contributes no rows. Therefore a returned DataFrame alone does not prove that every requested date was fetched successfully or completely.
- `src/tickerlake/transform.py` applies split factors to prices and inversely to volume; computes SMA-20/50/200, ATR-14, ATR%, ADR%, and volume SMA-20; creates Monday-labeled weekly bars and monthly bars labeled by each ticker's last observed trading day.
- Prices, volumes, metrics, and transactions currently use Float32/UInt32 types in relevant schemas. Polars calculations widen some intermediates, then cast results back. Period volume and transaction sums also cast back to these narrow types. The PostgreSQL design intentionally widens volume and count representations before aggregation; these outputs need not exactly match legacy overflow or Float32 rounding.
- `src/tickerlake/pipeline.py` currently rebuilds the consumer database from raw history, gets split and active ticker metadata on each run, filters bars to tickers in current metadata, and has a five-cached-date refresh window for revisions.
- `src/tickerlake/load.py` writes complete DuckDB tables through Parquet intermediaries. It does not provide PostgreSQL-style multi-table publication semantics.

Preserve the current calculation definitions and fractional units. Deliberately improve failure safety, integer widths, stable ticker identity, and retention of acquired history for currently inactive tickers. Make that metadata-filter behavior change explicit and test it: retain stored bars, while separating catalog membership from current screener eligibility. Ticker symbols can be reused or changed; stable integer IDs do not solve historical identity ambiguity by themselves. Do not claim identity-safe backtesting until a symbol-history policy is chosen.

## Proposed architecture

Use one PostgreSQL database with two schemas:

- `ingest`: private to tickerlake ETL roles. Holds durable raw bars, split events, load manifests, and any staging tables. Django should not query this schema.
- `market`: published tables intended for screeners and future readers. Tickerlake migrations own DDL and ETL owns DML. Django maps these tables as unmanaged models and does not create/alter them.

Create distinct database roles where deployment permits: migration role for DDL, ETL role for DML and staging/publish operations, and reader role for read-only access. No application should bootstrap PostgreSQL with a superuser account. Do not log DSNs, passwords, or API keys. Add `DATABASE_URL` (or a clearly documented equivalent) to configuration without printing secrets. psycopg 3 is the proposed Python driver; use parameterized SQL and PostgreSQL `COPY` for bulk staging. No pool is needed for the initial command-line ETL.

Keep transformations separate from database code. A storage boundary should accept validated Polars frames or Arrow-compatible batches, and have explicit operations for reading the required raw history, staging, validation, and publication. Avoid wrapping every existing function in a generic repository abstraction. The ETL orchestration owns the run state and transaction boundaries.

### Proposed tables and types

All keys, constraints, and names below are conceptual migration targets. Final SQL naming should be consistent and documented. Use `date` for exchange trading dates and `timestamptz` for operational timestamps only. Use `NOT NULL` for keys and observed core bar values; metric warmup results are nullable.

1. `market.ticker`
   - `ticker_id integer GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY` (or `GENERATED ALWAYS` if the loader explicitly uses `OVERRIDING SYSTEM VALUE` only when necessary).
   - `symbol text NOT NULL UNIQUE`, `name text`, `ticker_type text`, `primary_exchange text`, `cik text`, `active boolean NOT NULL`, `screen_eligible boolean NOT NULL`.
   - `cik` is not unique. Seed/resolve IDs from raw symbols as well as current metadata; metadata may omit delisted symbols. On reload/update, preserve existing IDs. New symbols receive new IDs. Never renumber all IDs during a consumer rebuild.
   - Eligibility is a published snapshot associated with the latest publication. Preserve historical rows even when `active` changes. `screen_eligible` must be derived from an explicitly defined metadata policy, not confused with history retention.

2. `ingest.raw_daily`
   - `date date NOT NULL`, `ticker_id integer NOT NULL REFERENCES market.ticker(ticker_id)`, primary key `(date, ticker_id)` to make date refresh and scoped replacement natural.
   - OHLC `real NOT NULL`; VWAP `real NULL` because provider values may be absent; volume `double precision NOT NULL`; transactions `bigint NOT NULL`.
   - Raw means unadjusted provider bars, consistent with `adjusted=False` today. Reject invalid/non-finite values before publishing. Add nonnegative constraints for volume and transaction count.

3. `ingest.split_event`
   - Internal identity primary key; ticker FK; execution date; split-from/to values as `real` if retaining provider values and constrained positive; cumulative adjustment factor `double precision` constrained positive; adjustment type text.
   - Do not assume `(ticker_id, execution_date)` is unique until the vendor contract and representative source records are checked. Establish a vendor event identifier or a verified natural key/deduplication rule. Keep this table private.

4. `market.adjusted_daily`, `market.adjusted_weekly`, `market.adjusted_monthly`
   - Each physically stores bars joined with its matching metrics. Primary key `(ticker_id, date)` and FK to ticker. Prices and VWAP `real`; volume `double precision`; transactions `bigint`.
   - `sma_20`, `sma_50`, `sma_200`, `atr_14`, `atr_pct`, `adr_pct` are nullable `real`; `volume_sma_20` nullable `double precision`.
   - A price/metric row is one logical observation. Combining them avoids a join for the common screen and prevents independent bar/metric publication. It does not change calculations.
   - Weekly `date` is the Monday week-start label. Monthly `date` is the last observed trading date in the ticker-month. When a monthly last-trading-date key changes, remove the old key and publish the new one. Label current/partial periods clearly; do not imply a partial period is complete.
   - Keep nullable warmups. Given current 10-year collection, monthly SMA-200 will normally be null; do not fabricate it.

5. `market.latest_daily`
   - One row per ticker, primary key `(ticker_id)`, FK to ticker, with the adjusted daily fields and metrics needed by the screener, optionally physically similar to `adjusted_daily`.
   - Rows must all refer to the same published session date, stored as `date` in each row or guaranteed by a single `market.publication_state` row. Prefer include `date` on each row for direct filtering/diagnostics; enforce same-session publication in ETL validation.
   - Only screen-eligible tickers with a valid row for that shared session are included. If the expected session is incomplete, do not publish it as current. Never combine stale rows from different sessions and call them today's screen.

6. `market.publication_state`
   - Singleton key/check constraint; `published_session date`, `published_at timestamptz`, run identifier, completeness/freshness status, and optional counts. Publish this state in the same transaction as derived data.

7. `ingest.fetch_manifest`
   - Run ID, requested date, start/end timestamps, status, row count, error detail safe for logs, provider request metadata where permitted, and completeness validation result. Record successful empty responses distinctly from failures. This manifest is an operational audit, not proof of vendor completeness.

### Numeric and date rules

Use PostgreSQL `real` for the existing Float32 price/indicator contract to avoid pretending that widening storage restores precision already lost by Float32 casts. Use `double precision` for split factors, volume, and volume averages; current volume is Float32 and adjusted volume can be fractional. Widen transaction counts and all period sums to bigint before summing, not after a narrow overflow. Confirm Polars schema/cast changes at the same time. Wider volume precision intentionally may differ from legacy Float32 results; compare against the defined mathematical result and document the expected difference rather than requiring exact legacy volume parity. Do not use `numeric` by default: it has cost and does not undo prior rounding. If users require exact decimal source prices, that is a separate precision decision requiring source fidelity evidence and parity criteria.

All ratios remain fractions: for example `0.04` means 4%. Validate finite values and bar invariants such as high not below open/close/low and low not above open/close/high. Use calendar `date` values for trading sessions; do not convert session labels through UTC timestamps. Preserve the existing exchange-calendar tz-naive timestamp convention.

## Index and query plan

Start with primary keys, ticker symbol uniqueness, and required foreign keys. The history keys `(ticker_id, date)` support per-ticker ordered history. They do not automatically guarantee efficient date-only scans. PostgreSQL 18 B-tree skip scan may help some predicates depending on ticker cardinality and distribution, but it is not a substitute for measured date-oriented access.

The latest screener is expected to inspect a few thousand rows. Benchmark the unindexed or minimal-index table first. A B-tree cannot generally solve arbitrary column-to-column filters such as `close > sma_50`; multiple individual indexes do not automatically make such a query fast. Add an index only for a stable selective predicate/order pattern, possibly a partial or expression index if query semantics are stable. Do not build indexes for every metric. A date-leading index on history is conditional on demonstrated cross-sectional history queries. Consider BRIN only if date order correlates with physical heap order and range scans benefit; it is lossy and needs measurement. Index-only scans depend on visibility-map state and have storage/write costs. Do not partition initially: it adds operational and Django composite-key complexity, while uniqueness on partitioned tables must include the partition key.

Benchmark representative parameterized screener SQL (filters, sort, limit, ticker metadata joins), not synthetic single-column lookups. Record `EXPLAIN (ANALYZE, BUFFERS)` for test fixtures and realistic-sized loaded data. Compare indexes by latency, size, and ingestion/WAL cost.

## Data loading, correctness, and failure semantics

### Initial cutover

1. Run migrations and backfill validation against a dedicated disposable PostgreSQL test database in a temporary Podman container. Do not use or alter the already-running local service for plan validation.
2. Bootstrap by fetching the full configured history from Massive into PostgreSQL raw storage. Fetch ticker metadata and splits, seed stable ticker IDs from raw symbols as well as metadata, and record per-date outcomes. Massive usage is unlimited, so do not add a DuckDB import path or compatibility backend for bootstrap.
3. Transform raw bars and split data with Polars. Validate keys, date coverage, null patterns, finite values, representative daily/weekly/monthly outputs, and source completeness checks before publishing.
4. Build all public period products and latest session in staging. Publish only after checks pass. Compare latest table date to the expected last closed XNYS session and actual source coverage.
5. Run benchmark and application-reader validation, then switch CLI storage configuration deliberately. The PostgreSQL bootstrap and ongoing operation must not require DuckDB files.

DuckDB files, when present, may be consulted for optional characterization, but their absence or corruption does not block the intended fresh Massive backfill. Do not silently substitute partial local data for a failed full fetch.

### Routine update and publication

- Acquire a PostgreSQL advisory lock for the tickerlake writer for the entire run. A failed process releases the lock on connection loss. This prevents overlapping fetch/publish runs.
- Record each requested session in a manifest. A network error, malformed response, or suspicious row-count drop is a failed/incomplete fetch, not an empty successful date. The current behavior of warning and skipping must not silently drive deletion or publication.
- Fetch and validate before replacing raw rows. For a successful refresh, stage a date's rows, validate its keys and expected coverage, then replace that date's existing rows in a transaction. If fetch/validation fails, preserve the old cache. For a successful empty result on an expected market session, require explicit policy and strong completeness evidence; default is to stop and retain prior published data.
- Commit validated raw cache and its manifest before rebuilding public views if useful for recovery. Track a run ID and source-input watermark so a later publication can be retried from the durable raw cache. Never mark a failed run as published.
- Bulk load via `COPY` to staging, validate columns, keys, finite values, constraints, expected date coverage, and transformation output before publication.
- Publish all adjusted daily/weekly/monthly rows, ticker eligibility metadata snapshot, latest-session rows, and publication state within one transaction. Stage ticker metadata and eligibility changes alongside derived data. Raw symbol identity inserts may happen earlier to satisfy FKs, but `active` and `screen_eligible` changes become visible only in the publication transaction. Use keyed upserts/update-only-when-values-changed to avoid needless WAL and table bloat. Scope historical deletion to validated raw corrections and obsolete derived period keys; eligibility changes only remove rows from latest projection. Never truncate public tables for routine runs.
- A transaction makes the publication atomic, but multiple reader statements at `READ COMMITTED` can still observe different committed snapshots if a publication occurs between statements. Django or another multi-query reader needing a consistent response must use repeatable-read transaction semantics, or read/check a generation ID before and after the query set.
- After large loads, run `ANALYZE` and monitor autovacuum, dead tuples, WAL, free disk, and query plans. Do not translate DuckDB `compact` into `VACUUM FULL`; it takes locks and requires extra disk. Ordinary vacuum/analyze policy is PostgreSQL operations work.

### Revision and recomputation rules

The current update refreshes a trailing five cached-date window and rebuilds all consumer data. Keep that correctness baseline initially. Later optimization may recompute only impacted data, but must account for:

- A changed or removed split can change every earlier adjusted price and inversely adjusted volume for that ticker.
- A corrected daily bar can affect at least subsequent 199 observations for SMA-200, prior-close-dependent ATR, rolling ADR and volume metrics, and any containing weekly/monthly aggregate and its metrics.
- Newly ineligible tickers are removed from `latest_daily` only; retain their adjusted daily/weekly/monthly history. Delete historical rows only for validated raw corrections or derived-key replacement, never solely because eligibility changed.
- A date correction should be retried idempotently and produce the same output as a full rebuild from the same raw inputs.

Keep full rebuild from raw as a repair path and use equivalence tests before enabling incremental derived-data updates. PostgreSQL should initially be the durable raw cache. Retaining a second long-term Parquet source creates cross-store consistency, backups, and recovery complexity; reassess only with measured disk, RAM, or throughput evidence.

## Source change map

Implement in small slices; these files are a map, not a mandate to rewrite them all at once.

| Area | Current location | Planned responsibility |
|---|---|---|
| Dependency and lockfile | `pyproject.toml`, `uv.lock` | Add psycopg 3 support; remove DuckDB only after no active storage/CLI path depends on it. |
| Configuration | `src/tickerlake/config.py` | Add DSN/environment validation with secret-safe representation; retain output-dir only if it has a defined artifact role. |
| Storage | `src/tickerlake/load.py` | Replace DuckDB readers/writers with PostgreSQL staging, COPY, validation, and transaction operations; separate migrations from routine DML. |
| Extraction | `src/tickerlake/extract.py`, `client.py` | Return per-date outcomes/manifests, not only a concatenated frame; distinguish failure from legitimate empty data. Keep `adjusted=False`; resolve inactive-symbol metadata policy without dropping retained raw history. |
| Transformations | `src/tickerlake/transform.py` | Keep functions pure; widen volume and transaction types safely; make outputs compatible with integer ticker IDs only at storage boundary or consistently change join key with parity tests. |
| Orchestration | `src/tickerlake/pipeline.py` | Add advisory lock, durable raw workflow, staging validation, atomic publication, freshness state, and rebuild/retry commands. |
| Calendar | `src/tickerlake/calendar.py` | Preserve trading-session date semantics; use it for expected closed session checks. |
| CLI | `src/tickerlake/__init__.py` | Add explicit schema migration/status behavior, adapt backfill/update/info, and retire or redefine `compact` and `output-dir` rather than leaving misleading DuckDB semantics. |
| SQL migrations | New `migrations/` or `src/tickerlake/migrations/` | Versioned, reviewed DDL with one owner and tested upgrade path. Avoid runtime `CREATE TABLE` bootstrap. |
| Tests and fixtures | `tests/` | Add isolated PostgreSQL integration coverage, migration fixtures, failure/retry behavior, and source parity cases. |
| Docs | `README.md`, project `AGENTS.md` | Document setup, roles, migration, CLI, backup/restore, no-live-test policy, and Django ownership contract. |

Review `src/tickerlake/__init__.py`, `pyproject.toml`, and current tests at implementation time for exact CLI and dependency behavior. `load.py` presently includes DuckDB-specific compaction and database-info operations that need explicit replacements or removal.

## Phases and acceptance gates

### Phase 0: contracts and characterization

- Optionally inspect available DuckDB schemas, row counts, date ranges, symbols, nulls, duplicates, and checksums for characterization. This is not a deployment prerequisite or migration input.
- Capture representative golden daily, weekly, monthly bars and metrics, including splits, warmup nulls, inactive symbols, and partial periods. Prefer deterministic fixtures where DuckDB files are unavailable.
- Document the source completeness limitation and decide what a successful-empty market day means.

**Gate:** Golden cases and unresolved identity/source-completeness assumptions are documented. No DuckDB files are required.

### Phase 1: schema and storage boundary

- Add versioned SQL migrations, roles/grants guidance, connection configuration, and isolated PostgreSQL integration test setup.
- Create schemas/tables/constraints with no extension or partition dependencies.
- Implement bulk staging and repository operations without changing CLI cutover.

**Gate:** Migrations apply cleanly to an empty disposable PostgreSQL 18 database and upgrade path is repeatable; key/FK/null constraints and rollback behavior are tested.

### Phase 2: fresh Massive backfill and full rebuild

- Fetch the full configured history, ticker metadata, and splits from Massive directly into PostgreSQL raw storage; do not implement a DuckDB import path or compatibility backend.
- Recompute adjusted products via Polars and publish transactionally.
- Compare against optional DuckDB/golden fixtures where available and inspect latest shared-session membership. Differences due to widened volume/count behavior are documented and reviewed, not treated as unexplained parity failures.

**Gate:** No unexplained missing/extra keys; source coverage and numeric expectations meet agreed rules; sums cannot overflow; a failed publication leaves the previous published generation intact; rerunning the same validated input is idempotent.

### Phase 3: correctness-first routine updates

- Implement manifests, advisory locking, fetch-then-validate raw date replacement, staging, atomic publication, and retry support.
- Start with full derived rebuilds and prove revision, split, and correction behavior.

**Gate:** Failure, omission, overlap, retry, and revision scenarios pass in isolated tests; no partial public state; update output matches full rebuild.

### Phase 4: screen read path and cutover

- Implement latest-session screener query and info/freshness status.
- Benchmark with representative data and screen predicates. Add only justified indexes.
- Switch CLI/readers to PostgreSQL. No DuckDB fallback is required.

**Gate:** Query correctness, latency, resource and disk budget meet agreed targets; published session freshness is explicit; PostgreSQL restore/rebuild rollback has been rehearsed on disposable data.

### Phase 5: incremental derived-data optimization

- Optimize impacted-ticker/period recomputation only after the correctness-first full rebuild is proven.
- Compare optimized output with full rebuild for revisions, split changes, removed observations, monthly key changes, and eligibility changes.

**Gate:** Full-rebuild equivalence passes across correction scenarios; measurable runtime or resource improvement justifies added complexity.

### Phase 6: Django integration

- Django app maps `market` tables as unmanaged models; migrations do not own or mutate those tables. App-owned tables remain in their own schema/app migrations.
- Confirm Django version and composite-PK support before mapping history tables. Prefer supported ORM/query patterns; do not add a redundant bigint surrogate key plus unique `(ticker_id, date)` index to every history table without a demonstrated Django or consumer requirement.
- Latest table's ticker PK can map as a one-to-one/primary-key relation to ticker; history uses `(ticker_id, date)` and must be tested for filtering, admin, foreign keys, and migration-state behavior under the chosen Django version.
- Django should read latest table for screening, avoid writing tickerlake-owned data, and use repeatable-read or generation checking when a multi-query response requires a consistent snapshot.

**Gate:** Django migration generation produces no DDL for market-owned tables; model/query behavior is tested against the documented schema and target Django release.

## Test matrix

All tests, including database-free unit tests, integration tests, migration checks, and benchmark database runs, must execute in a temporary Podman container with disposable data. Never run tests against the already-running local PostgreSQL service or any live database. No live Massive requests in tests; fake the API boundary.

| Scenario | Required assertion |
|---|---|
| Migration from empty DB | Schema, grants, PK/FK/check constraints apply; repeat/upgrade behavior is deterministic. |
| Massive bootstrap | Fresh full backfill stores expected dates and symbols; no DuckDB input or migration code is required. |
| Stable identity | Repeated load preserves IDs; newly observed raw symbol gets an ID; inactive metadata does not erase history; CIK duplicates are allowed. |
| Raw replacement | Failed fetch, validation, or COPY preserves prior raw date; successful correction replaces exactly that date with no duplicate keys. |
| Fetch manifest | API error, malformed response, successful empty response, and validated populated response remain distinguishable. |
| Completeness | Missing expected session, suspicious row-count drop, and omitted ticker do not silently clear published latest/history. |
| Atomic publication | Inject failure before commit and verify every old public table and state remains visible; successful commit switches all products together. |
| Concurrency | Second writer cannot overlap; reader transaction observes one generation; retry after connection loss is idempotent. |
| Split corrections | Split factor applies to correct earlier bars; changed/removed split recomputation equals full rebuild. |
| Indicators | SMA/ATR/ADR/volume warmups remain null; values and fractional units match golden Polars baseline within accepted tolerance. |
| Revisions | Daily correction updates required indicator horizon, containing periods, latest projection; full rebuild equals optimized update. |
| Counts and volume | Large transaction sums do not overflow; fractional adjusted and period volume preserved. |
| Period keys | Weekly Monday labels; monthly last-observed-session labels; changed monthly key removes old key; partial-period status is clear. |
| Data validity | Reject duplicate keys, non-finite values, invalid OHLC relationships, orphan ticker IDs, and prohibited nulls before publication. |
| Django ownership | Unmanaged market models cause no generated DDL; latest relation and history composite-key access work in target version. |
| Rollback | Failed publication preserves prior PostgreSQL generation; restore or dedicated-database rebuild procedure works on disposable instance without assuming DuckDB exists. |

Do not chase a coverage percentage in place of these guarantees. Tests should assert observable row state, generation, and output rather than private helper call counts.

## Benchmark and resource plan

Measure the PostgreSQL design against the agreed workload, recording machine CPU, RAM, storage, PostgreSQL configuration, table/index sizes, row counts, and cache state. If DuckDB is available, optional comparative measurements may be useful but are not a gate or migration dependency. Include:

- Latest-day screen with realistic filters, sort and limit; include ticker metadata joins and representative selective/non-selective predicate combinations.
- Per-ticker daily history, date-range cross-section, weekly/monthly history, and info/freshness queries.
- Full initial Massive backfill/COPY/rebuild duration, ordinary update duration, five-session revision refresh, split correction rebuild, and retry.
- Peak process RSS for Polars plus database, PostgreSQL shared memory, total database/index/WAL/staging disk, backup size, and WAL growth.
- Query plan and buffers, index size, write amplification, dead tuples, and post-load/analyze behavior.

A provisional latest-screen target may be p95 under 100 ms on the deployment machine, but this is a user decision, not a scale guarantee. Agree on target hardware, cache state, request concurrency, row count, and filters before using it as an acceptance gate. PostgreSQL adds indexes, WAL, staging and backup overhead; compare total footprint rather than table bytes alone.

## Operations, rollback, and ownership

- Never mutate the local Quadlet service as part of implementation. Deployment/operator changes require a separate explicit task.
- Keep migration DDL changes versioned and forward-reviewed. Apply schema upgrades separately from data publication; migrations must not silently delete or rebuild market history.
- Back up PostgreSQL with a documented tested method and verify restore to a disposable container. No DuckDB backup or file retention is required for PostgreSQL correctness or rollback.
- Rollback means retaining the prior PostgreSQL publication, restoring a PostgreSQL backup, or rebuilding PostgreSQL in a dedicated database from Massive after review. If a prior DuckDB copy happens to exist, it may be an optional temporary reader fallback, not a required rollback dependency. Do not dual-write stores without an explicit consistency protocol.
- A failed publication should leave the previous generation and freshness timestamp intact, while status reports the failed run separately. If a schema migration is incompatible, restore/rollback through a reviewed migration or rebuild a dedicated PostgreSQL database; do not drop unrelated databases. Massive redownload is permissible and is the intended bootstrap, but any recovery backfill must be deliberate and use the dedicated target database.
- PostgreSQL schema ownership belongs to tickerlake's SQL migration set. Future Django owns only its application tables. Django market models are unmanaged, read-only by policy, and tested against an explicit schema version. Changes to market tables require coordinated tickerlake migration and Django model updates, not Django auto-migration ownership.
- Replace DuckDB `compact` with meaningful PostgreSQL status/maintenance guidance or remove the command. Replace `info` with counts, date coverage, published session, last successful/failed run, relation/index/WAL metrics as available. Keep operations output free of secrets.

## References

Official documentation to consult while implementing against the deployed PostgreSQL and Django versions:

- PostgreSQL 18 numeric types: https://www.postgresql.org/docs/18/datatype-numeric.html
- PostgreSQL 18 multicolumn indexes: https://www.postgresql.org/docs/18/indexes-multicolumn.html
- PostgreSQL 18 partial indexes: https://www.postgresql.org/docs/18/indexes-partial.html
- PostgreSQL 18 index-only scans and visibility map: https://www.postgresql.org/docs/18/indexes-index-only-scans.html
- PostgreSQL 18 BRIN indexes: https://www.postgresql.org/docs/18/brin.html
- PostgreSQL 18 table partitioning and partitioned uniqueness: https://www.postgresql.org/docs/18/ddl-partitioning.html
- PostgreSQL 18 bulk population: https://www.postgresql.org/docs/18/populate.html
- PostgreSQL 18 `COPY`: https://www.postgresql.org/docs/18/sql-copy.html
- PostgreSQL 18 routine vacuuming: https://www.postgresql.org/docs/18/routine-vacuuming.html
- Django composite primary keys: https://docs.djangoproject.com/en/6.0/topics/composite-primary-key/

Check the selected Django release's current composite-primary-key limitations before committing to ORM access patterns. In particular, do not assume all ForeignKey, admin, or migration operations support composite-key models equally. This is a reason to keep the latest one-row-per-ticker projection simple and to avoid speculative surrogate-key duplication in all history tables.

## Open questions requiring product/operator answers

1. What RAM, CPU, and disk budget and benchmark machine define acceptable operation?
2. How many symbols/rows and years of raw and adjusted history must be retained? Is deletion ever permitted?
3. What exact screen eligibility rules and ticker types should define the published latest universe? Should newly inactive/delisted symbols remain queryable historically, and how should active metadata be refreshed?
4. Which screener predicates, ordering, limits, concurrency, and p95 target define success? Is 100 ms a suitable provisional target?
5. Is Float32-compatible price precision acceptable, or is exact/source decimal fidelity required? What comparison tolerance is approved?
6. What evidence constitutes a complete vendor response for a date, and how should an unexpectedly successful empty response be adjudicated?
7. How much publication delay after a market close is acceptable, and how should holidays, early closes, and provider delays affect latest freshness status?
8. What is the policy for symbol reuse, ticker renames, and historical ticker identity in backtests? Is provider security ID history available and required?
9. What is the rollback retention window, backup frequency, recovery-point objective, and recovery-time objective?
10. Which Django version and deployment roles/schema conventions will consume market tables, and which queries must work through the ORM versus raw SQL?
