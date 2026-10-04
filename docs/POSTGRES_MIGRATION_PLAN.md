# PostgreSQL storage migration plan

> **Status (2026-10-04):** the seven-PR migration sequence below is complete. PRs 1-7 all merged to `main`. PostgreSQL is the only durable storage; `tickerlake backfill`/`update`/`info` all route through `src/tickerlake/postgres/`, the `compact` CLI subcommand is gone, and the legacy DuckDB code path is fully deleted. Follow-up cleanups (dependency removal, CI/Makefile/coderabbit sweeps, postgres-backed `info`, schema conventions) landed in PRs #61, #62, #67, and #68. Remaining work (schema collapse into `tickerlake.*`, radon rank-E refactor) is tracked separately.

## Purpose and boundaries (historical context)

This document started as an implementation plan. The storage change to PostgreSQL is complete; Massive remains the source and Polars remains the transformation engine. DuckDB was never the final compatibility backend.

The initial PostgreSQL database is populated by a fresh full backfill from Massive. This is explicitly intended; Massive usage is unlimited. Existing DuckDB files were used optionally for characterization or golden comparisons during development, but are not an input or prerequisite. The initial deployment is designed for a single machine and one tickerlake writer, with future Django reads. Approximately 99% of searches are against adjusted data, primarily a latest-session stock screener; this is not a claim that 99% of database activity is reads.

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
| SQL ownership | Tickerlake-owned versioned SQL migrations; future Django models are unmanaged and access is limited by database reader grants | Recommended |
| Data model | Raw daily cache, adjusted daily/weekly/monthly history, ticker reference, split contents, latest-session projection, ingest run and cache state | Recommended |
| Latest semantics | One row per eligible ticker for the latest validated shared session at or before a frozen publication target | Recommended |
| Transformations | Polars remains the canonical calculation layer | Agreed context |
| Types | PostgreSQL `real` for existing Float32 prices and metrics; `double precision` for split factors and volume; widen counts | Recommended; numeric acceptance needs confirmation |
| Publication | Load and validate staging data, then publish atomically in one transaction | Recommended |
| Indexing | Primary/unique keys first; add query-specific indexes only after representative benchmarks; add explicit indexes for FK lookup paths where needed | Recommended |
| Partitions/extensions | None initially | Recommended |
| Raw storage | PostgreSQL is the durable cache; bootstrap it with a fresh full Massive backfill | Recommended storage choice; fresh backfill is user requirement |

## Current behavior to preserve or consciously change

Source inspection confirms:

- `src/tickerlake/client.py` fetches grouped daily aggregates with `adjusted=False`, excludes OTC, and requests active ticker metadata only.
- `src/tickerlake/extract.py` turns per-date API errors into warnings and skips those dates; an empty response contributes no rows. Failed and empty refreshes already preserve the cached date because no replacement occurs. The migration must preserve this behavior and add atomic replacement plus populated-result validation. A returned DataFrame alone does not prove that every requested date was fetched successfully or completely.
- `src/tickerlake/transform.py` applies split factors to prices and inversely to volume; computes SMA-20/50/200, ATR-14, ATR%, ADR%, and volume SMA-20; creates Monday-labeled weekly bars and monthly bars labeled by each ticker's last observed trading day.
- Prices, volumes, metrics, and transactions currently use Float32/UInt32 types in relevant schemas. Polars calculations widen some intermediates, then cast results back. Period volume and transaction sums also cast back to these narrow types. The PostgreSQL design intentionally widens volume and count representations before aggregation; these outputs need not exactly match legacy overflow or Float32 rounding.
- `src/tickerlake/pipeline.py` currently rebuilds the consumer database from raw history, gets split and active ticker metadata on each run, filters bars to tickers in current metadata, and has a five-cached-date refresh window for revisions. Update does not fill older missing raw dates; a rebuild cannot repair those gaps without an explicit correction range.
- `src/tickerlake/load.py` writes complete DuckDB tables through Parquet intermediaries. It does not provide PostgreSQL-style multi-table publication semantics.

Preserve the current calculation definitions and fractional units. Deliberately improve failure safety, integer widths, stable ticker identity, and retention of acquired history for currently inactive tickers. Make that metadata-filter behavior change explicit and test it: retain stored bars, while separating catalog membership from current screener eligibility. Ticker symbols can be reused or changed; stable integer IDs do not solve historical identity ambiguity by themselves. Do not claim identity-safe backtesting until a symbol-history policy is chosen.

## Proposed architecture

Use one PostgreSQL database with two schemas:

- `ingest`: private to tickerlake ETL roles. Holds durable raw bars, the current validated `ingest.ticker_reference` and split contents, run/cache state, per-request `ingest.fetch_manifest` records, and disposable staging. Future readers should not query this schema.
- `market`: published tables intended for screeners and future readers. Tickerlake migrations own DDL and ETL owns DML. Future Django models are unmanaged and do not create/alter these tables.

Create distinct database roles where deployment permits: migration role for DDL, ETL role for DML and staging/publish operations, and reader role with SELECT-only grants for future Django access. Unmanaged Django models are not read-only by themselves; enforce reader access with database grants. No application should bootstrap PostgreSQL with a superuser account. Do not log DSNs, passwords, or API keys. Add `DATABASE_URL` (or a clearly documented equivalent) to configuration without printing secrets. psycopg 3 is the proposed Python driver; use parameterized SQL and PostgreSQL `COPY` into staging, then SQL upserts for publication. No pool is needed for the initial command-line ETL.

Keep transformations separate from database code. Use one tested Polars-frame-to-PostgreSQL-COPY interface and ordinary typed functions for bounded reads, staging, validation, and publication. Process ticker IDs in bounded batches, reading each ticker's complete retained history and applicable split contents; run Polars transformations per batch. Do not concatenate the full symbol universe. COPY each batch into staging, then release its frames; publication still occurs once. Avoid a generic repository abstraction. The ETL orchestration owns run state and transaction boundaries.

### Proposed tables and types

All keys, constraints, and names below are conceptual migration targets. Final SQL naming should be consistent and documented. Use `date` for exchange trading dates and `timestamptz` for operational timestamps only. Use `NOT NULL` for keys and observed core bar values; metric warmup results are nullable.

1. `market.ticker`
   - `ticker_id integer GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY`.
   - `symbol text NOT NULL UNIQUE`, `name text`, `ticker_type text`, `primary_exchange text`, `cik text`, nullable `active boolean`, `screen_eligible boolean NOT NULL`. Unknown activity means not screen-eligible, not inactive.
   - `cik` is not unique. Seed/resolve IDs from raw symbols as well as current metadata; metadata may omit delisted symbols. On reload/update, preserve existing IDs. New symbols receive new IDs. Never renumber all IDs during a consumer rebuild.
   - These IDs identify stable symbols, not provider securities; symbol reuse and renames remain ambiguous. Existing metadata changes are staged and become visible only at publication. `screen_eligible` follows an explicit metadata policy and is not history retention. Define whether adjusted history includes all retained raw symbols or only catalog symbols; active catalog membership is not exact trading membership.

2. `ingest.raw_daily`
   - `date date NOT NULL`, `ticker_id integer NOT NULL REFERENCES market.ticker(ticker_id)`, primary key `(date, ticker_id)` to make date refresh and scoped replacement natural.
   - OHLC `real NOT NULL`; VWAP `real NULL` because provider values may be absent; volume `double precision NOT NULL`; transactions `bigint NOT NULL`.
   - If any positive-volume input in an aggregation has missing VWAP, propagate null VWAP rather than producing a misleading partial estimate. Lock this behavior with a golden test.
   - Raw means unadjusted provider bars, consistent with `adjusted=False` today. Reject invalid/non-finite values before publishing. Add nonnegative constraints for volume and transaction count.

3. `ingest.split_event`
   - Internal identity primary key; ticker FK; execution date; split-from/to values as `real` if retaining provider values and constrained positive; cumulative adjustment factor `double precision` constrained positive; adjustment type text.
   - Do not assume `(ticker_id, execution_date)` is unique until the vendor contract and representative source records are checked. Establish a vendor event identifier or a verified natural key/deduplication rule. Keep this table private.

4. `market.adjusted_daily`, `market.adjusted_weekly`, `market.adjusted_monthly`
   - Each physically stores bars joined with its matching metrics. Primary key `(ticker_id, date)` and FK to ticker. Prices and VWAP `real`; volume `double precision`; transactions `bigint`.
   - `sma_20`, `sma_50`, `sma_200`, `atr_14`, `atr_pct`, `adr_pct` are nullable `real`; `volume_sma_20` nullable `double precision`. Period products include explicit `left_truncated` and `calendar_closed` booleans. `left_truncated` is true exactly when the retained collection lower bound is later than the first XNYS session of that period; it does not mean the ticker IPO'd or did not trade earlier. `calendar_closed` is true exactly when the period's final scheduled XNYS session is on or before the frozen target.
   - A price/metric row is one logical observation. Combining them avoids a join for the common screen and prevents independent bar/metric publication. It does not change calculations.
   - Weekly `date` is the Monday week-start label. Monthly `date` is the last observed trading date in the ticker-month. When a monthly last-trading-date key changes, remove the old key and publish the new one. These flags describe period shape, not vendor coverage or completeness.
   - Keep nullable warmups. Given current 10-year collection, monthly SMA-200 will normally be null; do not fabricate it.

5. `market.latest_daily`
    - One row per ticker, primary key `(ticker_id)`, FK to ticker, with the adjusted daily fields and metrics needed by the screener, optionally physically similar to `adjusted_daily`.
   - Rows must all refer to the same published session date, stored as `date` in each row or guaranteed by a single `market.publication_state` row. Prefer include `date` on each row for direct filtering/diagnostics; enforce same-session publication in ETL validation.
   - Only screen-eligible tickers with a valid row for that shared session are included. If the expected session is incomplete, do not publish it as current. Never combine stale rows from different sessions and call them today's screen.

6. `market.publication_state`
   - Singleton key/check constraint; `published_session date`, `published_at timestamptz`, publishing run ID, and optional counts. The public generation is the run ID, not a separate counter. Publish this state in the same transaction as derived data. Calculate current freshness in `info` from the published target session and timestamp; do not persist an aging freshness enum.

7. `ingest.run`
   - Run ID, frozen target session, requested collection bounds, run state, input revision, code/schema/transform versions, start/end timestamps, and published marker. The run ID is the public generation and is written atomically with public products. The run record is not a historical raw-data archive.

8. `ingest.cache_state` and validated references
   - Cache state stores retained raw collection bounds and current `input_revision`. Raw bar replacements and validated private ticker-reference/split-content changes advance that revision atomically. `ingest.ticker_reference` stores current validated symbol IDs and metadata, including nullable `active`; do not retain historical snapshot copies or IDs. Resolve durable symbol IDs for raw symbols; a new raw symbol may receive an identity placeholder with `active = NULL` and `screen_eligible = false`. When metadata arrives, stage its changes until publication.
   - `ingest.fetch_manifest` records each run/request/date, timestamps, status, counts and safe diagnostics. Outcomes are operational evidence, not retry checkpoints or a raw/reference archive. Staging batches are disposable; after connection loss abandon them. Recovery starts a new run against the latest validated cache. Do not promise exact historical replay or archive raw/reference versions.

Fetch outcomes distinguish transport/API failures and unparseable envelopes (failed) from decoded but invalid schema, returned date, values, or anomalous completeness (quarantined). Validated populated and validated successful-empty responses are separate outcomes. Diagnostics support scoped policy review and operator adjudication, not an interactive approval command.

### Numeric and date rules

Use PostgreSQL `real` for the existing Float32 price/indicator contract to avoid pretending that widening storage restores precision already lost by Float32 casts. Use `double precision` for split factors, volume, and volume averages; current volume is Float32 and adjusted volume can be fractional. Widen transaction counts and all period sums to bigint before summing, not after a narrow overflow. Confirm Polars schema/cast changes at the same time. Wider volume precision intentionally may differ from legacy Float32 results; compare against the defined mathematical result and document the expected difference rather than requiring exact legacy volume parity. Do not use `numeric` by default: it has cost and does not undo prior rounding. If users require exact decimal source prices, that is a separate precision decision requiring source fidelity evidence and parity criteria.

Proposed first-release default: adjusted products use latest-known adjustment, applying current validated split contents to retained history. Historical-as-of adjustment is a different product. Latest-known semantics and the absence of exact historical replay are proposed defaults requiring agreement before relying on adjusted history for historical analysis.

All ratios remain fractions: for example `0.04` means 4%. Validate finite values and bar invariants such as high not below open/close/low and low not above open/close/high. Use calendar `date` values for trading sessions; do not convert session labels through UTC timestamps. Preserve the existing exchange-calendar tz-naive timestamp convention. Calendar completion does not prove vendor coverage. Preserve string-valued calendar bounds and aware current-time handling, provider UTC timestamp decoding, and validation that returned dates match the requested session.

## Index and query plan

Start with primary keys, ticker symbol uniqueness, and required foreign keys. PostgreSQL does not automatically create an index on the referencing side of a foreign key, so add such indexes when lookup/delete behavior needs them. The history keys `(ticker_id, date)` support per-ticker ordered history. They do not automatically guarantee efficient date-only scans. PostgreSQL 18 B-tree skip scan may help some predicates depending on ticker cardinality and distribution, but it is not a substitute for measured date-oriented access.

The latest screener is expected to inspect a few thousand rows. Benchmark the unindexed or minimal-index table first. A B-tree cannot generally solve arbitrary column-to-column filters such as `close > sma_50`; multiple individual indexes do not automatically make such a query fast. Add an index only for a stable selective predicate/order pattern, possibly a partial or expression index if query semantics are stable. Do not build indexes for every metric. A date-leading index on history is conditional on demonstrated cross-sectional history queries. Consider BRIN only if date order correlates with physical heap order and range scans benefit; it is lossy and needs measurement. Index-only scans depend on visibility-map state and have storage/write costs. Do not partition initially: it adds operational and Django composite-key complexity, while uniqueness on partitioned tables must include the partition key.

Benchmark representative parameterized screener SQL (filters, sort, limit, ticker metadata joins), not synthetic single-column lookups. Record `EXPLAIN (ANALYZE, BUFFERS)` for test fixtures and realistic-sized loaded data. Compare indexes by latency, size, and ingestion/WAL cost.

## Data loading, correctness, and failure semantics

### Initial cutover

1. Run migrations and backfill validation with host Python pytest using the disposable PostgreSQL 18 cluster created by pytest-postgresql. Do not use or alter a local or live PostgreSQL service for plan validation.
2. Bootstrap by fetching the full configured history from Massive into PostgreSQL raw storage. Fetch ticker metadata and splits, seed stable ticker IDs from raw symbols as well as metadata, and record per-date outcomes. Massive usage is unlimited, so do not add a DuckDB import path or compatibility backend for bootstrap.
3. Transform raw bars and split data with Polars in bounded ticker batches. Validate keys, date coverage, null patterns, finite values, representative daily/weekly/monthly outputs, and source completeness checks before publishing.
4. Build all public period products and latest session in staging. Initial cutover targets the expected last-closed XNYS session at or before the persisted frozen target, independently of current freshness. Publish only if that target session is validated; otherwise preserve the prior publication. Calendar and vendor metadata do not prove completeness.
5. Run benchmark and application-reader validation, then switch CLI storage configuration deliberately. The PostgreSQL bootstrap and ongoing operation must not require DuckDB files.

DuckDB files, when present, may be consulted for optional characterization, but their absence or corruption does not block the intended fresh Massive backfill. Do not silently substitute partial local data for a failed full fetch.

### Routine update and publication

- Every writer acquires a session-level advisory lock on the same live connection used for writes, starting with the first raw-cache writes. Keep transactions short and do not keep one open during network fetches. Connection loss releases the lock and aborts the run; reacquiring a lock is not permission to continue from uncertain in-memory state. Introduce and test this writer-lock primitive with the PostgreSQL foundation; backfill, rebuild, and update all use it.
- Record per-request outcomes in `ingest.fetch_manifest`: transport/API failures and unparseable envelopes are failed; decoded invalid schema/date/value or suspicious completeness is quarantined; validated populated and successful-empty results are distinct. A suspicious row-count drop, omitted population, or anomalous bar distribution triggers quarantine, not proof of incompleteness. Heuristics include changes from prior/session peer counts, abrupt ticker-set shrinkage, missing expected dates, invalid core fields, and extreme distribution shifts. Preserve safe evidence for scoped policy review and operator adjudication. Apply the same checks to ticker metadata and split contents; apparent removals or empty results must not silently delete prior state. Active metadata is not exact trading membership. A successful-empty expected session needs explicit policy and adjudication; calendar and row count alone do not prove completeness.
- Fetch and validate before replacing raw rows. For a validated populated refresh, stage a date's rows, validate keys and expected coverage, then atomically replace that date's existing rows. If fetch, validation, or operator adjudication fails, preserve the old cache. A validated successful-empty response is a distinct outcome and must not erase prior rows by default. Existing metadata changes are staged for publication; identity placeholders may remain unknown and ineligible.
- Persist `ingest.run` bounds, target, state, input revision, and code/schema/transform versions. A new run reads the latest validated cache and checks its revision at publication. Raw bars and current validated ticker/split contents are the inputs; there are no versioned copies or exact historical replay. Staging is disposable, not resumable. Never mark a failed run as published. Raw/reference changes advance cache `input_revision` atomically with the change.
- Fetch split coverage aligned with retained raw history, not merely the recent update window. Use bounded ticker-ID batches, each with complete retained bar history and applicable splits. Resolve stable IDs in bounded reads; transform Polars frames; COPY each batch into staging, then release its frames without constructing a universe-wide frame. Use SQL upserts for keyed publication.
- Publish all adjusted daily/weekly/monthly rows, staged metadata changes, latest-session rows, publication state, and the run's published marker within one transaction. Progressively load complete validated scopes into disposable staging, but expose none as the new generation until commit. Use null-safe changed-row comparison. Delete obsolete derived keys only inside complete validated scopes, never globally from an incomplete or narrow-range build. Compare full-history results for the publication target; remove obsolete monthly keys within those validated scopes. Never truncate public tables for routine runs.
- A transaction makes the publication atomic, but multiple reader statements at `READ COMMITTED` can still observe different committed snapshots if a publication occurs between statements. Django or another multi-query reader needing a consistent response must use repeatable-read transaction semantics, or read/check a generation ID before and after the query set.
- If COMMIT outcome is uncertain after connection loss, inspect that run's durable published marker and run ID in publication state before retrying. The marker commits atomically with publication, so later publications do not obscure whether this run succeeded. After large loads, run `ANALYZE` and monitor autovacuum, dead tuples, WAL, free disk, and query plans. Remove DuckDB `compact`; do not add a maintenance command.

### Revision and recomputation rules

The current update refreshes a trailing five cached-date window and rebuilds all consumer data. Keep that correctness baseline initially. It does not fill older gaps: backfill accepts an explicit correction range, with ordinary validation still applied; there is no generic validation-bypass force option. A rebuild alone cannot repair missing raw history. Support a frozen run target/end date so a historical backfill can publish the expected last-closed XNYS session at or before that target independently of current freshness. If that session is not validated, preserve the previous publication. Later optimization may recompute only impacted data, but must account for:

- A changed or removed split can change every earlier adjusted price and inversely adjusted volume for that ticker. The proposed latest-known adjustment default requires agreement; historical-as-of adjustment is not promised.
- A corrected daily bar can affect at least subsequent 199 observations for SMA-200, prior-close-dependent ATR, rolling ADR and volume metrics, and any containing weekly/monthly aggregate and its metrics.
- Newly ineligible tickers are removed from `latest_daily` only; retain their adjusted daily/weekly/monthly history. Delete historical rows only for validated raw corrections or derived-key replacement, never solely because eligibility changed.
- A date correction should produce the same output as a full rebuild from the latest validated cache. Recovery starts a new run; exact replay of past cache/reference versions is not promised.

Keep full rebuild from raw as the repair path. Routine update must equal a full rebuild from the same latest validated cache; incremental derived-data equivalence is deferred. PostgreSQL is the durable raw cache. Retaining a second long-term Parquet source creates cross-store consistency and recovery complexity; reassess only with measured evidence.

## Source change map

Implement in small slices; these files are a map, not a mandate to rewrite them all at once.

| Area | Current location | Planned responsibility |
|---|---|---|
| Dependency and lockfile | `pyproject.toml`, `uv.lock` | Add psycopg 3 support; remove DuckDB only after no active storage/CLI path depends on it. |
| Configuration | `src/tickerlake/config.py` | Add DSN/environment validation with secret-safe representation; retain output-dir only if it has a defined artifact role. |
| Storage | `src/tickerlake/load.py` | Replace DuckDB readers/writers with PostgreSQL staging, COPY, validation, and transaction operations; separate migrations from routine DML. |
| Extraction | `src/tickerlake/extract.py`, `client.py` | Return per-date outcomes/manifests, not only a concatenated frame; distinguish failure from legitimate empty data. Keep `adjusted=False`; resolve inactive-symbol metadata policy without dropping retained raw history. |
| Transformations | `src/tickerlake/transform.py` | Keep functions pure; widen volume and transaction types safely; make outputs compatible with integer ticker IDs only at storage boundary or consistently change join key with parity tests. |
| Orchestration | `src/tickerlake/pipeline.py` | Add shared writer lock, durable raw workflow, staging validation, atomic publication, cache rebuild and recovery. Calculate freshness in `info` from target session and timestamp. |
| Calendar | `src/tickerlake/calendar.py` | Preserve trading-session date semantics; use it for expected closed session checks. |
| CLI | `src/tickerlake/__init__.py` | Provide migrate, backfill, update, rebuild and info; remove `compact` and misleading DuckDB-only options. Do not add retry/resume/publish/approve/snapshot commands or a validation-bypass force option. |
| SQL migrations | New `migrations/` or `src/tickerlake/migrations/` | Versioned, reviewed DDL with one owner and tested upgrade path. Avoid runtime `CREATE TABLE` bootstrap. |
| Tests and fixtures | `tests/` | Add disposable PostgreSQL integration coverage, migration fixtures, failure/recovery behavior, and source parity cases. |
| Docs | `README.md`, project `AGENTS.md` | Document setup, roles, migration, CLI, backup/restore, no-live-test policy, and Django ownership contract. |

Review `src/tickerlake/__init__.py`, `pyproject.toml`, and current tests at implementation time for exact CLI and dependency behavior. `load.py` presently includes DuckDB-specific compaction and database-info operations; remove compaction rather than adding a PostgreSQL maintenance command.

## Release sequence and acceptance gates

Implementation is divided into seven release PRs. Tests and fixtures ship with each behavior change. The scope column suggests coherent commits within each PR; it does not authorize commits.

| PR | Scope and suggested coherent commits | Acceptance gate | Status |
|---|---|---|---|
| 1. Contracts, extraction and fixtures | Define outcome, cache-generation and publication contracts; implement extraction outcomes and quarantine diagnostics; add deterministic source fixtures. | Failed, quarantined, populated and successful-empty results differ; bar and metadata/split anomalies are covered. | Merged (`feat(postgres): add foundation schema and storage contracts` #51) |
| 2. Numeric and period behavior | Widen aggregation counts/volume, set VWAP null propagation, add explicit deterministic period bounds and flags. | Polars golden/boundary tests cover overflow, VWAP, left truncation and calendar closure without claiming vendor completeness. | Merged via follow-up period-contract tests |
| 3. PostgreSQL foundation and raw cache | Add configuration, migrations, disposable PostgreSQL harness, raw cache, `ingest.ticker_reference`, `ingest.run`, `ingest.cache_state`, bounded reads, tested Polars COPY interface and the shared writer-lock primitive. No end-to-end bootstrap yet. | Migrations and raw/reference writes pass disposable integration tests; identity placeholders remain unknown/ineligible; cache revision advances atomically; all writers use the tested lock primitive. | Merged (`feat(postgres): add foundation schema and storage contracts` #51 and successors) |
| 4. Bounded rebuild and atomic publication | Transform complete retained per-ticker history and applicable splits in bounded Polars batches; stage output with COPY; publish via SQL upserts and scoped obsolete-key deletion. | Full rebuild matches golden outputs; failed publication preserves generation and staged metadata; deletions are limited to complete validated scopes; uncertain COMMIT is resolved from durable run marker. | Merged (`feat(postgres): publish bounded cache rebuilds atomically` #53) |
| 5. Fresh backfill and correction range | Fetch full Massive history, metadata and splits; support frozen target and explicit correction ranges with normal validation; use the shared writer lock. | Fresh bootstrap publishes the expected last-closed session at/before target independently of current freshness; if it is unvalidated preserve the prior publication; older gaps repair only when fetched. | Merged (`feat(postgres): add fresh backfill and correction ranges` #55) |
| 6. Routine update | Add five-cached-date refresh using the shared writer lock, short DB transactions, raw/reference revision updates, and full rebuild publication. | Failure/revision/split scenarios pass; routine update output equals a full rebuild from the same latest validated cache. | Merged (`feat(postgres): add routine update workflow` #57) |
| 7. CLI cutover, benchmark and restore | Cut CLI to migrate/backfill/update/rebuild/info, remove `compact` and durable DuckDB dependency, document benchmark and restore. | Disposable PostgreSQL cutover, restore/rebuild and benchmark gates pass; no final DuckDB fallback is required. | Merged (`chore(postgres): cut over CLI to postgres backend and drop legacy duckdb code` #60). `info` and `rebuild` CLI verbs remain as follow-ups; the postgres-backed `info` command is tracked in `DUCKDB_BEHAVIOR_PORT.md`. |

Dependencies: PR 1 defines signatures first; PRs 2 and 3 can then proceed independently. PRs 2 and 3 precede PR 4; PRs 1 and 4 precede PR 5, then PR 6, then PR 7. Tests belong with the behavior each PR adds. Django implementation/admin compatibility, incremental derived optimization, specialized indexes and deployment changes are separate deferred work. Preserve SQL schema ownership and SELECT-only reader grants for future Django readers without making Django a first-release gate.

## Test matrix

Run tests with host Python pytest. Database-free unit tests do not require a PostgreSQL server. PostgreSQL integration tests and migration checks use the disposable PostgreSQL 18 cluster created by pytest-postgresql. Never run tests against a local or live PostgreSQL service. Pipeline integration tests use real extraction, Polars transformations and calendar logic with that database; fake Massive only at its client boundary. Patch internal functions only for explicit fault injection. Preserve existing focused and golden behavioral protection while adding persistence assertions.

| Scenario | Required assertion |
|---|---|
| Migration from empty DB | Schema, grants, PK/FK/check constraints apply; repeat/upgrade behavior is deterministic. |
| Massive bootstrap | Fresh full backfill stores expected dates and symbols; no DuckDB input is required. |
| Stable identity | Repeated load preserves IDs; newly observed raw symbol gets an ID; inactive metadata does not erase history; CIK duplicates are allowed. |
| Raw replacement | Failed fetch, validation, or COPY preserves prior raw date; successful correction replaces exactly that date with no duplicate keys. |
| Fetch outcomes | Transport/API errors and unparseable envelopes fail; decoded invalid schema/date/value or anomalous results are quarantined; successful-empty and populated outcomes are distinct. Metadata and split changes receive the same review. |
| Completeness | Missing expected session, suspicious row-count drop, and omitted ticker do not silently clear published latest/history. |
| Atomic publication | Inject failure before commit and verify every old public table, state and existing metadata value remains visible; identity placeholders for previously unknown symbols may persist only as `active = NULL` and screen-ineligible. |
| Concurrency | Second writer cannot overlap; lock connection loss aborts; publication and run marker are atomic; uncertain COMMIT is resolved from that marker; reader checks observe one repeatable-read snapshot/generation. |
| Split corrections | Split factor applies to correct earlier bars; changed/removed split recomputation equals full rebuild. |
| Indicators | SMA/ATR/ADR/volume warmups remain null; values and fractional units match golden Polars baseline within accepted tolerance. |
| Revisions | Daily correction updates required indicator horizon, containing periods and latest projection; routine full rebuild equals update from the same cache generation. |
| Counts and volume | Large transaction sums do not overflow; fractional adjusted and period volume preserved. |
| Period keys | Weekly Monday labels; monthly last-observed-session labels; explicit deterministic bounds, `left_truncated` and `calendar_closed`; changed monthly key is removed only within complete validated scope. |
| Data validity | Reject duplicate keys, non-finite values, invalid OHLC relationships, orphan ticker IDs, and prohibited nulls before publication. |
| VWAP and period status | Missing VWAP with positive-volume inputs propagates null; left-truncated and calendar-closed periods are represented without claiming vendor completeness. |
| Rollback | Failed publication preserves prior PostgreSQL generation; restore or dedicated-database rebuild procedure works on a disposable PostgreSQL database without assuming DuckDB exists. |

Do not chase a coverage percentage in place of these guarantees. Tests should assert observable row state, generation, and output rather than private helper call counts.

## Benchmark and resource plan

Measure the PostgreSQL design against the agreed workload, recording machine CPU, RAM, storage, PostgreSQL configuration, table/index sizes, row counts, and cache state. If DuckDB is available, optional comparative measurements may be useful but are not a gate or migration dependency. Include:

- Latest-day screen with realistic filters, sort and limit; include ticker metadata joins and representative selective/non-selective predicate combinations.
- Per-ticker daily history, date-range cross-section, weekly/monthly history, and info/freshness queries.
- Full initial Massive backfill/COPY/rebuild duration, ordinary update duration, five-session revision refresh, forced older correction range, split correction rebuild, and retry.
- Peak process RSS for Polars plus database, PostgreSQL shared memory, total database/index/WAL/staging disk, backup size, and WAL growth.
- Query plan and buffers, index size, write amplification, dead tuples, and post-load/analyze behavior.

A provisional latest-screen target may be p95 under 100 ms on the deployment machine, but this is a user decision, not a scale guarantee. Agree on target hardware, cache state, request concurrency, row count, and filters before using it as an acceptance gate. PostgreSQL adds indexes, WAL, staging and backup overhead; compare total footprint rather than table bytes alone.

## Operations, rollback, and ownership

- Never mutate the local Quadlet service as part of implementation. Deployment/operator changes require a separate explicit task.
- Keep migration DDL changes versioned and forward-reviewed. Apply schema upgrades separately from data publication; migrations must not silently delete or rebuild market history.
- Back up PostgreSQL with a documented tested method and verify restore to a disposable container. No DuckDB backup or file retention is required for PostgreSQL correctness or rollback.
- Rollback means retaining the prior PostgreSQL publication, restoring a PostgreSQL backup, or rebuilding PostgreSQL in a dedicated database from Massive after review. DuckDB may be temporary development continuity before cutover, but is not a final fallback or required rollback dependency. Do not dual-write stores without an explicit consistency protocol.
- A failed publication should leave the previous generation and published timestamp intact, while `info` reports the failed run separately and calculates freshness from target session and timestamp. If a schema migration is incompatible, restore/rollback through a reviewed migration or rebuild a dedicated PostgreSQL database; do not drop unrelated databases. Massive redownload is permissible and is the intended bootstrap, but any recovery backfill must be deliberate and use the dedicated target database.
- PostgreSQL schema ownership belongs to tickerlake's SQL migration set. Future Django owns only its application tables. Django market models are unmanaged, and reader grants enforce read-only access. Changes to market tables require coordinated tickerlake migration and Django model updates, not Django auto-migration ownership.
- Remove DuckDB `compact`. Replace `info` with counts, date coverage, published session, last successful/failed run, relation/index/WAL metrics as available. Keep operations output free of secrets.

## References

Official PostgreSQL documentation to consult during implementation:

- PostgreSQL 18 numeric types: https://www.postgresql.org/docs/18/datatype-numeric.html
- PostgreSQL 18 multicolumn indexes: https://www.postgresql.org/docs/18/indexes-multicolumn.html
- PostgreSQL 18 partial indexes: https://www.postgresql.org/docs/18/indexes-partial.html
- PostgreSQL 18 index-only scans and visibility map: https://www.postgresql.org/docs/18/indexes-index-only-scans.html
- PostgreSQL 18 BRIN indexes: https://www.postgresql.org/docs/18/brin.html
- PostgreSQL 18 table partitioning and partitioned uniqueness: https://www.postgresql.org/docs/18/ddl-partitioning.html
- PostgreSQL 18 bulk population: https://www.postgresql.org/docs/18/populate.html
- PostgreSQL 18 `COPY`: https://www.postgresql.org/docs/18/sql-copy.html
- PostgreSQL 18 routine vacuuming: https://www.postgresql.org/docs/18/routine-vacuuming.html

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
10. Agree before relying on adjusted history: are latest-known adjustment semantics and no exact historical replay acceptable, or is historical-as-of adjustment required?
11. Which future reader roles/schema conventions will consume market tables? Django implementation and ORM/admin/composite-key compatibility are separate deferred work.
