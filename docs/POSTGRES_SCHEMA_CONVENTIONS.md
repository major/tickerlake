# PostgreSQL schema conventions

> **Status (2026-10-04):** the conventions in this document describe the schemas, tables, and identifiers currently shipped by `src/tickerlake/postgres/`. Migrations applied by `postgres.migrations` create two schemas — `ingest` (private ETL state) and `market` (public products intended for future readers and the future Django application). PR C will collapse both schemas into a single `tickerlake.*` schema as a follow-up; the rest of this document applies either way.

## Schemas

tickerlake owns two PostgreSQL schemas in the deployed database:

- **`ingest`** — private to tickerlake ETL roles. Holds durable raw bars, the current validated ticker reference and split contents, the run ledger, the cache state singleton, and per-request fetch-manifest records. Future readers should not query this schema.
- **`market`** — published tables intended for screeners and future readers. Tickerlake migrations own DDL; ETL owns DML. Future Django models are unmanaged and do not create or alter these tables. Reader access is enforced through database grants, not Django.

Migration DDL is the single source of truth for schema and table names. Application code, tests, and future reader code refer to tables by their full `<schema>.<table>` name.

### Roles and grants

Three distinct database roles should exist where the deployment permits:

- **Migration role** — DDL. Used by the `migrate` workflow; never used at request time.
- **ETL role** — DML and staging / publish operations. Used by `tickerlake backfill` and `tickerlake update`. Acquires the writer advisory lock.
- **Reader role** — `SELECT`-only grants on the `market` schema. Used by future Django reads. Cannot query `ingest`.

No application should bootstrap PostgreSQL with a superuser account. Do not log DSNs, passwords, or API keys.

## Table inventory

### `ingest.*` (private)

| Table | Purpose | Key shape |
|---|---|---|
| `ingest.schema_migration` | Applied migration versions | singleton column |
| `ingest.run` | Run ledger (run_id, frozen target, requested bounds, state, input_revision, code / schema / transform versions, start / end timestamps, published marker) | primary key `run_id` |
| `ingest.cache_state` | Singleton: current `input_revision`, retained raw date bounds | singleton |
| `ingest.fetch_manifest` | One row per fetch request (transport / decode / validation outcome) | composite (run_id, source, ...) |
| `ingest.raw_session` | Daily validation evidence per session | composite (date, ticker_id) |
| `ingest.raw_daily` | Unadjusted provider bars; primary key `(date, ticker_id)` enables per-date refresh and scoped replacement | composite (date, ticker_id) |
| `ingest.ticker_reference` | Current validated symbol IDs and metadata (nullable `active`); durable identity resolution | primary key `ticker_id` |
| `ingest.split_event` | Split contents with cumulative adjustment factor; provider event identity or verified natural key | primary key internal |

### `market.*` (public)

| Table | Purpose | Key shape |
|---|---|---|
| `market.ticker` | Catalog of stable symbol IDs, name, type, primary_exchange, cik, nullable `active`, `screen_eligible` | primary key `ticker_id`; unique `symbol` |
| `market.adjusted_daily` | Split-adjusted daily bars joined with metrics (SMA-20/50/200, ATR-14, ATR%, ADR%, volume_sma_20) | composite (ticker_id, date) |
| `market.adjusted_weekly` | Same shape as adjusted_daily; Monday-labeled week-start date | composite (ticker_id, date) |
| `market.adjusted_monthly` | Same shape as adjusted_daily; month label = last observed trading date in the ticker-month | composite (ticker_id, date) |
| `market.latest_daily` | One row per screen-eligible ticker for the latest validated shared session | primary key `ticker_id` |
| `market.publication_state` | Singleton: published_session, published_at, publishing run id, optional counts | singleton |

## Identifier conventions

- **Schemas and tables**: snake_case; lower case; underscores only between words. Schema names are short (single word where possible). Table names describe the entity, not the lifecycle (`raw_daily`, not `raw_daily_temp`).
- **Columns**: snake_case; lower case. Date columns are `date` (XNYS trading session), operational timestamps are `timestamptz`.
- **Primary keys**:
  - `ticker_id` is the stable identity column on every market reference table.
  - Historical tables use the composite `(ticker_id, date)` so per-ticker ordered history and per-date scoped refresh are both natural.
  - Singleton tables (`cache_state`, `publication_state`) use a single boolean `singleton` column constrained to `true`.
- **Foreign keys**: every market / ingest row that references a ticker resolves through `ticker_id`. The reference side does not auto-create an index — add one when lookup / delete behavior needs it.
- **Booleans**: descriptive names (`active`, `screen_eligible`, `left_truncated`, `calendar_closed`, `published`). Do not use generic `flag` or `is_*` columns whose meaning is not self-evident.
- **Versioning columns**: `code_version`, `schema_version`, `transform_version` are stored verbatim on `ingest.run`. They are `text` columns; semantic comparison is out of scope for this contract.

## Data-type conventions

| Domain | Type | Notes |
|---|---|---|
| OHLC, VWAP, metrics | `real` | matches the existing Float32 price / indicator contract; widening storage does not restore precision already lost by Float32 casts |
| Split adjustment factors, volume, volume averages | `double precision` | adjustment compounds; volume is fractional after split adjustment |
| Transaction counts and period sums | `bigint` | widen before summing, never after a narrow overflow |
| Session dates | `date` | tz-naive; do not convert session labels through UTC timestamps |
| Operational timestamps | `timestamptz` | aware current-time only; preserve tz-aware `datetime` semantics at the Python boundary |
| Ratios | `double precision` fractions | `0.04` means 4%; do not store as `numeric` percent types |
| Identifiers | `integer GENERATED BY DEFAULT AS IDENTITY` for new ticker IDs; `text` for run_id UUIDs | seed / resolve IDs from raw symbols as well as current metadata; metadata may omit delisted symbols; on reload / update preserve existing IDs |

Do not use `numeric` by default: it has cost and does not undo prior rounding. If exact decimal source prices are required, that is a separate precision decision with its own criteria.

## Publication atomicity

All public products and the publication state row become visible in one transaction. The publish step:

1. Stages all adjusted rows, metadata changes, latest-session rows, and the publication state into disposable staging tables.
2. Within one transaction: `DELETE` obsolete keys (only inside complete validated scopes), `INSERT` / upsert the staged rows, `UPDATE` publication_state to the new session and run id.
3. Commits.

A failed publication leaves the prior generation visible. The `market.publication_state.published_session` and `published_at` columns are the canonical freshness fields. Reader code should check the publication state before assuming today's data is current.

## Staging and disposable objects

- Stage tables live in a per-run disposable namespace (typically `pg_temp_*` or a dedicated `staging` schema dropped at commit). Never write production code that selects from staging.
- Staging is not resumable. After connection loss, abandon the staged tables and start a new run against the latest validated cache.
- The `ingest.fetch_manifest` is operational evidence, not a raw / reference archive or a retry checkpoint.

## Validation and rejection

The publication pipeline rejects invalid input before staging completes:

- Duplicate primary keys (especially `(date, ticker_id)` on raw_daily and `(ticker_id, date)` on adjusted_*).
- Non-finite `real` / `double precision` values.
- Invalid OHLC relationships (high < open / close / low; low > open / close / high).
- Orphan ticker IDs (no row in `market.ticker`).
- Prohibited nulls (NOT NULL keys and observed core bar values).
- Missing VWAP with positive-volume inputs in a weekly / monthly aggregation propagates null rather than producing a misleading partial estimate.

These checks are golden-tested; lock them with explicit fixture coverage.

## Naming changes

Schema, table, and column renames go through versioned migrations with both forward and downgrade SQL. Do not rename a column or table in application code without a migration; Django (future) and other readers will silently lose access.

## See also

- `docs/POSTGRES_MIGRATION_PLAN.md` — the seven-PR migration sequence that produced these conventions.
- `docs/POSTGRES_FOUNDATION_CONTRACT.md`, `docs/POSTGRES_BACKFILL_CONTRACT.md`, `docs/POSTGRES_PUBLICATION_CONTRACT.md` — per-area contracts that consume these conventions.
- `src/tickerlake/postgres/migrations.py` — the migration set that owns the DDL.
- `src/tickerlake/postgres/state.py`, `src/tickerlake/postgres/publication.py` — the canonical writers / state holders.