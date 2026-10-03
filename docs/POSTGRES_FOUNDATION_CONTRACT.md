# PostgreSQL foundation contract

This contract defines the first PostgreSQL migration. Tickerlake's SQL migration set owns schema changes. Migrations run explicitly on the same live autocommit connection that holds the writer advisory lock. Each migration's DDL and ledger row are committed atomically. Re-running is a no-op; out-of-order, unknown, or checksum-changed applied migrations fail safely. The ledger is `ingest.schema_migration(version integer, filename text, checksum text, applied_at timestamptz)`.

The harness creates database roles `tickerlake_etl` and `tickerlake_reader` and a non-superuser database owner. Migrations do not create roles, passwords, logins, or tables on behalf of those roles. They revoke direct grants to `PUBLIC`, `tickerlake_etl`, and `tickerlake_reader` on the owned schemas, tables, and sequences before applying the explicit grants. These ACL revocations do not remove privileges inherited through memberships in unrelated roles. ETL receives DML on both schemas and required sequence access; the reader receives only `USAGE` on `market` and `SELECT` on market tables. Neither role can modify the migration ledger. ETL needs database `TEMP` only when granted by deployment/harness. No default privileges are changed. Supported ETL and reader logins must not belong to the database owner role or other roles whose privileges or ownership defeat these restrictions.

## Foundation relations

The first migration creates these exact relations and columns:

| Relation | Columns (name: PostgreSQL type; nullability/default) |
|---|---|
| `market.ticker` | `ticker_id integer` identity by default, PK; `symbol text` not null unique, nonblank; `name text`; `ticker_type text`; `primary_exchange text`; `cik text`; `active boolean`; `screen_eligible boolean` not null default false, requires `active IS TRUE` when true |
| `ingest.raw_daily` | `date date` not null; `ticker_id integer` not null FK to ticker; `open, high, low, close real` not null; `vwap real`; `volume double precision` not null; `transactions bigint` not null; PK `(date, ticker_id)` |
| `ingest.ticker_reference` | `ticker_id integer` PK/FK; `name text`; `ticker_type text`; `primary_exchange text`; `cik text`; `active boolean` |
| `ingest.split_event` | `split_id bigint` identity by default, PK; `ticker_id integer` not null FK; `execution_date date` not null; `split_from, split_to real` not null; `adjustment_factor double precision` not null; `adjustment_type text`; null-safe unique key `(ticker_id, execution_date, split_from, split_to, adjustment_factor, adjustment_type)` |
| `ingest.cache_state` | `singleton boolean` PK/default true/check true; `input_revision bigint` not null default 0/check nonnegative; `retained_start, retained_end date`; seeded singleton row at revision zero with null bounds; bounds are both null or both present and ordered |
| `ingest.run` | `run_id uuid` PK; `target_date date` not null; `requested_start, requested_end date`; `input_revision bigint` not null/nonnegative; `code_version, schema_version, transform_version text` not null; `state text` in running/completed/failed/published; `started_at timestamptz` not null; `ended_at, published_at timestamptz`; `failure_code text`; range and state/timestamp consistency checks |
| `ingest.fetch_manifest` | `manifest_id bigint` identity by default, PK; `run_id uuid` not null FK; `source text` in daily/tickers/splits; `requested_date date`; `requested_start, requested_end date`; `requested_ticker_types text[]` nullable and nonempty when present; `started_at timestamptz` not null; `finished_at timestamptz`; `status text` in failed/quarantined/populated/successful_empty; `row_count bigint` not null default 0/nonnegative; `diagnostic_code text` nullable and restricted to safe lowercase code characters |
| `ingest.schema_migration` | `version integer` PK; `filename text` not null unique; `checksum text` not null lowercase SHA-256; `applied_at timestamptz` not null default now() |

No FK cascades are used. CIK is not unique. There is no unique ticker/date split constraint. Raw daily data checks finite values, nonnegative volume and transactions, and OHLC ordering. Split components and factors must be finite and positive. Metadata and VWAP remain nullable. Run timestamps must match its state; published means it has both end and publication timestamps. Fetch manifests may represent an in-progress request with a null finish time. Diagnostics are safe identifiers, not exception messages or secrets.

This is only the ingest/reference foundation. It intentionally does not create derived market products, placeholders for future products, broad default grants, or publication tables.

## Shared Python contracts

`tickerlake.postgres.connection.writer_connection(database_url)` yields the locked autocommit connection, and `require_writer_connection(connection)` verifies ownership. `tickerlake.postgres.migrations.apply_migrations(connection) -> None` requires that lock and is the explicit migration runner. Storage/state lanes should use these relations and the exact names above. Fetch status values match `tickerlake.outcomes.FetchStatus`: `failed`, `quarantined`, `populated`, and `successful_empty`. New shared request dataclasses may be added by their owning lane; this contract does not require them.
