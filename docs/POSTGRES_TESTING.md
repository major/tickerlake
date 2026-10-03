# PostgreSQL integration tests

Run the complete suite, including PostgreSQL integration tests, with:

```sh
make test-postgres
```

The command requires Podman and pulls the official `postgres:18` and full
`python:3.14` images when they are not already available. It creates a uniquely
named private network and containers, publishes no host ports, and removes only
those containers and that network when it exits. PostgreSQL uses an anonymous
container volume, so test data is discarded with the container. The source is
mounted read-only and the Python environment and package cache stay in the test
container.

PostgreSQL tests use only credentials and DSNs created for this ephemeral
server. Each test gets a fresh database named with the `tickerlake_test_`
prefix. `pg_owner_dsn`, `pg_etl_dsn`, `pg_reader_dsn`, `pg_admin_dsn`, and
`pg_database` are provided by `tests/postgres/conftest.py`. `pg_database`
contains all four DSNs and the database name. `pg_migrated_database` is the
explicit fixture for tests that need the production schema; ordinary database
tests start empty. This fixture applies the repository migration set before
yielding the database.

Running pytest directly does not connect to PostgreSQL. PostgreSQL tests are
skipped unless the harness sets `TICKERLAKE_TEST_POSTGRES=1`; do not set that
flag against a host or production server. The harness clears inherited libpq
connection settings so a local or production database can never become a
fallback.
