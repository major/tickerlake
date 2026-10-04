# PostgreSQL integration tests

Run the PostgreSQL integration tests on the host with:

```sh
uv sync --locked
make test-postgres
```

pytest-postgresql starts and owns an isolated PostgreSQL cluster for the pytest
session. It needs the PostgreSQL server binaries (`initdb`, `pg_ctl`, and
`postgres`) on the host. Install them with `postgresql-server` on Fedora or
`postgresql` on Debian and Ubuntu.

The script accepts pytest arguments and passes them through. It defaults to two
xdist workers; use `./scripts/test-postgres.sh -n 0 tests/postgres/` to run
without xdist. With no arguments, it runs `tests/postgres/ -x --tb=short`.

`TICKERLAKE_TEST_POSTGRES=1` is set by `scripts/test-postgres.sh`. PostgreSQL
tests are skipped during `make test` and `make test-cov` unless this gate is
set.

The session fixture in `tests/postgres/conftest.py` starts one isolated
PostgreSQL server per xdist worker and creates test-only roles. Each test gets a
fresh database named with the `tickerlake_test_` prefix on its worker's server.
The following fixtures are provided:

- `pg_owner_dsn`, `pg_etl_dsn`, `pg_reader_dsn`, and `pg_admin_dsn`: one DSN per
  test role.
- `pg_database`: a `PostgresTestDatabase` dataclass holding the database name
  and all four DSNs.
- `pg_migrated_database`: the explicit fixture for tests that need the
  production schema; ordinary database tests start empty. It applies the
  repository migration set before yielding the database.

The `PostgresTestHarness` dataclass holds the cluster endpoint and generated
role credentials for one pytest session.

CI installs PostgreSQL 18 via the `ankane/setup-postgres@v1` action, and
pytest-postgresql starts a cluster from those binaries. See
`.github/workflows/ci.yml`.

Safety note: "Tests use only the server and credentials created by the
pytest-postgresql fixture. Never provide a production database URL to these
tests."
