# PostgreSQL integration tests

Run the PostgreSQL integration tests on the host with:

```sh
uv sync --locked
make test-postgres
```

The tests run in the host Python environment and use Testcontainers to start
the official `postgres:18` image. Docker must be available. To use rootless
Podman instead, configure its API socket and point Testcontainers to it, for
example:

```sh
systemctl --user start podman.socket
export DOCKER_HOST="unix://${XDG_RUNTIME_DIR}/podman/podman.sock"
```

Testcontainers uses Ryuk to clean up containers. Rootless Podman or restricted
container environments may prevent Ryuk from starting or connecting. On one
SELinux-enabled local Docker 29.7.2 setup, Ryuk exited with status 2 and
Testcontainers reported that its container did not become running. The verified
workaround there was:

```sh
TESTCONTAINERS_RYUK_PRIVILEGED=true make test-postgres
```

This grants Ryuk broader privileges. Use it only with a trusted container
runtime and trusted images. It is environment-specific, is not needed by default
in CI, and must not be used as a reason to disable Ryuk.

The script accepts pytest arguments and passes them through. It defaults to two
xdist workers; use `./scripts/test-postgres.sh -n 0 tests/postgres/` to run
without xdist. With no arguments, it runs `tests/postgres/ -x --tb=short`.
PostgreSQL tests remain skipped in the standard test suite unless
`TICKERLAKE_TEST_POSTGRES=1` is set.

The session fixture starts one isolated PostgreSQL server per xdist worker and
creates test-only roles. Each test gets a fresh database named with the
`tickerlake_test_` prefix on its worker's server. `pg_owner_dsn`, `pg_etl_dsn`,
`pg_reader_dsn`, `pg_admin_dsn`, and `pg_database` are provided by
`tests/postgres/conftest.py`. `pg_database` contains all four DSNs and the
database name. `pg_migrated_database` is the explicit fixture for tests that
need the production schema; ordinary database tests start empty. This fixture
applies the repository migration set before yielding the database.

Running pytest directly does not connect to PostgreSQL. PostgreSQL tests are
skipped unless `TICKERLAKE_TEST_POSTGRES=1` is set. This flag only enables the
container fixture; inherited database URLs or DSNs never select the test
server. Tests use only the server and credentials created by the
Testcontainers fixture. Never provide a production database URL to these tests.
