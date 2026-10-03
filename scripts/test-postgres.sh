#!/usr/bin/env bash
set -Eeuo pipefail

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
command -v podman >/dev/null || { echo "podman is required" >&2; exit 1; }
suffix="$(date +%s)-$$-${RANDOM}"
network="tickerlake-test-${suffix}"
pg_name="tickerlake-postgres-${suffix}"
test_name="tickerlake-python-${suffix}"
pg_password='tickerlake-postgres-test-only'
owner_password='tickerlake-owner-test-only'
etl_password='tickerlake-etl-test-only'
reader_password='tickerlake-reader-test-only'
admin_password='tickerlake-admin-test-only'

cleanup() {
    podman rm -f "$test_name" "$pg_name" >/dev/null 2>&1 || true
    podman network rm "$network" >/dev/null 2>&1 || true
}
trap cleanup EXIT INT TERM

podman network create "$network" >/dev/null
podman run -d --rm --name "$pg_name" --network "$network" --network-alias postgres \
    -e POSTGRES_PASSWORD="$pg_password" -e POSTGRES_DB=tickerlake_harness \
    docker.io/library/postgres:18 >/dev/null

ready=0
for attempt in $(seq 1 60); do
    if podman exec "$pg_name" pg_isready -h 127.0.0.1 -U postgres -d tickerlake_harness >/dev/null 2>&1; then
        ready=1
        break
    fi
    sleep 1
done
if [[ $ready != 1 ]]; then
    echo "PostgreSQL did not become ready" >&2
    podman logs "$pg_name" >&2 || true
    exit 1
fi

podman exec -i "$pg_name" psql -v ON_ERROR_STOP=1 -U postgres -d tickerlake_harness \
    -v owner_password="$owner_password" -v etl_password="$etl_password" \
    -v reader_password="$reader_password" -v admin_password="$admin_password" <<'SQL'
CREATE ROLE tickerlake_etl NOLOGIN;
CREATE ROLE tickerlake_reader NOLOGIN;
CREATE ROLE tickerlake_owner LOGIN PASSWORD :'owner_password' NOSUPERUSER NOCREATEDB NOCREATEROLE;
CREATE ROLE tickerlake_etl_login LOGIN PASSWORD :'etl_password' NOSUPERUSER NOCREATEDB NOCREATEROLE IN ROLE tickerlake_etl;
CREATE ROLE tickerlake_reader_login LOGIN PASSWORD :'reader_password' NOSUPERUSER NOCREATEDB NOCREATEROLE IN ROLE tickerlake_reader;
CREATE ROLE tickerlake_test_admin LOGIN PASSWORD :'admin_password' NOSUPERUSER CREATEDB NOCREATEROLE;
GRANT tickerlake_owner TO tickerlake_test_admin;
GRANT pg_signal_backend TO tickerlake_test_admin;
SQL

env -u DATABASE_URL -u PGHOST -u PGHOSTADDR -u PGPORT -u PGDATABASE -u PGUSER -u PGPASSWORD \
    -u PGSERVICE -u PGSERVICEFILE -u PGOPTIONS -u PGSSLMODE \
podman run --rm --name "$test_name" --network "$network" \
    --userns=keep-id --user "$(id -u):$(id -g)" --security-opt label=disable \
    --tmpfs /tmp:rw,exec,mode=1777 --tmpfs /work:rw,exec,mode=1777 \
    --mount "type=bind,src=$root,dst=/source,ro=true" --workdir /work \
    -e TICKERLAKE_TEST_POSTGRES=1 \
    -e PG_OWNER_PASSWORD="$owner_password" -e PG_ETL_PASSWORD="$etl_password" \
    -e PG_READER_PASSWORD="$reader_password" -e PG_ADMIN_PASSWORD="$admin_password" \
    -e HOME=/tmp/tickerlake-home -e PIP_CACHE_DIR=/tmp/pip-cache \
    -e UV_CACHE_DIR=/tmp/uv-cache -e UV_PROJECT_ENVIRONMENT=/tmp/tickerlake-venv \
    docker.io/library/python:3.14 bash -euc '
        cp -R /source/. /work/
        cd /work
        python -m pip install --user --disable-pip-version-check uv
        export PATH="$HOME/.local/bin:$PATH"
        uv sync --locked --group dev --project /work --no-config
        if (($#)); then
            uv run pytest "$@" -o cache_dir=/tmp/pytest-cache
        else
            uv run pytest tests/ -x --tb=short -o cache_dir=/tmp/pytest-cache
        fi
    ' bash "$@"
