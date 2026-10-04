#!/usr/bin/env bash
set -Eeuo pipefail

if (($#)); then
    exec env TICKERLAKE_TEST_POSTGRES=1 uv run pytest -n 2 "$@"
fi

exec env TICKERLAKE_TEST_POSTGRES=1 uv run pytest tests/postgres/ -n 2 -x --tb=short
