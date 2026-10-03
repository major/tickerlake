.PHONY: test test-cov lint format format-check complexity check sync mutmut mutation-focused mutmut-results mutmut-apply

test:
	uv run pytest tests/ -x --tb=short

test-cov:
	uv run pytest tests/ --cov=src/tickerlake --cov-branch --cov-report=html --cov-report=xml --cov-report=term-missing -x

lint:
	uv run ruff check src/ tests/

format:
	uv run ruff format src/ tests/

format-check:
	uv run ruff format --check src/ tests/

check: lint format-check complexity test-cov

sync:
	uv run tickerlake sync --verbose

mutmut:
	uv run mutmut run --max-children 2

export SCOPE
mutation-focused:
	@printf '%s\n' "$$SCOPE" | grep -Eq '^tickerlake(\.[A-Za-z_][A-Za-z0-9_]*)*\.x_[A-Za-z_][A-Za-z0-9_]*\*$$' || { echo 'SCOPE must be a tickerlake function pattern with one final wildcard' >&2; exit 2; }; \
	 podman run --rm -e SCOPE="$$SCOPE" -v "$(CURDIR):/workspace:Z" -w /workspace docker.io/library/python:3.14 bash -lc \
	 'python -m pip install -q uv==0.12.18 && UV_PROJECT_ENVIRONMENT=/opt/tickerlake-venv uv sync --locked --all-groups && UV_PROJECT_ENVIRONMENT=/opt/tickerlake-venv uv run mutmut run --max-children 2 "$${SCOPE}"'

mutmut-results:
	uv run mutmut results

mutmut-apply:
	uv run mutmut apply
