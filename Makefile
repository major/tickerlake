.PHONY: test test-postgres test-cov lint format format-check typecheck complexity check sync mutmut mutmut-results mutmut-apply

test:
	uv run pytest tests/ -x --tb=short -n auto

test-postgres:
	./scripts/test-postgres.sh

test-cov:
	uv run pytest tests/ -n auto --cov=src/tickerlake --cov-branch --cov-report=html --cov-report=xml --cov-report=term-missing -x

lint:
	uv run ruff check src/ tests/

format:
	uv run ruff format src/ tests/

format-check:
	uv run ruff format --check src/ tests/

typecheck:
	uv run ty check src/

complexity:
	uv run radon cc src/ -s -a

check: lint format-check typecheck complexity test-cov

sync:
	uv run tickerlake sync --verbose

mutmut:
	uv run mutmut run --max-children 2

mutmut-results:
	uv run mutmut results

mutmut-apply:
	uv run mutmut apply
