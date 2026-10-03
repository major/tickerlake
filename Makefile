.PHONY: test test-cov lint format format-check complexity check sync mutmut mutmut-results mutmut-apply

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
	uv run mutmut run

mutmut-results:
	uv run mutmut results

mutmut-apply:
	uv run mutmut apply
