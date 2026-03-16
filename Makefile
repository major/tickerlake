.PHONY: test test-cov lint format format-check complexity check sync

test:
	uv run pytest tests/ -x --tb=short

test-cov:
	uv run pytest tests/ --cov=src/tickerlake --cov-report=html -x

lint:
	uv run ruff check src/ tests/

format:
	uv run ruff format src/ tests/

format-check:
	uv run ruff format --check src/ tests/

complexity:
	uv run radon cc src/ -a -nc

check: lint format-check complexity test

sync:
	uv run tickerlake sync --verbose
