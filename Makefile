.PHONY: test lint typecheck complexity all

test:
	uv run pytest

lint:
	uv run ruff check --fix

typecheck:
	uv run pyright src/*

complexity:
	uv run radon cc src/ -s -a

all: lint test typecheck
