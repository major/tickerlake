.PHONY: test lint typecheck complexity deadcode deps all

test:
	uv run pytest

lint:
	uv run ruff check --fix

typecheck:
	uv run pyright src/*

complexity:
	uv run radon cc src/ -s -a

deadcode:
	uv run vulture src/ --min-confidence 80

deps:
	uv run deptry src/

all: lint test typecheck deadcode
