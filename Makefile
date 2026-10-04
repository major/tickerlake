.PHONY: test test-postgres test-cov lint format format-check typecheck complexity check sync mutmut mutmut-results mutmut-apply docker-build docker-run docker-push

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

IMAGE_REPO ?= ghcr.io/major/tickerlake
IMAGE_TAG ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)

docker-build:
	docker build -t $(IMAGE_REPO):$(IMAGE_TAG) -t $(IMAGE_REPO):latest .

docker-run:
	docker run --rm -it \
		-e MASSIVE_API_KEY=$$MASSIVE_API_KEY \
		-e DATABASE_URL=$$DATABASE_URL \
		$(IMAGE_REPO):$(IMAGE_TAG) $(ARGS)

docker-push:
	docker push $(IMAGE_REPO):$(IMAGE_TAG)
	docker push $(IMAGE_REPO):latest
