# syntax=docker/dockerfile:1.7
#
# tickerlake container image
#
# Base images:
#   Red Hat UBI 9 Python 3.14, pinned to the current stable microline 9.8.
#   The builder uses the full python-314 image and the runtime uses the
#   smaller python-314-minimal variant.
#
#   Source for the tag: `skopeo list-tags
#   docker://registry.access.redhat.com/ubi9/python-314` on 2026-10-01. The
#   catalog has no 9.6 microline; 9.8 is the current stable tag, so this file
#   uses 9.8 for both stages. registry.redhat.io requires a Red Hat login; the
#   public anonymous mirror registry.access.redhat.com serves the same UBI
#   content if you do not have credentials.
#
# Build tool:
#   uv is copied from the official ghcr.io/astral-sh/uv image, pinned to
#   0.12.18 to match the uv.lock revision and the uv_build backend version.
#   `uv sync --locked --no-dev` installs the runtime dependencies from the
#   lockfile. The venv is created with `--relocatable` because it is copied
#   between stages; a relocatable venv keeps its console scripts working at a
#   new path. pyproject.toml declares README.md as the project readme, so it
#   is copied before the project itself is installed.
#
# Build requirements:
#   Network access to PyPI (for `uv sync`) and to the base image registries.
#
# Build:
#   docker build -t tickerlake:test .
#   podman build -t tickerlake:test .
#   A Dockerfile symlink points at this file because docker does not
#   auto-detect the Containerfile name the way podman does.
#
# Runtime:
#   Runs as non-root UID/GID 1000 to match the Helm chart securityContext
#   (runAsUser, runAsGroup, runAsNonRoot). ENTRYPOINT is the tickerlake
#   binary; the subcommand (backfill or update) is supplied as an argument.

FROM ghcr.io/astral-sh/uv:0.12.18 AS uv

FROM registry.redhat.io/ubi9/python-314:9.8 AS builder
USER root
COPY --from=uv /uv /uvx /usr/local/bin/
WORKDIR /build
ENV UV_PYTHON_DOWNLOADS=never
COPY pyproject.toml uv.lock ./
RUN uv venv --relocatable
RUN uv sync --locked --no-dev --no-install-project
COPY README.md ./
COPY src ./src
RUN uv sync --locked --no-dev
ENV PATH="/build/.venv/bin:${PATH}" \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

FROM registry.redhat.io/ubi9/python-314-minimal:9.8 AS runtime
USER root
RUN groupadd --system --gid 1000 tickerlake \
    && useradd --system --uid 1000 --gid 1000 --home-dir /app --shell /sbin/nologin tickerlake
WORKDIR /app
COPY --from=builder --chown=tickerlake:tickerlake /build/.venv /app/.venv
COPY --from=builder --chown=tickerlake:tickerlake /build/src /app/src
ENV PATH="/app/.venv/bin:${PATH}" \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app/src
USER 1000
ENTRYPOINT ["tickerlake"]
