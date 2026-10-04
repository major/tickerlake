# syntax=docker/dockerfile:1.27@sha256:4edf897a3ffa55b89f906fc8cc78afdb3f1834cc9c7083565e611a8a7d5fe99e
#
# tickerlake container image
#
# Base images:
#   Red Hat UBI 9 Python 3.14. The builder uses the full python-314 image and
#   the runtime uses the smaller python-314-minimal variant.
#
#   Registry: registry.access.redhat.com is Red Hat's official anonymous
#   mirror for UBI content, so no Red Hat login is required to pull the base
#   images. The same content is also published on registry.redhat.io, but that
#   registry requires a Red Hat account.
#
#   Tag: :9.8 is the current UBI 9 microline as of 2026-10. The full image
#   also serves the stream tag :1, but the -minimal variant only publishes
#   microline tags (verified via a HEAD probe against the v2 manifest
#   endpoint), so both stages use :9.8 to keep their base image in lockstep.
#   Bump the tag together when upgrading. For reproducible builds, pin the
#   base images to a digest in production.
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

FROM ghcr.io/astral-sh/uv:0.12.23@sha256:61d393e44e249f2e4b526b6c7ddcecce245946826e608e11c93ad4f5bba55b21 AS uv

FROM registry.access.redhat.com/ubi9/python-314:9.8-1790838728@sha256:28f564643c2fe7d4607562f1f4057f162654316f9226530a34925a792edf2263 AS builder
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

FROM registry.access.redhat.com/ubi9/python-314-minimal:9.8-1790838707@sha256:e54394f1363659f28a9cde6e7467a0a16fd16d67c27416304ccc51e046655ba0 AS runtime
USER root
# Create tickerlake with UID/GID 1000 instead of using the image's built-in
# "default" user (UID 1001). This keeps the Helm chart's runAsUser: 1000
# working without a chart change.
RUN groupadd --system --gid 1000 tickerlake \
    && useradd --system --uid 1000 --gid 1000 --home-dir /app --shell /sbin/nologin tickerlake
WORKDIR /app
COPY --from=builder --chown=tickerlake:tickerlake /build/.venv /app/.venv
COPY --from=builder --chown=tickerlake:tickerlake /build/src /app/src
ENV PATH="/app/.venv/bin:${PATH}" \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app/src
# Match the chart securityContext runAsUser / runAsGroup: 1000.
USER 1000
ENTRYPOINT ["tickerlake"]
