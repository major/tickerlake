# syntax=docker/dockerfile:1
FROM ghcr.io/astral-sh/uv:python3.13-bookworm-slim AS builder

# Enable bytecode compilation for faster startup
ENV UV_COMPILE_BYTECODE=1

# Copy project files
WORKDIR /app
COPY pyproject.toml uv.lock ./

# Install dependencies using uv with the container group
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-install-project --group container

# Runtime stage
FROM python:3.13-slim-bookworm

# Copy virtual environment from builder
COPY --from=builder /app/.venv /app/.venv

# Set working directory and PATH
WORKDIR /app
ENV PATH="/app/.venv/bin:$PATH"

# Copy application data
COPY hvcs.db metadata.json inspect-data.json ./

# Generate inspect data at build time for faster container startup
RUN datasette inspect hvcs.db --inspect-file inspect-data.json

# Expose port
EXPOSE 8001

# Run datasette
CMD ["datasette", "serve", "hvcs.db", \
    "--port", "8001", \
    "--host", "0.0.0.0", \
    "--metadata", "metadata.json", \
    "--inspect-file", "inspect-data.json", \
    "--setting", "default_page_size", "50"]
