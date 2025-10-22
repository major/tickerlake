#!/usr/bin/env bash
# Build script for TickerLake Datasette container

set -euo pipefail

echo "📊 Building TickerLake Datasette container..."

# Ensure we're in the project root
cd "$(dirname "$0")/.."

# Step 1: Prepare the datasette database
echo "📦 Step 1: Preparing data..."
uv run python container/prepare_datasette.py

# Check that database was created
if [ ! -f "container/datasette.db" ]; then
    echo "❌ Error: datasette.db not found!"
    exit 1
fi

# Step 2: Build the container
echo "🐳 Step 2: Building container image..."
podman build -t tickerlake-datasette:latest -f container/Containerfile container/

echo "✅ Build complete!"
echo ""
echo "To run the container:"
echo "  podman run -p 8001:8001 tickerlake-datasette:latest"
echo ""
echo "Then visit: http://localhost:8001"
