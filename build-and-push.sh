#!/bin/bash

# Build and push Salla Airbyte Connector Docker image
# Usage: ./build-and-push.sh [version]
# Example: ./build-and-push.sh 0.2.7

set -e  # Exit on error

# Configuration
REGISTRY="europe-docker.pkg.dev"
PROJECT_ID="wow-ai-461911"
REPOSITORY="airbyte-connector"
IMAGE_NAME="source-salla-python"
PLATFORM="linux/amd64"

# Get version from argument or use default
VERSION=${1:-"0.2.7"}

# Full image tag
IMAGE_TAG="${REGISTRY}/${PROJECT_ID}/${REPOSITORY}/${IMAGE_NAME}:${VERSION}"

echo "=========================================="
echo "Building Salla Airbyte Connector"
echo "=========================================="
echo "Version: ${VERSION}"
echo "Platform: ${PLATFORM}"
echo "Image: ${IMAGE_TAG}"
echo "=========================================="
echo ""

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    echo "❌ Error: Docker daemon is not running"
    echo "Please start Docker Desktop and try again"
    exit 1
fi

# Check if we're in the right directory
if [ ! -f "Dockerfile" ]; then
    echo "❌ Error: Dockerfile not found"
    echo "Please run this script from the salla-airbyte-connector directory"
    exit 1
fi

# Build and push
echo "🔨 Building Docker image..."
docker buildx build \
    --platform ${PLATFORM} \
    -t ${IMAGE_TAG} \
    --push \
    .

if [ $? -eq 0 ]; then
    echo ""
    echo "✅ Successfully built and pushed ${IMAGE_TAG}"
    echo ""
    echo "Next steps:"
    echo "1. Update your Airbyte connector to use image tag: ${VERSION}"
    echo "2. Ensure order_items stream uses 'Overwrite' mode (not 'Overwrite + Deduped')"
    echo "3. Re-run the sync"
else
    echo ""
    echo "❌ Build failed"
    exit 1
fi
