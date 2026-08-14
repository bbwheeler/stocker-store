#!/usr/bin/env bash
set -euo pipefail

REGISTRY="containers.wheeli.ca"
IMAGE_NAME="stocker-list:latest"
PROJECT_DIR="$(cd "$(dirname "$0")/.." && pwd)"

echo "==> Building image for $REGISTRY"
podman build -t "${REGISTRY}/${IMAGE_NAME}" "$PROJECT_DIR"

echo "==> Pushing image to $REGISTRY"
podman push "${REGISTRY}/${IMAGE_NAME}"

echo "Done. Image pushed to ${REGISTRY}/${IMAGE_NAME}"