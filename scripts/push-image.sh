#!/usr/bin/env sh
set -eu

BUILDER="${ONLYDOGE_IMAGE_BUILDER:-onlydoge-multiarch}"
IMAGE="${ONLYDOGE_IMAGE:-ghcr.io/simonbetton/onlydoge-indexer:latest}"
INIT_ONLY="${1:-}"

if ! docker buildx inspect "$BUILDER" >/dev/null 2>&1; then
  docker buildx create --name "$BUILDER" --driver docker-container --use >/dev/null
else
  docker buildx use "$BUILDER"
fi

docker buildx inspect --builder "$BUILDER" --bootstrap >/dev/null

if [ "$INIT_ONLY" = "--init-only" ]; then
  exit 0
fi

docker buildx build \
  --builder "$BUILDER" \
  --platform linux/amd64,linux/arm64 \
  --target production \
  -t "$IMAGE" \
  --push \
  .
