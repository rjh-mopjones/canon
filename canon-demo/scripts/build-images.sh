#!/bin/bash
# Build all Canon demo Docker images sequentially to avoid OOM.
# Docker Desktop default memory (8 GiB) is not enough for parallel Rust builds.
set -e

cd "$(dirname "$0")/.."

echo "Building Canon demo images sequentially..."
for svc in fleet-service cargo-service navigation-service station-service supply-service gateway frontend; do
  echo "==> Building $svc..."
  COMPOSE_PARALLEL_LIMIT=1 docker compose build "$svc"
  echo "==> $svc built."
done

echo "All images built successfully."
