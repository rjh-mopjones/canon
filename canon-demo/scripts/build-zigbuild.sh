#!/bin/bash
# Cross-compile Canon demo services for Linux and build minimal Docker images.
# Requires: cargo-zigbuild, zig, rustup target aarch64-unknown-linux-gnu
set -e

cd "$(dirname "$0")/../.."

echo "==> Cross-compiling all services for aarch64-unknown-linux-gnu..."
cargo zigbuild --release --target aarch64-unknown-linux-gnu \
  -p fleet-service \
  -p cargo-service \
  -p navigation-service \
  -p station-service \
  -p supply-service \
  -p gateway

echo "==> Packaging Docker images..."
for svc in fleet-service cargo-service navigation-service station-service supply-service gateway; do
  echo "    Building canon-demo-$svc..."
  docker build -f canon-demo/Dockerfile.prebuilt \
    --build-arg BINARY_NAME="$svc" \
    -t "canon-demo-$svc:latest" .
done

echo "==> All images built successfully."
echo ""
echo "To start the demo:"
echo "  cd canon-demo && docker compose up -d"
