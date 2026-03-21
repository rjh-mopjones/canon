#!/bin/bash
# Cross-compile all Canon demo services and build Docker images.
#
# Prerequisites:
#   brew install zig           (or see https://ziglang.org/download/)
#   cargo install cargo-zigbuild
#   rustup target add aarch64-unknown-linux-gnu
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
TARGET="aarch64-unknown-linux-gnu"

SERVICES=(
  fleet-service
  cargo-service
  navigation-service
  station-service
  supply-service
  gateway
)

# ── Dependency checks ──────────────────────────────────────────────────

check_dependency() {
  local cmd="$1"
  local install_hint="$2"
  if ! command -v "$cmd" &>/dev/null; then
    echo "ERROR: '$cmd' not found."
    echo "  Install with: $install_hint"
    exit 1
  fi
}

check_dependency zig       "brew install zig  (or https://ziglang.org/download/)"
check_dependency cargo     "https://rustup.rs"
check_dependency trunk     "cargo install trunk"

if ! cargo zigbuild --help &>/dev/null; then
  echo "ERROR: 'cargo-zigbuild' not found."
  echo "  Install with: cargo install cargo-zigbuild"
  exit 1
fi

if ! rustup target list --installed | grep -q "$TARGET"; then
  echo "ERROR: Rust target '$TARGET' not installed."
  echo "  Install with: rustup target add $TARGET"
  exit 1
fi

# ── Step 1: Cross-compile Rust services ────────────────────────────────

echo "==> Cross-compiling all services for $TARGET..."
cd "$PROJECT_ROOT"

cargo zigbuild --release --target "$TARGET" \
  -p fleet-service \
  -p cargo-service \
  -p navigation-service \
  -p station-service \
  -p supply-service \
  -p gateway

echo "==> Cross-compilation complete."

# ── Step 2: Build frontend WASM ────────────────────────────────────────

echo "==> Building frontend (trunk build --release)..."
cd "$SCRIPT_DIR/frontend"
trunk build --release
cd "$PROJECT_ROOT"

echo "==> Frontend build complete."

# ── Step 3: Build Docker images & start services ───────────────────────

echo "==> Building Docker images..."
cd "$SCRIPT_DIR"
docker compose build

echo "==> Starting services..."
docker compose up
