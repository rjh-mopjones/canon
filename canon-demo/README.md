# Canon Demo

Multi-service event-sourcing demo built on the Canon framework.

## Services

| Service | Port | Description |
|---------|------|-------------|
| fleet-service | - | Ship aggregate (register, depart, resupply) |
| cargo-service | - | Manifest aggregate (load, unload cargo) |
| navigation-service | - | Route aggregate (plan, track positions) |
| station-service | - | Station aggregate (docking, stock levels) |
| supply-service | - | Inventory aggregate (resupply lifecycle) |
| gateway | 8080 | REST + WebSocket API |
| frontend | 3000 | Leptos WASM UI |

## Infrastructure

- **YugabyteDB** (5433) -- command store, outbox, snapshots, projections, inbox
- **Cassandra** (9042) -- event store
- **Kafka** (9092/9093) -- message queues

## Building

### Option 1: Sequential Docker builds (recommended)

```bash
./scripts/build-images.sh
docker compose up -d
```

### Option 2: Direct compose (requires 16+ GiB Docker memory)

```bash
docker compose up -d
```

**Note:** Docker Desktop defaults to 8 GiB RAM. Building all 7 Rust services
in parallel requires 16+ GiB. Use `scripts/build-images.sh` or set
`COMPOSE_PARALLEL_LIMIT=1` to build sequentially.

### Option 3: Cross-compilation with cargo-zigbuild (fastest)

Requires one-time setup:
```bash
cargo install cargo-zigbuild
brew install zig  # or your OS equivalent
rustup target add aarch64-unknown-linux-gnu
```

Then build and run:
```bash
./scripts/build-zigbuild.sh
cd canon-demo && docker compose up -d
```

This cross-compiles all 6 Rust services on your host (~30s) and packages them
into minimal Docker images (<1s each). No Rust compilation happens inside
Docker, eliminating OOM issues entirely.

**Note:** The frontend (Leptos WASM) is not cross-compiled by zigbuild and will
still be built by Docker compose on first run.
