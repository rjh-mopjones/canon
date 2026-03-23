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

### Prerequisites (one-time)

```bash
rustup target add aarch64-unknown-linux-musl
brew install filosottile/musl-cross/musl-cross   # provides aarch64-linux-musl-gcc
```

### Build and deploy to minikube

```bash
cd canon-demo && make k8s-up
```

This cross-compiles all 6 backend services locally to static musl binaries (~2 min),
builds slim alpine Docker images (COPY binary, ~2s each), builds the frontend WASM
image, loads all images into minikube, and deploys the full stack.

To rebuild images only (without redeploying):
```bash
cd canon-demo && make k8s-build
```

No Rust compilation happens inside Docker. The frontend (Leptos WASM) still builds
inside Docker via Trunk.
