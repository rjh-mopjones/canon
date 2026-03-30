# Deployment

This chapter covers building, deploying, and running Canon services in Kubernetes --
both locally with minikube and in production on GKE.

## ServiceBuilder

`ServiceBuilder` is the entry point for wiring a Canon service. It auto-discovers all
registered aggregates, handlers, and projections via `inventory`:

```rust
let service = ServiceBuilder::new()
    .for_aggregate::<Ship>()
    .subscribe_to("canon.navigation.events")
    .subscribe_to("canon.supply.events")
    .build();

service.start().await;
```

`service.start()` spawns all background tasks:
- **Outbox processor** -- drains outbox to outbound queue
- **Event store consumer** -- writes to Cassandra, creates snapshots
- **Projection consumer** -- updates read models
- **Publisher consumer** -- publishes to cross-service topics
- **Internal event consumer** -- routes own events back to inbox

Each runs as a `tokio::spawn` task with graceful shutdown via a `watch` channel.

## Build pipeline

Canon services are cross-compiled from macOS to Linux, producing static musl binaries.
Docker images are slim alpine containers that just copy the pre-built binary -- no Rust
compilation happens inside Docker.

### Prerequisites (one-time setup)

```bash
# Add the musl target
rustup target add aarch64-unknown-linux-musl

# Install the musl cross-compiler (macOS)
brew install filosottile/musl-cross/musl-cross
```

### Cross-compilation

```bash
# Build all services (~2 min)
cargo build --release --target aarch64-unknown-linux-musl \
    -p fleet-service -p cargo-service -p navigation-service \
    -p supply-service -p station-service -p gateway
```

### Docker images

Each service has a minimal Dockerfile:

```dockerfile
FROM alpine:3.19
COPY target/aarch64-unknown-linux-musl/release/fleet-service /usr/local/bin/
CMD ["fleet-service"]
```

Build time: approximately 2 seconds per service.

## Kubernetes deployment

All pods run in a single `canon` namespace.

### Infrastructure (StatefulSets with PVCs)

- YugabyteDB -- transactional store
- Cassandra -- event store
- Zookeeper -- Kafka dependency
- Kafka -- message transport

### Init jobs

Two init jobs run before services start:
1. `init-schema` -- creates YugabyteDB schemas and Cassandra keyspaces
2. `init-kafka-topics` -- creates all 15 Kafka topics

### Application pods

| Pod | Purpose | Ports |
|-----|---------|-------|
| fleet-service | Fleet aggregate processing | none |
| cargo-service | Cargo aggregate processing | none |
| navigation-service | Navigation aggregate processing | none |
| supply-service | Supply aggregate processing | none |
| station-service | Station aggregate processing | none |
| gateway | REST API + WebSocket | 8080 |
| frontend | Leptos WASM UI | 80 |
| canon-site | Landing page | 80 |

The 5 Canon services are background processors with no exposed ports -- only gateway
and frontend have Services.

## Minikube (local)

```bash
cd canon-demo

# Full stack: start minikube, build images, deploy
make k8s-up

# Or step by step:
minikube start --cpus=4 --memory=8g
make k8s-build    # cross-compile + docker build + minikube image load
make k8s-deploy   # kubectl apply -k k8s/overlays/minikube/

# Access the frontend
make k8s-tunnel   # exposes LoadBalancer on localhost:80

# Check status
make k8s-status

# View logs
make k8s-logs

# Tear down
make k8s-down
```

### Makefile targets

| Target | Description |
|--------|-------------|
| `k8s-up` | Full stack: start, build, deploy |
| `k8s-down` | Delete the canon namespace |
| `k8s-build` | Cross-compile + docker build + minikube load |
| `k8s-deploy` | Apply kustomize manifests |
| `k8s-status` | Show pod/job/service status |
| `k8s-logs` | Tail all canon pod logs |
| `k8s-tunnel` | Expose LoadBalancer services |
| `k8s-restart` | Rollout restart app deployments |
| `k8s-clean` | Delete namespace + stop minikube |
| `k8s-test-e2e` | Run Playwright smoke tests |

## GKE (production)

The demo runs on GKE at `https://canon.mopjones.com`.

### Cluster

- **Cluster**: `canon-demo` in `europe-west2-a`
- **Node pool**: 1 preemptible `e2-standard-4` node
- **Image registry**: `europe-west2-docker.pkg.dev/canon-demo-prod/canon/`

### Deploying

```bash
# Build, push, and deploy
cd canon-demo
make gke-build-push   # cross-compile x86_64, push to Artifact Registry
make gke-deploy       # apply GKE overlay

# Or automated: merging to main triggers GitHub Actions
```

### Access

- Frontend: `https://canon.mopjones.com`
- API: `https://canon.mopjones.com/health`
- WebSocket: `wss://canon.mopjones.com/events`
- Docs: `https://canon.mopjones.com/docs`

### Kustomize structure

```
canon-demo/k8s/
  base/                  # shared manifests
  overlays/
    minikube/            # imagePullPolicy: Never, local images
    gke/
      shared/            # Artifact Registry image refs
      prod/              # production overlay
      staging/           # staging overlay
```

The GKE overlay removes local infrastructure (YugabyteDB, Cassandra, Kafka StatefulSets)
because infrastructure runs in a separate `canon-infra` namespace on GKE.

## Game bootstrap

The gateway automatically bootstraps the demo state on startup:

1. Registers 4 stations with capacity settings
2. Seeds initial stock levels
3. Registers the VSS Meridian ship

Bootstrap is idempotent -- safe on every gateway restart. It checks if data already
exists before inserting.
