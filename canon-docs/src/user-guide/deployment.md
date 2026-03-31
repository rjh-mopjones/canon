# Deployment

This chapter covers the full lifecycle of building, deploying, and running Canon
services -- from local development with minikube to production on GKE.

---

## ServiceBuilder

`ServiceBuilder` is the entry point for wiring a Canon service. It auto-discovers
all registered aggregates, handlers, and projections via `inventory`:

```rust
let service = ServiceBuilder::new()
    .for_aggregate::<Ship>()
    .subscribe_to("canon.navigation.events")
    .subscribe_to("canon.supply.events")
    .build();

service.start().await;
```

`service.start()` spawns all background tasks as `tokio::spawn` workers with
graceful shutdown via a `watch` channel:

| Task | Purpose |
|------|---------|
| Outbox processor | Drains outbox to outbound Kafka queue |
| Event store consumer | Writes to Cassandra, creates snapshots when `version % N == 0` |
| Projection consumer | Updates materialised read models in YugabyteDB |
| Publisher consumer | Publishes to `canon.{service}.events` for other services |
| Internal event consumer | Routes own events back to inbox for event handler dispatch |

Each consumer uses the `ConsumerReceiver` trait to poll the outbound queue
independently.

---

## Build pipeline overview

Canon services are cross-compiled from macOS to Linux, producing static musl
binaries. Docker images are slim alpine containers that just copy the pre-built
binary -- no Rust compilation happens inside Docker.

```
cargo build --release --target aarch64-unknown-linux-musl   ~2 min
docker build (alpine + COPY binary)                          ~2s each
minikube image load                                          ~5s each
```

The frontend (Leptos WASM) still builds inside Docker via Trunk. The init-schema
job builds inside Docker as well (it is a small shell script with psql/cqlsh).

---

## Prerequisites (one-time setup)

### Rust musl target

```bash
# Add the Linux musl target for cross-compilation
rustup target add aarch64-unknown-linux-musl
```

### musl cross-compiler (macOS)

```bash
# Install the musl cross-compiler toolchain
brew install filosottile/musl-cross/musl-cross
```

This provides `aarch64-linux-musl-gcc`, which cargo uses as the linker when
targeting `aarch64-unknown-linux-musl`. Your `~/.cargo/config.toml` should
contain:

```toml
[target.aarch64-unknown-linux-musl]
linker = "aarch64-linux-musl-gcc"
```

### GKE target (x86_64)

For GKE deployment, you also need the x86_64 musl target:

```bash
rustup target add x86_64-unknown-linux-musl
brew install filosottile/musl-cross/musl-cross --with-x86-64
```

### Minikube

```bash
# macOS
brew install minikube kubectl
```

### Playwright (for e2e tests)

```bash
cd canon-demo/e2e
npm install
npx playwright install chromium --with-deps
```

---

## Cross-compilation

Backend services are cross-compiled locally. This is significantly faster than
compiling inside Docker and produces smaller, reproducible images.

### Minikube (ARM64 on Apple Silicon)

```bash
cargo build --release --target aarch64-unknown-linux-musl \
    -p fleet-service \
    -p cargo-service \
    -p navigation-service \
    -p supply-service \
    -p station-service \
    -p gateway
```

### GKE (x86_64)

```bash
cargo build --release --target x86_64-unknown-linux-musl \
    -p fleet-service \
    -p cargo-service \
    -p navigation-service \
    -p supply-service \
    -p station-service \
    -p gateway
```

The Makefile handles this automatically via the `CROSS_TARGET` and
`GKE_CROSS_TARGET` variables.

---

## Docker images

Each service has a minimal Dockerfile that copies the pre-built binary into
a slim alpine image:

```dockerfile
FROM alpine:3.19
ARG BINARY_PATH
COPY ${BINARY_PATH} /usr/local/bin/service
CMD ["service"]
```

Build time is approximately 2 seconds per service since no compilation occurs
inside the container.

### Building for minikube

When targeting minikube, images are built using the minikube Docker daemon
directly. Always `eval $(minikube docker-env)` before building, or use the
Makefile which handles this automatically:

```bash
# The Makefile builds all images and loads them into minikube
make k8s-build
```

Under the hood, this runs:

```bash
# Cross-compile all services
cargo build --release --target aarch64-unknown-linux-musl \
    -p fleet-service -p cargo-service -p navigation-service \
    -p supply-service -p station-service -p gateway

# Build Docker images
docker build -t canon-demo/fleet-service \
    --build-arg BINARY_PATH=target/aarch64-unknown-linux-musl/release/fleet-service \
    -f fleet-service/Dockerfile ../.

# ... repeat for each service ...

# Build frontend (WASM, built inside Docker)
docker build -t canon-demo/frontend -f frontend/Dockerfile ../.

# Build init-schema job
docker build -t canon-demo/init-schema init-schema/

# Load all images into minikube
minikube image load canon-demo/fleet-service
minikube image load canon-demo/frontend
# ... etc ...
```

### Building for GKE

GKE images are pushed to Artifact Registry:

```bash
make gke-build-push
```

This cross-compiles for x86_64, builds with `--no-cache` (to avoid stale
layers from cached minikube builds), tags with the registry prefix, and pushes:

```bash
docker build --no-cache \
    -t europe-west2-docker.pkg.dev/canon-demo-prod/canon/fleet-service:latest \
    --build-arg BINARY_PATH=target/x86_64-unknown-linux-musl/release/fleet-service \
    -f fleet-service/Dockerfile ../.

docker push europe-west2-docker.pkg.dev/canon-demo-prod/canon/fleet-service:latest
```

---

## Kubernetes architecture

All pods run in a single `canon` namespace (minikube) or separate
`canon-prod`/`canon-staging`/`canon-infra` namespaces (GKE).

### Infrastructure (StatefulSets with PVCs)

| Component | Type | Purpose |
|-----------|------|---------|
| YugabyteDB | StatefulSet | Transactional store (commands, outbox, inbox, projections, dead letters) |
| Cassandra | StatefulSet | Event store (append-only, LWT for concurrency) |
| Zookeeper | StatefulSet | Kafka dependency (metadata management) |
| Kafka | StatefulSet | Message transport (inbound, outbound, events topics) |

All infrastructure runs with PersistentVolumeClaims for data durability across
pod restarts.

### Init jobs

Two Kubernetes Jobs run before application services start:

**1. init-schema** -- creates all YugabyteDB schemas, tables, sequences, and
indexes for each service. Also creates Cassandra keyspaces and event tables.

**2. init-kafka-topics** -- creates all 15 Kafka topics explicitly. Canon does
not use Kafka auto-topic-creation.

The `k8s-deploy` target waits for both jobs to complete before starting
application pods:

```bash
kubectl wait --for=condition=complete job/init-schema -n canon --timeout=120s
kubectl wait --for=condition=complete job/init-kafka-topics -n canon --timeout=120s
```

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
| canon-site | Landing page (static) | 80 |
| canon-docs | mdBook documentation | 80 |

The 5 Canon services are background processors with no exposed ports -- they
communicate exclusively through Kafka and YugabyteDB. Only gateway, frontend,
canon-site, and canon-docs have Kubernetes Services with exposed ports.

### Kustomize structure

```
canon-demo/k8s/
  base/                          # Shared manifests
    namespace.yaml               # canon namespace
    config.yaml                  # ConfigMap with env vars
    infra/
      yugabytedb.yaml            # StatefulSet + Service + PVC
      cassandra.yaml             # StatefulSet + Service + PVC
      zookeeper.yaml             # StatefulSet + Service
      kafka.yaml                 # StatefulSet + Service + PVC
    jobs/
      init-schema.yaml           # Job: create DB schemas + keyspaces
      init-kafka-topics.yaml     # Job: create all 15 Kafka topics
    services/
      fleet-service.yaml         # Deployment (no Service)
      cargo-service.yaml
      navigation-service.yaml
      supply-service.yaml
      station-service.yaml
    gateway.yaml                 # Deployment + Service (port 8080)
    frontend.yaml                # Deployment + Service (port 80)
    canon-site.yaml              # Deployment + Service (port 80)
    canon-docs.yaml              # Deployment + Service (port 80)
    kustomization.yaml           # Base kustomization
  overlays/
    minikube/                    # imagePullPolicy: Never, local images
    gke/
      shared/                    # Artifact Registry image refs
      prod/                      # Production overlay (canon-prod namespace)
      staging/                   # Staging overlay (canon-staging namespace)
      infra-policies/            # Network policies for canon-infra
```

All resources carry the label `app.kubernetes.io/part-of: canon-demo` for
unified log tailing and management.

---

## Local development with minikube

### Full stack deploy

```bash
cd canon-demo

# One command: start minikube, cross-compile, build images, deploy
make k8s-up

# In a separate terminal, expose services:
make k8s-tunnel
# Frontend at http://localhost:80
# Gateway at http://localhost:8080
```

### Step-by-step deploy

```bash
# 1. Start minikube with sufficient resources
minikube start --cpus=4 --memory=8g

# 2. Cross-compile and build Docker images
make k8s-build

# 3. Apply Kustomize manifests
make k8s-deploy
# This waits for infra pods, init jobs, then app pods

# 4. Expose LoadBalancer services
make k8s-tunnel
```

### Monitoring

```bash
# Check pod, job, service, and statefulset status
make k8s-status

# Tail logs from all canon pods
make k8s-logs

# Watch a specific service
kubectl logs -f deployment/fleet-service -n canon
kubectl logs -f deployment/gateway -n canon
```

### Restarting services

```bash
# Restart all application deployments (keeps infra running)
make k8s-restart

# Restart a specific service
kubectl rollout restart deployment/fleet-service -n canon
```

### Teardown

```bash
# Delete the canon namespace (keeps minikube running)
make k8s-down

# Full cleanup: delete namespace and stop minikube
make k8s-clean
```

---

## Makefile targets reference

### Minikube targets

| Target | Description |
|--------|-------------|
| `k8s-up` | Full stack: start minikube, build images, deploy, wait for ready |
| `k8s-start` | Start minikube if not already running |
| `k8s-build` | Cross-compile all services, build Docker images, load into minikube |
| `k8s-deploy` | Apply Kustomize manifests, wait for infra, jobs, and app pods |
| `k8s-down` | Delete the canon namespace |
| `k8s-status` | Show pods, jobs, services, and statefulsets |
| `k8s-logs` | Tail logs from all canon pods (up to 20 concurrent streams) |
| `k8s-tunnel` | Expose LoadBalancer services via `minikube tunnel` |
| `k8s-restart` | Rollout restart all app deployments (keeps infra running) |
| `k8s-clean` | Delete namespace + stop minikube |
| `k8s-test-e2e` | Run Playwright smoke tests |
| `k8s-test-supply-chain` | Run full supply chain loop test (4 legs) |
| `k8s-test-multi-tab` | Run multi-tab session isolation test |
| `k8s-test-stress` | Run stress test (3 tabs x 2 rounds by default) |
| `k8s-test-all` | Run all e2e tests in sequence |

### GKE targets

| Target | Description |
|--------|-------------|
| `gke-build-push` | Cross-compile x86_64, build images, push to Artifact Registry |
| `gke-deploy` | Deploy both prod and staging + infra network policies |
| `gke-deploy-prod` | Apply prod overlay, wait for all rollouts in `canon-prod` |
| `gke-deploy-staging` | Apply staging overlay, wait for all rollouts in `canon-staging` |
| `gke-status` | Show pods, services, and ingress across all GKE namespaces |
| `gke-logs-prod` | Tail application logs in `canon-prod` |
| `gke-logs-staging` | Tail application logs in `canon-staging` |
| `gke-restart-prod` | Rollout restart all app deployments in `canon-prod` |
| `gke-restart-staging` | Rollout restart all app deployments in `canon-staging` |
| `gke-monitoring-setup` | Create uptime checks + email alerts (requires `ALERT_EMAIL=`) |

### Configurable variables

| Variable | Default | Description |
|----------|---------|-------------|
| `MINIKUBE_CPUS` | 4 | CPU cores allocated to minikube |
| `MINIKUBE_MEMORY` | 8192 | Memory in MB allocated to minikube |
| `MINIKUBE_DRIVER` | docker | Minikube driver |
| `GKE_TAG` | latest | Docker image tag for GKE pushes |

---

## GKE production deployment

### Cluster details

- **Cluster name**: `canon-demo`
- **Region**: `europe-west2-a`
- **Node pool**: 1 preemptible `e2-standard-4` node
- **Image registry**: `europe-west2-docker.pkg.dev/canon-demo-prod/canon/`

### Namespaces

GKE uses three namespaces for isolation:

| Namespace | Purpose |
|-----------|---------|
| `canon-prod` | Production application pods |
| `canon-staging` | Staging application pods |
| `canon-infra` | Shared infrastructure (YugabyteDB, Cassandra, Kafka) |

Infrastructure runs in a separate namespace because it is shared between prod
and staging. Network policies in `infra-policies/` control which namespaces
can access which infrastructure services.

### Deploying to GKE

```bash
cd canon-demo

# Build, push, and deploy everything
make gke-build-push
make gke-deploy

# Or deploy just prod:
make gke-deploy-prod

# Or deploy just staging:
make gke-deploy-staging
```

### Automated deployment

Merging to `main` automatically deploys via GitHub Actions. The CI pipeline:

1. Cross-compiles all services for x86_64
2. Builds Docker images with `--no-cache`
3. Pushes to Artifact Registry
4. Applies the GKE prod overlay
5. Waits for all rollouts to complete

### Access

- **Frontend**: `https://canon.mopjones.com`
- **API**: `https://canon.mopjones.com/health`, `/fleet/ships`, `/stations`, etc.
- **WebSocket**: `wss://canon.mopjones.com/events`
- **Docs**: `https://canon.mopjones.com/docs`

### Authentication

The gateway has an optional auth gate controlled by the `CANON_AUTH_PASSWORD`
environment variable. When set, all requests require authentication.

**Two authentication methods:**

1. **Header auth**: `X-Canon-Auth: <password>` (sets a cookie for subsequent
   browser requests)
2. **Debug key**: `X-Canon-Debug: <key>` (bypasses all auth, for CLI debugging)

**CLI usage:**

```bash
# Check health with debug key:
curl -H "X-Canon-Debug: $(cat ~/.canon-debug-key)" \
    https://canon.mopjones.com/health

# Run Playwright against live site with auth:
CANON_AUTH_PASSWORD=$(cat ~/.canon-auth-password) npx playwright test
```

**Toggle public/private:**

```bash
# Lock down (require password):
kubectl set env deployment/gateway -n canon-prod CANON_AUTH_PASSWORD=<password>

# Go public (remove auth gate):
kubectl set env deployment/gateway -n canon-prod CANON_AUTH_PASSWORD-
```

### Monitoring

```bash
# Set up uptime checks and email alerts
make gke-monitoring-setup ALERT_EMAIL=you@example.com
```

This creates:
- Uptime check for `canon.mopjones.com/health` (prod, every 5 minutes)
- Uptime check for `canon-staging.mopjones.com/health` (staging, every 5 minutes)
- Alert policies that email when either check fails for 5 minutes

---

## Environment variables

Application pods receive their configuration via a Kubernetes ConfigMap:

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: canon-config
  namespace: canon
data:
  CASSANDRA_NODES: "cassandra:9042"
  YUGABYTE_URL: "postgres://canon:canon@yugabytedb:5433/canon"
  KAFKA_BROKERS: "kafka:9092"
```

Each service also sets service-specific variables:

| Variable | Example | Purpose |
|----------|---------|---------|
| `CASSANDRA_NODES` | `cassandra:9042` | Cassandra contact points |
| `CASSANDRA_KEYSPACE` | `canon_fleet` | Per-service Cassandra keyspace |
| `YUGABYTE_URL` | `postgres://canon:canon@yugabytedb:5433/canon` | YugabyteDB connection |
| `KAFKA_BROKERS` | `kafka:9092` | Kafka broker list |
| `SERVICE_NAME` | `fleet` | Service identifier for topic derivation |
| `RUST_LOG` | `info,canon=debug` | Log level configuration |
| `CANON_AUTH_PASSWORD` | (secret) | Optional auth gate password |
| `CANON_DEBUG_KEY` | (secret) | Optional debug API key |

Secrets (`CANON_AUTH_PASSWORD`, `CANON_DEBUG_KEY`) must be stored in Kubernetes
Secrets, never in committed YAML. The `canon:canon` default password is
acceptable for local development ConfigMaps only.

---

## Game bootstrap

The gateway automatically bootstraps the demo game state on startup. This runs
before any user interaction is possible.

### Bootstrap sequence

1. **Register stations** (if not already registered):
   - Alpha Depot -- 5000kg capacity
   - Beta Relay -- 3000kg capacity
   - Gamma Outpost -- 2000kg capacity
   - Delta Prime -- 4000kg capacity

2. **Seed initial stock** via `RecordCargoReceived`:
   - Alpha Depot: 85% of capacity (4250kg)
   - Beta Relay: 60% of capacity (1800kg)
   - Gamma Outpost: 40% of capacity (800kg)
   - Delta Prime: 75% of capacity (3000kg)

3. **Register the VSS Meridian ship** (if not already registered):
   - 5000kg cargo capacity

### Idempotency

Bootstrap is idempotent -- safe on every gateway restart. Each step checks if
the data already exists before inserting. This means:

- Crashing mid-bootstrap and restarting will complete the remaining steps
- Multiple gateway replicas will not create duplicate stations or ships
- Restarting the gateway after schema changes will not corrupt existing data

### Stock drain

After a 15-second delay (to allow bootstrap and pipeline processing to
complete), the gateway starts a background stock drain task. Each station's
stock decreases at a fixed rate per 3-second tick:

| Station | Drain rate per tick | Starting stock |
|---------|-------------------|----------------|
| Alpha Depot | 0.15 | 85% |
| Beta Relay | 0.20 | 60% |
| Gamma Outpost | 0.25 | 40% |
| Delta Prime | 0.18 | 75% |

The user must load and deliver supplies via the ship to keep stations alive.
A station hitting 0% stock triggers game over.

---

## End-to-end testing

### Playwright smoke tests

After deploying the full stack, run the Playwright e2e tests:

```bash
cd canon-demo
make k8s-test-e2e
```

These tests verify:
- Stations have stock and stock levels are displayed
- The ship can fly between stations
- Events appear in the event log
- Scenarios page renders correctly
- WebSocket events flow correctly

### Supply chain loop test

```bash
make k8s-test-supply-chain
```

Tests the full supply chain loop: Alpha -> Beta -> Gamma -> Delta -> Alpha,
verifying that cargo loads, delivers, and stock levels change correctly.

### Multi-tab isolation test

```bash
make k8s-test-multi-tab
```

Verifies that multiple browser tabs (sessions) operate independently and
do not interfere with each other.

### Stress test

```bash
make k8s-test-stress
```

Runs concurrent browser sessions performing rapid operations to test
pipeline throughput and idempotency under load.

### Running all tests

```bash
make k8s-test-all
```

Runs smoke tests, supply chain test, and multi-tab test in sequence.

---

## Debugging

### Check API before infrastructure

When the UI shows broken state, always check the API first:

```bash
# Create a session and check game state
curl -s https://canon.mopjones.com/health
curl -s https://canon.mopjones.com/ships | python3 -m json.tool
curl -s https://canon.mopjones.com/stations | python3 -m json.tool
```

If the API returns correct data, the bug is in the frontend. Do not touch
Kafka, YugabyteDB, or Cassandra until you have confirmed the API itself
returns wrong data.

### Service logs

```bash
# Tail all service logs
make k8s-logs

# Or GKE prod:
make gke-logs-prod

# Specific service:
kubectl logs -f deployment/fleet-service -n canon
kubectl logs -f deployment/gateway -n canon
```

Look for `ERROR`, `command processing failed`, `OffsetOutOfRange`.

### Pod status

```bash
# Minikube:
make k8s-status

# GKE:
make gke-status
```

Check for pods in `CrashLoopBackOff`, `OOMKilled`, or `Pending` state.

### Common issues

| Symptom | Likely cause | Fix |
|---------|-------------|-----|
| Pods stuck in `Pending` | Insufficient resources | Increase minikube memory/CPU |
| Init jobs never complete | Infrastructure not ready | Check StatefulSet pods are Running |
| Services crash on start | Schema not created | Check init-schema job logs |
| Events not flowing | Kafka topics missing | Check init-kafka-topics job logs |
| Version conflicts | Concurrent commands | Normal -- retry mechanism handles this |
| Stale state after restart | Offset reset to 0 | Normal -- idempotency catches up |

### Kafka topic inspection

```bash
# List topics (from inside a Kafka pod)
kubectl exec -it kafka-0 -n canon -- \
    kafka-topics.sh --list --bootstrap-server localhost:9092

# Consume from a topic
kubectl exec -it kafka-0 -n canon -- \
    kafka-console-consumer.sh \
        --bootstrap-server localhost:9092 \
        --topic canon.fleet.outbound \
        --from-beginning
```

### Database inspection

```bash
# YugabyteDB (psql)
kubectl exec -it yugabytedb-0 -n canon -- \
    ysqlsh -U canon -d canon -c \
    "SELECT count(*) FROM canon_fleet.outbox WHERE delivered_at IS NULL;"

# Cassandra (cqlsh)
kubectl exec -it cassandra-0 -n canon -- \
    cqlsh -e \
    "SELECT count(*) FROM canon_fleet.events;"
```
