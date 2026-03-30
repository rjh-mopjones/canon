# Projections

Projections are read models -- materialised views built from the event stream. In a
CQRS (Command Query Responsibility Segregation) architecture, the write side produces
events and the read side consumes them to build queryable representations of domain
state. Projections are the read side.

A single event stream can feed many projections, each optimised for a different read
pattern. The station-service, for example, builds a `StationInventory` projection
that tracks stock levels per station -- a flat, denormalised view that can be queried
with a single `SELECT` instead of replaying hundreds of events.

---

## Why projections exist

Event sourcing stores state as a sequence of events, not as a mutable row. This is
powerful for auditing, debugging, and temporal queries, but it makes reads expensive:
to answer "what is the current stock at Alpha Station?", you would need to replay
every `CargoReceived`, `StockDrained`, and `StationRegistered` event for that station.

Projections solve this by maintaining a pre-computed read model that is updated
incrementally as each event arrives. The read model is eventually consistent -- there
is a brief propagation delay between when an event is committed and when the projection
reflects it -- but reads become a single query.

Canon provides two read strategies:

- **Read-ready (materialised view)** -- a persistent read model maintained by the
  projection consumer. Events update it as they flow through the outbound queue.
  Fast reads, eventually consistent.

- **Read-through (computed on demand)** -- no persistent read model. State is computed
  by replaying the event stream on each request. Always consistent, expensive at scale.

Both strategies use the same `Projection` trait. The difference is in the
`ProjectionStore` implementation and how the gateway queries the data.

---

## The Projection trait

The core trait that all projections implement:

```rust
#[async_trait]
pub trait Projection: Send + Sync + 'static {
    type Event: Send + Sync;
    type Store: ProjectionStore;
    type Error: std::error::Error + Send + Sync + 'static;

    async fn apply(
        &self,
        event: &Self::Event,
        store: &Self::Store,
    ) -> Result<(), Self::Error>;

    async fn rebuild(
        &self,
        events: impl Stream<Item = Self::Event> + Send,
        store: &Self::Store,
    ) -> Result<(), Self::Error>;

    fn projection_id(&self) -> &str;
}
```

You never implement this trait directly. Instead, use `#[projection]` and
`#[projection_handler]`. The macros generate the trait implementation, serde
derives, `Default`, and `inventory` registration.

The three associated types:

- **`Event`** -- the event type this projection consumes. The projection consumer
  deserialises `EventEnvelope` payloads into this type before calling `apply`.
- **`Store`** -- the storage backend. Must implement `ProjectionStore`. In-memory
  for tests, YugabyteDB for production.
- **`Error`** -- the projection's error type, using `thiserror`.

---

## The ProjectionHandler trait

Each `#[projection_handler]` applies one event type to the projection's read model:

```rust
pub trait ProjectionHandler<P>: Send + Sync + 'static {
    type Event: Send + Sync;
    fn apply(&self, event: &Self::Event, store: &mut P);
}
```

This is the synchronous, inner apply function. It receives a reference to the event
and a mutable reference to the projection struct, and updates the read model in place.
The projection consumer calls these handlers as events arrive from the outbound queue.

---

## Defining a projection

### Step 1: Declare the projection struct

The `#[projection]` macro marks a struct as a projection read model. Here is the
`StationInventory` projection from `canon-demo/station-service/src/projection.rs`:

```rust
use std::collections::HashMap;
use uuid::Uuid;
use chrono::{DateTime, Utc};

#[canon_core::projection]
pub struct StationInventory {
    pub stations: HashMap<Uuid, StationInventoryRow>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct StationInventoryRow {
    pub station_id: Uuid,
    pub name: String,
    pub capacity_kg: f32,
    pub current_stock_kg: f32,
    pub last_docking: Option<DateTime<Utc>>,
    pub offline: bool,
    pub updated_at: DateTime<Utc>,
}
```

The projection struct holds the in-memory representation of the read model. For
the `StationInventory`, this is a `HashMap` from station ID to a row struct. In
production, the projection store persists this to YugabyteDB as JSONB.

The `#[projection]` macro generates:

- Serde derives for serialisation
- A `projection_id()` method derived from the struct name
- `inventory` registration so `ServiceBuilder` discovers it automatically

### Step 2: Define handlers for each event type

Each event that affects the read model gets its own `#[projection_handler]`. Here
are the handlers for the `StationInventory` projection:

```rust
use crate::events::{
    StationRegistered, ShipDocked, CargoReceived,
    CapacityUpdated, StationOffline, StockDrained,
};

#[canon_core::projection_handler(StationInventory)]
impl StationRegisteredProjectionHandler {
    fn apply(&self, event: &StationRegistered, store: &mut StationInventory) {
        let now = Utc::now();
        store.stations.insert(
            event.station_id,
            StationInventoryRow {
                station_id: event.station_id,
                name: event.name.clone(),
                capacity_kg: event.capacity_kg,
                current_stock_kg: 0.0,
                last_docking: None,
                offline: false,
                updated_at: now,
            },
        );
    }
}

#[canon_core::projection_handler(StationInventory)]
impl ShipDockedProjectionHandler {
    fn apply(&self, event: &ShipDocked, store: &mut StationInventory) {
        if let Some(row) = store.stations.get_mut(&event.station_id) {
            row.last_docking = Some(Utc::now());
            row.updated_at = Utc::now();
        }
    }
}

#[canon_core::projection_handler(StationInventory)]
impl CargoReceivedProjectionHandler {
    fn apply(&self, event: &CargoReceived, store: &mut StationInventory) {
        if let Some(row) = store.stations.get_mut(&event.station_id) {
            row.current_stock_kg += event.weight_kg;
            row.updated_at = Utc::now();
        }
    }
}

#[canon_core::projection_handler(StationInventory)]
impl StockDrainedProjectionHandler {
    fn apply(&self, event: &StockDrained, store: &mut StationInventory) {
        if let Some(row) = store.stations.get_mut(&event.station_id) {
            row.current_stock_kg = event.remaining_kg;
            row.updated_at = Utc::now();
        }
    }
}

#[canon_core::projection_handler(StationInventory)]
impl StationOfflineProjectionHandler {
    fn apply(&self, event: &StationOffline, store: &mut StationInventory) {
        if let Some(row) = store.stations.get_mut(&event.station_id) {
            row.offline = true;
            row.current_stock_kg = 0.0;
            row.updated_at = Utc::now();
        }
    }
}
```

Notice several patterns:

- **Guard clauses** -- `ShipDockedProjectionHandler` checks `if let Some(row)` before
  updating. If a `ShipDocked` event arrives for a station that has not been registered
  yet, the handler is a no-op rather than a panic.

- **Absolute values** -- `StockDrainedProjectionHandler` sets `current_stock_kg` to
  `event.remaining_kg` rather than subtracting `event.drain_kg`. This makes the handler
  naturally idempotent: applying the same event twice produces the same result.

- **Insert for creation, update for mutation** -- `StationRegisteredProjectionHandler`
  uses `insert`, which overwrites any existing entry for that station ID. Subsequent
  handlers use `get_mut` to modify the existing row.

---

## Another example: ShipReadModel

The fleet-service has a simpler projection that tracks ship state. From
`canon-demo/fleet-service/src/projection.rs`:

```rust
#[canon_core::projection]
pub struct ShipReadModel {
    pub ship_id: uuid::Uuid,
    pub name: String,
    pub capacity_kg: f32,
    pub status: String,
    pub fuel_kg: f32,
}

#[canon_core::projection_handler(ShipReadModel)]
impl ShipRegisteredProjectionHandler {
    fn apply(&self, event: &ShipRegistered, store: &mut ShipReadModel) {
        store.ship_id = event.ship_id;
        store.name = event.name.clone();
        store.capacity_kg = event.capacity_kg;
        store.status = "Docked".to_string();
        store.fuel_kg = event.capacity_kg;
    }
}

#[canon_core::projection_handler(ShipReadModel)]
impl ShipDepartedProjectionHandler {
    fn apply(&self, event: &ShipDeparted, store: &mut ShipReadModel) {
        let _ = event;
        store.status = "InTransit".to_string();
    }
}

#[canon_core::projection_handler(ShipReadModel)]
impl ResupplyScheduledProjectionHandler {
    fn apply(&self, event: &ResupplyScheduled, store: &mut ShipReadModel) {
        store.fuel_kg = event.fuel_kg;
    }
}

#[canon_core::projection_handler(ShipReadModel)]
impl ShipDecommissionedProjectionHandler {
    fn apply(&self, event: &ShipDecommissioned, store: &mut ShipReadModel) {
        let _ = event;
        store.status = "Decommissioned".to_string();
    }
}
```

This projection is even simpler: it tracks a single ship's current state. The
gateway queries it to serve `GET /ships` responses without replaying events.

---

## A third example: ManifestReadModel

The cargo-service projection tracks cargo manifests. From
`canon-demo/cargo-service/src/projection.rs`:

```rust
#[canon_core::projection]
pub struct ManifestReadModel {
    pub manifest_id: Uuid,
    pub ship_id: Uuid,
    pub status: String,
    pub total_weight_kg: u32,
}

#[canon_core::projection_handler(ManifestReadModel)]
impl ManifestCreatedProjectionHandler {
    fn apply(&self, event: &ManifestCreated, store: &mut ManifestReadModel) {
        store.manifest_id = event.manifest_id;
        store.ship_id = event.ship_id;
        store.status = "Open".to_string();
        store.total_weight_kg = 0;
    }
}

#[canon_core::projection_handler(ManifestReadModel)]
impl CargoLoadedProjectionHandler {
    fn apply(&self, event: &CargoLoaded, store: &mut ManifestReadModel) {
        store.total_weight_kg += event.weight_kg.max(0.0).round() as u32;
    }
}

#[canon_core::projection_handler(ManifestReadModel)]
impl ManifestClosedProjectionHandler {
    fn apply(&self, event: &ManifestClosed, store: &mut ManifestReadModel) {
        let _ = event;
        store.status = "Closed".to_string();
    }
}
```

This projection accumulates cargo weight across `CargoLoaded` events and tracks
the manifest lifecycle through status transitions.

---

## Idempotency requirement

Projection handlers **must be idempotent** -- applying the same event twice must
produce the same result or be safely skippable. This is essential because:

- **Consumers restart from offset zero.** Canon uses application-layer idempotency
  as the safety net. On every restart, consumers replay from the beginning of the
  outbound Kafka topic and skip already-processed events via checkpoint comparison.

- **Kafka delivers at-least-once.** Network retries and consumer restarts can
  cause the same event to be delivered more than once.

- **Projection rebuilds replay all events.** When a projection is rebuilt (see
  below), every event is replayed from scratch through the handlers.

### Patterns for idempotent projections

**Set, do not increment** -- store the absolute value rather than applying a delta.
The `StockDrainedProjectionHandler` above sets `current_stock_kg = event.remaining_kg`
rather than `current_stock_kg -= event.drain_kg`. Applying the same drain event twice
still results in the correct remaining stock.

**Upsert** -- in SQL projections, use `INSERT ... ON CONFLICT UPDATE` rather than
plain `INSERT`. The YugabyteDB projection store uses this pattern:

```sql
INSERT INTO projections (projection_id, aggregate_id, state, updated_at)
VALUES ($1, $2, $3, now())
ON CONFLICT (projection_id, aggregate_id)
DO UPDATE SET state = EXCLUDED.state, updated_at = now()
```

**Checkpoint gating** -- the projection consumer tracks a `last_version` checkpoint.
Events at or before the checkpoint are skipped automatically. This is the framework's
first line of defence against duplicates.

**Guard clauses for missing state** -- handlers like `ShipDockedProjectionHandler`
check `if let Some(row)` before updating. If the projection is being rebuilt and
events arrive out of order relative to the creation event, the handler gracefully
skips rather than panicking.

---

## ProjectionStore trait and implementations

The `ProjectionStore` trait is a marker trait for projection storage backends:

```rust
pub trait ProjectionStore: Send + Sync + 'static {}
```

Actual data access is handled by the `ProjectionCheckpointStore` trait, which manages
checkpoints for tracking processing progress:

```rust
#[async_trait]
pub trait ProjectionCheckpointStore: Send + Sync + 'static {
    type Error: std::error::Error + Send + Sync + 'static;

    async fn get_checkpoint(
        &self,
        projection_id: &str,
    ) -> Result<Version, Self::Error>;

    async fn set_checkpoint(
        &self,
        projection_id: &str,
        version: Version,
    ) -> Result<(), Self::Error>;
}
```

### InMemoryProjectionStore

Used in `canon-test` and unit tests. Stores checkpoints in a `HashMap<String, ProjectionCheckpoint>`
behind an `Arc<Mutex<...>>`. No persistence -- state is lost when the process exits.

```rust
use canon_core::memory::InMemoryProjectionStore;

let store = InMemoryProjectionStore::new();

// Checkpoint starts at Version::initial() (0)
let v = store.get_checkpoint_sync("inventory").unwrap();
assert_eq!(v, Version::initial());

// Advance the checkpoint
store.set_checkpoint_sync("inventory", Version::from_u64(42)).unwrap();
```

### YugabyteProjectionStore

The production implementation. Persists projection state as JSONB and checkpoints
in the `projection_checkpoints` table. Created from a connection pool or
environment variable:

```rust
use canon_projection_store_yugabyte::YugabyteProjectionStore;

// From an existing pool (recommended -- pool is shared with other stores)
let store = YugabyteProjectionStore::from_pool(yugabyte_pool.clone());

// From a URL
let store = YugabyteProjectionStore::new("postgres://canon:canon@localhost:5433/canon").await?;

// From the YUGABYTE_URL environment variable
let store = YugabyteProjectionStore::from_env().await?;
```

The store provides full CRUD operations:

- `upsert(projection_id, aggregate_id, state)` -- insert or update a projection row
- `load(projection_id, aggregate_id)` -- load a projection row as bytes
- `update_last_version(projection_id, version)` -- advance the checkpoint
- `get_last_version(projection_id)` -- read the current checkpoint
- `set_rebuilding(projection_id, rebuilding)` -- toggle the rebuilding flag
- `is_rebuilding(projection_id)` -- check if a rebuild is in progress
- `reset_checkpoint(projection_id, target)` -- reset checkpoint and set `rebuilding = true`

---

## Projection checkpoints

Each projection tracks its processing progress via a checkpoint -- the sequence number
of the last event it successfully applied. The checkpoint is stored in the
`projection_checkpoints` table:

```sql
CREATE TABLE projection_checkpoints (
    projection_id TEXT PRIMARY KEY,
    last_version  BIGINT  NOT NULL DEFAULT 0,
    rebuilding    BOOLEAN NOT NULL DEFAULT FALSE,
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

The checkpoint uses a **global sequence number**, not a per-aggregate version. This
is a critical design decision. The outbox assigns a monotonically increasing sequence
number to every event regardless of which aggregate produced it. If the checkpoint
used per-aggregate versions, cross-aggregate events would be silently skipped:
processing aggregate A at version 100 would advance the checkpoint to 100, causing
aggregate B's event at version 5 to be dropped because 5 <= 100.

The projection consumer's processing loop:

1. Receive an event envelope with its global sequence number.
2. For each registered projection, read its checkpoint.
3. If `sequence_number <= checkpoint`, skip the event (already processed).
4. Otherwise, call the projection's `apply` function.
5. Advance the checkpoint to `sequence_number`.

This makes replay safe: on restart, the consumer replays from offset zero, and the
checkpoint comparison skips all events that were already applied.

---

## The projection consumer

The projection consumer is one of four independent consumers on the outbound queue.
It runs as a background `tokio::spawn` task started by `service.start()`.

```
Outbox processor --> Outbound Kafka topic
                          |
                          +--> Event store consumer (Cassandra writes)
                          |
                          +--> Projection consumer (read model updates)  <-- this one
                          |
                          +--> Publisher consumer (cross-service events)
                          |
                          +--> Internal event consumer (own events back to inbox)
```

The consumer is generic over `ProjectionCheckpointStore`, so the same logic works
with both `InMemoryProjectionStore` (in tests) and `YugabyteProjectionStore`
(in production).

### Consumer lifecycle

The consumer runs in a loop:

```
receive event from outbound queue
    |
    v
for each registered projection:
    read checkpoint
    if sequence_number <= checkpoint: skip
    else: apply event, advance checkpoint
    |
    v
commit offset to receiver
    |
    v
loop (until shutdown signal)
```

If an apply function returns an error, the `on_error` callback is invoked but
the loop continues. The checkpoint is not advanced for failed events, so they
will be retried on the next restart.

If a receive error occurs (Kafka timeout, network issue), the consumer sleeps
briefly (50ms) before retrying to avoid tight error loops.

### Notification-driven wakeup

When the outbox processor publishes a new event, it can notify the projection
consumer via a `tokio::sync::Notify`. This reduces latency from the poll timeout
(typically 100ms) to near-zero:

```rust
// In service wiring:
let (notify_tx, notify_rx) = new_outbox_notify_channel(16);
// Pass notify_tx to the outbox processor, notify_rx to the consumers
```

Without notification, the consumer waits for the receiver's poll timeout before
checking for new events.

---

## Projection rebuild

When a projection needs to be rebuilt -- because the handler logic changed, the
schema evolved, or a bug was fixed -- Canon uses a Kafka offset reset mechanism.

### The rebuild flow

1. **Start the rebuild.** Call `start_rebuild(projection_id, rebuild_from)` on the
   `ProjectionRebuildManager`. This sets `rebuilding = true` in the checkpoint table
   and resets the checkpoint to `rebuild_from` (or `Version::initial()` for a full
   replay).

2. **Read endpoints fall back to read-through.** While `rebuilding == true`, any
   endpoint that queries this projection must not serve stale materialised data.
   Instead, it computes the answer by replaying events on demand. The gateway
   checks `is_rebuilding(projection_id)` before serving read-ready responses.

3. **Consumer replays events.** The projection consumer detects the reset checkpoint
   and replays events from the target offset. Each event is applied through the
   projection handlers, overwriting the stale read model.

4. **Complete the rebuild.** Once replay finishes, call `complete_rebuild(projection_id)`.
   This sets `rebuilding = false`, and read endpoints resume serving from the
   materialised view.

### Starting a rebuild

```rust
use canon_core::ProjectionRebuildManager;

// Full replay from the beginning
rebuild_manager.start_rebuild("station_inventory", None).await?;

// Partial replay from a specific version
rebuild_manager
    .start_rebuild("station_inventory", Some(Version::from_u64(500)))
    .await?;
```

Constraints enforced by the rebuild manager:

- Cannot start a rebuild while one is already in progress (`AlreadyRebuilding` error).
- Cannot set `rebuild_from` to a version ahead of the current checkpoint
  (`VersionAhead` error).
- Cannot complete a rebuild that is not in progress (`NotRebuilding` error).

### In-memory rebuild manager

For tests, `InMemoryProjectionRebuildManager` wraps an `InMemoryProjectionStore`:

```rust
use canon_core::memory::{
    InMemoryProjectionStore, InMemoryProjectionRebuildManager,
};

let store = InMemoryProjectionStore::new();
store.set_checkpoint_sync("inventory", Version::from_u64(100)).unwrap();

let manager = InMemoryProjectionRebuildManager::new(store);

// Start a full rebuild
manager.start_rebuild("inventory", None).await.unwrap();
assert!(manager.is_rebuilding("inventory").await.unwrap());
assert_eq!(
    manager.get_checkpoint("inventory").await.unwrap(),
    Version::initial(),
);

// ... consumer replays events ...

// Complete the rebuild
manager.complete_rebuild("inventory").await.unwrap();
assert!(!manager.is_rebuilding("inventory").await.unwrap());
```

### YugabyteDB rebuild

The `YugabyteProjectionStore` implements rebuild via SQL:

```rust
// reset_checkpoint sets rebuilding=true and resets last_version in one upsert
store.reset_checkpoint("station_inventory", Version::from_u64(0)).await?;

// After replay completes:
store.set_rebuilding("station_inventory", false).await?;
```

The SQL behind `reset_checkpoint`:

```sql
INSERT INTO projection_checkpoints
    (projection_id, last_version, rebuilding, updated_at)
VALUES ($1, $2, true, now())
ON CONFLICT (projection_id)
DO UPDATE SET last_version = EXCLUDED.last_version,
              rebuilding = true,
              updated_at = now()
```

---

## Read-through during rebuild

While a projection is rebuilding, the materialised view contains stale or incomplete
data. Read endpoints must detect this and fall back to computing the answer on demand.

The gateway checks the rebuilding flag before serving read-ready responses:

```rust
// Pseudocode for a read endpoint
async fn get_station_inventory(station_id: Uuid) -> Response {
    if projection_store.is_rebuilding("station_inventory").await? {
        // Rebuild in progress -- compute from event stream
        let events = event_store.load_events(station_id).await?;
        let state = replay_events(events);
        return Ok(Json(state));
    }

    // Normal path -- serve from materialised view
    let state = projection_store.load("station_inventory", station_id).await?;
    Ok(Json(state))
}
```

This ensures that clients always receive correct data, even during a rebuild.
The trade-off is that read-through is more expensive (it replays the event stream),
but it only applies during the rebuild window.

---

## Testing projections

Projection handlers are synchronous and pure, making them straightforward to test.
Here is an example from the station-service's test suite:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use canon_core::ProjectionHandler;

    #[test]
    fn station_registered_creates_row() {
        let handler = StationRegisteredProjectionHandler;
        let mut store = StationInventory::default();
        let station_id = Uuid::new_v4();
        let event = StationRegistered {
            station_id,
            name: "Alpha Station".to_string(),
            capacity_kg: 1000.0,
        };
        handler.apply(&event, &mut store);
        assert!(store.stations.contains_key(&station_id));
        let row = &store.stations[&station_id];
        assert_eq!(row.name, "Alpha Station");
        assert!((row.capacity_kg - 1000.0).abs() < f32::EPSILON);
    }

    #[test]
    fn station_registered_is_idempotent() {
        let handler = StationRegisteredProjectionHandler;
        let mut store = StationInventory::default();
        let station_id = Uuid::new_v4();
        let event = StationRegistered {
            station_id,
            name: "Alpha Station".to_string(),
            capacity_kg: 1000.0,
        };
        handler.apply(&event, &mut store);
        handler.apply(&event, &mut store);
        assert_eq!(store.stations.len(), 1);
    }

    #[test]
    fn ship_docked_on_unknown_station_is_noop() {
        let handler = ShipDockedProjectionHandler;
        let mut store = StationInventory::default();
        let event = ShipDocked {
            station_id: Uuid::new_v4(),
            ship_id: Uuid::new_v4(),
        };
        handler.apply(&event, &mut store);
        assert!(store.stations.is_empty());
    }

    #[test]
    fn stock_drained_updates_current_stock() {
        let handler_reg = StationRegisteredProjectionHandler;
        let handler_drain = StockDrainedProjectionHandler;
        let mut store = StationInventory::default();
        let station_id = Uuid::new_v4();

        handler_reg.apply(
            &StationRegistered {
                station_id,
                name: "Alpha".to_string(),
                capacity_kg: 5000.0,
            },
            &mut store,
        );

        handler_drain.apply(
            &StockDrained {
                station_id,
                drain_kg: 7.5,
                remaining_kg: 4242.5,
            },
            &mut store,
        );

        assert!(
            (store.stations[&station_id].current_stock_kg - 4242.5).abs()
                < f32::EPSILON
        );
    }
}
```

Key testing patterns:

- **Construct the handler directly** -- `StationRegisteredProjectionHandler` is a
  unit struct that can be instantiated inline.
- **Create a default projection** -- `StationInventory::default()` gives you an
  empty read model to work with.
- **Test idempotency explicitly** -- apply the same event twice and assert the
  result is unchanged.
- **Test guard clauses** -- verify that events for unknown entities are no-ops.
- **Use the `ProjectionHandler` trait import** -- the `apply` method comes from
  the trait, so import `canon_core::ProjectionHandler` in your test module.

---

## Wiring projections into ServiceBuilder

Projections are automatically discovered by `ServiceBuilder` via `inventory`.
You do not need to register them manually. Just declare the projection and its
handlers in your service's modules, and ensure those modules are declared in
`lib.rs`:

```rust
// lib.rs
pub mod projection;  // contains #[projection] and #[projection_handler]
```

In `main.rs`, provide the projection checkpoint store to `ServiceBuilder`:

```rust
use canon_projection_store_yugabyte::YugabyteProjectionStore;

let projection_store = YugabyteProjectionStore::from_pool(yugabyte_pool.clone());

let service = ServiceBuilder::new("station")
    .for_aggregate::<Station>()
    .event_store(event_store)
    .snapshot_store(snapshot_store)
    .outbox_store(outbox_store)
    .outbox_publisher(outbox_publisher)
    .projection_checkpoint_store(projection_store)  // <-- this
    .publisher(publisher)
    .dead_letter_store(dead_letter_store)
    .retry_tracker(retry_tracker)
    .snapshot_state_provider(EventPayloadSnapshotProvider)
    .build()?;
```

When `service.start()` is called, the projection consumer is spawned as a
background task alongside the event store consumer, publisher consumer, and
outbox processor. Each consumer reads from the outbound Kafka topic independently.

---

## Per-service storage isolation

Each service uses its own YugabyteDB schema for projection checkpoints and read
model tables. The fleet-service uses `canon_fleet`, the station-service uses
`canon_station`, and so on. Services never share projection checkpoint tables.

This isolation means:

- The `station_inventory` checkpoint in `canon_station.projection_checkpoints` is
  completely independent from any checkpoint in `canon_fleet.projection_checkpoints`.
- Rebuilding one service's projection does not affect any other service.
- Schema migrations can happen per-service without coordination.

The schema is selected when creating the connection pool:

```rust
let schema_name = "canon_station";
let pool = canon_demo_shared::db::create_service_pool(&yugabyte_url, &schema_name).await?;
let projection_store = YugabyteProjectionStore::from_pool(pool);
```

---

## Summary

Projections are the query side of Canon's CQRS architecture:

| Concept | Purpose |
|---|---|
| `#[projection]` | Declares a read model struct |
| `#[projection_handler(P)]` | Applies one event type to the read model |
| `ProjectionCheckpointStore` | Tracks last processed sequence number |
| `ProjectionConsumer` | Background task that applies events to projections |
| `ProjectionRebuildManager` | Manages rebuild lifecycle (start/is_rebuilding/complete) |
| `InMemoryProjectionStore` | Test implementation -- no persistence |
| `YugabyteProjectionStore` | Production implementation -- persists to JSONB |

The flow: events are committed to the outbox, published to the outbound Kafka topic,
received by the projection consumer, applied through `#[projection_handler]` functions,
and checkpointed. Read endpoints query the materialised view for fast reads, falling
back to read-through during rebuilds.

Projection handlers must be idempotent because consumers restart from offset zero
and Kafka delivers at-least-once. Use absolute values, upserts, and guard clauses
to ensure safety.
