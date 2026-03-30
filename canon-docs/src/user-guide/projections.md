# Projections

Projections are read models -- materialised views built from events. They provide
queryable representations of your domain state optimised for specific read patterns.

## The Projection trait

```rust
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

You never implement this directly -- use `#[projection]` and `#[projection_handler]`.

## Defining a projection

### 1. Declare the projection

```rust
#[projection]
pub struct StationInventory {
    pub station_id: StationId,
    pub stock_levels: HashMap<CargoType, u32>,
}
```

### 2. Define handlers for each event type

```rust
#[projection_handler(StationInventory)]
impl CargoReceivedHandler {
    fn apply(&self, event: &CargoReceived, store: &mut StationInventory) {
        *store.stock_levels.entry(event.cargo_type).or_insert(0) += event.quantity;
    }
}

#[projection_handler(StationInventory)]
impl StockDrainedHandler {
    fn apply(&self, event: &StockDrained, store: &mut StationInventory) {
        if let Some(level) = store.stock_levels.get_mut(&event.cargo_type) {
            *level = level.saturating_sub(event.quantity);
        }
    }
}
```

## Idempotency requirement

Projection handlers **must be idempotent** -- applying the same event twice must
produce the same result. This is essential because:

- Consumers restart from offset zero on every boot
- Kafka delivers at-least-once, so duplicates are expected
- During projection rebuild, all events are replayed from scratch

Design your handlers so that re-applying an event is a no-op or produces identical
state. Common patterns:

- **Set, don't increment** -- store the absolute value, not a delta
- **Upsert** -- use `INSERT ... ON CONFLICT UPDATE`
- **Checkpoint** -- track `last_version` and skip events at or before it

## Read modes

Canon supports two read strategies:

### Read-ready (materialised view)

A persistent read model maintained by `apply()`. Events update it as they flow through
the projection consumer.

```
GET /stations/:id/inventory  ->  query StationInventory table directly
```

- Fast reads (single query)
- Eventually consistent (there is a propagation delay from event to read model)
- Requires checkpoint tracking for restart safety

### Read-through (computed on demand)

No persistent read model. State is computed by replaying the event stream on each request.

```
GET /ships/:id/history  ->  replay all ShipEvents from event store
```

- Always consistent (reads the latest events)
- Expensive at scale (replays the full stream)
- No checkpoint or rebuild needed

Both modes use the same `Projection` trait. The difference is in the `ProjectionStore`
implementation.

## Projection rebuild

When a projection needs to be rebuilt (schema change, bug fix, new projection), Canon
uses Kafka offset reset:

1. Set `projection_checkpoints.rebuilding = true`
2. While `rebuilding == true`, read endpoints fall back to read-through -- never serve
   stale materialised views
3. Reset the projection consumer's Kafka offset on `canon.{service}.outbound` to the
   target checkpoint
4. Kafka replays events in order -- the projection consumer applies them
5. Once the replay completes, set `rebuilding = false`

This means rebuild is automatic -- no custom rebuild logic against the event store.
The outbound queue already has all events in order.

### Triggering a rebuild

```rust
projection_store.start_rebuild("station_inventory").await?;
```

The projection consumer detects `rebuilding = true` and resets its offset.

## Projection consumer

Projections are updated by a dedicated consumer on the outbound queue -- the
**projection consumer**. This is one of four independent consumer groups:

1. Event store consumer (Cassandra writes)
2. **Projection consumer** (read model updates)
3. Internal event consumer (routes own events back to inbox for event handler dispatch)
4. Publisher (cross-service events)

Each consumer fails and recovers independently. If the projection consumer crashes,
it restarts from offset zero and re-applies events idempotently.

## Checkpoint tracking

The projection store maintains checkpoints to track processing progress:

```sql
CREATE TABLE projection_checkpoints (
    projection_id TEXT PRIMARY KEY,
    last_version BIGINT NOT NULL,
    rebuilding BOOLEAN NOT NULL DEFAULT FALSE,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL
);
```

- `last_version` -- the version of the last event applied
- `rebuilding` -- whether a rebuild is in progress
- On restart, the consumer can skip events at or before `last_version`

## Example: full projection lifecycle

```rust
// 1. Define the projection
#[projection]
pub struct ShipReadModel {
    pub ship_id: Uuid,
    pub name: String,
    pub status: String,
    pub fuel_level: f32,
}

// 2. Handle registration events
#[projection_handler(ShipReadModel)]
impl ShipRegisteredProjection {
    fn apply(&self, event: &ShipRegistered, store: &mut ShipReadModel) {
        store.name = event.name.clone();
        store.status = "Docked".to_string();
        store.fuel_level = 100.0;
    }
}

// 3. Handle departure events
#[projection_handler(ShipReadModel)]
impl ShipDepartedProjection {
    fn apply(&self, event: &ShipDeparted, store: &mut ShipReadModel) {
        store.status = "InFlight".to_string();
    }
}

// 4. Handle arrival events
#[projection_handler(ShipReadModel)]
impl ShipArrivedProjection {
    fn apply(&self, event: &ShipArrivedAtStation, store: &mut ShipReadModel) {
        store.status = "Docked".to_string();
    }
}
```

The projection consumer applies these handlers as events flow through the outbound queue,
maintaining a queryable read model of every ship's current state.
