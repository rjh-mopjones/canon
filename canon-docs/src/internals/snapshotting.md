# Snapshotting

Event sourcing rebuilds aggregate state by replaying every event from version zero.
For an aggregate with ten events this is trivial. For one with ten thousand events it
is not. Snapshotting solves this by periodically capturing the full aggregate state so
that hydration can start from the most recent snapshot rather than the beginning of
time.

This chapter covers why snapshotting matters, how Canon triggers and stores snapshots,
what the snapshot format looks like, and how the two implementations (in-memory and
YugabyteDB) differ.

---

## Why snapshotting matters

Every time a command arrives for an aggregate, the dispatcher must hydrate that
aggregate's current state. Hydration means loading events from the event store and
feeding them through version-matched `#[event_combiner]` implementations one by one.
The cost is linear in the number of events.

Consider a `Ship` aggregate that has processed 5,000 events over its lifetime. Without
snapshots, every single command -- even a simple status check -- requires loading and
replaying all 5,000 events. With `snapshot_every = 50`, the most recent snapshot
captures state at version 4,950. Hydration loads that snapshot (a single row) and
replays only the 50 events after it.

```
Without snapshots:     replay events [0, 1, 2, ..., 4999]   = 5000 events
With snapshot at 4950: load snapshot + replay [4951, ..., 4999] = 50 events
```

The performance gain scales with the lifetime of the aggregate. Short-lived aggregates
(a handful of events, then archived) need no snapshots. Long-lived aggregates with
unbounded event growth need them.

---

## Configuring snapshots with `#[aggregate]`

Snapshotting is enabled at the aggregate level via the `snapshot_every` attribute on
the `#[aggregate]` macro:

```rust
#[aggregate(snapshot_every = 50)]
pub struct Ship {
    status: ShipStatus,
    fuel_level: f32,
    current_station: Option<StationId>,
}
```

This tells Canon to write a snapshot every 50 events. The `#[aggregate]` macro
generates:

- `impl Aggregate` with `type State = Ship` (the aggregate struct is its own state).
- A version-matched hydration dispatch in `Aggregate::hydrate()` that reads
  `event_type` and `event_version` from each `EventEnvelope` and calls the
  corresponding `#[event_combiner]`.
- `Default`, `Serialize`, and `Deserialize` derives on the struct.
- An `inventory` registration that includes the `snapshot_every` value so the
  event store consumer can discover it at runtime.

If `snapshot_every` is omitted, no snapshots are taken. The aggregate still works
-- hydration simply replays all events every time.

```rust
#[aggregate]  // no snapshot_every -- snapshots disabled
pub struct Manifest {
    items: Vec<CargoItem>,
    status: ManifestStatus,
}
```

### Choosing a snapshot interval

The right value for `snapshot_every` depends on:

- **Event volume**: aggregates that accumulate hundreds or thousands of events need
  smaller intervals (25-100). Aggregates with fewer than 50 events in their lifetime
  may not need snapshots at all.
- **Event combiner cost**: if each combiner does heavy computation, even a short replay
  is expensive and a smaller interval helps.
- **Snapshot size**: the snapshot is the serialized aggregate state. If the state struct
  is large (e.g., contains large collections), snapshots consume more storage and
  write bandwidth. A larger interval reduces write frequency.
- **Hydration latency budget**: if the aggregate is on the critical path for user
  requests, tune the interval so the worst-case replay (events between snapshots) stays
  within your latency budget.

A good starting point is 50. Adjust based on profiling.

---

## How snapshots are triggered

Snapshots are written by the **event store consumer** -- the outbound queue consumer
responsible for persisting events to Cassandra. They are never written by the command
handler, the dispatcher, or the outbox processor.

The pipeline flow:

```
Command handler
      |
      v
YugabyteDB ACID txn (commands table + outbox table)
      |
      v
Outbox processor --> Outbound queue (Kafka)
      |
      v
Event store consumer:
  1. Write event to Cassandra (EventStore::append)
  2. On success, check: version % snapshot_every == 0
  3. If true:
       a. Call SnapshotStateProvider::state_at(aggregate_id, version)
       b. Write Snapshot to SnapshotStore::save()
```

This separation is deliberate:

- **Single responsibility**: the command handler only writes to the outbox. It does not
  know about Cassandra, snapshots, or any downstream consumers.
- **Confirmed writes**: snapshots happen after the event is confirmed in Cassandra. If
  the Cassandra write fails, no snapshot is attempted.
- **Fault isolation**: a snapshot write failure does not affect event persistence. The
  event is already safely stored. The next snapshot opportunity will capture a later
  version.

### The version check

The event store consumer performs the check on every successfully written event:

```rust
if self.config.snapshot_every > 0
    && envelope.version.as_u64().is_multiple_of(self.config.snapshot_every)
{
    let state = self.snapshot_state_provider
        .state_at(&aggregate_id, envelope.version)
        .await?;

    let snapshot = Snapshot {
        aggregate_id: aggregate_id.clone(),
        version: envelope.version,
        state,
        taken_at: chrono::Utc::now(),
    };
    self.snapshot_store.save(snapshot).await?;
}
```

When `snapshot_every` is set to 0 (or omitted from the aggregate macro), the check
short-circuits and no snapshot is ever written.

When the version is a multiple of `snapshot_every` (e.g., version 50, 100, 150 with
`snapshot_every = 50`), the consumer produces a snapshot.

### SnapshotStateProvider

The event store consumer is generic -- it does not know the concrete aggregate type. It
cannot call `Aggregate::hydrate()` directly because that requires knowledge of the
aggregate's `State` type and all registered `#[event_combiner]` implementations.

Instead, the consumer is parameterized by a `SnapshotStateProvider` trait:

```rust
#[async_trait]
pub trait SnapshotStateProvider: Send + Sync {
    async fn state_at(
        &self,
        aggregate_id: &AggregateId,
        version: Version,
    ) -> Result<Bytes, String>;
}
```

This trait bridges the gap: the concrete implementation has access to the aggregate
type and can load events, hydrate the state, serialize it, and return the bytes. The
event store consumer simply passes those bytes to `SnapshotStore::save()`.

For tests, Canon provides `EventPayloadSnapshotProvider`, a placeholder that returns
empty state. Production services inject a provider that performs real hydration.

---

## The Snapshot type

The `Snapshot` struct is defined in `canon-core/src/types.rs`:

```rust
pub struct Snapshot {
    pub aggregate_id: AggregateId,
    pub version: Version,
    pub state: Bytes,
    pub taken_at: DateTime<Utc>,
}
```

| Field          | Type                | Description                                            |
|----------------|---------------------|--------------------------------------------------------|
| `aggregate_id` | `AggregateId`       | The aggregate instance this snapshot belongs to.       |
| `version`      | `Version`           | The event version at which this snapshot was taken.    |
| `state`        | `Bytes`             | Serialized aggregate state (opaque byte payload).      |
| `taken_at`     | `DateTime<Utc>`     | Timestamp when the snapshot was created.               |

The `state` field holds the aggregate struct serialized via serde. The `#[aggregate]`
macro generates `Serialize` and `Deserialize` derives, so the state can be serialized
to JSON, MessagePack, or any serde-compatible format. The framework does not mandate a
format -- the `SnapshotStateProvider` controls serialization and the hydration path
controls deserialization.

---

## Hydration with snapshots

When the dispatcher needs to load an aggregate's current state, it follows this
sequence:

```
1. SnapshotStore::load(aggregate_id)
       |
       +-- Found snapshot at version 200
       |       state = <serialized Ship { status: Docked, fuel: 0.85, ... }>
       |
       +-- No snapshot found
               |
               v
           Start from Default::default(), version = 0

2. EventStore::load_from_version(aggregate_id, snapshot_version + 1)
       |
       v
   Events: [v201, v202, ..., v247]

3. Aggregate::hydrate(state, events.into_iter())
       |
       v
   For each event:
     - Read event_type + event_version from EventEnvelope
     - Dispatch to #[event_combiner] registered at that version
     - Combiner mutates state in place
       |
       v
   Fully hydrated state at version 247
```

### With a snapshot

```
Load snapshot at v200      O(1) read
Load events [201..247]     47 events
Replay 47 events           47 combiner calls
```

### Without a snapshot

```
Load all events [0..247]   247 events
Replay 247 events          247 combiner calls
```

The snapshot reduces the replay window from 247 events to 47. For an aggregate with
5,000 events and `snapshot_every = 50`, the worst case is 49 events replayed (snapshot
at 4,950, current at 4,999). The best case is 0 events replayed (snapshot at exactly
the current version).

### Deserialization

The snapshot's `state` field is deserialized into the aggregate's `State` type (which
is the aggregate struct itself, since `type State = Self`). This deserialized state
becomes the starting point for `Aggregate::hydrate()`, which then applies only the
events after the snapshot version.

---

## Snapshot storage

### The SnapshotStore trait

Defined in `canon-core/src/traits/snapshot_store.rs`:

```rust
#[async_trait]
pub trait SnapshotStore: Send + Sync + 'static {
    type Error: std::error::Error + Send + Sync + 'static;

    /// Upsert a snapshot for an aggregate. Replaces any existing snapshot.
    async fn save(&self, snapshot: Snapshot) -> Result<(), Self::Error>;

    /// Load the latest snapshot for an aggregate, or None if none exists.
    async fn load(
        &self,
        aggregate_id: &AggregateId,
    ) -> Result<Option<Snapshot>, Self::Error>;
}
```

The trait has two operations:

- **save**: writes (or overwrites) a snapshot. Implementations must be idempotent --
  writing the same snapshot twice should succeed without error.
- **load**: retrieves the most recent snapshot for an aggregate. Returns `None` if no
  snapshot exists.

### YugabyteDB implementation

The production implementation lives in `canon-snapshot-store-yugabyte`. It stores
snapshots in YugabyteDB (not Cassandra), providing ACID transactional reads and
simple key-value lookup by `aggregate_id`.

#### Schema

Each service has its own schema. The `snapshots` table:

```sql
CREATE TABLE canon_fleet.snapshots (
    aggregate_id UUID        NOT NULL,
    version      BIGINT      NOT NULL,
    state        BYTEA       NOT NULL,
    taken_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (aggregate_id, version)
);
```

The primary key is `(aggregate_id, version)`, which means multiple snapshot versions
can coexist for the same aggregate. The `load` query orders by `version DESC` and
takes the first row, returning the most recent snapshot:

```sql
SELECT aggregate_id, version, state, taken_at
FROM snapshots
WHERE aggregate_id = $1
ORDER BY version DESC
LIMIT 1
```

#### Save semantics

The `save` implementation uses `ON CONFLICT DO NOTHING` for idempotency:

```rust
async fn save(&self, snapshot: Snapshot) -> Result<(), Self::Error> {
    let v_i64 = i64::try_from(snapshot.version.as_u64())
        .map_err(|_| /* version overflow error */)?;

    let result = sqlx::query(
        "INSERT INTO snapshots (aggregate_id, version, state, taken_at) \
         VALUES ($1, $2, $3, $4) \
         ON CONFLICT (aggregate_id, version) DO NOTHING",
    )
    .bind(*snapshot.aggregate_id.as_uuid())
    .bind(v_i64)
    .bind(snapshot.state.as_ref())
    .bind(snapshot.taken_at)
    .execute(&self.pool)
    .await?;

    if result.rows_affected() == 0 {
        // Snapshot already exists at this version -- idempotent, no error.
    }

    Ok(())
}
```

If the same snapshot (same aggregate_id and version) is written twice, the second
write is silently ignored. This is important because the event store consumer may
process the same event more than once (Kafka does not commit offsets; idempotency is
the safety net).

#### Load semantics

The `load` implementation returns the snapshot with the highest version:

```rust
async fn load(
    &self,
    aggregate_id: &AggregateId,
) -> Result<Option<Snapshot>, Self::Error> {
    let row: Option<(Uuid, i64, Vec<u8>, DateTime<Utc>)> = sqlx::query_as(
        "SELECT aggregate_id, version, state, taken_at \
         FROM snapshots \
         WHERE aggregate_id = $1 \
         ORDER BY version DESC \
         LIMIT 1",
    )
    .bind(*aggregate_id.as_uuid())
    .fetch_optional(&self.pool)
    .await?;

    Ok(row.map(|(agg_id, version, state, taken_at)| Snapshot {
        aggregate_id: AggregateId::from_uuid(agg_id),
        version: u64::try_from(version).unwrap_or(0).into(),
        state: Bytes::from(state),
        taken_at,
    }))
}
```

If no snapshot exists for the aggregate, `None` is returned and hydration falls back to
replaying all events from version zero.

#### Construction

The store is created from a `PgPool`, a URL, or the `YUGABYTE_URL` environment
variable:

```rust
// From an existing pool (preferred in services)
let store = YugabyteSnapshotStore::new(pool);

// From a URL
let store = YugabyteSnapshotStore::from_url("postgres://canon:canon@yugabytedb:5433/canon").await?;

// From YUGABYTE_URL env var
let store = YugabyteSnapshotStore::from_env().await?;
```

### In-memory implementation

The test harness uses `InMemorySnapshotStore`, defined in
`canon-core/src/memory/snapshot_store.rs`. It stores snapshots in a
`HashMap<AggregateId, Snapshot>` behind an `Arc<Mutex<...>>`:

```rust
#[derive(Clone)]
pub struct InMemorySnapshotStore {
    inner: Arc<Mutex<HashMap<AggregateId, Snapshot>>>,
}
```

Key differences from the YugabyteDB implementation:

| Aspect              | InMemorySnapshotStore                  | YugabyteSnapshotStore                  |
|---------------------|----------------------------------------|----------------------------------------|
| Storage             | `HashMap` in process memory            | YugabyteDB `snapshots` table           |
| Durability          | Lost on process exit                   | Persisted to disk                      |
| Version history     | Only latest (HashMap replaces on insert) | All versions (PK includes version)   |
| Concurrency         | `Mutex` (single writer)                | PostgreSQL row-level locking           |
| Error type          | `SnapshotStoreError::Poisoned`         | `SnapshotStoreError::Store(sqlx::Error)` |
| Use case            | `canon-test` integration tests         | Production services                    |

The in-memory store's `save` replaces the entry for the aggregate. It keeps only the
latest snapshot. The YugabyteDB store retains all versions (keyed by
`(aggregate_id, version)`) but `load` always returns the most recent.

Both implementations satisfy the `SnapshotStore` trait contract: `save` is idempotent,
`load` returns the latest snapshot or `None`.

---

## The event store consumer in detail

The `EventStoreConsumer` is the component that ties event persistence and snapshot
creation together. It lives in `canon-core/src/consumers/event_store_consumer.rs` and
is generic over five traits:

```rust
pub struct EventStoreConsumer<ES, SS, DL, RT, SP>
where
    ES: EventStore,
    SS: SnapshotStore,
    DL: DeadLetterStore,
    RT: RetryTracker,
    SP: SnapshotStateProvider,
```

Configuration is provided via `EventStoreConsumerConfig`:

```rust
pub struct EventStoreConsumerConfig {
    pub snapshot_every: u64,
    pub max_retries: u32,
}

impl Default for EventStoreConsumerConfig {
    fn default() -> Self {
        Self {
            snapshot_every: 50,
            max_retries: 3,
        }
    }
}
```

### Processing an event

The `process` method handles a single `EventEnvelope`:

1. **Append to event store**: calls `EventStore::append()` with optimistic concurrency.
   The expected version is the event's version minus one.

2. **On success**:
   - Clear any retry tracking for this event ID.
   - Check `version % snapshot_every == 0`. If true, call `SnapshotStateProvider::state_at()`
     to produce serialized state, then `SnapshotStore::save()` to persist the snapshot.

3. **On version conflict**:
   - Increment retry count via `RetryTracker`.
   - If retries are exhausted (attempts >= max_retries), dead-letter the event and
     return `VersionConflictExhausted`.
   - Otherwise, return `VersionConflict` (retryable).

4. **On other errors**: return `EventStore` error directly.

### The run loop

The consumer runs as a background `tokio::spawn` task:

```rust
pub async fn run<R, F>(
    self,
    receiver: R,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
    outbound_notify: Option<Arc<Notify>>,
    on_error: F,
)
```

It polls the `ConsumerReceiver` for events, processes each one, commits offsets, and
stops when the shutdown signal fires. An optional `Notify` allows the outbox processor
to wake the consumer immediately after publishing, reducing pipeline latency.

---

## Snapshot consistency

Snapshots are **eventually consistent** with the event store. A snapshot at version 200
means the aggregate state at version 200 is fully captured. Events 201 and beyond are
not included -- they must be replayed on top of the snapshot.

### Failure modes

**Snapshot write fails**: the event is already persisted in Cassandra. Only the snapshot
write failed. The aggregate remains fully functional -- hydration simply replays more
events until the next successful snapshot.

**Stale snapshot**: if a snapshot is taken at version 200 but 100 more events arrive
before the next snapshot, hydration replays those 100 events. This is normal operation,
not an error.

**Duplicate snapshot write**: the event store consumer may process the same event twice
(Kafka restarts from offset 0). The YugabyteDB store uses `ON CONFLICT DO NOTHING`, so
writing the same snapshot version twice is harmless. The in-memory store overwrites the
existing entry, which is also harmless since the data is identical.

### Snapshots are not the source of truth

The event store (Cassandra) is the source of truth. Snapshots are a performance
optimization. If all snapshots are lost, the system continues to work correctly --
hydration falls back to replaying all events from version zero. It will be slower, but
correct.

---

## Snapshots and version-matched routing

Snapshots interact cleanly with Canon's version-matched event routing. The snapshot
captures the aggregate state at a point in time, serialized with the current schema.
Events after the snapshot version are replayed through their version-matched combiners,
which update the state incrementally.

This means:

- Old snapshots remain valid even as new event versions are added.
- No snapshot migration is needed when adding new event versions.
- The combiner chain handles schema evolution automatically.

For example, if `ShipDeparted` exists at both version 1 and version 2, and a snapshot
was taken when only version 1 events existed, later version 2 events are still replayed
correctly -- each event's `event_version` field routes it to the matching combiner.

---

## Wiring snapshots in ServiceBuilder

The `ServiceBuilder` accepts a snapshot store and snapshot state provider:

```rust
ServiceBuilder::new("fleet")
    .for_aggregate::<Ship>()
    .event_store(cassandra_event_store)
    .snapshot_store(yugabyte_snapshot_store)
    .snapshot_state_provider(fleet_snapshot_provider)
    .command_store(command_store)
    // ... other infrastructure
    .build()?;
```

The `snapshot_every` value from the `#[aggregate]` macro registration flows into the
`EventStoreConsumerConfig`. The consumer automatically discovers the interval and
performs the modulo check on every event.

---

## Performance characteristics

| Operation        | Cost (YugabyteDB)                          | Cost (In-memory)      |
|------------------|--------------------------------------------|-----------------------|
| save             | Single INSERT with ON CONFLICT DO NOTHING  | HashMap insert        |
| load             | Single SELECT with ORDER BY + LIMIT 1      | HashMap get           |
| Storage per snap | One row: UUID + BIGINT + BYTEA + TIMESTAMP | One HashMap entry     |

Snapshot size depends on the aggregate's serialized state. A simple aggregate like
`Ship` with a few fields serializes to hundreds of bytes. An aggregate with large
collections (e.g., a ledger with thousands of line items) may produce snapshots of
several kilobytes or more.

### Write frequency

With `snapshot_every = 50` and a throughput of 1,000 events per second across all
aggregates, the snapshot store sees at most 20 writes per second (1,000 / 50). Each
write is a single row upsert -- well within YugabyteDB's capacity.

### Read frequency

Snapshots are read once per aggregate hydration. In the worst case, every incoming
command triggers a snapshot load. In practice, the dispatcher may cache hydrated state
for the duration of a command batch, reducing snapshot reads.

### When to skip snapshots

Skip snapshots when:

- Aggregates have short lifetimes (created, a few events, then archived).
- Hydration is not on the critical path (batch processing, not real-time).
- The aggregate accumulates fewer events than the snapshot interval.
- You want the simplest possible setup during prototyping.

---

## Summary

Snapshotting in Canon is:

- **Opt-in**: enable with `snapshot_every = N` on `#[aggregate]`.
- **Consumer-driven**: triggered by the event store consumer after confirmed Cassandra writes.
- **Fault-tolerant**: snapshot failures do not affect event persistence.
- **Idempotent**: duplicate writes are harmless (ON CONFLICT DO NOTHING).
- **Version-compatible**: old snapshots remain valid as new event versions are added.
- **An optimization, not a source of truth**: events in Cassandra are always authoritative.
