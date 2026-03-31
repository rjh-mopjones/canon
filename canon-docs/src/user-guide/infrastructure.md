# Infrastructure

Canon's hexagonal architecture means every infrastructure concern sits behind a trait
defined in `canon-core`. This chapter provides a comprehensive reference for every
concrete implementation: the SQL and CQL schemas, the Kafka wire protocols, the
idempotency mechanisms, and the configuration options.

---

## Storage architecture overview

Canon splits infrastructure across three distinct technologies, each chosen for its
strength:

| Concern | Trait | Backend | Crate |
|---------|-------|---------|-------|
| Event store | `EventStore` | Cassandra | `canon-event-store-cassandra` |
| Command store | `CommandStore` | YugabyteDB | `canon-command-store-yugabyte` |
| Snapshot store | `SnapshotStore` | YugabyteDB | `canon-snapshot-store-yugabyte` |
| Inbox | `Inbox` | YugabyteDB | `canon-inbox-yugabyte` |
| Outbox | `OutboxStore` | YugabyteDB | `canon-command-store-yugabyte` (submodule) |
| Dispatcher store | `DispatcherStore` | YugabyteDB | `canon-command-store-yugabyte` (submodule) |
| Projection store | `ProjectionStore` | YugabyteDB | `canon-projection-store-yugabyte` |
| Dead letter store | `DeadLetterStore` | YugabyteDB | `canon-deadletter-yugabyte` |
| Retry tracker | `RetryTracker` | YugabyteDB | `canon-deadletter-yugabyte` (submodule) |

| Concern | Trait | Backend | Crate |
|---------|-------|---------|-------|
| Inbound queue | `InboundQueue` | Kafka | `canon-inbound-queue-kafka` |
| Outbound queue | `OutboundQueue` | Kafka | `canon-outbound-queue-kafka` |
| Publisher | `Publisher` | Kafka | `canon-publisher-kafka` |
| Adaptor | `EventAdaptor` | Kafka | `canon-adaptor-kafka` |

### Why this storage split?

- **Cassandra for events** -- append-optimised, high-volume, wide rows per aggregate
  stream. Events are immutable and append-only, which is Cassandra's sweet spot. The
  primary key `(aggregate_id, version)` provides optimistic concurrency via lightweight
  transactions and efficient per-aggregate streaming.

- **YugabyteDB for everything else** -- transactional, queryable, strong consistency.
  The outbox pattern requires ACID transactions (command + outbox in one commit).
  Inbox dedup requires composite key uniqueness. Projections need queryable read
  models with JSONB support. Dead letter management needs admin-queryable tables.

- **Kafka for messaging** -- durable, partitioned message transport between pipeline
  stages. All topics partitioned by `aggregate_id` for ordered processing. Canon uses
  `rskafka` exclusively -- pure Rust, no C dependencies, cross-compilable.

### Per-service storage isolation

Every demo service uses its own YugabyteDB schema and Cassandra keyspace:

```sql
CREATE SCHEMA IF NOT EXISTS canon_fleet;
CREATE SCHEMA IF NOT EXISTS canon_cargo;
CREATE SCHEMA IF NOT EXISTS canon_navigation;
CREATE SCHEMA IF NOT EXISTS canon_supply;
CREATE SCHEMA IF NOT EXISTS canon_station;
```

Services must never share outbox, commands, inbox, or event store tables. This is
enforced at the application level through `create_service_pool()` and
`CassandraEventStore::new_with_keyspace()`.

---

## Event Store (Cassandra)

**Crate**: `canon-event-store-cassandra`
**Trait**: `EventStore`
**Driver**: `scylla` (Rust driver, compatible with both Cassandra and ScyllaDB)

The event store is the system of record for all domain events. It is append-only
and uses lightweight transactions (LWT) for optimistic concurrency.

### Schema

Each service uses its own keyspace:

```cql
CREATE KEYSPACE IF NOT EXISTS canon_fleet
    WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

CREATE TABLE canon_fleet.events (
    aggregate_id UUID,
    version      BIGINT,
    event_id     UUID,
    event_type   TEXT,
    event_version INT,
    payload      BLOB,
    correlation_id UUID,
    causation_id UUID,
    created_at   TIMESTAMP,
    PRIMARY KEY (aggregate_id, version)
) WITH CLUSTERING ORDER BY (version ASC);
```

The primary key design:
- **Partition key** (`aggregate_id`): all events for one aggregate live in the same
  partition, enabling efficient single-query loading.
- **Clustering key** (`version`): events within a partition are stored in ascending
  version order. Range scans for `load_from_version` are a single Cassandra slice.

### Optimistic concurrency with LWT

The `append` method uses Cassandra's lightweight transactions to enforce optimistic
concurrency. Each event is inserted with `IF NOT EXISTS`:

```cql
INSERT INTO events
    (aggregate_id, version, event_id, event_type, event_version,
     payload, correlation_id, causation_id, created_at)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
IF NOT EXISTS
```

When the LWT succeeds, the `[applied]` column returns `true`. When a concurrent
writer has already claimed that version, it returns `false` and the store returns
a `VersionConflict` error:

```rust
if !applied {
    return Err(EventStoreError::VersionConflict {
        expected: expected_version,
        found: version,
    });
}
```

The implementation handles both Cassandra and ScyllaDB LWT response formats.
Cassandra returns a single `[applied]` column on success, while ScyllaDB always
returns all columns. The code inspects the column count to deserialize correctly:

```rust
let applied = if col_count == 1 {
    // Cassandra success: only [applied]=true
    rows_result.first_row::<(bool,)>()?.0
} else {
    // ScyllaDB (or Cassandra conflict): all columns present
    let (val, ..) = rows_result.first_row::<LwtRow>()?;
    val
};
```

### Prepared statements

All queries are prepared at construction time for performance. The store maintains
four prepared statements:

| Statement | Purpose |
|-----------|---------|
| `stmt_append` | Insert with LWT (`IF NOT EXISTS`) |
| `stmt_load` | Load all events for an aggregate, ordered by version |
| `stmt_load_from` | Load events from a specific version onwards |
| `stmt_current_version` | Get the highest version for an aggregate |

### Construction

```rust
// Per-service keyspace (recommended)
let event_store = CassandraEventStore::new_with_keyspace(
    "cassandra:9042",
    "canon_fleet",
).await?;

// From environment variable
let event_store = CassandraEventStore::from_env().await?;
// Reads CASSANDRA_NODES and CASSANDRA_KEYSPACE
```

The constructor validates that the keyspace exists by querying
`system_schema.keyspaces`, catching configuration errors early rather than
failing on the first real query.

### Trait methods

```rust
// Append events with optimistic concurrency
event_store.append(&aggregate_id, expected_version, events).await?;

// Load all events for an aggregate (full stream)
let events = event_store.load(&aggregate_id).await?;

// Load events from a specific version (for snapshot + replay)
let events = event_store.load_from_version(&aggregate_id, from_version).await?;

// Get the current highest version
let version = event_store.current_version(&aggregate_id).await?;
```

---

## Command Store (YugabyteDB)

**Crate**: `canon-command-store-yugabyte`
**Trait**: `CommandStore`
**Driver**: `sqlx` with the PostgreSQL wire protocol

The command store persists every command submitted to the system as an audit trail.
Commands are written as part of a single ACID transaction alongside the outbox.

### Schema

```sql
CREATE TABLE commands (
    command_id      UUID        PRIMARY KEY,
    aggregate_id    UUID        NOT NULL,
    command_type    TEXT        NOT NULL DEFAULT '',
    command_version INT         NOT NULL DEFAULT 1,
    payload         BYTEA       NOT NULL,
    correlation_id  UUID,
    causation_id    UUID,
    status          TEXT        NOT NULL DEFAULT 'pending',
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX commands_aggregate_idx
    ON commands (aggregate_id, created_at);
```

### Idempotent inserts

All command inserts use `ON CONFLICT (command_id) DO NOTHING`, making them
safe to call twice with the same `command_id`:

```sql
INSERT INTO commands
    (command_id, aggregate_id, command_type, command_version,
     payload, correlation_id, causation_id, created_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
ON CONFLICT (command_id) DO NOTHING
```

### Transactional write path

The command handler write path requires a single ACID transaction spanning both
the command store and the outbox. Use `append_in_tx` with a shared transaction:

```rust
let mut tx = command_store.pool().begin().await?;
command_store.append_in_tx(&mut tx, command_envelope).await?;
outbox_store.insert_in_tx(&mut tx, event_envelope).await?;
tx.commit().await?;
```

If the transaction is dropped without committing, the implicit rollback ensures
neither the command nor the outbox entry is visible. This is the outbox pattern
that guarantees at-least-once delivery.

### Query methods

```rust
// Load a single command by ID
let cmd = store.load(command_id).await?;

// Load all commands for an aggregate, ordered by creation time
let cmds = store.load_for_aggregate(&aggregate_id).await?;

// Load commands within a time range (for counterfactual replay)
let cmds = store.load_range(&aggregate_id, Some(from), Some(to)).await?;

// Update command status (pending -> executed / failed)
store.update_status(command_id, CommandStatus::Executed).await?;
```

---

## Outbox Store (YugabyteDB)

**Crate**: `canon-command-store-yugabyte` (submodule `outbox_store`)
**Trait**: `OutboxStore`

The outbox table is the commit point of the event sourcing pipeline. Events are
written to the outbox in the same ACID transaction as the command. A background
outbox processor drains the outbox to the outbound Kafka queue.

### Schema

```sql
CREATE SEQUENCE outbox_seq;

CREATE TABLE outbox (
    id              UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    sequence_number BIGINT      DEFAULT nextval('outbox_seq'),
    aggregate_id    UUID,
    payload         BYTEA,
    created_at      TIMESTAMPTZ DEFAULT now(),
    delivered_at    TIMESTAMPTZ
);

CREATE INDEX outbox_seq_idx
    ON outbox (sequence_number) WHERE delivered_at IS NULL;
```

The `sequence_number` column provides a total ordering for the outbox processor.
The partial index `WHERE delivered_at IS NULL` ensures that only undelivered
entries are scanned during polling.

### Polling with `FOR UPDATE SKIP LOCKED`

The outbox processor polls for undelivered entries using pessimistic row-level
locking. `SKIP LOCKED` prevents multiple outbox processor instances from
double-processing the same row:

```sql
SELECT id, sequence_number, aggregate_id, payload
FROM outbox
WHERE delivered_at IS NULL
ORDER BY sequence_number ASC
LIMIT $1
FOR UPDATE SKIP LOCKED
```

After publishing each entry to Kafka, the processor marks it delivered:

```sql
UPDATE outbox SET delivered_at = now() WHERE id = $1
```

### Transactional insert

Events enter the outbox via `insert_in_tx`, called within the same transaction
as the command store write:

```rust
outbox_store.insert_in_tx(&mut tx, &event_envelope).await?;
```

The event envelope is serialized to JSON and stored as `BYTEA`. On poll, it is
deserialized back into an `EventEnvelope`.

---

## Dispatcher Store (YugabyteDB)

**Crate**: `canon-command-store-yugabyte` (submodule `dispatcher_store`)
**Trait**: `DispatcherStore`

The dispatcher store is the glue between the inbox and the outbox. It is generic
over the event store implementation, allowing each service to supply its own
(Cassandra in production, in-memory in tests).

```rust
pub struct PgDispatcherStore<ES: EventStore> {
    pool: PgPool,
    event_store: ES,
    handler_id: String,
}
```

### Operations

The dispatcher store implements five operations:

**1. Poll inbox** -- fetches unprocessed commands addressed to this handler:

```sql
SELECT handler_id, message_id, aggregate_id, payload
FROM inbox_messages
WHERE handler_id = $1
ORDER BY received_at ASC
LIMIT $2
FOR UPDATE SKIP LOCKED
```

**2. Load events** -- hydrates aggregate state by combining confirmed events
from the event store (Cassandra) with pending outbox events that have not yet
reached Cassandra:

```rust
// 1. Load confirmed events from Cassandra
let mut events = self.event_store.load(aggregate_id).await?;

// 2. Load pending outbox events for this aggregate
let pending_rows = sqlx::query_as(
    "SELECT payload FROM outbox
     WHERE aggregate_id = $1 AND delivered_at IS NULL
     ORDER BY sequence_number ASC"
).fetch_all(&self.pool).await?;

// Append pending events to the confirmed set
for (payload,) in pending_rows {
    let envelope: EventEnvelope = serde_json::from_slice(&payload)?;
    events.push(envelope);
}
```

This prevents version conflicts when a second command arrives before the first
event has propagated through the outbox, Kafka, and back to Cassandra.

**3. Write outbox and mark processed** -- a single ACID transaction that:
- Locks the inbox row with `FOR UPDATE SKIP LOCKED`
- Inserts the resulting event into the outbox
- Deletes the inbox message (marks it processed)

**4. Record failure** -- atomically increments the retry counter in
`retry_attempts` via UPSERT.

**5. Dead letter** -- a single transaction that inserts into `dead_letters`,
deletes the inbox message, and cleans up `retry_attempts`.

---

## Snapshot Store (YugabyteDB)

**Crate**: `canon-snapshot-store-yugabyte`
**Trait**: `SnapshotStore`

Snapshots capture serialized aggregate state at a given version, avoiding full
event replay on every command. The event store consumer creates a snapshot
whenever `version % snapshot_every == 0`.

### Schema

```sql
CREATE TABLE snapshots (
    aggregate_id UUID        NOT NULL,
    version      BIGINT      NOT NULL,
    state        BYTEA       NOT NULL,
    taken_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (aggregate_id, version)
);
```

The composite primary key `(aggregate_id, version)` supports multiple snapshots
per aggregate. The `load` method returns the latest snapshot by ordering
descending and limiting to one:

```sql
SELECT aggregate_id, version, state, taken_at
FROM snapshots
WHERE aggregate_id = $1
ORDER BY version DESC
LIMIT 1
```

### Idempotent saves

Snapshot saves use `ON CONFLICT (aggregate_id, version) DO NOTHING` so that
replaying the event store consumer does not produce duplicate snapshots:

```sql
INSERT INTO snapshots (aggregate_id, version, state, taken_at)
VALUES ($1, $2, $3, $4)
ON CONFLICT (aggregate_id, version) DO NOTHING
```

### Usage pattern

The event store consumer calls `save` after confirming a Cassandra write:

```rust
if event.version.as_u64() % snapshot_every == 0 {
    let snapshot = Snapshot {
        aggregate_id: event.aggregate_id.clone(),
        version: event.version,
        state: serialize_state(&state),
        taken_at: Utc::now(),
    };
    snapshot_store.save(snapshot).await?;
}
```

On aggregate hydration, the snapshot is loaded first, then only events after
the snapshot version are replayed:

```rust
let snapshot = snapshot_store.load(&aggregate_id).await?;
let (mut state, from_version) = match snapshot {
    Some(snap) => (deserialize_state(&snap.state), snap.version),
    None => (State::default(), Version::initial()),
};
let events = event_store.load_from_version(&aggregate_id, from_version).await?;
Aggregate::hydrate(&mut state, events.into_iter())?;
```

---

## Inbox (YugabyteDB)

**Crate**: `canon-inbox-yugabyte`
**Trait**: `Inbox`

The inbox is the entry point for all messages into a service. It provides
idempotent intake, event handler windowing, oversight gating, and batch
idempotency via processed windows.

### Schema

```sql
-- Message deduplication
CREATE TABLE inbox_messages (
    handler_id   TEXT        NOT NULL,
    message_id   UUID        NOT NULL,
    aggregate_id UUID        NOT NULL,
    payload      BYTEA       NOT NULL,
    received_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (handler_id, message_id)
);

-- Event handler windowing
CREATE TABLE inbox_windows (
    handler_id      TEXT        NOT NULL,
    correlation_key UUID        NOT NULL,
    status          TEXT        NOT NULL DEFAULT 'open',
    messages        JSONB       NOT NULL DEFAULT '[]',
    expires_at      TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (handler_id, correlation_key)
);

-- Batch idempotency
CREATE TABLE processed_windows (
    window_id UUID PRIMARY KEY
);
```

### Idempotent intake

Every message submitted to the inbox is deduplicated by the composite key
`(handler_id, message_id)`. The `message_id` is the command's `command_id` or
the event's `event_id`. Duplicate submissions are silently ignored via
`ON CONFLICT DO NOTHING`.

### Windowing and oversight

Event handlers that declare `window_ttl` accumulate messages into windows keyed
by `(handler_id, correlation_key)`. The correlation key comes from the handler's
`correlate` function, or falls back to the envelope's `correlation_id`.

Each unique correlation key is an independent window -- a handler may have many
concurrent in-flight windows. The window's `status` transitions through:

1. `open` -- accumulating messages, oversight returns `NotReady`
2. `ready` -- oversight returns `Ready`, dispatcher can dispatch
3. `expired` -- TTL exceeded, moved to dead letter
4. `processed` -- window ID recorded in `processed_windows`

### Message serialization

`IncomingMessage` (an in-process enum of `Command`, `InternalEvent`,
`ExternalEvent`) is serialized to JSONB via an intermediate `StoredMessage`
type. This includes the message type discriminant and all envelope fields,
with payload bytes stored as a JSON array of integers.

### Window expiry

A background sweep task periodically checks for windows past their `expires_at`:

```rust
inbox.sweep_expired_windows().await?;
```

Expired windows are collected and their messages are forwarded to the dead
letter store with reason `window_expired`.

---

## Inbound Queue (Kafka)

**Crate**: `canon-inbound-queue-kafka`
**Trait**: `InboundQueue`

The inbound queue carries assembled message batches from the inbox to the
dispatcher. Each service has its own inbound topic: `canon.{service}.inbound`.

### Wire format

Messages are serialized as JSON with a tagged `type` discriminant:

```rust
#[derive(Serialize, Deserialize)]
#[serde(tag = "type")]
enum WireMessage {
    Command(CommandEnvelope),
    InternalEvent(EventEnvelope),
    ExternalEvent(EventEnvelope),
}
```

### Connection pattern

```rust
let queue = KafkaInboundQueue::new(
    "kafka:9092",     // brokers
    "fleet",          // service_name -> topic: canon.fleet.inbound
    "fleet-group",    // group_id (kept for API compat, unused by rskafka)
).await?;
```

The constructor builds an rskafka `PartitionClient` for partition 0 of the
derived topic with `UnknownTopicHandling::Retry`.

### Producing

The `publish` method sends each message individually, using the `aggregate_id`
as the partition key:

```rust
let record = Record {
    key: Some(aggregate_id.as_uuid().to_string().into_bytes()),
    value: Some(serde_json::to_vec(&wire_message)?),
    headers: BTreeMap::new(),
    timestamp: Utc::now(),
};
partition_client.produce(vec![record], Compression::NoCompression).await?;
```

### Consuming

The `receive` method fetches a single record per call using in-memory offset
tracking:

```rust
let records = partition_client
    .fetch_records(*offset, 1..1_048_576, 100)  // timeout_ms=100
    .await?;

if let Some(record) = records.first() {
    *offset = record.offset + 1;
    let wire: WireMessage = serde_json::from_slice(payload)?;
    Ok(Some(vec![wire.into()]))
}
```

### Offset management

Offset tracking is purely in-memory via `Mutex<i64>`. On process restart,
consumption resumes from offset 0. Application-layer idempotency in the inbox
(`handler_id + message_id` PK) prevents duplicate processing.

The `commit` method is a no-op -- there is no external offset storage.

---

## Outbound Queue (Kafka)

**Crate**: `canon-outbound-queue-kafka`
**Trait**: `OutboundQueue`, `OutboxPublisher`, `ConsumerReceiver`

The outbound queue sits between the outbox processor and the three downstream
consumer groups (event store, projection, publisher). It has separate producer
and consumer types.

### Producer (`KafkaOutboundProducer`)

The producer implements both `OutboundQueue::publish` and `OutboxPublisher`
(used by the outbox processor):

```rust
let producer = KafkaOutboundProducer::new(&KafkaOutboundProducerConfig {
    brokers: "kafka:9092".into(),
    topic: "canon.fleet.outbound".into(),
}).await?;

producer.publish(event_envelope).await?;
```

Event envelopes are serialized to JSON with the `aggregate_id` as the Kafka key.

### Consumer (`KafkaOutboundConsumer`)

The consumer implements `ConsumerReceiver` for integration with `Service::start()`:

```rust
let consumer = KafkaOutboundConsumer::new(&KafkaOutboundConsumerConfig {
    brokers: "kafka:9092".into(),
    topic: "canon.fleet.outbound".into(),
    group_id: "fleet-event-store".into(),
    receive_timeout_ms: 100,
    initial_offset: None,  // start from 0
    ..Default::default()
}).await?;

// ConsumerReceiver trait
let received = consumer.receive().await?;
// Returns ReceivedEnvelope { envelope, sequence_number }
```

The `sequence_number` field maps to `kafka_offset + 1` (Kafka offsets are
0-based, sequence numbers are 1-based).

### Combined wrapper (`KafkaOutboundQueue`)

For convenience, `KafkaOutboundQueue` wraps both producer and consumer:

```rust
let queue = KafkaOutboundQueue::new(&KafkaOutboundQueueConfig {
    brokers: "kafka:9092".into(),
    topic: "canon.fleet.outbound".into(),
    group_id: "fleet-consumers".into(),
    receive_timeout_ms: 100,
    ..Default::default()
}).await?;

queue.publish(envelope).await?;
let received = queue.receive().await?;
```

### Configuration from environment

```rust
let config = KafkaOutboundQueueConfig::from_env(
    "canon.fleet.outbound".into(),
    "fleet-consumers".into(),
)?;
// Reads KAFKA_BROKERS environment variable
```

---

## Publisher (Kafka)

**Crate**: `canon-publisher-kafka`
**Trait**: `Publisher`

The publisher distributes confirmed events to other services via
`canon.{service}.events` topics. It is the fourth consumer of the outbound queue.

### Construction

```rust
let publisher = KafkaPublisher::new("kafka:9092", "fleet").await?;
// Or from env:
let publisher = KafkaPublisher::from_env("fleet").await?;

// Topic name is derived automatically:
assert_eq!(publisher.topic(), "canon.fleet.events");
```

### Publishing

The `publish` method creates a new `PartitionClient` per call (via the cached
rskafka `Client`), serializes the event envelope to JSON, and produces it
with the `aggregate_id` as the partition key:

```rust
#[async_trait]
impl Publisher for KafkaPublisher {
    async fn publish(&self, envelope: EventEnvelope, topic: &str) -> Result<(), PublisherError> {
        let payload = serde_json::to_vec(&envelope)?;
        let key = envelope.aggregate_id.as_uuid().to_string();

        let record = Record {
            key: Some(key.into_bytes()),
            value: Some(payload),
            headers: BTreeMap::new(),
            timestamp: Utc::now(),
        };

        let partition_client = self.partition_client(topic).await?;
        partition_client.produce(vec![record], Compression::NoCompression).await?;
        Ok(())
    }
}
```

The publisher does not track idempotency itself. All downstream consumers in
Canon are required to be idempotent, so duplicate-suppression at the publisher
layer is unnecessary.

---

## Adaptor (Kafka)

**Crate**: `canon-adaptor-kafka`
**Trait**: `EventAdaptor`

The adaptor is the anti-corruption layer at the service boundary. It consumes
events from upstream services' `canon.{upstream}.events` topics and submits
them to the local inbox as `IncomingMessage::ExternalEvent`.

### Architecture

```
canon.fleet.events
    -> KafkaEventAdaptor (cargo-service)
        -> inbox.submit("ShipArrivalHandler", event_id, ExternalEvent(envelope))
            -> inbox deduplicates, windows, oversight
                -> dispatcher dispatches when Ready
```

### Construction

```rust
let adaptor = KafkaEventAdaptor::new(
    "kafka:9092",       // brokers
    "cargo-service",    // local service name (for logging)
    Arc::clone(&inbox), // the local inbox
);
```

### Consuming upstream events

The `consume_upstream` method spawns a background tokio task that polls an
upstream topic and forwards events to the inbox:

```rust
let handle = adaptor.consume_upstream(
    "fleet",           // upstream_service -> topic: canon.fleet.events
    "ShipArrivalHandler", // handler_id for inbox routing
).await?;
```

The spawned task runs an infinite polling loop:

1. Fetch records from partition 0, starting at offset 0
2. Deserialize each record as `EventEnvelope`
3. Call `inbox.submit(handler_id, event_id, ExternalEvent(envelope))`
4. Advance the in-memory offset

On Kafka fetch errors, the loop sleeps for 1 second before retrying. On
deserialization errors, the record is logged and skipped.

### Stream interface

For read-only/stateless consumers that do not need inbox integration, the
`subscribe` method returns a `Stream<Item = Result<EventEnvelope, AdaptorError>>`:

```rust
let mut stream = adaptor.subscribe("canon.fleet.events").await?;
while let Some(result) = stream.next().await {
    let envelope = result?;
    // Process without inbox dedup/windowing
}
```

This is implemented via an mpsc channel bridging the rskafka polling loop
to an async `Stream`.

---

## Projection Store (YugabyteDB)

**Crate**: `canon-projection-store-yugabyte`
**Trait**: `ProjectionStore`, `ProjectionCheckpointStore`

The projection store persists materialised read models and tracks processing
checkpoints per projection.

### Schema

```sql
-- Read model state
CREATE TABLE projections (
    projection_id TEXT        NOT NULL,
    aggregate_id  UUID        NOT NULL,
    state         JSONB       NOT NULL,
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (projection_id, aggregate_id)
);

-- Processing checkpoints
CREATE TABLE projection_checkpoints (
    projection_id TEXT        PRIMARY KEY,
    last_version  BIGINT      NOT NULL DEFAULT 0,
    rebuilding    BOOLEAN     NOT NULL DEFAULT false,
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

### Upsert semantics

Read model state is stored as JSONB and upserted on each event:

```sql
INSERT INTO projections (projection_id, aggregate_id, state, updated_at)
VALUES ($1, $2, $3, now())
ON CONFLICT (projection_id, aggregate_id)
DO UPDATE SET state = EXCLUDED.state, updated_at = now()
```

### Checkpoint tracking

The projection consumer uses checkpoints to skip already-processed events.
After applying an event, it updates the checkpoint:

```sql
INSERT INTO projection_checkpoints (projection_id, last_version, updated_at)
VALUES ($1, $2, now())
ON CONFLICT (projection_id)
DO UPDATE SET last_version = EXCLUDED.last_version, updated_at = now()
```

On startup, the consumer reads the checkpoint to determine where to resume:

```rust
let last_version = store.get_last_version("station_inventory").await?;
// Skip events with version <= last_version
```

### Projection rebuild

To rebuild a projection from scratch:

1. Set the rebuilding flag:
   ```rust
   store.set_rebuilding("station_inventory", true).await?;
   ```

2. Reset the checkpoint to the desired version:
   ```rust
   store.reset_checkpoint("station_inventory", Version::initial()).await?;
   ```

3. The consumer replays from the reset offset. Read endpoints detect
   `rebuilding = true` and fall back to read-through queries.

4. After replay completes:
   ```rust
   store.set_rebuilding("station_inventory", false).await?;
   ```

### Full checkpoint struct

The `get_checkpoint` method returns all checkpoint metadata:

```rust
let checkpoint = store.get_checkpoint("station_inventory").await?;
// Checkpoint {
//     projection_id: "station_inventory",
//     last_version: Version(42),
//     rebuilding: false,
//     updated_at: 2024-01-15T10:30:00Z,
// }
```

---

## Dead Letter Store (YugabyteDB)

**Crate**: `canon-deadletter-yugabyte`
**Trait**: `DeadLetterStore`

Messages that exhaust their retry budget are persisted in the dead letter store
for admin inspection, requeue, or discard.

### Schema

```sql
CREATE TABLE dead_letters (
    id              UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    message_id      UUID,
    handler_id      TEXT,
    aggregate_id    UUID,
    payload         BYTEA,
    error           TEXT,
    attempts        INT         DEFAULT 1,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_attempted  TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

### Operations

```rust
// Store a dead letter
let dl_id = store.store(
    message_id,
    "ShipCommandHandler",
    &aggregate_id,
    payload,
    "version conflict after 3 retries",
).await?;

// List all dead letters (optionally filtered by handler)
let all = store.list(None).await?;
let handler_only = store.list(Some("ShipCommandHandler")).await?;

// Requeue: removes from dead_letters, re-enters inbox with fresh expires_at
store.requeue(dl_id).await?;

// Discard: permanently removes from dead_letters
store.discard(dl_id).await?;
```

Both `requeue` and `discard` return `NotFound` if the dead letter ID does not
exist.

---

## Retry Tracker (YugabyteDB)

**Crate**: `canon-deadletter-yugabyte` (submodule `retry_tracker`)
**Trait**: `RetryTracker`

The retry tracker provides crash-safe retry counting. Counts survive process
restarts because they are persisted in YugabyteDB.

### Schema

```sql
CREATE TABLE retry_attempts (
    message_id     UUID         PRIMARY KEY,
    handler_id     TEXT         NOT NULL,
    attempts       INT          NOT NULL DEFAULT 0,
    last_attempted TIMESTAMPTZ  NOT NULL DEFAULT now()
);
```

### Atomic increment

The `increment` method uses an atomic UPSERT that either inserts a new row
with `attempts = 1` or increments the existing count:

```sql
INSERT INTO retry_attempts (message_id, handler_id, attempts, last_attempted)
VALUES ($1, $2, 1, now())
ON CONFLICT (message_id) DO UPDATE
    SET attempts = retry_attempts.attempts + 1,
        last_attempted = now()
RETURNING attempts
```

The returned count is compared against the max retry limit. When exceeded,
the message is moved to the dead letter store.

### Synchronous trait

`RetryTracker` is a synchronous trait (no async). The YugabyteDB implementation
uses `tokio::task::block_in_place` with `Handle::current().block_on()` to run
the async database call on the current runtime without creating a nested runtime.

---

## Kafka configuration details

### rskafka patterns

All four Kafka crates follow the same connection and messaging pattern:

1. **Connection**: `ClientBuilder::new(broker_list).build().await` then
   `client.partition_client(topic, 0, UnknownTopicHandling::Retry)`.

2. **Produce**: `partition_client.produce(vec![record], Compression::NoCompression)`
   with `Record { key, value, headers: BTreeMap::new(), timestamp }`.

3. **Consume**: `partition_client.fetch_records(next_offset, 1..1_048_576, timeout_ms)`
   in a polling loop. Offset tracked in-memory (`Mutex<i64>`, starts at 0).

4. **Commit**: Always a no-op. Application-layer idempotency handles duplicates.

5. **No consumer groups**: rskafka has no consumer group abstraction. Each consumer
   polls partition 0 independently. `group_id` fields are kept for API
   compatibility but unused.

6. **Errors**: each crate defines its own `thiserror` error type wrapping rskafka
   errors as strings.

### Topic naming

Each service has three Kafka topics:

| Pattern | Purpose | Example |
|---------|---------|---------|
| `canon.{service}.inbound` | Assembled batches from inbox to dispatcher | `canon.fleet.inbound` |
| `canon.{service}.outbound` | Committed events to consumers | `canon.fleet.outbound` |
| `canon.{service}.events` | Published events for other services | `canon.fleet.events` |

All 15 topics (5 services x 3 topics) are explicitly created at cluster startup
by the `init-kafka-topics` job. No auto-create.

### Offset management

Canon uses in-memory offset tracking that restarts from zero on each boot.
Application-layer idempotency is the safety net at every stage:

| Consumer | Idempotency mechanism |
|----------|----------------------|
| Inbox | `(handler_id, message_id)` composite PK rejects duplicates |
| Event store | Cassandra `(aggregate_id, version)` PK + LWT rejects duplicates |
| Projections | `last_version` checkpoint skips already-processed events |
| Publisher | Downstream consumers handle duplicates |

No Kafka consumer groups, no external offset commits.

---

## Environment variables

```bash
CASSANDRA_NODES=cassandra:9042        # Comma-separated Cassandra contact points
CASSANDRA_KEYSPACE=canon_fleet        # Per-service keyspace (defaults to "canon")
YUGABYTE_URL=postgres://canon:canon@yugabytedb:5433/canon
KAFKA_BROKERS=kafka:9092              # Comma-separated Kafka broker list
```

### Per-service pool creation

Use the shared helper to create isolated database pools:

```rust
use canon_demo_shared::db::create_service_pool;

let pool = create_service_pool("canon_fleet").await?;
```

For Cassandra:

```rust
use canon_event_store_cassandra::CassandraEventStore;

let event_store = CassandraEventStore::new_with_keyspace(
    &std::env::var("CASSANDRA_NODES")?,
    "canon_fleet",
).await?;
```

---

## Swapping implementations

Canon's trait architecture means you can swap any infrastructure crate. To use a
different event store (say DynamoDB), implement the `EventStore` trait and wire it
into your service:

```rust
ServiceBuilder::new()
    .for_aggregate::<Ship>()
    .with_event_store(my_dynamodb_event_store)
    .build()
```

The core framework code never changes. Every infrastructure crate depends only on
its trait crate and `canon-core` -- never on other infrastructure crates. This
strict DAG is enforced by the workspace dependency graph.
