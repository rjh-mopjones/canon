# Infrastructure

Canon's hexagonal architecture means every infrastructure concern sits behind a trait.
This chapter covers the concrete implementations and how to configure them.

## Storage backends

| Concern | Trait | Backend | Crate |
|---------|-------|---------|-------|
| Event store | `EventStore` | Cassandra | `canon-event-store-cassandra` |
| Command store | `CommandStore` | YugabyteDB | `canon-command-store-yugabyte` |
| Snapshot store | `SnapshotStore` | YugabyteDB | `canon-snapshot-store-yugabyte` |
| Inbox | `Inbox` | YugabyteDB | `canon-inbox-yugabyte` |
| Projection store | `ProjectionStore` | YugabyteDB | `canon-projection-store-yugabyte` |
| Dead letter store | `DeadLetterStore` | YugabyteDB | `canon-deadletter-yugabyte` |
| Outbox | (internal) | YugabyteDB | part of command handler write path |

| Concern | Trait | Backend | Crate |
|---------|-------|---------|-------|
| Inbound queue | `InboundQueue` | Kafka | `canon-inbound-queue-kafka` |
| Outbound queue | `OutboundQueue` | Kafka | `canon-outbound-queue-kafka` |
| Publisher | `EventPublisher` | Kafka | `canon-publisher-kafka` |
| Adaptor | `EventAdaptor` | Kafka | `canon-adaptor-kafka` |

## Why this storage split?

- **Cassandra for events** -- append-optimised, high-volume, wide rows per aggregate stream.
  Events are immutable and append-only, which is Cassandra's sweet spot.

- **YugabyteDB for everything else** -- transactional, queryable, strong consistency.
  The outbox pattern requires ACID transactions. Inbox dedup requires composite key
  uniqueness. Projections need queryable read models.

- **Kafka for messaging** -- durable, partitioned message transport between pipeline
  stages. All topics partitioned by `aggregate_id` for ordered processing.

## Kafka configuration

Canon uses `rskafka` exclusively -- pure Rust, no C dependencies, cross-compilable.

### Connection pattern

All four Kafka crates follow the same pattern:

```rust
use rskafka::client::ClientBuilder;
use rskafka::client::partition::UnknownTopicHandling;

let client = ClientBuilder::new(vec!["kafka:9092".to_owned()])
    .build()
    .await?;

let partition_client = client
    .partition_client("canon.fleet.outbound", 0, UnknownTopicHandling::Retry)
    .await?;
```

### Producing

```rust
use rskafka::record::Record;
use rskafka::client::partition::Compression;

partition_client.produce(
    vec![Record {
        key: Some(aggregate_id.as_uuid().as_bytes().to_vec()),
        value: Some(serialised_envelope),
        headers: BTreeMap::new(),
        timestamp: OffsetDateTime::now_utc(),
    }],
    Compression::NoCompression,
).await?;
```

### Consuming

```rust
let records = partition_client
    .fetch_records(next_offset, 1..1_048_576, 100)  // timeout_ms
    .await?;

for (record, _) in &records.0 {
    // Process record...
    next_offset += 1;
}
```

### Offset management

Canon uses **in-memory offset tracking** that restarts from zero on each boot.
Application-layer idempotency is the safety net:

- Inbox dedup via `handler_id + message_id`
- Cassandra primary key rejects duplicate event versions
- Projection checkpoint skips already-processed events

No Kafka consumer groups, no external offset commits.

### Topic naming

Each service has three Kafka topics:

| Pattern | Purpose | Example |
|---------|---------|---------|
| `canon.{service}.inbound` | Assembled batches from inbox to dispatcher | `canon.fleet.inbound` |
| `canon.{service}.outbound` | Committed events to consumers | `canon.fleet.outbound` |
| `canon.{service}.events` | Published events for other services | `canon.fleet.events` |

All 15 topics (5 services x 3 topics) are explicitly created at cluster startup.
No auto-create.

## YugabyteDB schema

Each service uses its own schema for complete storage isolation:

```sql
-- Schema per service
CREATE SCHEMA IF NOT EXISTS canon_fleet;
CREATE SCHEMA IF NOT EXISTS canon_cargo;
CREATE SCHEMA IF NOT EXISTS canon_navigation;
CREATE SCHEMA IF NOT EXISTS canon_supply;
CREATE SCHEMA IF NOT EXISTS canon_station;
```

Tables within each schema:

```sql
-- Inbox
CREATE TABLE canon_fleet.inbox_messages (
    handler_id TEXT NOT NULL,
    message_id UUID NOT NULL,
    PRIMARY KEY (handler_id, message_id)
);

CREATE TABLE canon_fleet.inbox_windows (
    handler_id TEXT NOT NULL,
    correlation_key UUID NOT NULL,
    -- window state, TTL, etc.
    PRIMARY KEY (handler_id, correlation_key)
);

CREATE TABLE canon_fleet.processed_windows (
    window_id UUID PRIMARY KEY
);

-- Commands
CREATE TABLE canon_fleet.commands (
    command_id UUID PRIMARY KEY,
    aggregate_id UUID NOT NULL,
    -- payload, metadata
    created_at TIMESTAMP WITH TIME ZONE NOT NULL
);

-- Outbox
CREATE TABLE canon_fleet.outbox (
    id UUID PRIMARY KEY,
    sequence_number BIGINT NOT NULL,
    -- event payload, metadata
    delivered_at TIMESTAMP WITH TIME ZONE
);

-- Supporting tables
CREATE TABLE canon_fleet.snapshots (aggregate_id UUID PRIMARY KEY, ...);
CREATE TABLE canon_fleet.projection_checkpoints (projection_id TEXT PRIMARY KEY, ...);
CREATE TABLE canon_fleet.dead_letters (id UUID PRIMARY KEY, ...);
CREATE TABLE canon_fleet.retry_attempts (message_id UUID PRIMARY KEY, ...);
```

## Cassandra keyspaces

Each service uses its own keyspace:

```cql
CREATE KEYSPACE IF NOT EXISTS canon_fleet
    WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

CREATE TABLE canon_fleet.events (
    aggregate_id UUID,
    version BIGINT,
    event_id UUID,
    event_type TEXT,
    event_version INT,
    payload BLOB,
    correlation_id UUID,
    causation_id UUID,
    timestamp TIMESTAMP,
    PRIMARY KEY (aggregate_id, version)
) WITH CLUSTERING ORDER BY (version ASC);
```

The primary key `(aggregate_id, version)` ensures:
- All events for an aggregate are stored together (partition key)
- Events are ordered by version (clustering key)
- Duplicate version writes are rejected (optimistic concurrency)

## Environment variables

```bash
CASSANDRA_NODES=cassandra:9042
YUGABYTE_URL=postgres://canon:canon@yugabytedb:5433/canon
KAFKA_BROKERS=kafka:9092
```

## Per-service pool creation

Use the shared helper to create isolated database pools:

```rust
use canon_demo_shared::db::create_service_pool;

let pool = create_service_pool("canon_fleet").await?;
```

For Cassandra:

```rust
use canon_event_store_cassandra::CassandraEventStore;

let event_store = CassandraEventStore::new_with_keyspace(
    &["cassandra:9042"],
    "canon_fleet",
).await?;
```

## Swapping implementations

Canon's trait architecture means you can swap any infrastructure crate. To use a
different event store (say DynamoDB), implement the `EventStore` trait and wire it
into `ServiceBuilder`:

```rust
ServiceBuilder::new()
    .for_aggregate::<Ship>()
    .with_event_store(my_dynamodb_event_store)
    .build()
```

The core framework code never changes.
