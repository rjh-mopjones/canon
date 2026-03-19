# canon-event-store-cassandra

Cassandra-backed implementation of the [`EventStore`](../canon-event-store) port for Canon.

The event store is the append-only source of truth. Every committed event lives here,
ordered by version within each aggregate partition.

## Responsibilities

- **Append** — writes `EventEnvelope` records using lightweight transactions
  (`IF NOT EXISTS` on `version`) to enforce optimistic concurrency.
- **Load** — returns the full ordered event history for an aggregate.
- **Load from version** — returns events at or after a given version, used during
  hydration from a snapshot checkpoint.
- **Snapshot trigger** — after a confirmed write, if `version % snapshot_every == 0`,
  delegates a snapshot write to the injected `SnapshotStore`.

## Optimistic concurrency

Cassandra LWTs guarantee exactly one writer wins per aggregate version. The loser
receives `EventStoreError::VersionConflict` and must reload state before retrying.

## Usage

```rust
use canon_event_store_cassandra::CassandraEventStore;

let store = CassandraEventStore::new(
    &std::env::var("CASSANDRA_NODES")?,
    snapshot_store,
    50, // snapshot_every
).await?;
```

## Environment

| Variable          | Description                              |
|-------------------|------------------------------------------|
| `CASSANDRA_NODES` | Comma-separated Cassandra node addresses |

## Dependencies

- [`canon-event-store`](../canon-event-store) — `EventStore` trait
- [`canon-snapshot-store`](../canon-snapshot-store) — `SnapshotStore` trait (injected)
- [`canon-core`](../canon-core) — `EventEnvelope`, `AggregateId`, `Version`
