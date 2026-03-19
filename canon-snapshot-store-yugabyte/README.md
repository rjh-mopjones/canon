# canon-snapshot-store-yugabyte

YugabyteDB-backed implementation of the [`SnapshotStore`](../canon-snapshot-store) port for Canon.

Snapshots allow aggregate hydration to skip replaying the full event history.

## Snapshot strategy

Written by the **event store consumer** after a confirmed Cassandra write, when
`version % snapshot_every == 0`. Never written by the command handler.

## Hydration with snapshots

```
1. Load latest snapshot  →  (state_bytes, snapshot_version)
2. Load events from snapshot_version + 1  →  Vec<EventEnvelope>
3. Apply via #[event_combiner]  →  current state
```

## Usage

```rust
use canon_snapshot_store_yugabyte::YugabyteSnapshotStore;

let store = YugabyteSnapshotStore::new(&std::env::var("YUGABYTE_URL")?).await?;
```

## Environment

| Variable       | Description                  |
|----------------|------------------------------|
| `YUGABYTE_URL` | YugabyteDB connection string |

## Dependencies

- [`canon-snapshot-store`](../canon-snapshot-store) — `SnapshotStore` trait
- [`canon-core`](../canon-core) — `AggregateId`, `Version`
