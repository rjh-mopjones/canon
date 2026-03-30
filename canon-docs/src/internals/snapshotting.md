# Snapshotting

Snapshotting is Canon's mechanism for optimising aggregate hydration. Without snapshots,
loading an aggregate requires replaying its entire event history from version zero.
With snapshots, hydration starts from the most recent snapshot and only replays events
after that point.

## How snapshotting works

### Configuration

Enable snapshotting with the `snapshot_every` attribute on your aggregate:

```rust
#[aggregate(snapshot_every = 50)]
pub struct Ship {
    status: ShipStatus,
    fuel_level: f32,
    current_station: Option<StationId>,
}
```

This tells Canon to write a snapshot every 50 events.

### Hydration with snapshots

When Canon needs to load an aggregate's state:

```
1. Load most recent snapshot for aggregate_id
   -> Found snapshot at version 200

2. Load events from version 201 forward
   -> Events [201, 202, ..., 247]

3. Apply events via version-matched combiners
   -> Current state at version 247
```

Without a snapshot, step 1 returns nothing and all 247 events must be replayed.

### Hydration without snapshots

```
1. Load most recent snapshot -> None

2. Load all events from version 0
   -> Events [0, 1, 2, ..., 247]

3. Apply all 247 events
   -> Current state at version 247
```

For aggregates with thousands of events, the difference is dramatic.

## Who writes snapshots

Snapshots are written by the **event store consumer** on the outbound queue -- never by
the command handler or outbox processor.

The flow:

```
Command handler
      |
      v
Outbox (YugabyteDB ACID txn)
      |
      v
Outbox processor -> Outbound queue (Kafka)
      |
      v
Event store consumer:
  1. Write event to Cassandra
  2. If version % snapshot_every == 0:
       Write snapshot to YugabyteDB
```

This separation is intentional:
- The command handler only writes to the outbox (single-responsibility)
- Snapshot writes happen after confirmed event store writes
- Snapshot failures do not affect event persistence

## Snapshot store

Snapshots are stored in YugabyteDB (not Cassandra), providing:
- Transactional reads
- Simple key-value lookup by `aggregate_id`
- Independent scaling from the event store

```sql
CREATE TABLE canon_fleet.snapshots (
    aggregate_id UUID PRIMARY KEY,
    version BIGINT NOT NULL,
    state BYTEA NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);
```

Each aggregate has at most one snapshot row. New snapshots overwrite the previous one.

## Version check

The event store consumer checks whether to write a snapshot after each confirmed
Cassandra write:

```rust
if event.version.as_u64() % snapshot_every == 0 {
    let state = hydrate_from_events(aggregate_id).await?;
    snapshot_store.save(aggregate_id, event.version, &state).await?;
}
```

The `snapshot_every` value comes from the `#[aggregate]` macro registration.

## Snapshot format

Snapshots are serialised using the same serde format as events (typically JSON or
MessagePack). The `#[aggregate]` macro generates `Serialize` and `Deserialize` derives
on the aggregate struct.

## Snapshot consistency

Snapshots are eventually consistent with the event store. A snapshot at version 200
means the aggregate state at version 200 is fully captured. Events 201+ must still be
replayed on top.

If a snapshot write fails:
- The event store still has all events (snapshots are an optimisation, not the source of truth)
- The next successful snapshot will capture a later version
- Hydration falls back to replaying more events

## When to use snapshots

**Use snapshots when:**
- Aggregates accumulate many events over time
- Hydration latency matters for your use case
- You can tolerate the storage cost of snapshot rows

**Skip snapshots when:**
- Aggregates have short lifetimes (few events)
- Hydration is not on the critical path
- You want the simplest possible setup

## Snapshot and version-matched routing

Snapshots interact cleanly with version-matched routing. The snapshot captures the
aggregate state at a point in time, serialised with the current schema. Events after
the snapshot version are replayed through their version-matched combiners, updating
the state incrementally.

This means:
- Old snapshots remain valid even as new event versions are added
- No snapshot migration needed when adding new event versions
- The combiner chain handles schema evolution automatically
