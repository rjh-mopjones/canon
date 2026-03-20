# canon-projection-store-yugabyte

YugabyteDB-backed implementation of the [`ProjectionStore`](../canon-projection-store) port for Canon.

Persists materialised read models and tracks the event version checkpoint per projection.

## Rebuild flow

```
1. reset_checkpoint(projection_id, target_version)   -- sets rebuilding=true + last_version atomically
2. Reset Kafka consumer offset on canon.{service}.outbound to target checkpoint
3. Replay → apply() for each event
4. set_rebuilding(projection_id, false)               -- marks rebuild complete
```

While `rebuilding == true`, read endpoints fall back to read-through and never serve stale materialised views. The `rebuild_from` checkpoint allows resetting to a last known good version rather than replaying from the beginning.

## Methods

- `get_checkpoint(projection_id)` -- returns full `Checkpoint` with `last_version`, `rebuilding`, `updated_at`
- `reset_checkpoint(projection_id, target)` -- atomically sets `last_version = target` and `rebuilding = true`

## Usage

```rust
use canon_projection_store_yugabyte::YugabyteProjectionStore;

let store = YugabyteProjectionStore::new(&std::env::var("YUGABYTE_URL")?).await?;
```

## Environment

| Variable       | Description                  |
|----------------|------------------------------|
| `YUGABYTE_URL` | YugabyteDB connection string |

## Dependencies

- [`canon-projection-store`](../canon-projection-store) — `ProjectionStore` trait
- [`canon-core`](../canon-core) — `AggregateId`
