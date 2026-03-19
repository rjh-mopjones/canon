# canon-projection-store-yugabyte

YugabyteDB-backed implementation of the [`ProjectionStore`](../canon-projection-store) port for Canon.

Persists materialised read models and tracks the event version checkpoint per projection.

## Rebuild flow

```
1. set_rebuilding(projection_id, true)
2. Reset Kafka consumer offset
3. Replay → apply() for each event
4. set_rebuilding(projection_id, false)
```

While rebuilding, read endpoints fall back to read-through.

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
