# canon-command-store-yugabyte

YugabyteDB-backed implementation of the [`CommandStore`](../canon-command-store) port for Canon.

The command store is the permanent audit trail for every command processed by a service.
Written as part of the single ACID transaction that also stages events into the outbox —
the command record and its resulting outbox entries are always consistent.

## Responsibilities

- **Store** — persist a `CommandEnvelope` on every successful command handler invocation.
- **Load** — retrieve a command by `command_id` (used by counterfactual replay).
- **Load for aggregate** — retrieve the full ordered command history for an aggregate.
- **Update status** — track command lifecycle: `pending` → `processed` / `failed`.

## Usage

```rust
use canon_command_store_yugabyte::YugabyteCommandStore;

let store = YugabyteCommandStore::new(&std::env::var("YUGABYTE_URL")?).await?;
```

## Environment

| Variable       | Description                  |
|----------------|------------------------------|
| `YUGABYTE_URL` | YugabyteDB connection string |

## Dependencies

- [`canon-command-store`](../canon-command-store) — `CommandStore` trait
- [`canon-core`](../canon-core) — `CommandEnvelope`, `AggregateId`
