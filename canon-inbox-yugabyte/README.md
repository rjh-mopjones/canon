# canon-inbox-yugabyte

YugabyteDB-backed implementation of the [`Inbox`](../canon-inbox) port for Canon.

The inbox is the entry point for all messages entering a service. It provides idempotent
message acceptance, correlated window assembly, and oversight-gated dispatch to the
inbound queue.

## Responsibilities

- **Deduplication** — `ON CONFLICT DO NOTHING` on `(handler_id, message_id)`. Duplicate
  submissions are silently dropped.
- **Window assembly** — messages are accumulated into a per-`(handler_id, aggregate_id)`
  window using JSONB append (`||`). Each window carries a stable `window_id`.
- **Oversight** — after every non-duplicate submission, the registered oversight function
  is evaluated against the accumulated window. It returns `Ready`, `NotReady`, or `Discard`.
- **Dispatch** — on `Ready`, the assembled batch is published to `canon-queue` and
  the window is cleared. On `Discard`, the window is cleared without dispatch. On
  `NotReady`, the window is left to accumulate further messages.

## Window status lifecycle

```
pending → dispatched    (Oversight::Ready — batch published, window cleared)
pending → expired       (TTL exceeded — moved to dead letter by cleanup task)
pending → dead_lettered (processing failure — moved to dead letter)
```

## Usage

```rust
use canon_inbox_yugabyte::YugabyteInbox;
use std::sync::Arc;

let pool = sqlx::PgPool::connect(&std::env::var("YUGABYTE_URL")?).await?;
let queue: Arc<dyn canon_queue::InboundQueue> = /* ... */;
let inbox = YugabyteInbox::new(pool, queue);
```

## Environment

| Variable       | Description                  |
|----------------|------------------------------|
| `YUGABYTE_URL` | YugabyteDB connection string |

## Schema

Three tables managed via sqlx migrations: `inbox_messages`, `inbox_windows`,
`processed_windows`. See `migrations/001_inbox.sql`.

## Dependencies

- [`canon-inbox`](../canon-inbox) — `Inbox` trait
- [`canon-queue`](../canon-queue) — dispatch target on `Oversight::Ready`
- [`canon-core`](../canon-core) — `IncomingMessage`, `Oversight`, `AggregateId`
