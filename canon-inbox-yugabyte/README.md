# canon-inbox-yugabyte

YugabyteDB-backed implementation of the [`Inbox`](../canon-inbox) port for Canon.

The inbox is the entry point for all messages entering a service. It provides idempotent
message acceptance, correlated window assembly, oversight-gated dispatch to the
inbound queue, and window expiry with dead-letter integration.

## Responsibilities

- **Deduplication** -- `ON CONFLICT DO NOTHING` on `(handler_id, message_id)`. Duplicate
  submissions are silently dropped.
- **Window assembly** -- messages are accumulated into a per-`(handler_id, aggregate_id)`
  window using JSONB append (`||`). Each window carries a stable `window_id`.
- **Oversight** -- after every non-duplicate submission, the registered oversight function
  is evaluated against the accumulated window. It returns `Ready`, `NotReady`, or `Discard`.
- **Dispatch** -- on `Ready`, the assembled batch is published to `canon-inbound-queue` and
  the window is cleared. On `Discard`, the window is cleared without dispatch. On
  `NotReady`, the window is left to accumulate further messages.
- **Batch idempotency** -- the inbound queue consumer calls `try_mark_window_processed`
  before processing a batch. Uses `INSERT INTO processed_windows ... ON CONFLICT DO NOTHING`
  to detect duplicate delivery after Kafka rebalances. Returns `true` if the batch is
  new and should be processed, `false` if it should be skipped.
- **Window expiry** -- handlers registered with a `window_ttl` get an `expires_at`
  timestamp on new windows. A background cleanup task (`spawn_cleanup_task`) periodically
  sweeps expired windows and moves them to the dead letter store with reason
  `window_expired`.
- **Dead letter requeue** -- `requeue_expired_window` re-inserts messages into the inbox
  with a fresh `expires_at` and `Pending` status. Oversight runs again from scratch.

## Window status lifecycle

```
pending -> dispatched    (Oversight::Ready -- batch published, window cleared)
pending -> expired       (TTL exceeded -- sweep marks window expired)
expired -> dead_lettered (cleanup task moves to dead letter store)
```

## Cleanup task

`spawn_cleanup_task` starts a background tokio task that:

1. **Sweeps** -- marks pending windows past their TTL as `expired`
2. **Collects** -- transitions expired windows to `dead_lettered` and returns them
3. **Dead-letters** -- invokes a user-provided callback to persist to the dead letter store

```rust,ignore
use canon_inbox_yugabyte::{spawn_cleanup_task, CleanupConfig};

let handle = spawn_cleanup_task(
    inbox.clone(),
    CleanupConfig::default(), // 30s interval
    |entries| async move {
        for entry in entries {
            dead_letter_store.store(/* ... */).await?;
        }
        Ok(())
    },
);
```

## Usage

```rust,ignore
use canon_inbox_yugabyte::YugabyteInbox;
use std::sync::Arc;

let pool = sqlx::PgPool::connect(&std::env::var("YUGABYTE_URL")?).await?;
let queue: Arc<dyn canon_inbound_queue::InboundQueue> = /* ... */;
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

- [`canon-inbox`](../canon-inbox) -- `Inbox` trait
- [`canon-inbound-queue`](../canon-inbound-queue) -- dispatch target on `Oversight::Ready`
- [`canon-core`](../canon-core) -- `IncomingMessage`, `Oversight`, `AggregateId`, `WindowStatus`
