# Dead Letter Handling

Dead letter handling ensures that failed messages are captured, inspectable, and
recoverable. No event silently disappears in Canon.

## When messages become dead letters

Messages are dead-lettered in two situations:

1. **Max retry failures** -- the event store consumer retries a Cassandra write (e.g.,
   version conflict) up to a configured maximum (default: 3). After exhausting retries,
   the message is moved to the dead letter store.

2. **Window expiry** -- an inbox window that never reaches `Ready` before its TTL
   expires is dead-lettered with reason `window_expired`.

## Retry mechanism

### Crash-safe retry counting

Retry counts are persisted in a `retry_attempts` table in YugabyteDB:

```sql
CREATE TABLE canon_fleet.retry_attempts (
    message_id UUID PRIMARY KEY,
    handler_id TEXT NOT NULL,
    attempt_count INTEGER NOT NULL DEFAULT 0,
    last_attempt_at TIMESTAMP WITH TIME ZONE NOT NULL,
    error_message TEXT
);
```

This is crash-safe -- if the process dies between retries, the count is preserved on
restart. No in-memory retry counters that reset on crash.

### Retry flow

```
Event arrives at consumer
      |
      v
Process event
      |
   Success?
   /       \
  Yes       No
   |         |
   v         v
  Done    Increment retry_attempts
            |
         At max retries?
         /         \
        No          Yes
         |           |
         v           v
     Retry later   Dead letter
                     |
                     v
              Remove from retry_attempts
              Insert into dead_letters
```

### Event store consumer retries

The most common retry scenario is the event store consumer encountering a Cassandra
version conflict. This happens when:

- Two replicas process the same event concurrently
- A snapshot write interferes with an event write
- Network issues cause a timeout that the server actually processed

The consumer retries up to 3 times with backoff before dead-lettering.

## Dead letter store

```sql
CREATE TABLE canon_fleet.dead_letters (
    id UUID PRIMARY KEY,
    handler_id TEXT NOT NULL,
    message_id UUID NOT NULL,
    message_type TEXT NOT NULL,
    payload BYTEA NOT NULL,
    reason TEXT NOT NULL,
    error_message TEXT,
    original_timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    dead_lettered_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);
```

Key fields:
- `reason` -- why the message was dead-lettered (`max_retries_exceeded`, `window_expired`, etc.)
- `error_message` -- the last error encountered before dead-lettering
- `original_timestamp` -- when the message was originally created (for auditing)

## The DeadLetterStore trait

```rust
pub trait DeadLetterStore: Send + Sync + 'static {
    type Error: std::error::Error + Send + Sync + 'static;

    async fn store(&self, entry: DeadLetterEntry) -> Result<(), Self::Error>;
    async fn list(&self) -> Result<Vec<DeadLetterEntry>, Self::Error>;
    async fn get(&self, id: Uuid) -> Result<Option<DeadLetterEntry>, Self::Error>;
    async fn requeue(&self, id: Uuid) -> Result<(), Self::Error>;
    async fn discard(&self, id: Uuid) -> Result<(), Self::Error>;
}
```

## Requeue

Dead-lettered messages can be requeued via the admin API. Requeue:

1. Reads the dead letter entry
2. Re-inserts the message into `inbox_windows` with:
   - Fresh `expires_at` (new TTL countdown)
   - Status `pending` (oversight runs again from scratch)
3. Preserves the original `message_id` (inbox idempotency deduplicates naturally)
4. Removes the entry from the dead letter store

The message goes through the full inbox pipeline again -- dedup, oversight, dispatch.
Oversight is not bypassed; the handler evaluates the message fresh.

### Admin API

The gateway exposes dead letter management endpoints:

```
GET  /admin/deadletters          -- list all dead-lettered messages
GET  /admin/deadletters/:id      -- get a specific dead letter
POST /admin/deadletters/:id/requeue  -- requeue for reprocessing
POST /admin/deadletters/:id/discard  -- permanently discard
```

## Window expiry dead letters

When a window's TTL expires:

1. The cleanup background task finds windows with `expires_at < NOW()` and
   `status = 'pending'`
2. Sets the window status to `expired`
3. Collects all messages in the window
4. Writes a dead letter entry with reason `window_expired`
5. The messages are available for inspection and requeue

This ensures that messages stuck in `NotReady` windows are not lost forever.

## Monitoring dead letters

Dead-lettered messages are a signal that something is wrong. Common causes:

| Reason | Likely cause | Resolution |
|--------|-------------|------------|
| `max_retries_exceeded` | Cassandra version conflict | Check for concurrent writes, consider increasing retries |
| `window_expired` | Expected events never arrived | Check upstream service health, verify event routing |
| `handler_error` | Bug in event handler code | Fix the handler, requeue the message |

## Dead letters in the demo

The demo gateway's admin endpoints expose dead letters to the frontend. The Scenarios
page includes Mission 04 ("The Cassandra Incident") which demonstrates dead letter
handling:

- Deliberately trigger Cassandra write failures
- Show dead-lettered events in the UI
- Requeue or discard with visual feedback
