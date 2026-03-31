# Dead Letter Handling

Dead letter handling ensures that no message silently disappears in Canon. When a
message fails repeatedly or an inbox window expires before reaching `Ready`, the
message is captured in a dead letter store where it can be inspected, requeued, or
permanently discarded.

## When messages become dead letters

Messages are dead-lettered in two situations:

1. **Max retry failures.** A consumer (typically the event store consumer) encounters
   a processing error -- for example, a Cassandra version conflict. Each failure
   increments a crash-safe retry counter. After the counter reaches the configured
   maximum (default: 3), the message is moved to the dead letter store.

2. **Window expiry.** An inbox window with a TTL that never reaches `Oversight::Ready`
   before the TTL expires. The cleanup task finds expired windows, collects all their
   messages, and writes them to the dead letter store with reason `window_expired`.

---

## The DeadLetterStore trait

The `DeadLetterStore` trait defines the contract for storing and managing dead-lettered
messages. It lives in `canon-core/src/traits/dead_letter_store.rs`:

```rust
#[async_trait]
pub trait DeadLetterStore: Send + Sync + 'static {
    type Error: std::error::Error + Send + Sync + 'static;

    /// Store a dead-lettered message. Returns the dead letter ID.
    async fn store(
        &self,
        message_id: Uuid,
        handler_id: &str,
        aggregate_id: &AggregateId,
        payload: bytes::Bytes,
        error: &str,
    ) -> Result<Uuid, Self::Error>;

    /// List dead letters, optionally filtered by handler ID.
    async fn list(
        &self,
        handler_id: Option<&str>,
    ) -> Result<Vec<DeadLetter>, Self::Error>;

    /// Re-enter a dead letter into the inbox for reprocessing.
    async fn requeue(&self, id: Uuid) -> Result<(), Self::Error>;

    /// Permanently remove a dead letter.
    async fn discard(&self, id: Uuid) -> Result<(), Self::Error>;
}
```

Four operations cover the full lifecycle: store (capture), list (inspect), requeue
(recover), and discard (remove). Every operation is async and returns a crate-local
error type per the Canon `thiserror` rule.

### The DeadLetter struct

The `DeadLetter` struct is the read model returned by `list`:

```rust
pub struct DeadLetter {
    pub id: Uuid,
    pub message_id: Uuid,
    pub handler_id: String,
    pub aggregate_id: AggregateId,
    pub payload: Bytes,
    pub error: String,
    pub attempts: u32,
    pub created_at: DateTime<Utc>,
    pub last_attempted: DateTime<Utc>,
}
```

Key fields:
- `id` -- unique dead letter identifier (used for requeue/discard)
- `message_id` -- the original message's ID (command_id or event_id)
- `handler_id` -- which handler was processing when the failure occurred
- `error` -- the last error message before dead-lettering
- `attempts` -- how many times the message was attempted

---

## Retry tracking

### The RetryTracker trait

Retry tracking is a separate concern from dead letter storage. The `RetryTracker`
trait provides crash-safe attempt counting:

```rust
pub trait RetryTracker: Send + Sync + 'static {
    type Error: std::error::Error + Send + Sync + 'static;

    /// Increment the attempt count for a message. Returns the new count.
    fn increment(&self, message_id: Uuid, handler_id: &str) -> Result<u32, Self::Error>;

    /// Get the current retry attempt for a message.
    fn get(&self, message_id: Uuid) -> Result<Option<RetryAttempt>, Self::Error>;

    /// Remove the retry record (after success or dead-lettering).
    fn remove(&self, message_id: Uuid) -> Result<(), Self::Error>;
}
```

The `RetryTracker` is synchronous (not async). The `RetryAttempt` struct carries
the current state:

```rust
pub struct RetryAttempt {
    pub message_id: Uuid,
    pub handler_id: String,
    pub attempts: u32,
    pub last_attempted: DateTime<Utc>,
}
```

### Crash-safe retry counting

The retry count is persisted in a `retry_attempts` table in YugabyteDB. The
`YugabyteRetryTracker` uses an atomic UPSERT so that the count survives process
crashes:

```sql
INSERT INTO retry_attempts (message_id, handler_id, attempts, last_attempted)
VALUES ($1, $2, 1, now())
ON CONFLICT (message_id) DO UPDATE
  SET attempts = retry_attempts.attempts + 1,
      last_attempted = now()
RETURNING attempts
```

If the process dies between retries, the count is preserved on restart. This is
critical -- in-memory retry counters that reset on crash would allow messages to
retry indefinitely, never reaching the dead letter threshold.

### The retry_attempts table

```sql
CREATE TABLE retry_attempts (
    message_id     UUID         PRIMARY KEY,
    handler_id     TEXT         NOT NULL,
    attempts       INT          NOT NULL DEFAULT 0,
    last_attempted TIMESTAMPTZ  NOT NULL DEFAULT now()
);
```

Each service has its own `retry_attempts` table in its own schema (`canon_fleet`,
`canon_cargo`, etc.).

---

## The RetryPolicy coordinator

The `RetryPolicy` struct in `canon-core/src/memory/retry_policy.rs` coordinates
the retry tracker and dead letter store into a single decision point. On each
failure, the caller calls `record_failure`. The policy increments the retry
counter and, when the count reaches `max_retries`, writes the message to the
dead letter store and removes the retry record:

```rust
pub struct RetryPolicy {
    tracker: InMemoryRetryTracker,
    dead_letters: InMemoryDeadLetterStore,
    max_retries: u32,
}

impl RetryPolicy {
    pub fn record_failure(
        &self,
        message_id: Uuid,
        handler_id: &str,
        aggregate_id: &AggregateId,
        payload: Bytes,
        error: &str,
    ) -> Result<RetryOutcome, RetryPolicyError> {
        let attempts = self.tracker.increment(message_id, handler_id)?;

        if attempts >= self.max_retries {
            self.dead_letters
                .store(message_id, handler_id, aggregate_id, payload, error)?;
            self.tracker.remove(message_id)?;
            return Ok(RetryOutcome::DeadLettered);
        }

        Ok(RetryOutcome::Retry { attempt: attempts })
    }
}
```

The `RetryOutcome` enum communicates the decision:

```rust
pub enum RetryOutcome {
    /// The message should be retried. Contains the current attempt count.
    Retry { attempt: u32 },
    /// The message has reached max retries and was dead-lettered.
    DeadLettered,
}
```

The default maximum is 3 retries (`DEFAULT_MAX_RETRIES`), configurable via
`ServiceBuilder`.

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
  Clear    RetryPolicy::record_failure()
  retry       |
  record   Increment retry_attempts
              |
           At max retries?
           /         \
          No          Yes
           |           |
           v           v
    RetryOutcome::   RetryOutcome::DeadLettered
    Retry              |
                       v
                Remove from retry_attempts
                Insert into dead_letters
```

### Event store consumer retries

The most common retry scenario is the event store consumer encountering a Cassandra
version conflict. This happens when:

- Two instances process the same event concurrently (race condition after restart)
- A snapshot write interferes with a concurrent event write
- A network timeout masked a successful server-side write

The event store consumer calls `RetryPolicy::record_failure` on each failure. After
`max_retries` (default: 3), the event is dead-lettered.

---

## Dead letter store implementations

### In-memory (for tests)

The `InMemoryDeadLetterStore` in `canon-core/src/memory/dead_letter.rs` uses
`Arc<Mutex<Vec<InMemoryDeadLetter>>>`:

```rust
#[derive(Clone)]
pub struct InMemoryDeadLetterStore {
    inner: Arc<Mutex<Vec<InMemoryDeadLetter>>>,
}
```

It implements both synchronous methods (for direct test assertions) and the async
`DeadLetterStore` trait. The `requeue` method sets a `requeue: bool` flag on the
entry. The `discard` method removes the entry entirely.

### YugabyteDB (for production)

The `YugabyteDeadLetterStore` in `canon-deadletter-yugabyte/src/lib.rs` persists
dead letters to a `dead_letters` table using `sqlx`:

```rust
#[derive(Clone)]
pub struct YugabyteDeadLetterStore {
    pool: PgPool,
}
```

The `store` method uses `INSERT ... ON CONFLICT (id) DO NOTHING` for idempotency:

```rust
async fn store(
    &self,
    message_id: Uuid,
    handler_id: &str,
    aggregate_id: &AggregateId,
    payload: Bytes,
    error: &str,
) -> Result<Uuid, Self::Error> {
    let id = Uuid::new_v4();
    let now = Utc::now();

    sqlx::query(
        "INSERT INTO dead_letters \
         (id, message_id, handler_id, aggregate_id, payload, error, attempts, \
          created_at, last_attempted) \
         VALUES ($1, $2, $3, $4, $5, $6, 1, $7, $8) \
         ON CONFLICT (id) DO NOTHING",
    )
    .bind(id).bind(message_id).bind(handler_id)
    .bind(aggregate_id.as_uuid()).bind(payload.as_ref())
    .bind(error).bind(now).bind(now)
    .execute(&self.pool)
    .await?;

    Ok(id)
}
```

Both `requeue` and `discard` use `DELETE FROM dead_letters WHERE id = $1` and return
`NotFound` if no rows were affected:

```rust
async fn requeue(&self, id: Uuid) -> Result<(), Self::Error> {
    let result = sqlx::query("DELETE FROM dead_letters WHERE id = $1")
        .bind(id)
        .execute(&self.pool)
        .await?;

    if result.rows_affected() == 0 {
        return Err(YugabyteDeadLetterStoreError::NotFound { id });
    }
    Ok(())
}
```

### The dead_letters table

```sql
CREATE TABLE dead_letters (
    id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    message_id       UUID,
    handler_id       TEXT,
    aggregate_id     UUID,
    payload          BYTEA,
    error            TEXT,
    attempts         INT DEFAULT 1,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_attempted   TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

Each service has its own `dead_letters` table in its own schema. Dead letters from
the fleet service's event store consumer go to `canon_fleet.dead_letters`. Dead
letters from the station service go to `canon_station.dead_letters`. Services never
share dead letter tables.

---

## Requeue via admin API

Dead-lettered messages can be requeued for reprocessing. The requeue operation:

1. Reads the dead letter entry from the store
2. Removes the entry from the dead letter store
3. Re-inserts the message into the inbox with:
   - Fresh `expires_at` (new TTL countdown)
   - Status `Pending` (oversight runs again from scratch)
4. The message goes through the full inbox pipeline again: dedup, oversight, dispatch

Oversight is **not** bypassed. The handler evaluates the message fresh. If the
underlying issue has been fixed, the message will process successfully. If not,
it will fail, increment retries, and potentially be dead-lettered again.

### In-memory requeue

The `InMemoryInbox` implements requeue by clearing dedup entries for the messages
and re-submitting them through the normal `submit` path:

```rust
pub fn requeue_window(
    &self,
    handler_id: &str,
    _aggregate_id: &AggregateId,
    messages: Vec<IncomingMessage>,
    inbound_queue: &InMemoryInboundQueue,
) -> Result<(), InboxError> {
    // Clear dedup entries for the messages being requeued
    {
        let mut state = self.inner.lock().map_err(|_| InboxError::Poisoned)?;
        for msg in &messages {
            let dedup_key = (handler_id.to_owned(), msg.message_id());
            state.dedup.remove(&dedup_key);
        }
    }

    // Re-submit each message through the normal path
    for msg in messages {
        self.submit(handler_id, msg, inbound_queue)?;
    }
    Ok(())
}
```

After requeue, the retry record was already cleaned up by the `RetryPolicy`. A
fresh failure starts at attempt 1 again -- the message gets a full fresh retry budget.

### Gateway admin endpoints

The gateway exposes dead letter management endpoints:

```
GET  /admin/deadletters              -- list all dead-lettered messages
GET  /admin/deadletters/:id          -- get a specific dead letter
POST /admin/deadletters/:id/requeue  -- requeue for reprocessing
POST /admin/deadletters/:id/discard  -- permanently discard
```

The frontend's Scenarios page (Mission 04, "The Cassandra Incident") uses these
endpoints to demonstrate dead letter handling with visual feedback.

---

## Window expiry dead letters

When an inbox window's TTL expires before oversight returns `Ready`, the window
and all its messages are dead-lettered. This is handled by a background cleanup
task in two phases.

### Phase 1: Sweep

The sweep task finds all windows past their TTL and marks them as `Expired`:

```rust
pub fn sweep_expired_windows(&self) -> Result<u64, InboxError> {
    let mut state = self.inner.lock().map_err(|_| InboxError::Poisoned)?;
    let now = Utc::now();
    let mut count = 0u64;
    for window in state.windows.values_mut() {
        if window.status == WindowStatus::Pending {
            if let Some(expires_at) = window.expires_at {
                if expires_at < now {
                    window.status = WindowStatus::Expired;
                    count += 1;
                }
            }
        }
    }
    Ok(count)
}
```

Windows without a TTL (`expires_at: None`) are never swept. Only handlers that
declare `window_ttl` in their `#[event_handler]` attribute get TTL-based windows.

### Phase 2: Collect and dead-letter

After sweeping, the collect phase removes expired windows and returns them for
dead-lettering:

```rust
pub fn collect_expired_windows(&self) -> Result<Vec<ExpiredWindow>, InboxError> {
    let mut state = self.inner.lock().map_err(|_| InboxError::Poisoned)?;
    let expired_keys: Vec<(String, AggregateId)> = state.windows.iter()
        .filter(|(_, w)| w.status == WindowStatus::Expired)
        .map(|(k, _)| k.clone())
        .collect();

    let mut result = Vec::with_capacity(expired_keys.len());
    for key in expired_keys {
        if let Some(window) = state.windows.remove(&key) {
            result.push(ExpiredWindow {
                handler_id: key.0,
                aggregate_id: key.1,
                messages: window.messages,
            });
        }
    }
    Ok(result)
}
```

Each `ExpiredWindow` contains the handler ID, aggregate ID, and all accumulated
messages. The caller writes these to the dead letter store with reason
`window_expired`.

### Why windows expire

A window expires when the expected events never arrive. Common causes:

- An upstream service is down and never publishes the expected event
- A cross-service event was lost or delayed beyond the TTL
- The handler's oversight function requires events that will never come

Without window expiry, messages stuck in `NotReady` windows would accumulate
indefinitely, consuming memory and never being processed.

---

## Monitoring dead letters

Dead-lettered messages are a signal that something is wrong. The admin API and
WebSocket provide visibility into the dead letter queue.

### Common causes and resolutions

| Reason | Likely cause | Resolution |
|--------|-------------|------------|
| `max_retries_exceeded` | Cassandra version conflict from concurrent writes | Check for duplicate consumers, increase snapshot interval |
| `window_expired` | Expected upstream events never arrived | Check upstream service health, verify event routing |
| `handler_error` | Bug in event handler or command handler code | Fix the handler, requeue the message |
| `serialisation_error` | Payload format mismatch between versions | Check event versioning, fix the deserialiser |

### WebSocket notifications

The gateway broadcasts dead letter events over the WebSocket connection:

```rust
WsMessage::DeadLetter(DeadLetterEntry)
```

The frontend receives these in real-time and can display them in the admin panel
or scenario visualisation (Mission 04).

### Metrics to watch

- **Dead letter count per handler**: a handler with many dead letters may have a bug
- **Dead letter rate**: a spike indicates a systemic issue (infrastructure, schema change)
- **Window expiry rate**: a high rate indicates upstream service health problems
- **Retry rate approaching max**: a leading indicator of impending dead letters

---

## Dead letters in the demo

The demo gateway exposes dead letters to the frontend via the admin endpoints.
The Scenarios page includes Mission 04 ("The Cassandra Incident") which demonstrates
the full dead letter lifecycle:

1. Deliberately trigger Cassandra write failures
2. Show dead-lettered events in the UI as red cards
3. Requeue a dead letter -- the card transitions from red to green
4. Discard a dead letter -- the card fades out
5. Verify the requeued message processes successfully

### Initial hydration

On mount, the frontend fetches existing dead letters:

```
GET /admin/deadletters  -->  Vec<DeadLetterEntry>
```

### Live updates

New dead letters arrive via WebSocket and are appended to the UI in real-time.

---

## Testing dead letter handling

### Tier 1: In-memory tests

The `dead_letter.rs` test module in `canon-test/tests/` exercises the full lifecycle:

```rust
#[tokio::test]
async fn test_dead_letter_after_max_retries() {
    let harness = TestHarness::new();
    let id = AggregateId::new();
    let max_retries = 3;

    for attempt in 1..=max_retries {
        if attempt == max_retries {
            harness.dead_letter_store.store(
                message_id, handler_id, &id,
                Bytes::from_static(b"failed_payload"),
                "max retries exceeded",
            ).unwrap();
        }
    }

    let letters = harness.dead_letters(Some(handler_id));
    assert_eq!(letters.len(), 1);
    assert_eq!(letters[0].error, "max retries exceeded");
}
```

The `RetryPolicy` unit tests in `canon-core/src/memory/retry_policy.rs` verify the
coordination between tracker and dead letter store:

```rust
#[test]
fn dead_letters_at_max() {
    let policy = make_policy(3);
    let msg = Uuid::new_v4();
    let agg = AggregateId::new();
    let payload = Bytes::from_static(b"{}");

    // Attempts 1 and 2: Retry
    policy.record_failure(msg, "h1", &agg, payload.clone(), "conflict").unwrap();
    policy.record_failure(msg, "h1", &agg, payload.clone(), "conflict").unwrap();

    // Attempt 3: dead-lettered
    let outcome = policy
        .record_failure(msg, "h1", &agg, payload, "conflict")
        .unwrap();
    assert_eq!(outcome, RetryOutcome::DeadLettered);

    // Retry record removed, dead letter created
    assert!(policy.tracker().get(msg).unwrap().is_none());
    assert_eq!(policy.dead_letters().list(Some("h1")).unwrap().len(), 1);
}
```

### Tier 2: Testcontainers tests

The `canon-deadletter-yugabyte` crate has testcontainer tests that verify real SQL
behaviour against a Postgres container:

```rust
#[tokio::test(flavor = "multi_thread")]
async fn test_requeue_removes_entry() {
    let (_container, pool) = setup_container().await;
    let store = YugabyteDeadLetterStore::new(pool);
    let agg_id = AggregateId::new();

    let dl_id = store
        .store(Uuid::new_v4(), "h1", &agg_id, Bytes::from_static(b"{}"), "err")
        .await.expect("store");

    store.requeue(dl_id).await.expect("requeue");

    let remaining = store.list(None).await.expect("list");
    assert!(remaining.is_empty());
}
```

The `YugabyteRetryTracker` tests verify crash-safe UPSERT semantics:

```rust
#[tokio::test(flavor = "multi_thread")]
async fn increment_accumulates() {
    let (_container, pool) = setup_container().await;
    let tracker = YugabyteRetryTracker::new(pool);
    let msg_id = Uuid::new_v4();

    assert_eq!(tracker.increment(msg_id, "h1").unwrap(), 1);
    assert_eq!(tracker.increment(msg_id, "h1").unwrap(), 2);
    assert_eq!(tracker.increment(msg_id, "h1").unwrap(), 3);
}
```
