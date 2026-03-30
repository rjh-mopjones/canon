# Inbox and Windowing

The inbox is the entry point for all incoming messages in Canon -- commands from the
gateway, internal events from a service's own outbound queue, and external events from
other services via the adaptor. It provides idempotent intake, windowed accumulation,
oversight-gated dispatch, batch-level idempotency, and window expiry with dead lettering.

This chapter covers every layer of the inbox in detail, from the database schema to the
in-memory implementation to the cleanup background task.

---

## Inbox responsibilities

The inbox performs five distinct operations:

1. **Idempotent intake** -- deduplication via `(handler_id, message_id)` composite key.
2. **Windowed accumulation** -- grouping messages by `(handler_id, correlation_key)`.
3. **Oversight evaluation** -- calling the handler's oversight function after each
   non-duplicate submission to decide whether the window is ready for dispatch.
4. **Batch dispatch** -- forwarding ready windows to the inbound Kafka queue.
5. **Expiry management** -- sweeping windows that exceed their TTL and routing them
   to the dead letter store.

Each of these is implemented in both the YugabyteDB-backed `YugabyteInbox` (production)
and the `InMemoryInbox` (testing).

---

## Idempotent intake

Every message submitted to the inbox is deduplicated using a composite primary key.
The deduplication is the first step in the `submit` method and happens inside the same
database transaction as the window accumulation and oversight evaluation.

### The deduplication query

```sql
INSERT INTO inbox_messages (handler_id, message_id, aggregate_id, message_type, payload)
VALUES ($1, $2, $3, $4, $5)
ON CONFLICT (handler_id, message_id) DO NOTHING;
```

The `ON CONFLICT DO NOTHING` clause makes this operation idempotent. If the same
`(handler_id, message_id)` pair already exists, the insert is silently ignored and
the method returns `false` (duplicate detected). If it is new, the insert succeeds
and the method returns `true`.

### What the composite key guarantees

| Scenario | Result |
|----------|--------|
| Same event delivered to the same handler twice | Second insert is a no-op |
| Same event delivered to two different handlers | Both handlers process it independently |
| Kafka consumer restarts from offset 0 | All previously-seen messages are deduplicated |
| Two different events with different `message_id` | Both are accepted, even for the same handler |

The composite key `(handler_id, message_id)` is essential. Using `message_id` alone
would prevent different handlers from independently processing the same event. Using
`handler_id` alone would prevent the same handler from processing different events.

### Message types stored

The `message_type` column records which variant of `IncomingMessage` was submitted:

- `command` -- a `CommandEnvelope` from the gateway or from `InboxPort` re-entry.
- `internal_event` -- an `EventEnvelope` from the service's own outbound queue.
- `external_event` -- an `EventEnvelope` from another service via the adaptor.

This is used for deserialisation when the window's accumulated messages are loaded
back from the `inbox_windows` table's JSONB `messages` column.

### Retention and cleanup

Inbox message rows are write-once deduplication records. They grow over time. A periodic
cleanup job should delete rows older than the configured retention period (e.g., 7 days)
to prevent unbounded growth. The dedup guarantee holds as long as the retention period
exceeds the maximum expected redelivery window.

---

## Windows

A window is a collection of messages being assembled for a single handler invocation.
The window key is `(handler_id, correlation_key)`:

- **`handler_id`** -- which handler this window belongs to (e.g., `"CargoUnloadingHandler"`).
- **`correlation_key`** -- derived from the handler's `correlate()` function, or falling
  back to the envelope's `correlation_id`.

### Why correlation_key, not aggregate_id

The window key is deliberately `(handler_id, correlation_key)`, not
`(handler_id, aggregate_id)`. Event handlers are aggregate-agnostic -- they do not
have an aggregate type parameter. A single event handler may react to events from
multiple aggregate types, or correlate events by a domain concept that does not map
to any single aggregate (e.g., a voyage ID that spans ship, cargo, and navigation
events).

The `correlate` function on the handler extracts whatever UUID makes sense as the
grouping key:

```rust
fn correlate(&self, message: &IncomingMessage) -> Uuid {
    match message {
        IncomingMessage::ExternalEvent(e) => {
            // Extract a domain-specific correlation key from the payload
            extract_voyage_id(&e.payload)
        }
        _ => message.correlation_id(),
    }
}
```

If the handler omits `correlate`, the default implementation returns
`message.correlation_id()`, which is the `correlation_id` field from the event or
command envelope. This groups all events that share the same causal chain.

### Concurrent windows

Each unique correlation key creates an independent window. A single handler may have
many concurrent in-flight windows. For example, a cargo unloading handler might have
separate windows for each ship voyage:

```
CargoUnloadingHandler:
  Window (voyage-001): [UnloadingStarted, CargoUnloaded(100kg)]  -- NotReady
  Window (voyage-002): [UnloadingStarted]                        -- NotReady
  Window (voyage-003): [UnloadingStarted, CargoUnloaded(500kg), ManifestClosed]  -- Ready
```

Each window independently accumulates messages and evaluates oversight.

---

## Window lifecycle

A window progresses through the following states:

```
1. First message arrives     --> Window created (status: pending)
2. More messages arrive       --> Added to window, oversight evaluated
3. Oversight returns Ready    --> Batch dispatched to inbound queue
4. Oversight returns NotReady --> Wait for more messages
5. Oversight returns Discard  --> Window deleted without dispatch
6. TTL expires                --> Window marked expired --> dead letter
```

### Window status enum

```rust
pub enum WindowStatus {
    /// Waiting for more messages or oversight to report Ready.
    Pending,
    /// Oversight reported Ready; batch was published.
    Dispatched,
    /// The window's TTL elapsed before oversight reported Ready.
    Expired,
    /// The expired window's messages have been moved to the dead letter store.
    DeadLettered,
}
```

### State transitions

```
pending --> dispatched     (oversight returned Ready)
pending --> expired        (TTL reached before Ready)
pending --> [deleted]      (oversight returned Discard)
expired --> dead_lettered  (cleanup task collected the window)
```

Windows that reach `dispatched`, `expired`, or `dead_lettered` are terminal. A
garbage-collection method (`cleanup_expired_windows`) periodically deletes terminal
windows older than a configurable retention period.

---

## Oversight

Oversight is the mechanism that controls when a window's accumulated messages are
dispatched to the handler. The handler author defines the oversight function:

```rust
fn oversight(&self, accumulated: &[IncomingMessage]) -> Oversight {
    // Inspect all messages in the window so far.
    // Return Ready, NotReady, or Discard.
}
```

Oversight is called after every non-duplicate submission to the window. It receives
the full list of messages accumulated so far, not just the latest one.

### Oversight::Ready

The batch is ready to be dispatched. The inbox:

1. Marks the window's status as `dispatched`.
2. Deletes the window row from `inbox_windows`.
3. Publishes the full batch of messages to the inbound Kafka queue.
4. The window's `window_id` travels with the batch for batch-level idempotency.

### Oversight::NotReady

The window needs more messages. The inbox does nothing. The window stays in `pending`
status, and the next submission will trigger another oversight evaluation.

### Oversight::Discard

The window should be abandoned. The inbox deletes the window row without publishing
any messages. This is useful when a later event invalidates the entire window. For
example, if a `ShipDecommissioned` event arrives while the window is waiting for
cargo unloading prerequisites, the window is discarded because the ship will never
dock again.

### Default behaviour

If a handler omits the oversight method, the default implementation returns
`Oversight::Ready` on every message. This means the handler processes each event
immediately as a single-message batch with no windowing. This is the common case
for simple event handlers that react to one event at a time.

### The YugabyteDB evaluation path

In the `YugabyteInbox`, oversight evaluation happens inside the `submit` transaction:

```rust
async fn evaluate_oversight(
    &self,
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    handler_id: &str,
    aggregate_id: &AggregateId,
) -> Result<Option<(Vec<IncomingMessage>, AggregateId, Uuid)>, YugabyteInboxError>
```

1. Load the current window from `inbox_windows` (status must be `pending`).
2. Deserialise the accumulated messages from the JSONB `messages` column.
3. Look up the handler's oversight function in the in-memory registry.
4. Call the oversight function with the accumulated messages.
5. If `Ready`: mark dispatched, delete the window row, return the batch.
6. If `NotReady`: do nothing, return `None`.
7. If `Discard`: delete the window row, return `None`.

The batch is returned to the caller but published **after** the transaction commits.
This ensures the window state change and the batch dispatch are consistent -- if the
commit fails, the batch is never published.

---

## Window accumulation

When a new (non-duplicate) message arrives, it is appended to the window's message
list. The YugabyteDB implementation uses a JSONB append:

```sql
INSERT INTO inbox_windows (handler_id, aggregate_id, messages, expires_at)
VALUES ($1, $2, $3, $4)
ON CONFLICT (handler_id, aggregate_id)
DO UPDATE SET messages = inbox_windows.messages || $3,
              updated_at = now()
```

The `ON CONFLICT` clause handles both cases:

- **New window**: inserts a fresh row with the message wrapped in a JSON array.
  Sets `expires_at` based on the handler's `window_ttl` (or `NULL` if no TTL).
- **Existing window**: appends the message to the existing JSONB array using the
  `||` operator. Does NOT reset `expires_at` -- the TTL is based on window creation
  time, not last activity.

### Message serialisation

`IncomingMessage` is an in-process enum without `Serialize`/`Deserialize`. The
YugabyteDB inbox maps it through an intermediate `StoredMessage` type:

```rust
struct StoredMessage {
    message_type: StoredMessageType,   // command | internal_event | external_event
    // Flattened envelope fields:
    command_id: Option<Uuid>,
    event_id: Option<Uuid>,
    aggregate_id: Uuid,
    correlation_id: Uuid,
    causation_id: Uuid,
    timestamp: DateTime<Utc>,
    payload: Vec<u8>,                  // base64-encoded domain payload
    // ... version fields etc.
}
```

This is serialised to JSONB for storage and deserialised back into `IncomingMessage`
when the window is loaded for oversight evaluation or batch dispatch.

---

## Window TTL and expiry

Windows that never reach `Ready` must not accumulate indefinitely. The `window_ttl`
attribute on `#[event_handler]` sets an expiration time:

```rust
#[event_handler(window_ttl = "30m")]
impl CargoUnloadingHandler {
    fn oversight(&self, accumulated: &[IncomingMessage]) -> Oversight { ... }
    // ...
}
```

### How TTL is set

When a handler is registered with a `window_ttl`, the inbox computes `expires_at` for
new windows:

```rust
async fn compute_expires_at(&self, handler_id: &str) -> Option<DateTime<Utc>> {
    self.handler_ttl(handler_id).await.map(|ttl| {
        let delta = chrono::Duration::from_std(ttl).unwrap_or_else(|_| {
            // Overflow safety: clamp to 24 hours
            chrono::TimeDelta::hours(24)
        });
        Utc::now() + delta
    })
}
```

The `expires_at` is set once when the window is first created and is NOT reset when
subsequent messages are appended. This is intentional: a window that keeps receiving
messages but never becomes `Ready` will still expire.

### Compile-time safety

`window_ttl` without an `oversight` method is a compile error. The proc-macro
enforces this because:

- Without oversight, the default always returns `Ready`.
- If oversight always returns `Ready`, every window dispatches immediately.
- A TTL on a window that dispatches immediately is meaningless.

The compile error prevents this misconfiguration.

### The expiry process

Expiry is a two-phase process managed by a background cleanup task:

**Phase 1 -- Sweep**: mark pending windows past their TTL as expired.

```sql
UPDATE inbox_windows
SET status = 'expired', updated_at = now()
WHERE expires_at IS NOT NULL
  AND expires_at < now()
  AND status = 'pending'
```

**Phase 2 -- Collect**: load expired windows, transition them to `dead_lettered`,
and return them for dead-letter storage.

```sql
SELECT handler_id, aggregate_id, window_id, messages, status, expires_at
FROM inbox_windows
WHERE status = 'expired'
FOR UPDATE SKIP LOCKED
```

After loading:

```sql
UPDATE inbox_windows
SET status = 'dead_lettered', updated_at = now()
WHERE handler_id = $1 AND aggregate_id = $2 AND status = 'expired'
```

The `FOR UPDATE SKIP LOCKED` on the collect query ensures that multiple cleanup
tasks (if running) do not process the same expired windows.

### The cleanup background task

The `spawn_cleanup_task` function starts a tokio task that runs the sweep/collect
cycle on a configurable interval:

```rust
pub fn spawn_cleanup_task<F, Fut>(
    inbox: YugabyteInbox,
    config: CleanupConfig,      // interval: Duration (default 30s)
    dead_letter_fn: F,
) -> tokio::task::JoinHandle<()>
```

The `dead_letter_fn` callback receives each batch of expired windows and is
responsible for persisting them to the dead letter store. If the callback fails,
the windows remain in `expired` status and will be retried on the next sweep.

### Garbage collection

Terminal windows (`expired`, `dead_lettered`, `dispatched`) accumulate in the
`inbox_windows` table. The `cleanup_expired_windows` method deletes rows older
than a configurable retention period:

```sql
DELETE FROM inbox_windows
WHERE status IN ('expired', 'dead_lettered', 'dispatched')
  AND updated_at < $1
```

---

## Batch idempotency via window_id

Each window is assigned a `window_id` (UUID) at creation time. This ID travels with
the batch through the inbound queue to the consumer. Before processing, the consumer
checks batch-level idempotency:

```sql
INSERT INTO processed_windows (window_id, handler_id)
VALUES ($1, $2)
ON CONFLICT (window_id) DO NOTHING;
```

The `try_mark_window_processed` method returns:

- `true` -- the window was newly marked. The caller should process the batch.
- `false` -- the window was already processed. The caller should skip the batch.

This closes the duplicate processing window that exists during Kafka consumer restarts.
Without this guard, a consumer restarting from offset 0 would reprocess every batch.
With it, the `processed_windows` table acts as a high-water mark.

### Retention

`processed_windows` rows are idempotency guards. They grow over time. A periodic
cleanup job should delete rows older than the configured retention period to prevent
unbounded growth.

---

## InboxPort: local re-entry

When an event handler returns `Some(CommandEnvelope)`, the dispatcher submits it back
into the local inbox via the `InboxPort` trait:

```rust
#[async_trait]
pub trait InboxPort: Send + Sync + 'static {
    /// Submit a command envelope to the local inbox for dispatch.
    ///
    /// Implementations must be idempotent: re-submitting the same
    /// command_id is a safe no-op.
    async fn submit(&self, command: CommandEnvelope) -> Result<(), InboxPortError>;
}
```

This is the mechanism by which event handlers trigger command processing within the
same service. The re-entered command goes through the full inbox path: deduplication,
oversight (commands get `Ready` by default since they have no windowing), dispatch to
inbound queue, and command handler execution.

### Why InboxPort and not direct command execution

The inbox path provides several guarantees that direct execution does not:

1. **Deduplication** -- if the event handler fires twice (idempotent replay), the
   command is deduplicated by `command_id`.
2. **Audit trail** -- the command appears in `inbox_messages` and `commands`.
3. **Consistent flow** -- the command follows the same path as gateway-submitted
   commands, through the dispatcher and outbox.

### Cross-service boundary

`InboxPort` is for local re-entry only. Commands that need to reach another service
go via REST to that service's gateway endpoint. The gateway then submits the command
to the target service's inbox. Cross-service commands never flow through `InboxPort`.

---

## YugabyteDB schema

Each service maintains its own inbox tables within its isolated schema. The full DDL:

### inbox_messages -- message deduplication

```sql
CREATE TABLE canon_fleet.inbox_messages (
    handler_id   TEXT        NOT NULL,
    message_id   UUID        NOT NULL,
    aggregate_id UUID,
    message_type TEXT,
    payload      BYTEA,
    received_at  TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (handler_id, message_id)
);

-- Dispatcher polls with handler_id filter + received_at ordering
CREATE INDEX inbox_messages_handler_received_idx
    ON canon_fleet.inbox_messages (handler_id, received_at ASC);
```

The primary key `(handler_id, message_id)` provides the deduplication guarantee.
The secondary index supports the dispatcher's polling query, which filters by
`handler_id` and orders by `received_at` to process messages in arrival order.

### inbox_windows -- window tracking

```sql
CREATE TABLE canon_fleet.inbox_windows (
    handler_id      TEXT        NOT NULL,
    correlation_key UUID        NOT NULL,
    window_id       UUID        DEFAULT gen_random_uuid(),
    messages        JSONB       DEFAULT '[]',
    status          TEXT        DEFAULT 'pending',
    expires_at      TIMESTAMPTZ,
    created_at      TIMESTAMPTZ DEFAULT now(),
    updated_at      TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (handler_id, correlation_key)
);

-- Cleanup queries filter by terminal status
CREATE INDEX inbox_windows_status_idx
    ON canon_fleet.inbox_windows (status)
    WHERE status IN ('expired', 'dead_lettered', 'dispatched');

-- TTL cleanup by updated_at for terminal windows
CREATE INDEX inbox_windows_cleanup_idx
    ON canon_fleet.inbox_windows (updated_at)
    WHERE status IN ('expired', 'dead_lettered', 'dispatched');
```

The primary key `(handler_id, correlation_key)` enforces one active window per
handler per correlation key. The `messages` JSONB column stores the accumulated
`StoredMessage` array, which is appended via the `||` operator on each new message.

The two partial indexes support the cleanup operations:
- `inbox_windows_status_idx` supports the sweep query that finds expired windows.
- `inbox_windows_cleanup_idx` supports garbage collection of old terminal windows.

### processed_windows -- batch idempotency

```sql
CREATE TABLE canon_fleet.processed_windows (
    window_id    UUID PRIMARY KEY,
    handler_id   TEXT,
    processed_at TIMESTAMPTZ DEFAULT now()
);
```

A single row per processed batch. The `window_id` primary key provides the
`ON CONFLICT DO NOTHING` idempotency guard.

---

## The submit flow in detail

Here is the complete flow when `YugabyteInbox::submit` is called, step by step:

```
1. BEGIN TRANSACTION

2. Deduplicate:
   INSERT INTO inbox_messages (handler_id, message_id, aggregate_id,
                               message_type, payload)
   VALUES ($1, $2, $3, $4, $5)
   ON CONFLICT (handler_id, message_id) DO NOTHING

   If rows_affected == 0 --> duplicate, skip to COMMIT (no-op)

3. Accumulate:
   INSERT INTO inbox_windows (handler_id, aggregate_id, messages, expires_at)
   VALUES ($1, $2, $3, $4)
   ON CONFLICT (handler_id, aggregate_id)
   DO UPDATE SET messages = inbox_windows.messages || $3,
                 updated_at = now()

4. Evaluate oversight:
   SELECT ... FROM inbox_windows
   WHERE handler_id = $1 AND aggregate_id = $2 AND status = 'pending'

   Deserialise messages, call oversight_fn(messages):
   - Ready:    UPDATE status = 'dispatched', DELETE window row,
               capture batch for post-commit publish
   - NotReady: do nothing
   - Discard:  DELETE window row

5. COMMIT

6. If oversight was Ready:
   Publish batch to inbound Kafka queue (outside transaction)
```

Publishing happens after the commit because:
- If the commit fails, no batch should be published (consistency).
- If the publish fails after commit, the window is already dispatched. The messages
  will not be reprocessed. This is a gap in the current design that future work
  may address with an outbox pattern for the inbox itself.

---

## Requeue from dead letter

Expired windows that have been dead-lettered can be requeued via the admin API. The
requeue flow:

```rust
async fn requeue_expired_window(
    &self,
    handler_id: &str,
    correlation_key: Uuid,
    messages: Vec<IncomingMessage>,
) -> Result<(), InboxError>
```

1. **Clear dedup records**: delete the `inbox_messages` rows for each message in the
   requeued batch. This is necessary because the dedup check would otherwise reject
   them as duplicates.

   ```sql
   DELETE FROM inbox_messages WHERE handler_id = $1 AND message_id = $2
   ```

2. **Delete the dead-lettered window row**: remove the old window so a fresh one
   can be created.

   ```sql
   DELETE FROM inbox_windows
   WHERE handler_id = $1 AND aggregate_id = $2 AND status = 'dead_lettered'
   ```

3. **Re-submit each message**: call `submit()` for each message through the normal
   path. This gives them fresh `expires_at` timestamps and runs oversight from
   scratch.

The requeue operation is the admin's tool for recovering from window expiry. If the
conditions that prevented the window from reaching `Ready` have been resolved (e.g.,
a missing event has arrived, a bug has been fixed), the requeued window may now
succeed.

---

## In-memory implementation

The `InMemoryInbox` in `canon-core/src/memory/inbox.rs` faithfully reproduces the
YugabyteDB inbox behaviour for testing:

```rust
pub struct InMemoryInbox {
    inner: Arc<Mutex<InboxState>>,
}

struct InboxState {
    dedup: HashSet<(String, Uuid)>,                    // (handler_id, message_id)
    windows: HashMap<(String, AggregateId), Window>,   // (handler_id, agg_id) -> Window
    oversight: HashMap<String, OversightFn>,            // handler_id -> oversight fn
    processed_windows: HashSet<Uuid>,                   // window_id dedup
    handler_ttl: HashMap<String, Duration>,             // handler_id -> TTL
}
```

Key methods:

- `register_handler(handler_id, oversight_fn)` -- registers the oversight function.
- `set_handler_ttl(handler_id, ttl)` -- configures window TTL.
- `submit(handler_id, message, inbound_queue)` -- the full submit flow (dedup,
  accumulate, oversight, dispatch).
- `sweep_expired_windows()` -- marks pending windows past TTL as expired.
- `collect_expired_windows()` -- removes expired windows and returns them.
- `try_mark_window_processed(window_id, handler_id)` -- batch idempotency.
- `requeue_window(handler_id, aggregate_id, messages, inbound_queue)` -- requeue.

### Testing oversight accumulation

```rust
let inbox = InMemoryInbox::new();
let queue = InMemoryInboundQueue::new();
let id = AggregateId::new();

// Handler requires 2 messages before dispatching
inbox.register_handler("h1", |accumulated| {
    if accumulated.len() >= 2 {
        Oversight::Ready
    } else {
        Oversight::NotReady
    }
}).unwrap();

// First message: window created, oversight returns NotReady
inbox.submit("h1", make_command(&id), &queue).unwrap();
assert!(queue.receive().unwrap().is_none());  // Nothing dispatched

// Second message: oversight returns Ready, batch dispatched
inbox.submit("h1", make_command(&id), &queue).unwrap();
let batch = queue.receive().unwrap().unwrap();
assert_eq!(batch.len(), 2);  // Both messages in the batch
```

### Testing window expiry

```rust
let inbox = InMemoryInbox::new();
let queue = InMemoryInboundQueue::new();
let id = AggregateId::new();

// Handler never returns Ready; TTL is 0 seconds (expires immediately)
inbox.register_handler("h1", |_| Oversight::NotReady).unwrap();
inbox.set_handler_ttl("h1", Duration::from_secs(0)).unwrap();

inbox.submit("h1", make_command(&id), &queue).unwrap();

// Wait for clock to advance past expires_at
std::thread::sleep(Duration::from_millis(10));

// Sweep marks the window as expired
let swept = inbox.sweep_expired_windows().unwrap();
assert_eq!(swept, 1);

// Collect returns expired windows for dead lettering
let expired = inbox.collect_expired_windows().unwrap();
assert_eq!(expired.len(), 1);
assert_eq!(expired[0].handler_id, "h1");
```

---

## Integration with the pipeline

The inbox sits at the convergence point of three message sources:

```
Gateway (REST)
    |
    v
Adaptor (external events from other services)  -->  INBOX  -->  Inbound Queue
    ^                                                              |
    |                                                              v
Internal event consumer (service's own events)              Dispatcher
```

### Command path

1. Gateway receives a REST request (e.g., `POST /fleet/ships/:id/depart`).
2. Gateway constructs a `CommandEnvelope` and submits it to the inbound Kafka topic.
3. The adaptor receives the command and calls `inbox.submit(handler_id, message_id, Command(envelope))`.
4. The inbox deduplicates, creates a single-message window, evaluates oversight
   (default `Ready` for commands), and dispatches to the inbound queue.
5. The dispatcher polls the inbound queue, runs the command handler, and writes the
   resulting event to the outbox.

### Internal event path

1. The outbox processor publishes an event to the outbound Kafka queue.
2. The internal event consumer receives the event.
3. It checks the `EventHandlerRegistration` inventory for matching handlers.
4. For each matching handler, it calls `inbox.submit(handler_id, event_id, InternalEvent(envelope))`.
5. The inbox deduplicates, accumulates into the handler's window, evaluates oversight.
6. When `Ready`, the batch is dispatched to the inbound queue.
7. The dispatcher polls the batch, calls `EventHandler::handle(events)`.
8. If the handler returns `Some(CommandEnvelope)`, it re-enters via `InboxPort`.

### External event path

1. Another service publishes an event to `canon.{other_service}.events`.
2. The adaptor subscribes to that topic and receives the event.
3. It checks the `EventHandlerRegistration` inventory for matching handlers.
4. For each matching handler, it calls `inbox.submit(handler_id, event_id, ExternalEvent(envelope))`.
5. From here, the flow is identical to internal events.

---

## Real examples from the demo

### Fleet service: command dispatch

The fleet service registers `"Ship"` as the handler for ship commands:

```rust
let dispatcher_store = PgDispatcherStore::new(yugabyte_pool.clone(), event_store.clone(), "Ship");
```

When the gateway POSTs to `/fleet/ships/:id/depart`, the command flows:
gateway -> inbound Kafka -> inbox (dedup, Ready) -> inbound queue -> dispatcher ->
`DepartForStationHandler::handle` -> outbox -> outbound Kafka -> event store / projections / publisher.

### Cross-service: ShipDeparted triggers navigation

When the fleet service publishes `ShipDeparted` to `canon.fleet.events`:

1. The navigation service's adaptor subscribes to `canon.fleet.events`.
2. It receives `ShipDeparted` and calls `inbox.submit("NavigationDepartureHandler", event_id, ExternalEvent(...))`.
3. The inbox deduplicates, evaluates oversight (default `Ready`), dispatches.
4. The dispatcher calls `NavigationDepartureHandler::handle(events)`.
5. The handler returns `Some(RecordDeparture(...))`, which re-enters via `InboxPort`.
6. The `RecordDeparture` command flows through the inbox -> dispatcher -> outbox cycle.

### Station service: oversight windowing

A station may require multiple conditions before processing cargo:

```rust
#[event_handler(window_ttl = "30m")]
impl CargoUnloadingHandler {
    fn oversight(&self, accumulated: &[IncomingMessage]) -> Oversight {
        let has_docking = accumulated.iter().any(|m| is_docking_event(m));
        let has_manifest = accumulated.iter().any(|m| is_manifest_event(m));
        if has_docking && has_manifest {
            Oversight::Ready
        } else {
            Oversight::NotReady
        }
    }
}
```

This handler waits for both a docking event and a manifest event before dispatching.
If neither arrives within 30 minutes, the window expires and is dead-lettered.

---

## Failure modes

### Duplicate message delivery (Kafka restart from offset 0)

All Canon Kafka consumers restart from offset 0 on process restart. Every previously-
delivered message will be resubmitted to the inbox. The `(handler_id, message_id)`
dedup key silently ignores all duplicates. No action needed.

### Window never reaches Ready

If oversight never returns `Ready`, the window accumulates messages indefinitely.
The `window_ttl` guard prevents this: after the TTL expires, the cleanup task sweeps
the window to `expired` status and dead-letters it.

If the handler has no `window_ttl` (and the proc-macro enforces that it therefore
has no `oversight`, so the default `Ready` applies), then every message dispatches
immediately and this scenario cannot occur.

### Cleanup task crash

If the cleanup task crashes, expired windows remain in `expired` status. They are
not lost. On restart, the cleanup task picks them up again. The `FOR UPDATE SKIP LOCKED`
query ensures concurrent cleanup tasks (if any) do not double-process.

### Requeue after dead letter

The admin can requeue dead-lettered windows. The requeue clears dedup records and
re-submits messages through the normal path. If the underlying issue has been fixed,
the window will now succeed. If not, it will expire again and return to the dead
letter store.

### Concurrent dispatchers

Multiple dispatcher replicas can safely poll inbox messages concurrently. The
`FOR UPDATE SKIP LOCKED` clause on both the inbox message poll and the window
evaluation ensures each replica processes different rows without blocking.

---

## Summary

The inbox provides the following guarantees:

1. **Idempotent intake** -- `(handler_id, message_id)` composite key ensures no
   message is processed twice by the same handler.
2. **Windowed accumulation** -- messages are grouped by `(handler_id, correlation_key)`
   into independent windows.
3. **Oversight-gated dispatch** -- the handler controls when its window is ready
   via the `oversight` function. `Ready`, `NotReady`, and `Discard` are the three
   possible outcomes.
4. **Batch idempotency** -- `processed_windows` table ensures a batch is not
   processed twice even after Kafka consumer restarts.
5. **Window expiry** -- `window_ttl` prevents windows from accumulating indefinitely.
   Expired windows are swept, collected, and dead-lettered by a background task.
6. **Local re-entry** -- `InboxPort` allows event handlers to submit commands back
   into the inbox, following the same dedup/dispatch path as external commands.
7. **Safe requeue** -- dead-lettered windows can be requeued via the admin API,
   clearing dedup records and running oversight from scratch.

The inbox is the single entry point for all messages in a Canon service. Whether a
message originates from the gateway, from the service's own outbound queue, or from
another service's published events, it flows through the same dedup, windowing, and
oversight pipeline before reaching the dispatcher.
