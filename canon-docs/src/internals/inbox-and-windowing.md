# Inbox & Windowing

The inbox is the entry point for all incoming messages in Canon. It handles idempotent
intake, event assembly into windows, oversight evaluation, and batch dispatch.

## Inbox responsibilities

1. **Idempotent intake** -- deduplication via `handler_id + message_id` composite key
2. **Event assembly** -- accumulating messages for a handler until oversight signals readiness
3. **Oversight evaluation** -- calling the handler's oversight function on each new message
4. **Queue dispatch** -- forwarding ready batches to the inbound Kafka queue

## Idempotent intake

Every message submitted to the inbox is deduplicated:

```sql
INSERT INTO inbox_messages (handler_id, message_id, payload, received_at)
VALUES ($1, $2, $3, NOW())
ON CONFLICT (handler_id, message_id) DO NOTHING;
```

The `(handler_id, message_id)` composite key ensures:
- The same event delivered to the same handler twice is silently ignored
- Different handlers can independently process the same event
- Kafka consumer restarts (from offset 0) are safe

## Windows

A window is a collection of messages being assembled for a single handler invocation.
The window key is `(handler_id, correlation_key)`:

- `handler_id` -- which handler this window belongs to
- `correlation_key` -- derived from the handler's `correlate()` function or the envelope's
  `correlation_id`

Each unique correlation key creates an independent window. A handler may have many
concurrent in-flight windows. For example, a cargo unloading handler might have separate
windows for each ship voyage, all running concurrently.

### Window lifecycle

```
1. First message arrives   ->  Window created (status: pending)
2. More messages arrive    ->  Added to window, oversight evaluated
3. Oversight returns Ready ->  Batch dispatched to inbound queue
4. Oversight returns NotReady  ->  Wait for more messages
5. Oversight returns Discard   ->  Window abandoned
6. TTL expires             ->  Window expired -> dead letter
```

### Window states

```
pending -> dispatched   (oversight returned Ready)
pending -> expired      (TTL reached before Ready)
pending -> discarded    (oversight returned Discard)
expired -> dead_lettered
```

## Oversight

Oversight is the mechanism that controls when a window's accumulated messages are
dispatched to the handler. The handler author defines the oversight function:

```rust
fn oversight(&self, accumulated: &[IncomingMessage]) -> Oversight {
    // Inspect all messages in the window so far
    // Return Ready, NotReady, or Discard
}
```

### Oversight::Ready

The batch is ready to be dispatched. The inbox:
1. Assigns a `window_id` to the batch
2. Publishes the batch to the inbound Kafka queue
3. Updates the window status to `dispatched`

### Oversight::NotReady

The window needs more messages. The inbox does nothing and waits for the next message.

### Oversight::Discard

The window should be abandoned. This is useful when a later event invalidates the entire
window. For example, if a `ShipDecommissioned` event arrives while waiting for cargo
unloading prerequisites, the window is discarded.

### Default behaviour

If a handler omits the oversight method, the default returns `Oversight::Ready` on every
message. This means the handler processes each event immediately with no windowing.

## Correlation keys

The correlation key determines which events belong to the same window.

### Default: envelope correlation_id

If the handler omits `correlate()`, Canon uses the envelope's `correlation_id`. Events
sharing the same correlation ID are grouped together.

### Custom correlation

Override `correlate()` to extract a domain-specific key:

```rust
fn correlate(&self, message: &IncomingMessage) -> Uuid {
    match message {
        IncomingMessage::ExternalEvent(e) => {
            // Parse the payload to extract a domain key
            extract_voyage_id(&e.payload)
        }
        _ => message.correlation_id(),
    }
}
```

This allows multiple independent windows per handler, each tracking a different
domain entity.

## Window TTL and expiry

Windows that never reach `Ready` must not accumulate indefinitely. The `window_ttl`
attribute sets an expiration time:

```rust
#[event_handler(window_ttl = "30m")]
```

### Expiry process

1. `inbox_windows.expires_at` is set at window creation: `NOW() + window_ttl`
2. A background cleanup task (spawned by `Service`) periodically scans for expired windows
3. Expired windows are:
   - Marked with status `expired`
   - Moved to the dead letter store with reason `window_expired`
4. Dead-lettered windows can be inspected and requeued via the admin API

### Compile-time safety

`window_ttl` without an `oversight` method is a compile error. This prevents accidentally
creating windows that can never become ready (since the default oversight always returns
`Ready`, a TTL would be meaningless).

## Batch idempotency via window_id

Each window is assigned a `window_id` (UUID) at creation time. This ID travels with
the batch through the inbound queue to the consumer.

The consumer checks batch idempotency before processing:

```sql
INSERT INTO processed_windows (window_id)
VALUES ($1)
ON CONFLICT DO NOTHING;
```

If the insert is a no-op (conflict), the batch was already processed and is skipped.
This closes the duplicate processing window that exists during Kafka rebalancing.

## Inbox tables

```sql
-- Message deduplication
CREATE TABLE inbox_messages (
    handler_id TEXT NOT NULL,
    message_id UUID NOT NULL,
    payload BYTEA NOT NULL,
    received_at TIMESTAMP WITH TIME ZONE NOT NULL,
    PRIMARY KEY (handler_id, message_id)
);

-- Window tracking
CREATE TABLE inbox_windows (
    handler_id TEXT NOT NULL,
    correlation_key UUID NOT NULL,
    window_id UUID NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    expires_at TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL,
    PRIMARY KEY (handler_id, correlation_key)
);

-- Batch idempotency
CREATE TABLE processed_windows (
    window_id UUID PRIMARY KEY,
    processed_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);
```

## InboxPort

When an event handler produces a `CommandEnvelope`, it re-enters the local inbox via
the `InboxPort` trait. This is local re-entry only -- cross-service commands go via
REST, not via the inbox.

The inbox then processes the command through the standard path: dedup, oversight
(commands get `Ready` by default), dispatch to inbound queue, command handler execution.
