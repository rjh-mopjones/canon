# Architecture

Canon implements a multi-stage event sourcing pipeline. This chapter provides a
comprehensive view of how messages flow through the system, from external events arriving
to projected read models being updated.

## Full pipeline diagram

```
External world
      |
      v
canon-adaptor-kafka          <-- inbound events from other services
      |
      v
canon-inbox-yugabyte         <-- idempotency, assembly, oversight
      |
      v
canon-inbound-queue-kafka    <-- assembled batches to handlers
      |                          (partitioned by aggregate_id)
      v
Dispatcher
  |-> Command handler
  |-> Internal event handlers
  |-> External event handlers
      |
      v
YugabyteDB transaction
  |-- commands table          <-- audit trail (direct write)
  |-- outbox table            <-- event staging (sequence_number ordered)
      |
      v
Outbox processor              <-- single responsibility:
      |                            drain outbox -> outbound queue
      v
canon-outbound-queue-kafka   <-- committed events fanning out
      |                          (partitioned by aggregate_id)
      |
      |-> Event store consumer     -> Cassandra (+ snapshot writes)
      |-> Projection consumer      -> YugabyteDB read models
      |-> Internal event consumer  -> inbox (for event handler dispatch)
      |-> Publisher (Kafka)        -> canon.{service}.events
                                      -> other services
```

## Data flow walkthrough

### 1. External event arrival

Another service publishes an event to its `canon.{service}.events` topic. The local
service's adaptor (`canon-adaptor-kafka`) subscribes to that topic and consumes the
event.

The adaptor wraps the event in an `IncomingMessage::ExternalEvent` and submits it to
the inbox.

### 2. Inbox processing

The inbox is the central coordination point. It handles:

- **Idempotent intake** -- deduplication via `handler_id + message_id` composite key
- **Window assembly** -- accumulating messages until oversight says they are ready
- **Oversight evaluation** -- calling the handler's `oversight()` function on each new message

When the inbox receives an event, it:

1. Checks `EventHandlerRegistration` inventory for all handlers that match this event
   type and version
2. For each matching handler, inserts into `inbox_messages` (dedup via primary key)
3. Adds the message to the handler's window, keyed by `(handler_id, correlation_key)`
4. Evaluates the handler's oversight function
5. If `Ready`, publishes the batch to the inbound queue with a `window_id`

### 3. Inbound queue and dispatcher

The inbound queue (`canon-inbound-queue-kafka`) carries assembled batches from the inbox
to the dispatcher.

The dispatcher routes `IncomingMessage` by type:
- `Command` -> command handler
- `InternalEvent` -> registered internal event handlers
- `ExternalEvent` -> registered external event handlers

Handler registration is automatic via `inventory` -- macros emit static registrations,
and `ServiceBuilder` discovers them at startup.

### 4. Command handler write path

After a command handler processes a command, a single YugabyteDB ACID transaction writes:

```sql
BEGIN
  INSERT INTO commands (...)     -- audit trail, direct write
  INSERT INTO outbox (...) x N  -- one row per event produced
COMMIT
```

Commands are written directly to the command store (not via outbox). Events are written
to the outbox (never directly to Cassandra). The outbox is the durable commit point.

### 5. Outbox processor

A background `tokio::spawn` task with a single responsibility: drain the outbox table
and publish events to the outbound Kafka queue.

```sql
SELECT * FROM outbox
WHERE delivered_at IS NULL
ORDER BY sequence_number
FOR UPDATE SKIP LOCKED
```

After confirmed Kafka publish, it sets `delivered_at` on each processed row.

The outbox processor:
- Does NOT write to Cassandra
- Does NOT trigger projections
- Does NOT publish to external Kafka topics
- Only moves events from outbox to outbound queue

Backpressure is managed via a bounded channel (default capacity: 1024).

### 6. Outbound queue consumers

Four independent consumer groups process events from the outbound queue:

**Event store consumer**
- Writes events to Cassandra
- Checks `version % N == 0` after confirmed write; creates snapshot if true
- Retries up to 3 times on Cassandra version conflict
- After max failures: writes to dead letter store

**Projection consumer**
- Applies events to read models via `ProjectionHandler`
- Updates `last_version` checkpoint
- Supports rebuild via Kafka offset reset

**Internal event consumer**
- Routes a service's own events back to the inbox for event handler dispatch
- Checks `EventHandlerRegistration` inventory for matching handlers
- Calls `Inbox::submit(handler_id, event_id, InternalEvent(envelope))` for each match

**Publisher consumer**
- Publishes events to `canon.{service}.events` topic
- Other services consume these via their adaptors

Each consumer fails and recovers independently. All restart from offset zero and rely
on downstream idempotency.

## Hexagonal architecture

Canon follows a strict hexagonal (ports and adapters) architecture:

```
                          +------------------+
                          |                  |
              +---------->|   canon-core     |<-----------+
              |           |                  |            |
              |           | Traits, types,   |            |
              |           | in-memory impls, |            |
              |           | proc-macros      |            |
              |           +------------------+            |
              |                    |                      |
     +--------+--------+   +------+------+   +-----------+---------+
     |                 |   |             |   |                     |
     | Trait crates    |   | Trait crates|   | Trait crates        |
     | (event-store,   |   | (inbox,     |   | (publisher,         |
     |  command-store, |   |  inbound-q, |   |  adaptor,           |
     |  snapshot-store)|   |  outbound-q)|   |  deadletter)        |
     +--------+--------+   +------+------+   +-----------+---------+
              |                    |                      |
     +--------+--------+   +------+------+   +-----------+---------+
     |                 |   |             |   |                     |
     | cassandra       |   | kafka       |   | yugabyte            |
     | (event-store)   |   | (queues)    |   | (projections,       |
     |                 |   |             |   |  deadletter)        |
     +--------+--------+   +------+------+   +-----------+---------+
```

The strict DAG rule: implementation crates depend on their trait crate + `canon-core`
only. No cross-impl dependencies.

## Graceful shutdown

Each background task receives a `watch` channel for shutdown signalling:

```rust
let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

// In each spawned task:
loop {
    tokio::select! {
        _ = shutdown_rx.changed() => break,
        result = process_next() => { /* handle */ }
    }
}

// To shut down:
shutdown_tx.send(true)?;
```

`service.start()` spawns all tasks and returns a handle. Dropping the handle or calling
`shutdown()` triggers graceful termination of all background tasks.
