# Architecture

Canon implements a multi-stage event sourcing pipeline where commands enter at
one end and projected read models emerge at the other. Every stage is connected
by explicit, typed channels. No component can be skipped. This chapter is the
definitive walkthrough of the entire pipeline -- from the moment an external
event or a user command arrives to the point where a read model is updated and
other services are notified.

---

## Full pipeline diagram

```
                              +-----------------------+
                              | External world        |
                              | (other services,      |
                              |  gateway REST calls)  |
                              +-----------+-----------+
                                          |
           +------------------------------+------------------------------+
           |                                                             |
           v                                                             v
+---------------------+                                    +-----------------------+
| canon-adaptor-kafka |                                    | Gateway (axum)        |
| subscribes to       |                                    | POST /fleet/ships/:id |
| canon.{svc}.events  |                                    | /depart               |
+----------+----------+                                    +-----------+-----------+
           |                                                           |
           | ExternalEvent                                             | CommandEnvelope
           v                                                           v
+-------------------------------------------------------------------------+
|                       canon-inbox-yugabyte                              |
|  +--------------------------+  +-----------------------------+          |
|  | inbox_messages table     |  | inbox_windows table         |          |
|  | PK: (handler_id,        |  | PK: (handler_id,            |          |
|  |      message_id)         |  |      correlation_key)        |          |
|  +--------------------------+  +-----------------------------+          |
|  dedup -> window accumulation -> oversight evaluation -> dispatch       |
+-----------------------------------+-------------------------------------+
                                    |
                                    | assembled batch (IncomingMessage)
                                    v
                   +-------------------------------+
                   | canon-inbound-queue-kafka     |
                   | topic: canon.{svc}.inbound    |
                   | partitioned by aggregate_id   |
                   +---------------+---------------+
                                   |
                                   v
                   +-------------------------------+
                   |         Dispatcher            |
                   |  polls inbox_messages for     |
                   |  unprocessed commands          |
                   +------+--------+-------+-------+
                          |        |       |
             +------------+   +----+   +---+-----------+
             v                v        v               v
   Command Handler    Internal EH   External EH    (version-matched
   (version-matched)  (no aggregate  (no aggregate    routing via
                       param)        param)           inventory)
             |
             | Result<Event, Error>
             v
+-------------------------------------------------------+
|          YugabyteDB ACID Transaction                  |
|                                                       |
|   BEGIN                                               |
|     INSERT INTO commands (...);    -- audit trail     |
|     INSERT INTO outbox (...) x N;  -- event staging   |
|   COMMIT                                              |
+----------------------------+--------------------------+
                             |
                             | notify (bounded mpsc channel)
                             v
              +-------------------------------+
              |      Outbox Processor         |
              |  SELECT ... FOR UPDATE        |
              |  SKIP LOCKED                  |
              |  WHERE delivered_at IS NULL   |
              |  ORDER BY sequence_number     |
              +---------------+---------------+
                              |
                              | publish confirmed -> set delivered_at
                              v
              +-------------------------------+
              | canon-outbound-queue-kafka    |
              | topic: canon.{svc}.outbound   |
              | partitioned by aggregate_id   |
              +---+--------+--------+----+----+
                  |        |        |    |
    +-------------+   +----+   +---+    +------------------+
    v                 v        v                            v
+----------+  +-----------+  +----------+     +---------------------+
| Event    |  |Projection |  |Publisher |     | Internal event      |
| Store    |  | Consumer  |  | Consumer |     | Consumer            |
| Consumer |  |           |  |          |     | (routes own events  |
+----+-----+  +-----+-----+  +----+----+     |  back to inbox)     |
     |              |              |           +----------+----------+
     v              v              v                      |
 Cassandra    YugabyteDB    canon.{svc}.events           |
 (events +    (read models  (Kafka topic for             |
  snapshots)   + checkpts)   other services)             |
                                   |                     |
                                   v                     v
                           Other services'          Local inbox
                           canon-adaptor-kafka      (re-entry)
```

---

## Stage 1: Message ingress

Messages enter a Canon service from two sources: external events published by
other services, and commands submitted by the gateway (or by event handlers
within the service itself).

### External events via the adaptor

The `canon-adaptor-kafka` crate subscribes to one or more external Kafka
topics -- for example, the fleet service subscribes to `canon.supply.events`
and `canon.navigation.events`. When an event arrives, the adaptor wraps it in
an `IncomingMessage::ExternalEvent` and submits it to the inbox.

The adaptor uses `rskafka` with a consistent connection pattern:

```rust
let client = ClientBuilder::new(broker_list).build().await?;
let partition_client = client
    .partition_client(topic, 0, UnknownTopicHandling::Retry)
    .await?;
```

Consumption is a polling loop that calls `partition_client.fetch_records()`:

```rust
loop {
    let (records, _watermark) = partition_client
        .fetch_records(next_offset, 1..1_048_576, timeout_ms)
        .await?;
    // process records, advance next_offset
}
```

Offset tracking is in-memory. When the process restarts, it resumes from
offset 0. This is safe because every downstream component is idempotent --
the inbox deduplicates, the event store rejects version conflicts, and
projection checkpoints skip already-processed events.

### Commands via the gateway

When a user clicks "Depart for Beta Relay" in the frontend, the gateway
receives a POST request, constructs a `CommandEnvelope`, and inserts it
directly into the service's `inbox_messages` table in YugabyteDB.

```rust
pub struct CommandEnvelope {
    pub command_id: Uuid,
    pub aggregate_id: AggregateId,
    pub command_type: String,
    pub correlation_id: Uuid,
    pub causation_id: Uuid,
    pub timestamp: DateTime<Utc>,
    pub payload: Bytes,
    pub command_version: u32,
}
```

The `command_type` and `command_version` fields are critical: they determine
which handler the dispatcher will route this command to. There is no casting --
the version number is read from the envelope and matched against the handler
registered at that exact version.

---

## Stage 2: The inbox

The inbox (`canon-inbox-yugabyte`) is the central coordination point for all
incoming messages. It serves three purposes:

1. **Idempotent intake** -- prevents duplicate processing via a composite
   primary key of `(handler_id, message_id)`.
2. **Window assembly** -- accumulates related messages into batches keyed by
   `(handler_id, correlation_key)`.
3. **Oversight evaluation** -- calls the handler's `oversight()` function to
   determine when a window is ready for dispatch.

### Deduplication

Every message that enters the inbox is checked against the `inbox_messages`
table:

```sql
INSERT INTO inbox_messages (handler_id, message_id, ...)
ON CONFLICT (handler_id, message_id) DO NOTHING;
```

If the insert is a no-op (the message was already processed), the inbox
silently skips it. This is the first layer of idempotency in the pipeline.

### Window accumulation

Messages are grouped into windows keyed by `(handler_id, correlation_key)`.
The correlation key comes from the handler's `correlate()` function, or falls
back to the envelope's `correlation_id` if the handler does not override it.

Each unique correlation key creates an independent window. A single handler
may have many concurrent in-flight windows -- one per correlation key.

The window tracks its lifecycle via `WindowStatus`:

```
pending -> dispatched    (Oversight::Ready -- batch published)
pending -> expired       (TTL exceeded before Ready)
expired -> dead_lettered (cleanup task moves to dead letter store)
```

### Oversight evaluation

When a new message arrives in a window, the inbox evaluates the handler's
`oversight()` function against the accumulated messages:

```rust
fn oversight(&self, accumulated: &[IncomingMessage]) -> Oversight {
    // Ready   -- dispatch the batch now
    // NotReady -- wait for more messages
    // Discard -- abandon this window entirely
}
```

Most event handlers do not set `window_ttl`, so oversight defaults to
`Oversight::Ready`. This means the window dispatches immediately on the first
message -- there is no waiting. Handlers with `window_ttl` (like the cargo
unloading handler) accumulate multiple events and wait for a domain condition
to be met before dispatching.

The `window_ttl` attribute requires an `oversight()` implementation. This is
enforced at compile time by the proc macros -- the build fails if `window_ttl`
is set without an `oversight` method.

### Window expiry

When a window has `window_ttl` set and oversight never returns `Ready`, the
TTL eventually expires. A background cleanup task transitions the window to
`expired` status and moves its messages to the dead letter store with reason
`window_expired`.

---

## Stage 3: The inbound queue

The inbound queue (`canon-inbound-queue-kafka`) carries assembled batches from
the inbox to the dispatcher. Its Kafka topic is `canon.{service}.inbound`,
partitioned by `aggregate_id` to preserve per-aggregate ordering.

The inbound queue exists as a decoupling buffer between the inbox (which runs
in the database transaction context) and the dispatcher (which runs as an
independent polling loop). This separation means the inbox can acknowledge
message receipt without waiting for the dispatcher to process it.

---

## Stage 4: The dispatcher

The dispatcher (`canon-core/src/dispatcher.rs`) is the central routing
component. It runs as a background `tokio::spawn` task that polls the inbox
for unprocessed commands and dispatches them to version-matched handlers.

### Dispatcher architecture

```rust
pub struct Dispatcher<S: DispatcherStore> {
    store: S,
    config: DispatcherConfig,
    outbox_notify: Option<OutboxNotifySender>,
    dispatcher_notify: Option<DispatcherNotifyReceiver>,
}
```

The dispatcher is generic over `DispatcherStore`, which abstracts the database
operations. In production this is `PgDispatcherStore` (YugabyteDB); in tests
it is `InMemoryDispatcherStore`.

### The command processing flow

Each poll cycle, the dispatcher:

1. **Polls the inbox** for unprocessed commands via `store.poll_inbox(batch_size)`.
   This returns a batch of `InboxCommandRow` records.

2. **For each command**, loads the aggregate's event history:
   ```rust
   let events = self.store.load_events(&row.aggregate_id).await?;
   ```

3. **Dispatches to the version-matched handler** via the `inventory` registry:
   ```rust
   let result = __dispatch_command(
       &row.envelope.command_type,    // e.g., "DepartForStation"
       row.envelope.command_version,  // e.g., 1
       row.envelope.payload.as_ref(), // serialized command bytes
       &events,                       // event history for hydration
       self.config.aggregate_type_id, // TypeId of the aggregate
   )?;
   ```

   This function looks up the handler in a lazily-initialized `HashMap`
   keyed by `(command_type_name, command_version)`. The lookup is O(1)
   after first use. The handler:
   - Deserializes the command payload
   - Hydrates the aggregate state from the event history using
     version-matched event combiners
   - Calls the user's `handle()` function
   - Serializes the resulting event

4. **Builds the event envelope**:
   ```rust
   let event_envelope = EventEnvelope {
       event_id: Uuid::new_v4(),
       aggregate_id: row.aggregate_id.clone(),
       version: current_version.next(),
       event_type: result.event_type.to_owned(),
       event_version: result.event_version,
       payload: Bytes::from(result.event_payload),
       correlation_id: row.envelope.correlation_id,
       causation_id: row.envelope.command_id,
       timestamp: Utc::now(),
   };
   ```

   Note how `causation_id` is set to the command ID -- this creates a
   causal chain from command to event. The `correlation_id` is propagated
   unchanged from the original command, allowing the full causal chain to
   be traced end-to-end.

5. **Writes to outbox and marks processed in a single ACID transaction**:
   ```rust
   self.store
       .write_outbox_and_mark_processed(
           row.message_id,
           &row.handler_id,
           event_envelope,
       )
       .await?;
   ```

### Version-matched routing

Version routing is the core mechanism that makes Canon's event sourcing work
across schema changes. When a command handler is registered:

```rust
#[command_handler(Ship, version = 1)]
impl DepartForStationHandler { ... }
```

The proc macro emits an `inventory` registration:

```rust
inventory::submit! {
    CommandHandlerRegistration {
        aggregate_type_name: "Ship",
        command_type_name: "DepartForStation",
        command_version: 1,
        handler_type_name: "DepartForStationHandler",
        dispatch_fn: __dispatch_DepartForStation_v1,
        produces_event_type: "ShipDeparted",
        produces_event_version: 1,
    }
}
```

The `dispatch_fn` is a static function pointer that deserializes the command,
hydrates aggregate state, runs the handler, and serializes the result -- all
without any runtime casting or reflection. When the dispatcher receives a
command with `command_type = "DepartForStation"` and `command_version = 1`,
it looks up this registration in a `HashMap` and calls the dispatch function
directly.

This same mechanism applies to event combiners. When aggregate state is
hydrated from events, each event's `event_type` and `event_version` are used
to look up the registered combiner:

```rust
pub fn __apply_event_combiner(
    aggregate_type_id: TypeId,
    envelope: &EventEnvelope,
    state: &mut dyn Any,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let key = (aggregate_type_id, envelope.event_type.clone(), envelope.event_version);
    match COMBINER_MAP.get(&key) {
        Some(apply_fn) => apply_fn(envelope.payload.as_ref(), state),
        None => Err("no event combiner registered".into()),
    }
}
```

### Error handling and dead lettering

When a command handler fails, the dispatcher does not crash. Instead:

1. The error is logged via `tracing::warn`.
2. A failure is recorded in the `retry_attempts` table:
   ```rust
   let attempts = self.store
       .record_failure(row.message_id, &row.handler_id, &error_msg)
       .await?;
   ```
3. If `attempts >= max_retries` (default: 3), the message is moved to the
   dead letter store:
   ```rust
   self.store
       .dead_letter(&row, &error_msg, attempts)
       .await?;
   ```
4. Otherwise, the message remains in the inbox for retry on the next poll cycle.

### Notification channels

The dispatcher uses two notification channels to minimize latency:

- **Outbox notify** (`OutboxNotifySender`): after writing events to the outbox,
  the dispatcher sends a `()` to wake the outbox processor immediately instead
  of waiting for its next poll cycle.
- **Dispatcher notify** (`DispatcherNotifyReceiver`): cross-service consumers
  send a `()` after inserting commands into the inbox, waking the dispatcher
  immediately.

Both channels are bounded `tokio::sync::mpsc` channels. If the channel is
full, the notification is silently dropped -- the recipient will pick up the
work on its next poll cycle.

### The dispatcher run loop

```rust
pub async fn run<F>(
    &mut self,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
    on_error: F,
) -> Result<(), DispatcherError>
{
    loop {
        if *shutdown.borrow() { return Ok(()); }

        match self.process_batch().await {
            Ok(0) => {
                // No commands -- wait for notification, timeout, or shutdown
                tokio::select! {
                    _ = sleep(poll_interval_ms) => {}
                    _ = dispatcher_notify.recv() => {}
                    _ = shutdown.changed() => { return Ok(()); }
                }
            }
            Ok(_n) => {
                // Processed commands -- immediately check for more
                tokio::task::yield_now().await;
            }
            Err(e) => {
                on_error(&e);
                // Sleep briefly before retrying
                tokio::select! {
                    _ = sleep(poll_interval_ms) => {}
                    _ = shutdown.changed() => { return Ok(()); }
                }
            }
        }
    }
}
```

This pattern -- process, sleep-on-empty, loop-on-work, sleep-on-error -- is
shared by every background task in Canon (outbox processor, all three
consumers).

---

## Stage 5: The YugabyteDB ACID transaction

The write path is the most critical part of the pipeline. When a command
handler succeeds, the dispatcher must atomically:

1. Insert the command into the `commands` table (audit trail).
2. Insert one or more events into the `outbox` table (event staging).
3. Delete the command from the `inbox_messages` table (mark processed).

All three operations happen in a single YugabyteDB ACID transaction:

```sql
BEGIN;

INSERT INTO commands (
    command_id, aggregate_id, command_type, command_version,
    payload, correlation_id, causation_id, created_at, status
) VALUES (...);

INSERT INTO outbox (
    id, aggregate_id, event_id, event_type, event_version,
    payload, correlation_id, causation_id, sequence_number, created_at
) VALUES (...);

DELETE FROM inbox_messages
WHERE handler_id = $1 AND message_id = $2;

COMMIT;
```

The `outbox` table has a PostgreSQL sequence (`outbox_seq`) that assigns a
monotonically increasing `sequence_number` to each row. This sequence number
is the global ordering key that all downstream consumers use to track their
position.

**The outbox is the durable commit point.** Once the transaction commits,
the event is guaranteed to eventually reach Cassandra, the projections, and
the publisher. If the process crashes between the commit and the outbox
processor publishing to Kafka, the event persists in the outbox table and
will be picked up when the processor restarts.

### Why YugabyteDB for the write path

YugabyteDB was chosen for the write path because it provides:

- **ACID transactions** spanning multiple tables (commands + outbox + inbox)
  in a single atomic commit.
- **PostgreSQL wire compatibility**, so the codebase can use `sqlx` with
  standard SQL.
- **Distributed architecture** with automatic sharding and replication,
  allowing the write path to scale horizontally.
- **`SELECT ... FOR UPDATE SKIP LOCKED`** support, which the outbox processor
  relies on for safe concurrent draining.

---

## Stage 6: The outbox processor

The outbox processor (`canon-core/src/outbox.rs`) has a single
responsibility: drain committed events from the outbox table and publish them
to the outbound Kafka queue.

```
Outbox table (YugabyteDB)
    |
    | SELECT * FROM outbox
    | WHERE delivered_at IS NULL
    | ORDER BY sequence_number
    | FOR UPDATE SKIP LOCKED
    |
    v
Outbox Processor (tokio task)
    |
    | publish(envelope)
    |
    v
Outbound Queue (Kafka)
    |
    | UPDATE outbox SET delivered_at = NOW()
    | WHERE id = $1
    v
Done
```

### What the outbox processor does NOT do

This is a crucial design constraint. The outbox processor:

- Does NOT write to Cassandra.
- Does NOT trigger projections.
- Does NOT publish to external Kafka topics.
- Does NOT evaluate event handlers.

It only moves events from the outbox table to the outbound queue. All
downstream processing is handled by independent consumers.

### Implementation

```rust
pub struct OutboxProcessor<S: OutboxStore, P: OutboxPublisher> {
    store: S,
    publisher: P,
    config: OutboxProcessorConfig,
}
```

The processor is generic over two traits:

- `OutboxStore` -- abstracts the outbox table. The real implementation uses
  `SELECT ... FOR UPDATE SKIP LOCKED` to prevent double-processing across
  replicas.
- `OutboxPublisher` -- abstracts the publish operation. In production, this
  is `KafkaOutboundProducer`.

The drain cycle is straightforward:

```rust
pub async fn drain_once(&self) -> Result<usize, OutboxProcessorError> {
    let entries = self.store.poll_undelivered(self.config.batch_size).await?;

    let mut processed = 0usize;
    for entry in entries {
        self.publisher.publish(entry.envelope).await?;
        self.store.mark_delivered(entry.id).await?;
        processed += 1;
    }

    Ok(processed)
}
```

Each entry is published individually, and `mark_delivered` is called after
confirmed Kafka publish. If publish fails mid-batch, the remaining entries
stay in the outbox and will be retried on the next cycle.

### Configuration

```rust
pub struct OutboxProcessorConfig {
    pub batch_size: usize,         // default: 100
    pub channel_capacity: usize,   // default: 1024
    pub poll_interval_ms: u64,     // default: 50
}
```

### Backpressure

The outbox processor uses a bounded `tokio::sync::mpsc` channel between the
command handler write path and itself. After the dispatcher writes events to
the outbox, it sends a `()` on this channel to wake the processor. If the
channel is full (capacity 1024 by default), the notification is dropped and
the processor will pick up the entries on its next poll cycle.

When the processor successfully processes entries, it also notifies downstream
consumers via a `tokio::sync::Notify`:

```rust
if let Some(ref outbound) = outbound_notify {
    outbound.notify_waiters();
}
```

This reduces end-to-end pipeline latency from ~100ms (poll timeout) to
near-zero.

---

## Stage 7: The outbound queue

The outbound queue (`canon-outbound-queue-kafka`) is a Kafka topic
(`canon.{service}.outbound`) that carries committed events from the outbox
processor to four independent consumer groups.

### Producer

`KafkaOutboundProducer` serializes the `EventEnvelope` to JSON and publishes
it to Kafka, using `aggregate_id` as the partition key:

```rust
let key = envelope.aggregate_id.as_uuid().to_string();
let payload = serde_json::to_vec(&envelope)?;

let record = Record {
    key: Some(key.into_bytes()),
    value: Some(payload),
    headers: BTreeMap::new(),
    timestamp: Utc::now(),
};

self.partition_client
    .produce(vec![record], Compression::NoCompression)
    .await?;
```

### Consumer

`KafkaOutboundConsumer` implements the `ConsumerReceiver` trait from
`canon-core`:

```rust
#[async_trait]
pub trait ConsumerReceiver: Send + Sync + 'static {
    async fn receive(&self) -> Result<Option<ReceivedEnvelope>, ConsumerReceiverError>;
    async fn commit(&self) -> Result<(), ConsumerReceiverError>;
}
```

The consumer polls Kafka with `fetch_records()` and deserializes the payload
back into an `EventEnvelope`. The `sequence_number` field maps to
`kafka_offset + 1` (Kafka offsets are 0-based; sequence numbers are 1-based).

Commit is a no-op -- `rskafka` has no consumer group abstraction, and
application-layer idempotency is the safety net.

### The rskafka pattern

All four Kafka crates in Canon use `rskafka` with a consistent pattern:

```
Connection:  ClientBuilder::new(brokers).build().await
             -> client.partition_client(topic, 0, UnknownTopicHandling::Retry)

Produce:     partition_client.produce(vec![record], Compression::NoCompression)

Consume:     partition_client.fetch_records(next_offset, 1..1_048_576, timeout_ms)
             offset tracked in Mutex<i64>, starts at 0

Commit:      No-op. Application-layer idempotency handles duplicates.

Errors:      Each crate owns its error type via thiserror, wrapping rskafka errors
             as strings.
```

No `rdkafka`. No C dependencies. All Kafka crates are pure Rust and
cross-compilable.

---

## Stage 8: The four outbound consumer groups

Four independent consumer groups read from the outbound queue. Each processes
every event independently. They share no state and can fail/recover
independently.

### Event store consumer

The event store consumer (`canon-core/src/consumers/event_store_consumer.rs`)
writes events to Cassandra and takes periodic snapshots.

```rust
pub struct EventStoreConsumer<ES, SS, DL, RT, SP> {
    event_store: ES,       // Cassandra
    snapshot_store: SS,    // YugabyteDB
    dead_letter_store: DL, // YugabyteDB
    retry_tracker: RT,     // YugabyteDB
    snapshot_state_provider: SP,
    config: EventStoreConsumerConfig,
}
```

Processing a single event:

1. **Append to event store** with optimistic concurrency:
   ```rust
   self.event_store
       .append(&aggregate_id, expected_version, vec![envelope.clone()])
       .await
   ```
   The `expected_version` is `envelope.version - 1`. If Cassandra already has
   an event at this version, the write is rejected with a version conflict.

2. **On success**, clean up the retry tracker and check the snapshot condition:
   ```rust
   if self.config.snapshot_every > 0
       && envelope.version.as_u64().is_multiple_of(self.config.snapshot_every)
   {
       // Hydrate aggregate state and write snapshot
       let state = self.snapshot_state_provider
           .state_at(&aggregate_id, envelope.version)
           .await?;
       let snapshot = Snapshot {
           aggregate_id, version: envelope.version, state, taken_at: Utc::now(),
       };
       self.snapshot_store.save(snapshot).await?;
   }
   ```

3. **On version conflict**, increment the retry count. If retries are
   exhausted (default: 3), dead-letter the event:
   ```rust
   let attempts = self.retry_tracker
       .increment(event_id, "event_store_consumer")?;

   if attempts >= self.config.max_retries {
       self.dead_letter_store.store(
           event_id, "event_store_consumer", &aggregate_id,
           envelope.payload.clone(),
           &format!("version conflict after {attempts} attempts"),
       ).await?;
   }
   ```

### Why Cassandra for the event store

Cassandra was chosen for the event store because:

- **Write-optimized** -- events are append-only, and Cassandra excels at
  sequential writes.
- **Partition key = aggregate_id, clustering key = version** -- events for
  a single aggregate are stored together on disk, making hydration a single
  sequential read.
- **No single point of failure** -- Cassandra's peer-to-peer replication
  means no leader election or failover.
- **Optimistic concurrency** via lightweight transactions (`IF NOT EXISTS`)
  on the `(aggregate_id, version)` primary key.

### Projection consumer

The projection consumer (`canon-core/src/consumers/projection_consumer.rs`)
applies events to read models in YugabyteDB.

```rust
pub struct ProjectionConsumer<CS: ProjectionCheckpointStore> {
    projections: Vec<RegisteredProjection>,
    checkpoint_store: CS,
}
```

For each registered projection, the consumer:

1. Reads the checkpoint (last processed sequence number).
2. Skips events with sequence numbers older than or equal to the checkpoint.
3. Applies the event via the projection's type-erased apply function.
4. Advances the checkpoint.

```rust
pub async fn process(
    &self,
    envelope: &EventEnvelope,
    sequence_number: u64,
) -> Result<(), ProjectionConsumerError> {
    for projection in &self.projections {
        let checkpoint = self.checkpoint_store
            .get_checkpoint(&projection.projection_id)
            .await?;

        if sequence_number <= checkpoint.as_u64() {
            continue; // already processed
        }

        (projection.apply_fn)(&projection.projection_id, envelope)?;

        self.checkpoint_store
            .set_checkpoint(&projection.projection_id, Version::from_u64(sequence_number))
            .await?;
    }
    Ok(())
}
```

The checkpoint uses the **global sequence number** (from the outbox sequence),
not the per-aggregate version. This is critical: if the checkpoint tracked
per-aggregate versions, processing an event from aggregate A at version 100
would advance the checkpoint to 100, causing events from aggregate B at
version 5 to be silently skipped. Global sequence numbers ensure every event
is processed regardless of which aggregate produced it.

### Projection rebuild

Projections can be rebuilt from scratch:

1. Set `rebuilding = true` on the projection.
2. Read endpoints fall back to read-through (query the event store directly).
3. Reset the consumer offset to 0.
4. Kafka replays all events from the beginning.
5. Set `rebuilding = false` when complete.

### Publisher consumer

The publisher consumer (`canon-core/src/consumers/publisher_consumer.rs`) is
the simplest of the three. It publishes events to `canon.{service}.events`
for consumption by other services:

```rust
pub struct PublisherConsumer<P: Publisher> {
    publisher: P,
    topic: String,  // e.g., "canon.fleet.events"
}
```

Other services subscribe to this topic via `canon-adaptor-kafka`, completing
the cross-service event loop.

### Internal event consumer

The fourth consumer routes a service's own events back to the inbox for event
handler dispatch. When the fleet service produces a `ShipDeparted` event,
the internal event consumer checks the `EventHandlerRegistration` inventory
for any handlers that `#[handles]` that event type and version. For each
match, it calls `Inbox::submit(handler_id, event_id, InternalEvent(envelope))`.

From the inbox onwards, the flow is identical to external events: dedup,
window accumulation, oversight, dispatch.

### Shared consumer run loop

All four consumers share the same run loop pattern:

```rust
loop {
    if *shutdown.borrow() { return; }

    let received = tokio::select! {
        r = receiver.receive() => r,
        _ = outbound_notify.notified() => {
            receiver.receive().await
        }
        _ = shutdown.changed() => return,
    };

    match received {
        Ok(Some(re)) => {
            self.process(re.envelope).await;
            receiver.commit().await;
        }
        Ok(None) => {
            tokio::task::yield_now().await;
        }
        Err(e) => {
            sleep(50ms).await;
        }
    }
}
```

When `outbound_notify` is provided, the consumer wakes immediately when the
outbox processor publishes new events, reducing pipeline latency to near-zero.

---

## Kafka topic structure

Canon uses 15 Kafka topics in the demo (3 per service, 5 services). All
topics are explicitly created during cluster initialization -- no auto-create.

```
+-----------------------+-------------------------------------------+
| Topic pattern         | Purpose                                   |
+-----------------------+-------------------------------------------+
| canon.{svc}.inbound   | Inbox -> Dispatcher                       |
|                       | (assembled batches from inbox)             |
+-----------------------+-------------------------------------------+
| canon.{svc}.outbound  | Outbox processor -> 4 consumer groups     |
|                       | (committed events fanning out)            |
+-----------------------+-------------------------------------------+
| canon.{svc}.events    | Publisher -> Other services' adaptors     |
|                       | (cross-service event distribution)        |
+-----------------------+-------------------------------------------+
```

All topics are partitioned by `aggregate_id`, ensuring that events for a
single aggregate are always processed in order.

**Inbound topics** (5): `canon.fleet.inbound`, `canon.cargo.inbound`,
`canon.navigation.inbound`, `canon.supply.inbound`, `canon.station.inbound`

**Outbound topics** (5): `canon.fleet.outbound`, `canon.cargo.outbound`,
`canon.navigation.outbound`, `canon.supply.outbound`, `canon.station.outbound`

**Published events** (5): `canon.fleet.events`, `canon.cargo.events`,
`canon.navigation.events`, `canon.supply.events`, `canon.station.events`

---

## Service lifecycle

### ServiceBuilder

The `ServiceBuilder` is the entry point for creating a runnable service. It
uses a type-state pattern where each infrastructure component is a generic
parameter, starting as `()` and becoming the concrete type when provided:

```rust
let service = ServiceBuilder::new("fleet")
    .for_aggregate::<Ship>()
    .event_store(cassandra_event_store)
    .snapshot_store(yugabyte_snapshot_store)
    .dead_letter_store(yugabyte_dead_letter_store)
    .retry_tracker(yugabyte_retry_tracker)
    .snapshot_state_provider(EventPayloadSnapshotProvider)
    .outbox_store(yugabyte_outbox_store)
    .outbox_publisher(kafka_outbound_producer)
    .projection_checkpoint_store(yugabyte_projection_store)
    .publisher(kafka_publisher)
    .topic("canon.fleet.events")
    .build()?;
```

### Validation at build time

When `build()` is called, the `ServiceBuilder`:

1. **Scans `inventory`** for all command, event, handler, and combiner
   registrations.
2. **Validates exhaustiveness**: every `#[command(X, v=N)]` must have a
   matching `#[command_handler(X, v=N)]`. Every `#[event(X, v=N)]` must
   have a matching `#[event_combiner(X, v=N)]`.
3. **Verifies required components** are present (event store, snapshot store,
   outbox store, etc.).
4. **Assembles the `Service`** with all background processors wired:
   - `EventStoreConsumer` (event store + snapshot store + dead letter store +
     retry tracker)
   - `OutboxProcessor` (outbox store + outbox publisher)
   - `ProjectionConsumer` (projection checkpoint store)
   - `PublisherConsumer` (publisher + topic)

### Service::start()

The `start()` method spawns all background tasks:

```rust
service.start(
    shutdown_rx,       // watch channel for graceful shutdown
    Some(notify_rx),   // outbox notify channel
    es_receiver,       // Kafka consumer for event store
    proj_receiver,     // Kafka consumer for projections
    pub_receiver,      // Kafka consumer for publisher
).await;
```

Each consumer runs as an independent `tokio::spawn` task. The outbox
processor also runs as a spawned task. All tasks share the same shutdown
watch channel.

### Graceful shutdown

Every background task receives a `tokio::sync::watch::Receiver<bool>` for
shutdown signalling:

```rust
let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

// Each spawned task checks:
loop {
    if *shutdown.borrow() { return; }
    // ... process next item ...
    tokio::select! {
        _ = process_next() => { ... }
        _ = shutdown.changed() => return,
    }
}

// To shut down all tasks:
shutdown_tx.send(true)?;
```

This pattern ensures all tasks drain their current work item and exit
cleanly. The main function waits for all `JoinHandle`s before terminating.

### Demo service wiring

A complete service wiring (from `canon-demo/fleet-service/src/main.rs`)
shows how everything fits together:

```rust
#[tokio::main]
async fn main() -> Result<(), StartupError> {
    // 1. Connect to infrastructure
    let yugabyte_pool = create_service_pool(&yugabyte_url, "canon_fleet").await?;
    let event_store = Arc::new(
        CassandraEventStore::new_with_keyspace(&cassandra_nodes, "canon_fleet").await?
    );

    // 2. Create infrastructure stores
    let outbox_store = YugabyteOutboxStore::new(yugabyte_pool.clone());
    let outbox_publisher = KafkaOutboundProducer::new(&config).await?;
    let snapshot_store = YugabyteSnapshotStore::new(yugabyte_pool.clone());
    let projection_store = YugabyteProjectionStore::from_pool(yugabyte_pool.clone());
    let publisher = KafkaPublisher::new(&kafka_brokers, "fleet").await?;

    // 3. Create 3 independent Kafka consumers
    let es_receiver = KafkaOutboundConsumer::new(&es_config).await?;
    let proj_receiver = KafkaOutboundConsumer::new(&proj_config).await?;
    let pub_receiver = KafkaOutboundConsumer::new(&pub_config).await?;

    // 4. Build the service
    let service = ServiceBuilder::new("fleet")
        .for_aggregate::<Ship>()
        .event_store(event_store.clone())
        .snapshot_store(snapshot_store)
        .outbox_store(outbox_store)
        .outbox_publisher(outbox_publisher)
        .projection_checkpoint_store(projection_store)
        .publisher(publisher)
        .topic("canon.fleet.events")
        .build()?;

    // 5. Create the dispatcher
    let dispatcher_store = PgDispatcherStore::new(
        yugabyte_pool.clone(), event_store.clone(), "Ship"
    );
    let (notify_tx, notify_rx) = new_outbox_notify_channel(16);
    let (disp_notify_tx, disp_notify_rx) = new_dispatcher_notify_channel(16);
    let mut dispatcher = Dispatcher::new(dispatcher_store, config)
        .with_outbox_notify(notify_tx)
        .with_dispatcher_notify(disp_notify_rx);

    // 6. Spawn all background tasks
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    tokio::spawn(dispatcher.run(shutdown_rx.clone(), |err| warn!(%err)));
    tokio::spawn(service.start(shutdown_rx, Some(notify_rx),
        es_receiver, proj_receiver, pub_receiver));

    // 7. Spawn cross-service event consumers
    tokio::spawn(consume_supply_events(..., disp_notify_tx.clone()));
    tokio::spawn(consume_navigation_events(..., disp_notify_tx));

    // 8. Wait for shutdown signal
    tokio::signal::ctrl_c().await?;
    shutdown_tx.send(true)?;
}
```

---

## The strict DAG dependency graph

Canon enforces a strict directed acyclic graph (DAG) for crate dependencies.
No crate may depend on another implementation crate. Each implementation
depends only on its trait crate and `canon-core`.

```
                       canon-core
                     (traits, types,
                      in-memory impls,
                      proc-macros)
                     /     |      \
                    /      |       \
                   v       v        v
            canon-       canon-     canon-
            event-       inbox      outbound-
            store                   queue
            (trait)      (trait)    (trait)
              |            |          |
              v            v          v
            canon-       canon-     canon-
            event-       inbox-     outbound-
            store-       yugabyte   queue-
            cassandra               kafka
```

This means:

- `canon-event-store-cassandra` depends on `canon-event-store` + `canon-core`.
  It does NOT depend on `canon-outbound-queue-kafka` or `canon-inbox-yugabyte`.
- `canon-outbound-queue-kafka` depends on `canon-outbound-queue` + `canon-core`.
  It does NOT depend on `canon-event-store-cassandra`.
- No cross-impl dependencies ever.

This constraint keeps the dependency graph clean, compile times fast, and
allows infrastructure crates to be swapped without cascading changes.

### Per-service storage isolation

Each demo service uses its own YugabyteDB schema and Cassandra keyspace:

```
+-------------------+---------------------+-------------------+
| Service           | YugabyteDB schema   | Cassandra keyspace|
+-------------------+---------------------+-------------------+
| fleet-service     | canon_fleet         | canon_fleet       |
| cargo-service     | canon_cargo         | canon_cargo       |
| navigation-svc    | canon_navigation    | canon_navigation  |
| supply-service    | canon_supply        | canon_supply      |
| station-service   | canon_station       | canon_station     |
+-------------------+---------------------+-------------------+
```

Services never share outbox, commands, inbox, or event store tables. This
isolation prevents event leaking across services -- a bug discovered early
in development when shared tables caused events from the fleet service to
appear in the cargo service's projections.

---

## Infrastructure choices rationale

### YugabyteDB (write path, read models, operational state)

YugabyteDB handles the transactional write path (commands + outbox in a
single ACID transaction), projection read models, snapshot storage,
projection checkpoints, dead letter storage, retry attempt tracking, and
inbox tables. It was chosen because:

- ACID transactions spanning multiple tables in a single commit.
- PostgreSQL wire compatibility (`sqlx` + standard SQL).
- `SELECT ... FOR UPDATE SKIP LOCKED` for the outbox processor.
- Distributed architecture for horizontal scaling.

### Cassandra (event store)

Cassandra stores the immutable event log. It was chosen because:

- Append-only write pattern matches event sourcing perfectly.
- Partition key = `aggregate_id`, clustering key = `version` -- events for
  one aggregate are co-located on disk.
- No single point of failure.
- Excellent read performance for sequential scans (aggregate hydration).

### Kafka (messaging)

Kafka carries messages between pipeline stages. It was chosen because:

- Durable, partitioned log with ordering guarantees per partition.
- `aggregate_id` as partition key ensures per-aggregate ordering.
- Consumer groups allow independent processing of the same event stream.
- `rskafka` provides a pure-Rust implementation with no C dependencies,
  enabling cross-compilation from macOS to Linux (musl) for deployment.

---

## Cross-service event flows

The demo implements a supply chain loop where events cascade across services:

```
Fleet:ShipDeparted
    |
    v
Navigation service (via adaptor)
    -> PlanRoute, UpdatePosition, RecordArrival
    -> publishes ShipArrivedAtStation
          |
          v
    Cargo service (via adaptor)         Station service (via adaptor)
    -> CreateManifest, LoadCargo        -> RecordDocking, RecordCargoReceived
                                        -> if stock low: StationStockLow
                                              |
                                              v
                                        Supply service (via adaptor)
                                        -> RequestResupply, DispatchResupply
                                        -> publishes ResupplyDispatched
                                              |
                                              v
                                        Fleet service (via adaptor)
                                        -> ScheduleResupply
```

Each arrow represents a message flowing through the full pipeline: adaptor
to inbox to dispatcher to outbox to outbound queue to publisher to the next
service's adaptor. The total number of pipeline stages for a single
cross-service hop is approximately 8.

---

## Summary of idempotency layers

Canon uses defense-in-depth for exactly-once processing:

```
+-------------------+---------------------------------------------+
| Layer             | Mechanism                                   |
+-------------------+---------------------------------------------+
| Inbox             | (handler_id, message_id) PK dedup           |
| Inbox windows     | (window_id) in processed_windows table      |
| Event store       | (aggregate_id, version) PK rejects dupes    |
| Projection        | sequence_number checkpoint skip             |
| Retry tracker     | Crash-safe attempt counting                 |
| Dead letter       | Permanent storage after max retries         |
| Kafka consumers   | Restart from offset 0, all downstream idempotent |
+-------------------+---------------------------------------------+
```

No single layer is solely responsible for correctness. Each layer handles
duplicates independently, so a failure at any level is caught by the next.
