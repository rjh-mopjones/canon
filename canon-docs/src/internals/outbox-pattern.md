# Outbox Pattern

The outbox pattern is Canon's solution to the dual-write problem. It guarantees that
events produced by command handlers are never lost between the YugabyteDB transaction
and downstream Kafka consumers. This chapter covers the theory, the concrete
implementation, the background processor, failure modes, and how the outbox feeds the
four independent consumer groups that form the rest of the pipeline.

---

## The dual-write problem

Every command handler in Canon must do two things:

1. Persist the command and its resulting event(s) to a durable store.
2. Publish those events so downstream consumers can react (event store, projections,
   cross-service publisher).

In a naive implementation these are two independent writes -- one to YugabyteDB and one
to Kafka. If the process crashes between steps 1 and 2, the event is persisted but never
published. Consumers never see it. If you reverse the order, the event is published but
the command record is lost. There is no way to make two independent writes to different
systems atomic without a distributed transaction protocol.

Distributed transactions (two-phase commit, XA) are heavyweight, slow, and introduce a
coordinator as a single point of failure. Canon avoids them entirely.

---

## Canon's solution: the outbox table

Instead of writing directly to Kafka, the command handler write path stages events in an
**outbox table** within the same YugabyteDB ACID transaction as the command insert.
Either both the command and all its events are persisted, or neither is. The outbox is the
**commit point** -- once the transaction commits, the events are guaranteed to eventually
reach Kafka.

### The single ACID transaction

The dispatcher's `write_outbox_and_mark_processed` method executes the following steps
inside a single YugabyteDB transaction:

```
BEGIN

  -- 1. Lock the inbox message to prevent concurrent dispatchers
  SELECT message_id
  FROM inbox_messages
  WHERE handler_id = $1 AND message_id = $2
  FOR UPDATE SKIP LOCKED;

  -- 2. Insert the event into the outbox
  INSERT INTO outbox (aggregate_id, payload, created_at)
  VALUES ($1, $2, now());

  -- 3. Delete the processed inbox message
  DELETE FROM inbox_messages
  WHERE handler_id = $1 AND message_id = $2;

COMMIT
```

This is the full transaction boundary. If any step fails, the entire transaction rolls
back: the inbox message stays untouched, no outbox entry is created, and the dispatcher
will retry the message on the next poll cycle.

### What makes this safe

The key insight is that both the command processing artefacts (outbox entry) and the
acknowledgement (inbox message deletion) happen in one atomic unit. There is no window
where an event could be committed but not visible, or acknowledged but not persisted.

The `FOR UPDATE SKIP LOCKED` on the inbox row serves a second purpose: it prevents
multiple dispatcher replicas from processing the same message concurrently. If another
dispatcher has already locked the row, the current one gets zero rows back and skips the
message without blocking.

---

## The outbox table schema

Each service maintains its own outbox table within its isolated YugabyteDB schema:

```sql
-- Sequence provides globally ordered, monotonically increasing IDs
CREATE SEQUENCE canon_fleet.outbox_seq;

CREATE TABLE canon_fleet.outbox (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    sequence_number BIGINT DEFAULT nextval('canon_fleet.outbox_seq'),
    aggregate_id    UUID,
    payload         BYTEA,
    created_at      TIMESTAMPTZ DEFAULT now(),
    delivered_at    TIMESTAMPTZ
);

-- Partial index: only scans undelivered rows
CREATE INDEX outbox_seq_idx
    ON canon_fleet.outbox (sequence_number)
    WHERE delivered_at IS NULL;
```

### Column semantics

| Column            | Purpose                                                     |
|-------------------|-------------------------------------------------------------|
| `id`              | Primary key (UUID). Used to mark individual entries delivered. |
| `sequence_number` | Assigned by `outbox_seq`. Guarantees strict global ordering.  |
| `aggregate_id`    | The aggregate that produced this event. Used for Kafka partitioning. |
| `payload`         | JSON-serialised `EventEnvelope`. Contains event type, version, correlation/causation IDs, and the domain payload. |
| `created_at`      | When the row was inserted. Diagnostic only.                   |
| `delivered_at`    | Set by the outbox processor after confirmed Kafka publish. `NULL` means undelivered. |

### The partial index

The partial index on `sequence_number WHERE delivered_at IS NULL` is critical for
performance. As the outbox accumulates thousands of delivered rows, the processor's
polling query only needs to scan the (small) set of undelivered entries. Without this
index, every poll would scan the entire table.

### Sequence ordering

The PostgreSQL sequence (`outbox_seq`) assigns monotonically increasing numbers. This
guarantees:

- Events are drained in the order they were committed.
- Concurrent transactions produce interleaved but ordered sequence numbers.
- The outbox processor processes events in strict commit order.

Note that sequence numbers may have gaps (e.g., if a transaction rolls back after
acquiring a sequence number). The outbox processor does not care about gaps -- it simply
processes whatever undelivered rows exist, in order.

---

## The outbox processor

The outbox processor is a dedicated background tokio task spawned by `ServiceBuilder`.
It has a single responsibility: read committed events from the outbox table, publish
them to the outbound Kafka queue, and mark them as delivered.

### What the outbox processor does NOT do

This bears repeating because it is a common source of confusion. The outbox processor
does NOT:

- Write to Cassandra (that is the event store consumer's job).
- Update projections (that is the projection consumer's job).
- Publish to external Kafka topics (that is the publisher consumer's job).
- Handle dead letters or retries.
- Route events to internal event handlers.

It only moves events from the outbox table to the outbound Kafka queue.

### The OutboxStore trait

The processor is generic over the `OutboxStore` trait, defined in `canon-core`:

```rust
#[async_trait]
pub trait OutboxStore: Send + Sync + 'static {
    /// Fetch the next batch of undelivered outbox entries, ordered by
    /// sequence_number. Returns an empty vec when none are available.
    async fn poll_undelivered(
        &self,
        batch_size: usize,
    ) -> Result<Vec<OutboxEntry>, OutboxProcessorError>;

    /// Mark a single outbox entry as delivered (sets delivered_at).
    async fn mark_delivered(
        &self,
        entry_id: Uuid,
    ) -> Result<(), OutboxProcessorError>;
}
```

The YugabyteDB implementation (`YugabyteOutboxStore`) provides the real SQL. The
in-memory implementation (`InMemoryOutboxStore`) uses a `BTreeMap` keyed by sequence
number for testing.

### Polling strategy

The YugabyteDB implementation polls with:

```sql
SELECT id, sequence_number, aggregate_id, payload
FROM outbox
WHERE delivered_at IS NULL
ORDER BY sequence_number ASC
LIMIT $1
FOR UPDATE SKIP LOCKED
```

Three clauses matter:

- **`WHERE delivered_at IS NULL`** -- only undelivered events. Hits the partial index.
- **`ORDER BY sequence_number ASC`** -- preserves commit ordering.
- **`FOR UPDATE SKIP LOCKED`** -- prevents double-processing if multiple outbox
  processor replicas run simultaneously. Each replica picks up different rows.
  Unlike `FOR UPDATE` (which blocks), `SKIP LOCKED` returns immediately with
  whatever rows are not currently locked by another transaction.

### The drain cycle

A single `drain_once` call:

1. Polls up to `batch_size` undelivered entries.
2. For each entry, publishes the event envelope to the outbound Kafka queue via
   `OutboxPublisher::publish`.
3. After confirmed publish, calls `mark_delivered(entry_id)` to set `delivered_at`.
4. Returns the count of entries successfully processed.

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

If a publish fails mid-batch, the cycle stops and returns the error. Entries already
published and marked delivered in this cycle are not rolled back -- the outbound queue
consumers are idempotent and will handle duplicates.

### Delivery confirmation

After each confirmed Kafka publish, the processor sets `delivered_at`:

```sql
UPDATE outbox SET delivered_at = now() WHERE id = $1
```

If this update fails (e.g., network partition to YugabyteDB), the entry remains
undelivered and will be picked up again on the next poll cycle. This may cause a
duplicate publish to Kafka, which is safe because all downstream consumers are
idempotent.

### The run loop

The processor's `run` method is the entry point for the background tokio task:

```rust
pub async fn run<F>(
    &self,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
    mut notify: Option<OutboxNotifyReceiver>,
    outbound_notify: Option<Arc<Notify>>,
    on_error: F,
) -> Result<(), OutboxProcessorError>
```

The loop follows this pattern:

1. Check for shutdown signal. If `true`, return immediately.
2. Call `drain_once()`.
3. If zero entries were processed, sleep until one of:
   - The notify channel receives a signal (immediate wake).
   - The `poll_interval_ms` timer elapses.
   - The shutdown signal fires.
4. If entries were processed, immediately loop to drain more. Drain any pending
   notifications to prevent spurious wakes. Notify outbound consumers that new
   events are available.
5. If an error occurred, invoke the `on_error` callback (for logging/metrics),
   sleep for `poll_interval_ms`, and retry.

The processor never propagates transient errors -- it logs them and retries. This
makes it resilient to temporary database or Kafka outages.

---

## Backpressure via bounded channel

The outbox processor uses a bounded tokio MPSC channel for backpressure between the
command handler write path and the processor:

```rust
pub const DEFAULT_CHANNEL_CAPACITY: usize = 1024;

pub type OutboxNotifySender = tokio::sync::mpsc::Sender<()>;
pub type OutboxNotifyReceiver = tokio::sync::mpsc::Receiver<()>;

pub fn new_outbox_notify_channel(
    capacity: usize,
) -> (OutboxNotifySender, OutboxNotifyReceiver) {
    tokio::sync::mpsc::channel(capacity)
}
```

The flow works as follows:

1. The dispatcher commits events to the outbox table.
2. After commit, it sends a `()` on the notify channel.
3. The outbox processor, if sleeping, wakes immediately and drains.

If the outbound Kafka queue is slow, the processor cannot drain fast enough, and
eventually the channel fills up (default 1024 pending notifications). At that point
`try_send` on the sender side fails, which signals the write path that the outbox is
backed up. The write path does not block -- it continues operating, and the processor
will catch up via its next poll cycle. The bounded channel prevents unbounded memory
growth.

### Configuration

```rust
pub struct OutboxProcessorConfig {
    /// Maximum outbox rows fetched per poll cycle. Default: 100.
    pub batch_size: usize,
    /// Bounded channel capacity. Default: 1024.
    pub channel_capacity: usize,
    /// Sleep duration when outbox is empty, in milliseconds. Default: 50.
    pub poll_interval_ms: u64,
}
```

Tuning these values:

- **`batch_size`**: larger batches reduce poll frequency but increase per-cycle
  latency. 100 is a good default.
- **`channel_capacity`**: 1024 accommodates bursts without back-pressure. Reduce
  for tighter flow control.
- **`poll_interval_ms`**: 50ms gives near-real-time draining. Increase to reduce
  database load at the cost of latency.

---

## How the outbox feeds the four consumer groups

Once an event reaches the outbound Kafka queue, four independent consumer groups
process it in parallel:

```
                          +-- Event store consumer      --> Cassandra
                          |
Outbox --> outbound queue +-- Projection consumer       --> YugabyteDB read models
                          |
                          +-- Internal event consumer   --> Inbox (event handler dispatch)
                          |
                          +-- Publisher consumer        --> canon.{service}.events topic
```

Each consumer group reads from the same outbound topic independently. They have
separate Kafka offsets and process events at their own pace. A slow projection
consumer does not block the event store consumer.

### Event store consumer

Writes events to Cassandra. After a confirmed write, checks whether
`version % snapshot_every == 0` and takes a snapshot if so. On version conflict,
retries up to 3 times, then dead-letters.

### Projection consumer

Applies events to YugabyteDB read models via `#[projection_handler]` impls. Updates
`projection_checkpoints.last_version` after each successful apply. Supports projection
rebuild via Kafka offset reset.

### Publisher consumer

Publishes events to the cross-service topic `canon.{service}.events`. Other services'
adaptors subscribe to this topic to receive events from this service.

All four consumers restart from offset 0 on process restart and rely on application-
layer idempotency to skip already-processed events:

- **Event store**: Cassandra PK `(aggregate_id, version)` rejects duplicates.
- **Projections**: `projection_checkpoints.last_version` skips old events.
- **Internal event consumer**: inbox dedup via `(handler_id, message_id)` handles duplicates.
- **Publisher**: downstream service inbox dedup handles duplicates.

---

## Demo service wiring

Here is how the fleet-service wires the outbox in `main.rs`:

```rust
// YugabyteDB-backed outbox store
let outbox_store = YugabyteOutboxStore::new(yugabyte_pool.clone());

// Kafka outbox publisher: outbox processor --> outbound queue
let outbox_publisher = KafkaOutboundProducer::new(&KafkaOutboundProducerConfig {
    brokers: kafka_brokers.clone(),
    topic: outbound_topic.clone(),
})
.await?;

// Four independent consumer groups on the same outbound topic
let es_receiver = KafkaOutboundConsumer::new(&KafkaOutboundConsumerConfig {
    brokers: kafka_brokers.clone(),
    topic: outbound_topic.to_owned(),
    group_id: "canon.fleet.event-store-consumer".to_owned(),
    ..Default::default()
}).await?;

let proj_receiver = KafkaOutboundConsumer::new(&KafkaOutboundConsumerConfig {
    brokers: kafka_brokers.clone(),
    topic: outbound_topic.to_owned(),
    group_id: "canon.fleet.projection-consumer".to_owned(),
    ..Default::default()
}).await?;

let internal_receiver = KafkaOutboundConsumer::new(&KafkaOutboundConsumerConfig {
    brokers: kafka_brokers.clone(),
    topic: outbound_topic.to_owned(),
    group_id: "canon.fleet.internal-event-consumer".to_owned(),
    ..Default::default()
}).await?;

let pub_receiver = KafkaOutboundConsumer::new(&KafkaOutboundConsumerConfig {
    brokers: kafka_brokers.clone(),
    topic: outbound_topic.to_owned(),
    group_id: "canon.fleet.publisher-consumer".to_owned(),
    ..Default::default()
}).await?;

// Wire into ServiceBuilder
let service = ServiceBuilder::new()
    .outbox_store(outbox_store)
    .outbox_publisher(outbox_publisher)
    // ... other stores ...
    .build()?;

// Outbox notify channel: dispatcher wakes outbox processor immediately
let (notify_tx, notify_rx) = new_outbox_notify_channel(16);

let mut dispatcher = Dispatcher::new(dispatcher_store, dispatcher_config)
    .with_outbox_notify(notify_tx);

// service.start() spawns the outbox processor as a background task
service.start(shutdown_rx, Some(notify_rx)).await;
```

---

## In-memory implementation for testing

Canon provides `InMemoryOutboxStore` and `InMemoryOutboxPublisher` in `canon-core`
for use in test harnesses:

```rust
let outbox_store = InMemoryOutboxStore::new();
let outbound_queue = InMemoryOutboundQueue::new();
let publisher = InMemoryOutboxPublisher::new(outbound_queue.clone());
let processor = OutboxProcessor::new(
    outbox_store.clone(),
    publisher,
    OutboxProcessorConfig::default(),
);

// Simulate the command handler write path
outbox_store.insert(envelope)?;

// Drain the outbox
let processed = processor.drain_once().await?;
assert_eq!(processed, 1);
assert_eq!(outbox_store.undelivered_count(), 0);
```

The in-memory store uses a `BTreeMap<i64, StoredEntry>` keyed by sequence number,
so polling always returns entries in the correct order. Useful for testing the
outbox processor logic without any infrastructure dependencies.

---

## Failure modes and recovery

### Process crash after commit, before Kafka publish

The events are in the outbox table with `delivered_at IS NULL`. On restart, the
processor resumes from where it left off. No events are lost.

### Process crash after Kafka publish, before marking delivered

The event was published to Kafka but `delivered_at` was not set. On restart, the
processor picks up the same entry and publishes it again. This produces a duplicate
in Kafka. All downstream consumers are idempotent, so the duplicate is harmless.

### Kafka is temporarily unavailable

`drain_once` returns a publish error. The `on_error` callback logs the failure.
The processor sleeps for `poll_interval_ms` and retries. Events accumulate in the
outbox table until Kafka recovers. No data loss.

### YugabyteDB is temporarily unavailable

The processor cannot poll or mark entries delivered. It retries after
`poll_interval_ms`. The outbound Kafka queue does not receive events during the
outage, but no data is lost because events remain in the outbox table.

### Multiple processor replicas

`FOR UPDATE SKIP LOCKED` ensures each replica picks up different rows. No
coordination is needed. However, ordering guarantees across replicas are weaker --
events from the same aggregate may be processed by different replicas in
non-deterministic order. Canon mitigates this by partitioning Kafka topics by
`aggregate_id`, so events for the same aggregate always land on the same partition.

### Transaction rollback

If the command handler transaction rolls back, no outbox entry is created. The
sequence number that was acquired (via `nextval`) is lost, creating a gap.
The outbox processor does not depend on contiguous sequence numbers and handles
gaps correctly.

---

## Why not Change Data Capture?

Change Data Capture (CDC) is an alternative to the outbox pattern where the database's
write-ahead log is tailed to detect new rows. Canon uses the outbox pattern instead
because:

1. **Explicit control** -- the application decides exactly what to publish. CDC
   publishes raw row changes, which may not map cleanly to domain events.
2. **Ordering guarantees** -- sequence numbers provide strict, application-controlled
   ordering. CDC ordering depends on the database's WAL format and replication lag.
3. **Simplicity** -- no CDC infrastructure (Debezium, Kafka Connect) to configure
   and maintain.
4. **Portability** -- works with any SQL database that supports ACID transactions.
   CDC is database-specific.
5. **Testability** -- the in-memory outbox implementation is trivial. CDC requires
   a running database with WAL access.

---

## Summary

The outbox pattern ensures exactly-once semantics (at the application level) for the
command handler write path:

1. The dispatcher executes the command handler.
2. The resulting event and the inbox acknowledgement are written in a single ACID
   transaction to the outbox table.
3. The outbox processor drains undelivered entries in sequence order using
   `FOR UPDATE SKIP LOCKED`.
4. Each entry is published to the outbound Kafka queue.
5. After confirmed publish, the entry is marked delivered.
6. Four independent consumer groups process the event: event store (Cassandra),
   projections (YugabyteDB), internal event consumer (inbox for event handler
   dispatch), and publisher (cross-service Kafka topic).
7. All consumers are idempotent, so duplicate publishes caused by crash recovery
   are handled safely.

The outbox table is the single source of truth. Once an event is committed there,
it will eventually reach all consumers. The processor can crash, Kafka can be
temporarily unavailable, and the system will recover without data loss.
