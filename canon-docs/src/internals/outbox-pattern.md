# Outbox Pattern

The outbox pattern is Canon's solution to the dual-write problem. It guarantees that
events are never lost between the command handler and downstream consumers.

## The dual-write problem

In a naive implementation, handling a command requires two writes:

1. Write the command to the database
2. Publish the event to Kafka

If the process crashes between steps 1 and 2, the event is lost. If you reverse the
order, the command is lost. There is no way to make two independent writes atomic
without a distributed transaction.

## Canon's solution

Canon stages events in an **outbox table** within the same YugabyteDB ACID transaction
as the command write:

```sql
BEGIN
  INSERT INTO commands (command_id, aggregate_id, payload, ...)
    VALUES ($1, $2, $3, ...);

  INSERT INTO outbox (id, sequence_number, aggregate_id, event_type, payload, ...)
    VALUES ($1, nextval('outbox_seq'), $2, $3, $4, ...);
  -- (repeated for each event produced by the command)
COMMIT
```

Either both the command and all events are persisted, or neither is. The outbox is the
**commit point** -- once the transaction commits, the events are guaranteed to be
delivered.

## Outbox processor

A dedicated background task (spawned by `ServiceBuilder`) has a single responsibility:
drain the outbox table and publish events to the outbound Kafka queue.

### Polling strategy

```sql
SELECT *
FROM outbox
WHERE delivered_at IS NULL
ORDER BY sequence_number
FOR UPDATE SKIP LOCKED
```

- `WHERE delivered_at IS NULL` -- only undelivered events
- `ORDER BY sequence_number` -- preserves ordering
- `FOR UPDATE SKIP LOCKED` -- prevents double-processing across replicas

### Delivery confirmation

After confirmed Kafka publish, the processor sets `delivered_at`:

```sql
UPDATE outbox
SET delivered_at = NOW()
WHERE id = $1
```

### What the outbox processor does NOT do

The outbox processor has a single responsibility. It does NOT:

- Write to Cassandra (that is the event store consumer's job)
- Update projections (that is the projection consumer's job)
- Publish to external Kafka topics (that is the publisher's job)
- Handle dead letters or retries

It only moves events from the outbox table to the outbound Kafka queue.

## Backpressure

The outbox processor uses a bounded channel (default capacity: 1024) to control flow.
If the outbound queue is slow, the channel fills up, and the processor slows its polling
rate. This prevents unbounded memory growth.

## Sequence ordering

The outbox uses a PostgreSQL sequence (`outbox_seq`) to assign monotonically increasing
sequence numbers. This guarantees:

- Events are drained in the order they were committed
- Multiple concurrent transactions produce interleaved but ordered sequence numbers
- The outbox processor processes events in strict sequence order

## Recovery

If the outbox processor crashes:

1. Events with `delivered_at IS NULL` remain in the outbox
2. On restart, the processor resumes from where it left off
3. No events are lost (the outbox is durable)
4. No events are double-published (Kafka dedup + downstream idempotency)

The `FOR UPDATE SKIP LOCKED` clause also handles the case of multiple replicas running
outbox processors simultaneously -- each processor picks up different rows.

## Outbox table schema

```sql
CREATE TABLE canon_fleet.outbox (
    id UUID PRIMARY KEY,
    sequence_number BIGINT NOT NULL DEFAULT nextval('canon_fleet.outbox_seq'),
    aggregate_id UUID NOT NULL,
    event_type TEXT NOT NULL,
    event_version INTEGER NOT NULL,
    payload BYTEA NOT NULL,
    correlation_id UUID NOT NULL,
    causation_id UUID NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    delivered_at TIMESTAMP WITH TIME ZONE
);

CREATE SEQUENCE canon_fleet.outbox_seq;

CREATE INDEX idx_outbox_undelivered
    ON canon_fleet.outbox (sequence_number)
    WHERE delivered_at IS NULL;
```

The partial index on `sequence_number WHERE delivered_at IS NULL` ensures the polling
query is efficient even as the outbox grows -- it only scans undelivered rows.

## Why not CDC?

Change Data Capture (CDC) is an alternative to the outbox pattern. Canon uses the
outbox pattern because:

1. **Explicit control** -- the application decides exactly what to publish
2. **Ordering guarantees** -- sequence numbers provide strict ordering
3. **Simplicity** -- no CDC infrastructure to configure and maintain
4. **Portability** -- works with any SQL database that supports transactions
5. **Testability** -- the in-memory outbox implementation is trivial
