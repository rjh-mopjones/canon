# Testing

Canon has a layered testing strategy with three tiers. Every tier runs without
`#[ignore]` markers. Tests that need infrastructure use testcontainers to spin up
real databases and Kafka brokers automatically -- no Docker Compose, no manual setup.

## Testing philosophy

The Canon testing strategy rests on two principles:

1. **Test logic fast, test infrastructure thoroughly.** In-memory tests verify the
   pipeline wiring, event routing, snapshotting, oversight, idempotency, and dead
   lettering in sub-second time. Testcontainers tests verify SQL, CQL, and Kafka
   protocol correctness against real databases.

2. **Never `#[ignore]`.** Ignored tests rot. They are never run and silently break.
   If a test needs a database, it gets a testcontainer. If a test needs a full
   cluster, it runs as Playwright against minikube. Both run on every `cargo test`
   or `make k8s-test-e2e` invocation respectively.

The three tiers complement each other:

| Tier | What it tests | Speed | Infrastructure |
|------|---------------|-------|----------------|
| 1. In-memory | Full pipeline logic, wiring, idempotency | Sub-second | None |
| 2. Testcontainers | Real SQL, CQL, Kafka protocol | 5--30 seconds | Docker (automatic) |
| 3. Playwright | Browser UI + full pipeline end-to-end | 1--3 minutes | Full K8s cluster |

---

## Tier 1: In-memory e2e tests (canon-test)

The `canon-test` crate wires all `InMemory*` stores into a real pipeline via the
`TestHarness`. Tests exercise the full command-to-projection flow with zero external
infrastructure.

### The TestHarness

The `TestHarness` struct is the foundation of all Tier 1 tests. It instantiates every
in-memory store and exposes them as public fields for direct assertion:

```rust
pub struct TestHarness {
    pub event_store: InMemoryEventStore,
    pub command_store: InMemoryCommandStore,
    pub snapshot_store: InMemorySnapshotStore,
    pub inbox: InMemoryInbox,
    pub inbound_queue: InMemoryInboundQueue,
    pub outbound_queue: InMemoryOutboundQueue,
    pub projection_store: InMemoryProjectionStore,
    pub publisher: InMemoryPublisher,
    pub adaptor: InMemoryAdaptor,
    pub dead_letter_store: InMemoryDeadLetterStore,
    pub replay_event_store: InMemoryReplayEventStore,
}
```

Creating a harness is one line:

```rust
let harness = TestHarness::new();
```

The harness also provides convenience methods that panic on failure (acceptable in
tests) to reduce boilerplate:

```rust
// Append events
harness.append_events(&id, Version::initial(), events);

// Load and assert
harness.assert_event_count(&id, 3);
harness.assert_projection_at("ship-count", 5);
harness.assert_dead_letter_count(Some("handler_a"), 1);

// Snapshots
harness.save_snapshot(snapshot);
let snap = harness.load_snapshot(&id);

// Outbound queue and publisher
harness.publish_to_outbound(event);
harness.publish_external(event, "canon.fleet.events");
let published = harness.published_events();

// Dead letters
let letters = harness.dead_letters(Some("handler_id"));
```

### The TestHarnessBuilder

For tests that need compile-time validation of aggregate registrations, use the builder:

```rust
let harness = TestHarness::builder()
    .for_aggregate::<Ship>()
    .build();
```

The builder uses `ServiceBuilder::validate_registrations` to check that every
`#[command]` has a matching `#[command_handler]` and every `#[event]` has a matching
`#[event_combiner]` for the registered aggregate. It panics at build time if any
are missing -- catching wiring errors before the test runs.

### The test domain

The `canon-test` crate includes a complete test domain in `src/domain.rs` that
exercises all Canon macro features:

```rust
#[aggregate]
pub struct OrderAggregate {
    pub placed: bool,
    pub cancelled: bool,
    pub priority: Option<u8>,
}

#[command(OrderAggregate, version = 1, produces = [OrderPlaced])]
pub struct PlaceOrder { pub order_id: Uuid }

#[event(OrderAggregate, version = 1)]
pub struct OrderPlaced { pub order_id: Uuid }

#[event_combiner(OrderAggregate, version = 1)]
impl OrderPlaced {
    fn combine(&self, state: &mut OrderAggregate) {
        state.placed = true;
    }
}

#[command_handler(OrderAggregate, version = 1)]
impl PlaceOrderHandler {
    type Error = OrderError;
    fn handle(&self, state: &OrderAggregate, cmd: PlaceOrder)
        -> Result<OrderPlaced, OrderError> {
        if state.placed { return Err(OrderError::AlreadyPlaced); }
        Ok(OrderPlaced { order_id: cmd.order_id })
    }
}
```

The domain includes v2 types (`PlaceOrderV2`, `OrderPlacedV2`) for versioning tests,
event handlers (`ProducingHandler`, `SilentHandler`), and a projection (`OrderProjection`).
Helper functions create properly structured `EventEnvelope` and `CommandEnvelope` values:

```rust
use canon_test::domain::*;

let envelope = make_placed_envelope(&id, order_id);
let cancelled = make_cancelled_envelope(&id, "changed mind");
let v2 = make_placed_v2_envelope(&id, order_id, 5);
let cmd = make_command_envelope(&id, b"payload");
```

### The PipelineFixture

For full end-to-end pipeline tests, `e2e_pipeline.rs` defines a `PipelineFixture` that
wires the complete command-to-event-store-to-projection-to-publisher flow:

```rust
struct PipelineFixture {
    dispatcher: Dispatcher<InMemoryDispatcherStore>,
    dispatcher_store: InMemoryDispatcherStore,
    outbox_processor: OutboxProcessor<InMemoryOutboxStore, InMemoryOutboxPublisher>,
    outbound_queue: InMemoryOutboundQueue,
    event_store_consumer: EventStoreConsumer<...>,
    projection_consumer: ProjectionConsumer<InMemoryProjectionStore>,
    publisher_consumer: PublisherConsumer<InMemoryPublisher>,
    // Shared stores for assertions
    event_store: InMemoryEventStore,
    snapshot_store: InMemorySnapshotStore,
    dead_letter_store: InMemoryDeadLetterStore,
    publisher: InMemoryPublisher,
    projection_store: InMemoryProjectionStore,
    // Consumer handles
    es_consumer_handle: ConsumerHandle,
    proj_consumer_handle: ConsumerHandle,
    pub_consumer_handle: ConsumerHandle,
}
```

The fixture's `run_pipeline_for_command` method steps through every stage:

1. Enqueue command into the dispatcher inbox
2. Dispatcher processes: hydrate state, call handler, write outbox
3. Outbox processor drains outbox to outbound queue
4. Event store consumer writes to event store (+ snapshot if version % N == 0)
5. Projection consumer applies to read models
6. Publisher consumer publishes to external topic

This is the same flow as production, but stepped manually for determinism.

### In-memory implementations

Every trait in Canon has a corresponding in-memory implementation in
`canon-core/src/memory/`. These implementations are the test harness -- they must
never be used in production service wiring (that is what the YugabyteDB, Cassandra,
and Kafka implementations are for).

| Trait | In-memory impl | Storage |
|-------|----------------|---------|
| `EventStore` | `InMemoryEventStore` | `HashMap<AggregateId, Vec<EventEnvelope>>` |
| `CommandStore` | `InMemoryCommandStore` | `VecDeque<CommandEnvelope>` |
| `SnapshotStore` | `InMemorySnapshotStore` | `HashMap<AggregateId, Snapshot>` |
| `Inbox` | `InMemoryInbox` | `HashMap` of dedup set + windows + oversight fns |
| `InboundQueue` | `InMemoryInboundQueue` | `VecDeque<Vec<IncomingMessage>>` |
| `OutboundQueue` | `InMemoryOutboundQueue` | Per-consumer `VecDeque<EventEnvelope>` |
| `ProjectionStore` | `InMemoryProjectionStore` | `HashMap<String, Version>` checkpoints |
| `Publisher` | `InMemoryPublisher` | `Vec<(EventEnvelope, String)>` |
| `Adaptor` | `InMemoryAdaptor` | `VecDeque<EventEnvelope>` |
| `DeadLetterStore` | `InMemoryDeadLetterStore` | `Vec<InMemoryDeadLetter>` |
| `RetryTracker` | `InMemoryRetryTracker` | `HashMap<Uuid, RetryAttempt>` |
| `RetryPolicy` | `RetryPolicy` | Coordinates tracker + dead letters |

Each implementation is wrapped in `Arc<Mutex<...>>` for safe concurrent access across
`tokio::spawn` boundaries. They implement both synchronous (direct method) and async
(trait) interfaces. The synchronous methods are used in tests for convenience; the async
trait methods are used by the framework's generic consumers and dispatcher.

The `InMemoryEventStore` enforces optimistic concurrency, just like the real Cassandra
implementation:

```rust
pub fn append(
    &self,
    aggregate_id: &AggregateId,
    expected_version: Version,
    mut events: Vec<EventEnvelope>,
) -> Result<(), EventStoreError> {
    let mut store = self.inner.lock().map_err(|_| EventStoreError::Poisoned)?;
    let stored = store.entry(aggregate_id.clone()).or_default();
    let current = stored.last().map(|e| e.version)
        .unwrap_or_else(Version::initial);

    if current != expected_version {
        return Err(EventStoreError::VersionConflict {
            expected: expected_version,
            actual,
        });
    }
    // ... assign sequential versions, extend stored events
}
```

The `InMemoryInbox` implements deduplication via a `HashSet<(String, Uuid)>` keyed on
`(handler_id, message_id)`, window accumulation, oversight evaluation, and window
expiry with TTL -- faithfully reproducing the YugabyteDB inbox behaviour.

---

## Test modules explained

Each test module in `canon-test/tests/` targets a specific Canon feature. Here is what
each module tests and how to read it.

### snapshotting.rs

Tests the event store consumer's snapshot-on-threshold logic. Verifies that:

- A snapshot is written when `version % snapshot_every == 0`
- No snapshot is written below the threshold
- Multiple thresholds produce multiple snapshots (latest wins via upsert)
- Hydration from a snapshot skips earlier events and applies only remaining ones

```rust
#[tokio::test]
async fn test_snapshot_written_every_n_events() {
    let harness = TestHarness::new();
    let id = AggregateId::new();

    // Append 50 events
    let events: Vec<EventEnvelope> = (0..50)
        .map(|_| make_placed_envelope(&id, Uuid::new_v4()))
        .collect();
    harness.append_events(&id, Version::initial(), events);

    // Simulate event store consumer: snapshot at version 50
    let loaded = harness.load_events(&id);
    for event in &loaded {
        if event.version.as_u64() % 50 == 0 {
            harness.save_snapshot(Snapshot {
                aggregate_id: id.clone(),
                version: event.version,
                state: Bytes::from_static(b"snapshot_state"),
                taken_at: Utc::now(),
            });
        }
    }

    let snap = harness.load_snapshot(&id).unwrap();
    assert_eq!(snap.version.as_u64(), 50);
}
```

### oversight.rs

Tests the inbox oversight gate mechanism. Verifies:

- `NotReady` accumulates messages without dispatching
- `Discard` clears the window without dispatching
- `Ready` drains the window and enqueues a batch to the inbound queue
- Transitions from `NotReady` to `Ready` when enough messages accumulate
- Complex domain-specific oversight (e.g., discard on decommission, ready when arrival + manifest both present)
- Submitting to an unregistered handler returns an error

```rust
#[tokio::test]
async fn test_oversight_transitions_not_ready_to_ready() {
    let harness = TestHarness::new();
    let id = AggregateId::new();

    harness.inbox.register_handler("h1", |accumulated| {
        if accumulated.len() >= 3 { Oversight::Ready }
        else { Oversight::NotReady }
    }).unwrap();

    harness.inbox.submit("h1", make_command_msg(&id), &harness.inbound_queue).unwrap();
    assert!(harness.inbound_queue.receive().unwrap().is_none());

    harness.inbox.submit("h1", make_command_msg(&id), &harness.inbound_queue).unwrap();
    assert!(harness.inbound_queue.receive().unwrap().is_none());

    harness.inbox.submit("h1", make_command_msg(&id), &harness.inbound_queue).unwrap();
    let batch = harness.inbound_queue.receive().unwrap().unwrap();
    assert_eq!(batch.len(), 3);
}
```

### idempotency.rs

Tests all idempotency layers in the pipeline:

- **Inbox deduplication**: submitting the same `command_id` or `event_id` twice produces only one dispatch
- **Event store optimistic concurrency**: appending at the wrong expected version fails with `VersionConflict`
- **Projection idempotency**: applying the same event twice produces the same checkpoint
- **External event deduplication**: duplicate external events are deduped by the inbox

### counterfactual.rs

Tests the counterfactual replay engine. Verifies:

- Substituting a command with the same payload produces an unchanged diff
- Substituting with different payload produces added/removed entries in the diff
- Branching beyond the end of history appends the substitute as a new command
- Mid-history substitution preserves unchanged commands before and after the branch point
- Original commands are preserved in the result for auditing

### dead_letter.rs

Tests the dead letter lifecycle:

- Messages are dead-lettered after exhausting max retries
- Dead letters can be requeued (sets a flag)
- Dead letters can be discarded (permanently removed)
- Multiple handlers have isolated dead letter entries
- Retry tracking simulation (count increments until threshold)

### inbox_window_expiry.rs

Tests the window TTL and dead letter escalation:

- A window that never reaches `Ready` before its TTL is dead-lettered
- All messages in an expired window are dead-lettered
- Expired windows do not affect other handlers

### outbound_fan_out.rs

Tests the outbound queue's consumer group independence:

- All three consumer groups (event store, projection, publisher) receive each event independently
- One consumer's read position does not affect another
- Late-registered consumers do not see events published before registration
- Multiple events fan out correctly to all consumers

### projection_rebuild.rs

Tests the projection rebuild mechanism:

- The `rebuilding` flag causes read endpoints to fall back to the event store
- Offset reset and Kafka replay re-applies all events
- Read-through fallback reads directly from the event store
- Rebuild is idempotent (same result after double rebuild)

### versioning.rs

Tests version-matched routing:

- v1 and v2 event combiners dispatch correctly based on `event_version`
- v1 and v2 command handlers produce the correct event types
- `command_version` is persisted to the command store
- No upcasting during hydration -- raw payload bytes survive the round-trip

### event_handlers.rs

Tests event handler fan-out:

- A producing handler returns `Some(CommandEnvelope)`
- A silent handler returns `None`
- Multiple handlers can process the same events independently

### e2e_pipeline.rs

The most comprehensive Tier 1 test module. Uses the `PipelineFixture` to test:

- Command to event store (full pipeline)
- Command to projection (with counting projection)
- Command to publisher (correct topic)
- Snapshot trigger at configurable threshold
- Dead letter on command handler failure
- Idempotent event store (version conflict on duplicate)
- Outbox ordering (versions 1, 2, 3 in sequence)
- Full pipeline smoke test (all three consumers)
- Outbox to all consumer groups (fan-out)
- Dispatcher uses inventory-registered handlers

---

## How to write a new Tier 1 test

1. Create a new test file in `canon-test/tests/` or add to an existing one.
2. Import the harness and domain:

```rust
use canon_core::*;
use canon_test::harness::TestHarness;
use canon_test::domain::*;
```

3. Create the harness and exercise the component:

```rust
#[tokio::test]
async fn test_my_feature() {
    let harness = TestHarness::new();
    let id = AggregateId::new();

    // Set up state
    let events = vec![make_placed_envelope(&id, Uuid::new_v4())];
    harness.append_events(&id, Version::initial(), events);

    // Exercise the feature
    // ...

    // Assert
    harness.assert_event_count(&id, 1);
}
```

4. If testing a new aggregate, define it in `domain.rs` using Canon macros and add
   `for_aggregate::<MyAggregate>()` to a `TestHarnessBuilder`.

5. Run with `cargo test -p canon-test`.

---

## Tier 2: Testcontainers tests

Tier 2 tests exercise real database queries, Kafka protocol handling, and
serialisation against actual infrastructure. The `testcontainers` crate manages
container lifecycle automatically -- no manual Docker Compose.

### Container setup pattern

Each infrastructure crate and the central `tier2_e2e.rs` module share containers
via `OnceLock<tokio::sync::OnceCell<Container>>`. Containers start once per test
module and are reused across tests within that module:

```rust
static PG: OnceLock<tokio::sync::OnceCell<PgContainer>> = OnceLock::new();

async fn get_pg() -> &'static PgContainer {
    pg_cell().get_or_init(|| async {
        let container = Postgres::default()
            .start()
            .await
            .expect("failed to start Postgres container");

        let host_port = container.get_host_port_ipv4(5432).await.expect("port");
        let url = format!(
            "postgres://postgres:postgres@127.0.0.1:{host_port}/postgres"
        );

        // Run schema migration
        let pool = PgPool::connect(&url).await.expect("connect");
        run_init_schema(&pool).await;

        PgContainer { _container: container, url }
    }).await
}
```

The three container types used are:

- **Postgres** (simulating YugabyteDB -- wire-compatible) for command store, snapshot
  store, inbox, projection store, dead letter store, and retry tracker
- **ScyllaDB** (Cassandra-compatible) for the event store
- **Apache Kafka** for inbound/outbound queues, publisher, and adaptor

### What Tier 2 catches

- SQL query correctness and schema compatibility
- CQL (Cassandra) serialisation and wide-row behaviour
- Kafka record format, produce/fetch protocol, and offset tracking
- Connection pool behaviour under concurrent access
- UPSERT and ON CONFLICT semantics in real Postgres/YugabyteDB
- Edge cases in real database behaviour that in-memory cannot replicate

### Example: dead letter store with testcontainers

From `canon-deadletter-yugabyte/src/lib.rs`:

```rust
async fn setup_container() -> (ContainerAsync<Postgres>, PgPool) {
    let container = Postgres::default()
        .start()
        .await
        .expect("Failed to start postgres container");

    let port = container.get_host_port_ipv4(5432).await.expect("port");
    let url = format!("postgres://postgres:postgres@127.0.0.1:{}/postgres", port);
    let pool = PgPool::connect(&url).await.expect("connect");

    sqlx::query(
        "CREATE TABLE IF NOT EXISTS dead_letters (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            message_id UUID,
            handler_id TEXT,
            aggregate_id UUID,
            payload BYTEA,
            error TEXT,
            attempts INT DEFAULT 1,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            last_attempted TIMESTAMPTZ NOT NULL DEFAULT now()
        )",
    )
    .execute(&pool)
    .await
    .expect("create dead_letters table");

    (container, pool)
}

#[tokio::test(flavor = "multi_thread")]
async fn test_store_and_list() {
    let (_container, pool) = setup_container().await;
    let store = YugabyteDeadLetterStore::new(pool);
    let agg_id = AggregateId::new();
    let msg_id = Uuid::new_v4();

    let dl_id = store
        .store(msg_id, "handler-1", &agg_id, Bytes::from_static(b"{}"), "boom")
        .await
        .expect("store failed");

    let all = store.list(None).await.expect("list failed");
    assert_eq!(all.len(), 1);
    assert_eq!(all[0].id, dl_id);
    assert_eq!(all[0].handler_id, "handler-1");
}
```

### Example: retry tracker with testcontainers

From `canon-deadletter-yugabyte/src/retry_tracker.rs`:

```rust
#[tokio::test(flavor = "multi_thread")]
async fn increment_accumulates() {
    let (_container, pool) = setup_container().await;
    let tracker = YugabyteRetryTracker::new(pool);
    let msg_id = Uuid::new_v4();

    assert_eq!(tracker.increment(msg_id, "h1").unwrap(), 1);
    assert_eq!(tracker.increment(msg_id, "h1").unwrap(), 2);
    assert_eq!(tracker.increment(msg_id, "h1").unwrap(), 3);

    let attempt = tracker.get(msg_id).unwrap().unwrap();
    assert_eq!(attempt.attempts, 3);
}
```

### How to write a new Tier 2 test

1. Add the appropriate testcontainers dependency to `[dev-dependencies]`:

```toml
[dev-dependencies]
testcontainers = "0.27"
testcontainers-modules = { version = "0.15", features = ["postgres"] }
```

2. Write a `setup_container` function that starts the container and runs schema:

```rust
async fn setup_container() -> (ContainerAsync<Postgres>, PgPool) {
    let container = Postgres::default().start().await.expect("start");
    let port = container.get_host_port_ipv4(5432).await.expect("port");
    let pool = PgPool::connect(&format!(
        "postgres://postgres:postgres@127.0.0.1:{port}/postgres"
    )).await.expect("connect");

    // Run your CREATE TABLE statements
    sqlx::query("CREATE TABLE IF NOT EXISTS ...").execute(&pool).await.unwrap();

    (container, pool)
}
```

3. Use `#[tokio::test(flavor = "multi_thread")]` -- testcontainers requires a multi-
   thread runtime.

4. Each test gets a fresh aggregate ID to avoid cross-test interference. The container
   is shared, but data is isolated by UUID.

5. Never use `#[ignore]`. The test runs on every `cargo test` invocation.

---

## Tier 3: Playwright e2e tests

Browser-based smoke tests verify the complete user experience against a running cluster.
These tests run against minikube locally or against the live GKE deployment.

### Setup

```bash
cd canon-demo
make k8s-up          # deploy the full stack to minikube
make k8s-test-e2e    # run Playwright tests
```

Or against the live site:

```bash
CANON_AUTH_PASSWORD=$(cat ~/.canon-auth-password) npx playwright test
```

### What Playwright tests verify

- Stations have stock and display correctly
- Stock drains over time (driven by real pipeline events, not local timers)
- Ship popup works with version/snapshot data
- Events appear in the event log
- Scenario missions render and complete
- WebSocket connection delivers live updates
- The full bootstrap sequence completes (stations registered, stock seeded, ship created)

### DOM stability rules for Leptos

Leptos re-renders can detach DOM elements between signal updates. Playwright tests
must account for this:

- **Never use `page.$()` + `handle.click()`** -- the element handle goes stale.
  Use `page.locator(selector).click()` or `page.click(selector)` instead.
- **Never use `page.$()` + `handle.evaluate()`** for checking disabled state. Use
  `page.locator(selector + ':not([disabled])').count()` instead.
- **Always use `.dest-tab` class** for destination buttons (not bare `:has-text("Alpha")`)
  to avoid matching multiple elements.
- **Wrap flight clicks in try/catch** in stress tests so failures report instead of crashing.

### The trace-flight diagnostic script

`canon-demo/e2e/trace-flight.js` is a Playwright-based diagnostic tool that traces
a single flight lifecycle. It is not a pass/fail test but a debugging instrument
that logs every state transition:

```bash
node canon-demo/e2e/trace-flight.js
```

It reports session readiness timing, button state, flight status transitions
(EN_ROUTE, PENDING, DOCKED), and game state via the API.

---

## Test coverage by Canon feature

| Feature | Tier 1 module | Tier 2 | Tier 3 |
|---------|---------------|--------|--------|
| Command handling | `command_handling.rs`, `e2e_pipeline.rs` | `tier2_e2e.rs` | Implicit |
| Event store | `event_store.rs`, `e2e_pipeline.rs` | Cassandra tests | Implicit |
| Snapshotting | `snapshotting.rs`, `e2e_pipeline.rs` | Snapshot store tests | Scenario 02 |
| Oversight gates | `oversight.rs` | Inbox YugabyteDB tests | Scenario 01 |
| Idempotency | `idempotency.rs`, `e2e_pipeline.rs` | All store tests | Scenario 05 |
| Dead lettering | `dead_letter.rs`, `e2e_pipeline.rs` | Dead letter YugabyteDB tests | Scenario 04 |
| Counterfactual replay | `counterfactual.rs` | -- | -- |
| Projection rebuild | `projection_rebuild.rs` | Projection store tests | -- |
| Inbox window expiry | `inbox_window_expiry.rs` | Inbox YugabyteDB tests | -- |
| Outbound fan-out | `outbound_fan_out.rs`, `e2e_pipeline.rs` | Kafka tests | -- |
| Event handlers | `event_handlers.rs` | -- | -- |
| Version routing | `versioning.rs` | -- | -- |
| Cross-service flow | `resupply_chain.rs` | `tier2_e2e.rs` | Scenario 03 |
| Aggregate hydration | `aggregate_hydration.rs` | -- | -- |
| Service startup | `e2e_service_start.rs` | `tier2_e2e.rs` | Full cluster |

---

## Running tests

### All Tier 1 + Tier 2 tests

```bash
cargo test --workspace
```

This runs all in-memory tests instantly and spins up testcontainers for Tier 2
tests. Docker must be available for Tier 2.

### Specific test module

```bash
cargo test -p canon-test --test snapshotting
cargo test -p canon-test --test e2e_pipeline
cargo test -p canon-test --test tier2_e2e
```

### Tier 1 only (no Docker)

```bash
cargo test -p canon-test --test snapshotting --test oversight --test idempotency \
    --test dead_letter --test counterfactual --test outbound_fan_out \
    --test projection_rebuild --test inbox_window_expiry --test versioning \
    --test event_handlers --test command_handling --test aggregate_hydration \
    --test projections --test e2e_pipeline
```

### Tier 3 (Playwright)

```bash
cd canon-demo && make k8s-test-e2e
```

### With nextest (parallel, faster)

```bash
cargo nextest run --workspace
```

---

## Best practices

1. **Test logic in Tier 1.** In-memory tests are fast and catch most bugs. Start here.
2. **Test infrastructure in Tier 2.** When your SQL query or CQL statement is non-trivial,
   write a testcontainer test.
3. **Test UX in Tier 3.** Playwright catches integration issues between frontend and backend
   that unit tests cannot.
4. **Never `#[ignore]`.** If a test needs infrastructure, use testcontainers.
5. **Idempotency tests are critical.** Every handler, projection, and consumer must be
   safe to call twice. Test it.
6. **Test cross-service flows.** Use the harness to simulate multi-service event chains.
7. **Use unique aggregate IDs per test.** Avoid cross-test interference. `AggregateId::new()`
   generates a fresh UUID each time.
8. **Prefer `assert_eq!` with descriptive messages.** The harness convenience methods include
   messages automatically, but custom assertions should too.
