# Testing

Canon provides a layered testing strategy: fast in-memory tests for logic, testcontainers
for infrastructure integration, and Playwright for end-to-end browser tests.

## Testing tiers

| Tier | What | Speed | Infrastructure |
|------|------|-------|----------------|
| 1. In-memory | Full pipeline logic | Sub-second | None |
| 2. Testcontainers | Real databases + Kafka | Seconds | Docker |
| 3. Playwright | Browser UI + pipeline | Minutes | Full cluster |

## Tier 1: In-memory tests (canon-test)

The `canon-test` crate provides a `TestHarness` that wires all in-memory implementations
into a real `Service` via `ServiceBuilder`. Tests exercise the full pipeline logic with
zero external infrastructure.

### Setting up the harness

```rust
use canon_test::TestHarness;

#[tokio::test]
async fn test_command_produces_event() {
    let harness = TestHarness::new()
        .for_aggregate::<Ship>()
        .build();

    // Submit a command
    let ship_id = AggregateId::new();
    harness.submit_command(RegisterShip {
        name: "USS Test".into(),
        capacity_kg: 5000,
    }, ship_id).await.unwrap();

    // Step through the pipeline
    harness.process_outbox().await;
    harness.process_event_store_consumer().await;

    // Assert the event was stored
    let events = harness.event_store().load(ship_id).await.unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].event_type, "ShipRegistered");
}
```

### What the harness tests

- Command handling and event production
- Outbox draining to outbound queue
- Event store consumer writing to Cassandra (in-memory)
- Projection consumer updating read models
- Publisher consumer publishing cross-service events
- Snapshotting (version % N)
- Oversight gates (Ready/NotReady/Discard)
- Inbox deduplication
- Window expiry and dead lettering
- Counterfactual replay
- Event handler fan-out

### In-memory implementations

Every trait in Canon has an in-memory implementation in `canon-core/src/memory/`:

| Trait | In-memory implementation |
|-------|-------------------------|
| `EventStore` | `InMemoryEventStore` |
| `CommandStore` | `InMemoryCommandStore` |
| `SnapshotStore` | `InMemorySnapshotStore` |
| `Inbox` | `InMemoryInbox` |
| `InboundQueue` | `InMemoryInboundQueue` |
| `OutboundQueue` | `InMemoryOutboundQueue` |
| `ProjectionStore` | `InMemoryProjectionStore` |
| `EventPublisher` | `InMemoryPublisher` |
| `EventAdaptor` | `InMemoryAdaptor` |
| `DeadLetterStore` | `InMemoryDeadLetterStore` |

These are the test harness -- never use them in production service wiring.

## Tier 2: Testcontainers

For testing real database queries, Kafka protocol, and serialisation, use the
`testcontainers` crate to spin up real infrastructure per test module:

```rust
use testcontainers::clients::Cli;
use testcontainers_modules::kafka::Kafka;
use testcontainers_modules::postgres::Postgres;

#[tokio::test]
async fn test_yugabyte_command_store() {
    let docker = Cli::default();
    let postgres = docker.run(Postgres::default());
    let port = postgres.get_host_port_ipv4(5432);

    let pool = create_pool(&format!(
        "postgres://postgres:postgres@localhost:{}/postgres", port
    )).await;

    // Run schema migration
    run_init_schema(&pool).await;

    // Test real SQL queries
    let store = YugabyteCommandStore::new(pool);
    let cmd = test_command_envelope();
    store.save(&cmd).await.unwrap();

    let loaded = store.load(cmd.command_id).await.unwrap();
    assert_eq!(loaded.command_id, cmd.command_id);
}
```

### What testcontainers catch

- SQL query correctness and schema compatibility
- CQL (Cassandra) serialisation issues
- Kafka record format and protocol handling
- Connection handling and pool behaviour
- Edge cases in real database behaviour

### Rules

- Never use `#[ignore]` for infrastructure tests -- use testcontainers instead
- Containers are managed automatically per test module
- Each test gets isolated infrastructure -- no cross-test contamination
- Tests run on every `cargo test` invocation

## Tier 3: Playwright e2e

Browser-based smoke tests verify the complete user experience against a running cluster.

### Setup

```bash
cd canon-demo
make k8s-up          # deploy the full stack
make k8s-test-e2e    # run Playwright tests
```

### What Playwright tests verify

- Stations have stock and display correctly
- Stock drains over time (real pipeline events)
- Ship popup works with version/snapshot data
- Events appear in the event log
- Scenario missions render and complete
- WebSocket connection delivers live updates

### DOM stability rules for Leptos

Leptos re-renders can detach elements, so:

- Never use `page.$()` + `handle.click()` -- the handle goes stale
- Use `page.locator(selector).click()` instead
- Use CSS class selectors (`.dest-tab`) not text matchers
- Wrap flight clicks in try/catch for stress tests

### Running against live site

```bash
CANON_AUTH_PASSWORD=$(cat ~/.canon-auth-password) npx playwright test
```

## Best practices

1. **Test logic in Tier 1** -- in-memory tests are fast and catch most bugs
2. **Test infrastructure in Tier 2** -- testcontainers catch serialisation and query bugs
3. **Test UX in Tier 3** -- Playwright catches integration issues between frontend and backend
4. **Never `#[ignore]`** -- if a test needs infrastructure, use testcontainers
5. **Idempotency tests are critical** -- test that applying events twice produces the same state
6. **Test cross-service flows** -- use the harness to simulate multi-service event chains
