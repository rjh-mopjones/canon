# canon-test

Integration test crate for the Canon event sourcing framework.

**Tier 1** (in-memory): Wires all in-memory implementations from `canon-core` into a `TestHarness` and exercises the framework with zero external infrastructure.

**Tier 2** (testcontainers): Exercises the full pipeline against real YugabyteDB (Postgres), Cassandra (ScyllaDB), and Kafka instances managed by testcontainers. No `#[ignore]` -- containers are started and torn down automatically.

## Modules

| Module | Contents |
|--------|----------|
| `harness` | `TestHarness`, `TestHarnessBuilder` |
| `domain` | `OrderAggregate` test domain -- `OrderState`, `OrderCommand`, `OrderEvent`, helpers |

## TestHarness

`TestHarness` provides direct field access to all in-memory stores for asserting state in tests, plus convenience methods for common patterns:

```rust
let harness = TestHarness::new();
// or via builder
let harness = TestHarness::builder().for_aggregate::<MyAggregate>().build();

// access stores directly
harness.event_store.append(&id, version, events).unwrap();
harness.command_store.append(cmd).unwrap();

// convenience: submit command, append events, assert state
let cmd_id = harness.submit_command(&id, "PlaceOrder", b"payload");
harness.append_events(&id, Version::initial(), events);
harness.assert_event_count(&id, 2);
harness.assert_projection_at("my-projection", 5);
harness.assert_dead_letter_count(Some("handler"), 0);

// outbound queue / publisher
harness.publish_to_outbound(event);
harness.publish_external(event, "canon.test.events");
let published = harness.published_events();

// snapshot
harness.save_snapshot(snapshot);
let snap = harness.load_snapshot(&id);

// counterfactual replay backed by harness's command store
let replay = harness.counterfactual_replay();
```

## Test coverage

| Test module | What it covers |
|-------------|----------------|
| `aggregate_hydration` | Hydration from events, hydration from snapshot + events |
| `command_handling` | Valid/invalid command handling, version increments |
| `counterfactual` | Same-payload unchanged diff, different-payload diff, branch beyond end, mid-history substitution, original commands preserved |
| `dead_letter` | Dead lettering after max retries, requeue, discard, handler isolation, retry tracking |
| `event_handlers` | Handler produces command, produces nothing, fan-out to multiple handlers |
| `event_store` | Append/load, optimistic concurrency conflict, load from version |
| `idempotency` | Duplicate command/event/external-event/window deduplication, event store version conflict idempotency, projection idempotent apply |
| `inbox_window_expiry` | Window expiry to dead letter, multiple messages expired, handler isolation |
| `outbound_fan_out` | All three consumers receive events, independent consumption, late consumer, multi-event fan-out, outbox-to-publisher pipeline |
| `oversight` | Ready dispatch, NotReady accumulation, Discard, transition to Ready, cargo-style arrival+manifest oversight, decommission discard, unregistered handler error |
| `projection_rebuild` | Rebuilding flag, offset reset, read-through fallback, idempotent rebuild |
| `projections` | Apply, idempotent apply |
| `snapshotting` | Snapshot every N events, no snapshot below threshold, multiple thresholds, hydrate from snapshot skipping earlier events |
| `versioning` | Version-matched routing for combiners and command handlers |

### Tier 2 — Testcontainers e2e (`tests/tier2_e2e.rs`)

Requires Docker. Containers are shared across tests in the module via `OnceLock`.

| Test | What it covers |
|------|----------------|
| `tier2_command_to_cassandra_event_store` | Event reaches real Cassandra with correct schema via `EventStoreConsumer` |
| `tier2_command_to_yugabyte_projection` | Projection checkpoint written to real YugabyteDB |
| `tier2_command_to_kafka_publish_consume` | Event round-trips through real Kafka topic |
| `tier2_cross_service_cascade` | ShipDeparted published by fleet, consumed by navigation consumer group |
| `tier2_snapshotting` | 50 events trigger snapshot in real YugabyteDB snapshots table |
| `tier2_idempotent_replay` | Duplicate event rejected by Cassandra `IF NOT EXISTS` |
| `tier2_dead_letter` | Failed command persisted in real `dead_letters` table, requeue removes it |
| `tier2_outbox_ordering` | Sequence ordering preserved through real Kafka for same partition key |
| `tier2_concurrent_dispatchers` | `FOR UPDATE SKIP LOCKED` prevents double-processing on real Postgres |
| `tier2_projection_rebuild` | Reset checkpoint, set rebuilding flag, replay, and clear flag on real YugabyteDB |
| `tier2_command_store_roundtrip` | Command store append/load on real YugabyteDB |
| `tier2_command_store_idempotent` | `ON CONFLICT DO NOTHING` idempotency on real YugabyteDB |
| `tier2_retry_tracker` | Retry count increment/get/remove on real YugabyteDB |
| `tier2_snapshot_store_roundtrip` | Snapshot save/load on real YugabyteDB |
