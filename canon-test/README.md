# canon-test

Integration test crate for the Canon event sourcing framework. Wires all in-memory implementations from `canon-core` into a `TestHarness` and exercises the framework with zero external infrastructure.

## Modules

| Module | Contents |
|--------|----------|
| `harness` | `TestHarness`, `TestHarnessBuilder` |
| `domain` | `OrderAggregate` test domain -- `OrderState`, `OrderCommand`, `OrderEvent`, helpers |

## TestHarness

`TestHarness` provides direct field access to all in-memory stores for asserting state in tests:

```rust
let harness = TestHarness::new();
// or via builder
let harness = TestHarness::builder().for_aggregate::<MyAggregate>().build();

// access stores directly
harness.event_store.append(&id, version, events).unwrap();
harness.command_store.append(cmd).unwrap();

// counterfactual replay backed by harness's command store
let replay = harness.counterfactual_replay();
```

## Test coverage

| Test module | What it covers |
|-------------|----------------|
| `aggregate_hydration` | Hydration from events, hydration from snapshot + events |
| `command_handling` | Valid/invalid command handling, version increments |
| `counterfactual` | Same-payload unchanged diff, different-payload diff |
| `dead_letter` | Dead lettering after max retries |
| `event_handlers` | Handler produces command, produces nothing, fan-out to multiple handlers |
| `event_store` | Append/load, optimistic concurrency conflict, load from version |
| `idempotency` | Duplicate command/event/window deduplication |
| `inbox_window_expiry` | Window expiry to dead letter |
| `outbound_fan_out` | All three consumers receive events |
| `oversight` | Ready dispatch, NotReady accumulation, Discard |
| `projection_rebuild` | Rebuilding flag, offset reset |
| `projections` | Apply, idempotent apply |
| `snapshotting` | Snapshot written every N events |
| `versioning` | Version-matched routing for combiners and command handlers |
