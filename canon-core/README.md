# canon-core

Foundation crate for the Canon event sourcing framework. Contains all domain traits, core types, proc-macros, in-memory implementations, and outbound queue consumers.

## Modules

| Module | Contents |
|--------|----------|
| `types` | `AggregateId`, `Version`, `EventEnvelope`, `CommandEnvelope`, `IncomingMessage`, `Oversight`, counterfactual types |
| `traits` | `Aggregate`, `CommandHandler`, `CommandStore`, `EventStore`, `SnapshotStore`, `DeadLetterStore`, `RetryTracker`, `Publisher`, `EventHandler`, `EventCombiner`, `Projection`, `ProjectionStore`, `ProjectionCheckpointStore`, `ProjectionHandler`, `ProjectionRebuildManager`, `CounterfactualReplay` |
| `error` | `EventStoreError`, `InboxError`, `DeadLetterError`, `MacroError`, `RetryError` |
| `memory` | In-memory implementations of every trait (see below) |
| `consumers` | Outbound queue consumers: `EventStoreConsumer`, `ProjectionConsumer`, `PublisherConsumer` |
| `registration` | `inventory`-based auto-registration types for macro-generated impls |

## Traits

Core traits that define the framework's contracts. Users never implement these directly -- proc-macros generate all impls.

- **`Aggregate`** -- state hydration via version-matched event combiners
- **`CommandHandler`** -- one per command type per version, returns a single event
- **`CommandStore`** -- async `append` and `load_range` for command persistence
- **`EventStore`** -- async event storage with optimistic concurrency
- **`SnapshotStore`** -- async snapshot persistence and retrieval
- **`DeadLetterStore`** -- async dead letter storage for failed messages
- **`RetryTracker`** -- crash-safe retry count tracking per message
- **`Publisher`** -- async event publishing to external topics
- **`EventHandler`** -- aggregate-agnostic, optional oversight and `correlate`, produces at most one command. `correlate` extracts a domain correlation key from an incoming message to determine window grouping; when omitted, falls back to the envelope's `correlation_id`. Window key is `(handler_id, correlation_key)`.
- **`EventCombiner`** -- synchronous state folding, one per event per version
- **`Projection` / `ProjectionStore` / `ProjectionCheckpointStore` / `ProjectionHandler`** -- read model maintenance with checkpoint tracking
- **`ProjectionRebuildManager`** -- orchestrates projection rebuild lifecycle (`start_rebuild` / `is_rebuilding` / `complete_rebuild` / `get_checkpoint`). While rebuilding, read endpoints fall back to read-through.
- **`CounterfactualReplay`** -- what-if simulation over command history
- **`RetryTracker`** -- crash-safe retry counting for message processing failures

## In-memory implementations (`memory/`)

Every trait has an in-memory implementation for use in tests. These are the test harness backend.

| Struct | Trait |
|--------|-------|
| `InMemoryEventStore` | `EventStore` -- event storage with optimistic concurrency |
| `InMemoryCommandStore` | `CommandStore` |
| `InMemorySnapshotStore` | `SnapshotStore` -- snapshot storage |
| `InMemoryRetryTracker` | `RetryTracker` -- crash-safe retry tracking |
| `InMemoryInbox` | idempotent intake, oversight, correlation-keyed window management |
| `InMemoryInboundQueue` | FIFO queue |
| `InMemoryOutboundQueue` | fan-out to multiple consumers |
| `InMemoryProjectionStore` | `ProjectionStore` + `ProjectionCheckpointStore` -- checkpoint tracking with rebuilding flag |
| `InMemoryProjectionRebuildManager` | `ProjectionRebuildManager` |
| `InMemoryPublisher` | `Publisher` -- event publishing |
| `InMemoryAdaptor` | external event ingestion |
| `InMemoryDeadLetterStore` | `DeadLetterStore` -- dead letter storage with requeue |
| `RetryPolicy` | coordinates retry tracking with dead-letter escalation (configurable max retries, default 3) |
| `DefaultCounterfactualReplay<C>` | `CounterfactualReplay`, generic over any `CommandStore` |

## Outbound queue consumers (`consumers/`)

Three independent consumer groups that drain the outbound queue. All consumers are generic over their infrastructure traits.

| Consumer | Description |
|----------|-------------|
| `EventStoreConsumer<ES, SS, DL, RT, SP>` | Writes events to the event store. Takes snapshots every N versions (`version % snapshot_every == 0`). Retries on version conflict, dead-letters on exhaustion. Generic over `EventStore`, `SnapshotStore`, `DeadLetterStore`, `RetryTracker`, and `SnapshotStateProvider`. |
| `ProjectionConsumer<CS>` | Applies events to registered projection read models. Tracks per-projection checkpoints for idempotent replay. Generic over `ProjectionCheckpointStore`. |
| `PublisherConsumer<P>` | Publishes events to an external topic (e.g., `canon.fleet.events`) for cross-service consumption. Generic over `Publisher`. |

## Proc-macros

Re-exported from the `canon-core-macros` subcrate:

`#[aggregate]`, `#[command]`, `#[event]`, `#[event_combiner]`, `#[command_handler]`, `#[event_handler]`, `#[projection]`, `#[projection_handler]`
