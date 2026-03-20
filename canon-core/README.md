# canon-core

Foundation crate for the Canon event sourcing framework. Contains all domain traits, core types, proc-macros, and in-memory implementations.

## Modules

| Module | Contents |
|--------|----------|
| `types` | `AggregateId`, `Version`, `EventEnvelope`, `CommandEnvelope`, `IncomingMessage`, `Oversight`, counterfactual types |
| `traits` | `Aggregate`, `CommandHandler`, `CommandStore`, `EventHandler`, `EventCombiner`, `Projection`, `ProjectionStore`, `ProjectionHandler`, `ProjectionRebuildManager`, `CounterfactualReplay` |
| `error` | `EventStoreError`, `InboxError`, `DeadLetterError`, `MacroError` |
| `memory` | In-memory implementations of every trait (see below) |
| `registration` | `inventory`-based auto-registration types for macro-generated impls |

## Traits

Core traits that define the framework's contracts. Users never implement these directly -- proc-macros generate all impls.

- **`Aggregate`** -- state hydration via version-matched event combiners
- **`CommandHandler`** -- one per command type per version, returns a single event
- **`CommandStore`** -- async `append` and `load_range` for command persistence
- **`EventHandler`** -- aggregate-agnostic, optional oversight and `correlate`, produces at most one command. `correlate` extracts a domain correlation key from an incoming message to determine window grouping; when omitted, falls back to the envelope's `correlation_id`. Window key is `(handler_id, correlation_key)`.
- **`EventCombiner`** -- synchronous state folding, one per event per version
- **`Projection` / `ProjectionStore` / `ProjectionHandler`** -- read model maintenance
- **`ProjectionRebuildManager`** -- orchestrates projection rebuild lifecycle (`start_rebuild` / `is_rebuilding` / `complete_rebuild` / `get_checkpoint`). While rebuilding, read endpoints fall back to read-through.
- **`CounterfactualReplay`** -- what-if simulation over command history

## In-memory implementations (`memory/`)

Every trait has an in-memory implementation for use in tests. These are the test harness backend.

| Struct | Trait |
|--------|-------|
| `InMemoryEventStore` | event storage with optimistic concurrency |
| `InMemoryCommandStore` | `CommandStore` |
| `InMemorySnapshotStore` | snapshot storage |
| `InMemoryInbox` | idempotent intake, oversight, correlation-keyed window management |
| `InMemoryInboundQueue` | FIFO queue |
| `InMemoryOutboundQueue` | fan-out to multiple consumers |
| `InMemoryProjectionStore` | checkpoint tracking with rebuilding flag |
| `InMemoryProjectionRebuildManager` | `ProjectionRebuildManager` |
| `InMemoryPublisher` | event publishing |
| `InMemoryAdaptor` | external event ingestion |
| `InMemoryDeadLetterStore` | dead letter storage with requeue |
| `DefaultCounterfactualReplay<C>` | `CounterfactualReplay`, generic over any `CommandStore` |

## Proc-macros

Re-exported from the `canon-core-macros` subcrate:

`#[aggregate]`, `#[command]`, `#[event]`, `#[event_combiner]`, `#[command_handler]`, `#[event_handler]`, `#[projection]`, `#[projection_handler]`
