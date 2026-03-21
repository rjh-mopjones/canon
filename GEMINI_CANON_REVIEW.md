# In-depth Review: Canon Framework

**Date:** Friday, 20 March 2026
**Reviewer:** Gemini CLI

---

## 1. Overview
**Canon** is a sophisticated, multi-crate Rust framework designed for building Event Sourced and CQRS-based microservices. It prioritizes developer experience (DX) via procedural macros and ensures high reliability through the "Outbox Pattern" and ACID-compliant storage adapters (primarily YugabyteDB).

## 2. Current Status: Framework-in-Progress
The most significant finding of this review is that **Canon is currently in an advanced "building blocks" stage**. 

- **Strengths**: The core traits (`Aggregate`, `CommandHandler`, `EventStore`), procedural macros (`#[aggregate]`, `#[command_handler]`), and storage adapters (`canon-command-store-yugabyte`) are well-designed and functional.
- **Current Gap**: The framework lacks the final "glue" — a **`ServiceBuilder`** orchestrator. Without it, the services in `canon-demo` (e.g., `fleet-service`, `cargo-service`) are currently minimal and do not yet run the full domain logic.

## 3. Core Architectural Patterns

### Event Sourcing & CQRS
State is reconstructed (hydrated) by replaying versioned events through `#[event_combiner]` implementations. The separation of concerns between command handling (write side) and projections (read side) is strictly enforced.

### Reliability & The Outbox Pattern
The `OutboxProcessor` in `canon-core` is a standout component. It ensures that every event produced by a command handler is reliably delivered to an outbound queue (like Kafka).
- Uses `SELECT ... FOR UPDATE SKIP LOCKED` for safe horizontal scaling.
- Guarantees at-least-once delivery with ordered polling by sequence number.

### Boilerplate Reduction via Macros
The `canon-core-macros` are a high-value part of the framework.
- **Ergonomics**: They allow developers to write synchronous-looking domain logic while the framework wraps it in `async-trait` implementations.
- **Orphan Rule Bypass**: The `#[event_combiner]` macro replaces `self` with `__canon_self`, allowing event logic to be defined even if the event or aggregate types are in foreign crates.

## 4. Technical Analysis & Findings

### Runtime Discovery
Canon uses the `inventory` crate for compile-time registration of handlers and combiners. 
- **The Good**: It eliminates the need for manual registration in a `main()` function.
- **The Risk**: The current `__apply_event_combiner` implementation performs an $O(N)$ linear search through all registered combiners. In very large binaries, this could benefit from an internal `HashMap` optimization on the first call.
- **Collision Risk**: Registrations are based on the last segment of an event's name (e.g., `ShipRegistered`). If multiple modules define events with the same name, there is a risk of collision during hydration.

### Concurrency Control
Optimistic concurrency is cleanly handled at the `EventStore` layer. Appends must provide an `expected_version`, preventing "lost update" scenarios in a distributed environment.

### Backend Implementations
The YugabyteDB adapters (using `sqlx`) are idiomatic and robust. They leverage ACID transactions to ensure that commands, events, and outbox entries are committed atomically.

## 5. Developer Experience (DX)

- **Testing**: The `TestHarness` in `canon-test` is excellent. It provides high-fidelity in-memory versions of all stores, allowing developers to test complex domain scenarios without infrastructure overhead.
- **Clarity**: The code is well-commented and uses `tracing` extensively for observability.
- **Error Handling**: Uses `thiserror` for precise, descriptive error types.

## 6. Summary of Findings

| Feature | Assessment |
| :--- | :--- |
| **Macros/DX** | **Exceptional**. Greatly reduces boilerplate and hides complexity. |
| **Reliability** | **High**. Robust Outbox pattern and ACID transaction support. |
| **Scalability** | **Good**. Trait-based design supports swapping in-memory for distributed backends. |
| **Maturity** | **Early/Beta**. Critical orchestration (`ServiceBuilder`) is missing. |

## 7. Recommendations

1.  **Prioritize ServiceBuilder**: The framework's utility is limited until the orchestration layer is completed to wire the various components together.
2.  **Optimize Discovery**: Convert the linear $O(N)$ registration search into a `HashMap`-based lookup for faster hydration in large services.
3.  **Namespace Registrations**: Use full type paths in `EventCombinerRegistration` to prevent name collisions across different modules or crates.
4.  **Async Handlers**: Consider providing an `#[async_command_handler]` variant for cases where handlers need to perform external I/O during command validation.

---
*End of Review*
