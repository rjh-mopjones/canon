# Getting Started

This guide walks you through adding Canon to a Rust project and defining your first
event-sourced aggregate.

## Prerequisites

- Rust 1.75+ (stable)
- A Cargo workspace (Canon is designed as a multi-crate workspace)

## Adding Canon to your project

Add `canon-core` to your `Cargo.toml`:

```toml
[dependencies]
canon-core = { path = "../canon-core" }
```

Canon is currently distributed as path dependencies within a workspace. Crates.io
publication is planned for a future release.

## Your first aggregate

An aggregate is the consistency boundary in your domain. Let's model a simple `Counter`
that can be incremented and decremented.

### 1. Define the aggregate

```rust
use canon_core::prelude::*;

#[aggregate(snapshot_every = 100)]
pub struct Counter {
    value: i64,
}
```

The `#[aggregate]` macro generates:
- An `impl Aggregate for Counter` with `type State = Counter`
- A `Default` implementation (all fields start at their zero values)
- Serde derives for serialisation
- An `inventory` registration so `ServiceBuilder` discovers it automatically
- Version-matched hydration dispatch (explained in [Core Concepts](./core-concepts.md))

The `snapshot_every = 100` attribute tells Canon to write a snapshot of the aggregate
state every 100 events, speeding up future hydration.

### 2. Define commands

Commands represent intent -- what the user wants to happen. Each command is versioned
and declares which event it produces.

```rust
#[command(Counter, version = 1, produces = [CounterIncremented])]
pub struct IncrementCounter {
    pub amount: i64,
}

#[command(Counter, version = 1, produces = [CounterDecremented])]
pub struct DecrementCounter {
    pub amount: i64,
}
```

The `produces` attribute is declarative metadata -- it documents which event type the
handler returns and is used for macro wiring and compile-time enforcement.

### 3. Define events

Events are facts -- they record what happened. Each event is versioned and must have
a matching `#[event_combiner]`.

```rust
#[event(Counter, version = 1)]
pub struct CounterIncremented {
    pub amount: i64,
}

#[event(Counter, version = 1)]
pub struct CounterDecremented {
    pub amount: i64,
}
```

### 4. Define event combiners

Event combiners are pure, synchronous state folding functions. They define how each
event modifies the aggregate state.

```rust
#[event_combiner(Counter, version = 1)]
impl CounterIncremented {
    fn combine(&self, state: &mut Counter) {
        state.value += self.amount;
    }
}

#[event_combiner(Counter, version = 1)]
impl CounterDecremented {
    fn combine(&self, state: &mut Counter) {
        state.value -= self.amount;
    }
}
```

### 5. Define command handlers

Command handlers contain business logic. They receive the current aggregate state and
a command, then return either a single event or an error.

```rust
#[command_handler(Counter, version = 1)]
impl IncrementCounterHandler {
    type Error = CounterError;

    fn handle(
        &self,
        _state: &Counter,
        cmd: IncrementCounter,
    ) -> Result<CounterIncremented, CounterError> {
        if cmd.amount <= 0 {
            return Err(CounterError::InvalidAmount);
        }
        Ok(CounterIncremented { amount: cmd.amount })
    }
}

#[command_handler(Counter, version = 1)]
impl DecrementCounterHandler {
    type Error = CounterError;

    fn handle(
        &self,
        state: &Counter,
        cmd: DecrementCounter,
    ) -> Result<CounterDecremented, CounterError> {
        if cmd.amount <= 0 {
            return Err(CounterError::InvalidAmount);
        }
        if state.value - cmd.amount < 0 {
            return Err(CounterError::WouldGoNegative);
        }
        Ok(CounterDecremented { amount: cmd.amount })
    }
}
```

### 6. Define your error type

Each domain defines its own error types using `thiserror`:

```rust
use thiserror::Error;

#[derive(Error, Debug)]
pub enum CounterError {
    #[error("amount must be positive")]
    InvalidAmount,
    #[error("counter would go negative")]
    WouldGoNegative,
}
```

### 7. Wire it up with ServiceBuilder

`ServiceBuilder` auto-discovers all registered handlers via `inventory`:

```rust
let service = ServiceBuilder::new()
    .for_aggregate::<Counter>()
    .build();

service.start().await;
```

That's it. Canon handles all the wiring: inbox, dispatcher, outbox, event store,
projections, and publisher. Your domain code is just the aggregate, commands, events,
combiners, and handlers.

## Compile-time safety

Canon enforces completeness at compile time:

- Every `#[command(X, version = N)]` must have a matching `#[command_handler(X, version = N)]`
- Every `#[event(X, version = N)]` must have a matching `#[event_combiner(X, version = N)]`
- The `#[command_handler]` return type must match the event declared in `produces`
- `window_ttl` without `oversight` is a compile error

If you forget a handler or combiner, the compiler tells you.

## Next steps

- [Core Concepts](./core-concepts.md) -- understand aggregates, events, and the pipeline in depth
- [Macros Reference](./macros-reference.md) -- complete reference for all 8 macros
- [Event Handlers](./event-handlers.md) -- react to events and produce commands
- [Testing](./testing.md) -- test your domain with the in-memory harness
