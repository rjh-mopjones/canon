# Canon

**Macro-driven event sourcing for Rust.**

Canon is a Rust framework for building event-sourced services. It provides an opinionated,
production-ready pipeline that takes you from command handling through guaranteed event
delivery to projected read models -- with zero boilerplate.

## What Canon gives you

- **Proc-macro driven domain modelling** -- define aggregates, commands, events, and handlers
  with attribute macros. Canon generates all trait implementations, dispatch logic, and
  `inventory` registrations.

- **Guaranteed delivery via the outbox pattern** -- events are staged in a YugabyteDB
  transaction alongside the command write. An outbox processor drains them to Kafka.
  No dual-write bugs, ever.

- **Pluggable infrastructure** -- every infrastructure concern sits behind a trait.
  Swap Cassandra for DynamoDB, Kafka for Pulsar. The core never changes.

- **Full pipeline out of the box** -- inbox with idempotency and windowing, oversight
  gates, snapshotting, projections with rebuild, dead letter handling, cross-service
  event routing, and counterfactual replay.

- **In-memory test harness** -- every trait has an in-memory implementation in `canon-core`.
  Integration tests run in milliseconds with zero external infrastructure.

## The pipeline

Canon's event sourcing pipeline processes messages through a series of stages:

```
External world
      |
      v
Adaptor (Kafka)          -- inbound events from other services
      |
      v
Inbox (YugabyteDB)       -- idempotency, assembly, oversight
      |
      v
Inbound Queue (Kafka)    -- assembled batches to handlers
      |
      v
Dispatcher
  |-> Command handler
  |-> Internal event handlers
  |-> External event handlers
      |
      v
YugabyteDB transaction
  |-- commands table      -- audit trail (direct write)
  |-- outbox table        -- event staging (sequence_number ordered)
      |
      v
Outbox processor          -- drain outbox -> outbound queue
      |
      v
Outbound Queue (Kafka)    -- committed events fanning out
      |
      |-> Event store consumer     -> Cassandra (+ snapshots)
      |-> Projection consumer      -> YugabyteDB read models
      |-> Publisher (Kafka)        -> canon.{service}.events -> other services
```

A single command produces one or more events. Events are staged in the outbox within a
YugabyteDB ACID transaction, then drained to the outbound queue by the outbox processor.
Three independent consumers handle event persistence, projection updates, and cross-service
publishing.

## Philosophy

Canon is built on a few core principles:

- **Hexagonal architecture** -- every infrastructure concern is behind a trait. Swap the
  crate, keep the domain.
- **Append-only truth** -- the event store is the source of truth. Everything else is derived.
- **Crash safety** -- all durable state survives process death. The outbox is the commit
  point; the outbound queue is the delivery mechanism.
- **Testability** -- in-memory implementations of every port ship in `canon-core`. A
  dedicated `canon-test` crate provides a `TestHarness` with zero external infrastructure.
- **Macro-driven ergonomics** -- users never implement framework traits directly. Proc-macros
  generate all implementations from clean, declarative annotations.

## An experiment

Canon is an experiment to see how far AI-assisted development can go -- can it generate an
entire framework? Every line of Canon was written through human-AI collaboration using
Claude Code.

## Quick links

- [Getting Started](./user-guide/getting-started.md) -- install Canon and build your first aggregate
- [Core Concepts](./user-guide/core-concepts.md) -- understand the building blocks
- [Macros Reference](./user-guide/macros-reference.md) -- complete macro API
- [Architecture](./internals/architecture.md) -- deep dive into the pipeline
- [Live Demo](https://canon.mopjones.com/demo) -- see Canon running a spaceship logistics game
- [GitHub](https://github.com/rjh-mopjones/canon) -- source code and issues
