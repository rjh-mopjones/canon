# Stress Test Canon Design

Adversarially review the Canon event sourcing framework design. Play the role of a hostile senior distributed systems engineer who has been burned by bad event sourcing designs in production. Your job is to find every failure mode, race condition, broken guarantee, and bad assumption in the Canon design — then research how to fix them.

This runs in an **iterative deepening loop**. Each pass must go deeper than the last. Do not stop until you are told to or run out of context.

---

## Setup

Read the following files in full before starting. Do not skip any:

- `CLAUDE.md` — canonical architecture reference
- `canon-design.md` — design decisions and rationale
- All `Cargo.toml` files across the workspace — understand every crate's dependencies and boundaries
- All trait definitions in `canon-core/src/` — understand every abstraction
- All schema files (PostgreSQL and Cassandra) — understand the storage layer
- `canon-demo/` — understand what the design must actually support in practice
- Any `wave-*.sh` scripts — understand what is being built and in what order

Build a mental model of the complete system before writing anything.

---

## Iteration Structure

Each iteration follows this exact structure:

### Phase 1 — Attack

Identify failure modes. For each one, document:

- **Category**: one of `data-loss`, `ordering`, `consistency`, `performance`, `operability`, `abstraction-leak`, `correctness`, `security`
- **Component**: which crate or subsystem is affected
- **Scenario**: the exact sequence of events that triggers the failure
- **Blast radius**: what breaks and how badly
- **Detectability**: would this show up in tests? Only in production?
- **Severity**: `critical` / `high` / `medium` / `low`

Go after every layer:
- The inbox idempotency guarantee — what breaks it?
- RabbitMQ manual ack/nack — what are the redelivery semantics, and does Canon handle them correctly?
- The command store / event store write ordering — is there a window where an event is lost?
- Cassandra append semantics — are there partition hotspots, tombstone accumulation, or read-before-write races?
- The `Oversight` enum decision gate — can it be bypassed or produce inconsistent results under concurrent commands?
- The `#[canon::handler]` macro dispatch — what happens with handler panics, partial fan-out, or duplicate delivery?
- Snapshotting every 50 events — what happens during a snapshot write failure? During replay across a snapshot boundary?
- Counterfactual replay — what invariants must hold for this to be correct, and are they enforced?
- Cross-service event flows — what happens when a downstream service is unavailable when an event is published?
- The `InboxPort` trait — can a handler submit a command that creates an infinite loop?
- Upcasting (v1→v2 in cargo-service) — what happens if an upcaster has a bug? Is the original event preserved?
- The Kafka publisher/adaptor boundary — delivery guarantees, ordering, duplicate events
- In-memory implementations used in tests — are they faithful enough to catch real infrastructure bugs?

### Phase 2 — Research

For every `critical` or `high` severity finding, research deeply:

- Have other event sourcing frameworks (Axon, Marten, EventStoreDB, Commanded for Elixir) encountered this problem?
- What did they do about it?
- What does the academic literature say? (CRDT approaches, saga patterns, outbox pattern, etc.)
- Does the Canon design already have a mitigation that you missed on first read? If so, is it sufficient?

Use web search extensively. Fetch primary sources — Greg Young's blog, EventStoreDB docs, Axon documentation, relevant papers. Do not rely on summaries.

### Phase 3 — Prescriptions

For each finding, write a concrete prescription:

- **Recommended fix**: specific change to Canon's design, schema, trait definition, or documentation
- **Tradeoff**: what does the fix cost? (complexity, performance, operational burden)
- **Alternative**: if there's a simpler mitigation that accepts the risk, describe it
- **Test**: what test would catch this in CI?

### Phase 4 — Gap Analysis

Before starting the next iteration, explicitly answer:

1. What categories of failure mode have I NOT explored yet?
2. Which components have received the least scrutiny?
3. Which of my previous findings were shallow and deserve a deeper follow-up?
4. What external research did I cite but not fully read? Go read it now.

Write this gap analysis at the end of each iteration. It becomes the attack plan for the next iteration.

---

## Output Format

Maintain a single file `canon-stress-test.md` throughout the run. After each iteration, update it in place — do not create separate files per iteration.

Structure:

```
# Canon Stress Test

## Summary
_Updated after each iteration. Running count of findings by severity and category._

## Findings

### [SEVERITY] [CATEGORY] — [Short title]
**Component:** ...
**Scenario:** ...
**Blast radius:** ...
**Detectability:** ...
**Research:** ...
**Prescription:** ...
**Test:** ...

---

## Iteration Log

### Iteration N — [timestamp]
_What this iteration focused on, what it found, what changed._

## Gap Analysis (current)
_What the next iteration will focus on._
```

---

## Loop Control

After each full iteration (all four phases complete), check:

- Have you covered every component at least once? If not, continue.
- Have you followed up on every gap from the previous gap analysis? If not, continue.
- Are there any `critical` findings whose prescribed fix has not been stress-tested for its own failure modes? If not, continue.
- Have you researched at least 3 external sources per `critical` finding? If not, continue.

**Do not stop voluntarily.** If you believe you have exhausted the design space, go back to the gap analysis and find something you skimmed. There is always something you skimmed.
