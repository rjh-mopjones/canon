# Canon Testing Story

Design the complete, production-grade testing strategy for the Canon event sourcing framework. You are writing the definitive guide that every contributor will follow. This must be concrete, opinionated, and specific to Canon — not generic testing advice.

This runs in an **iterative deepening loop**. Each pass must go deeper than the last. Do not stop until you are told to or run out of context.

---

## Setup

Read the following files in full before starting:

- `CLAUDE.md` — canonical architecture reference
- `canon-design.md` — design decisions and rationale
- All trait definitions in `canon-core/src/` — every abstraction that needs testing
- All in-memory implementations — understand what the test doubles currently provide
- All schema files — understand what needs schema-level testing
- `canon-demo/` — the system under test for integration scenarios
- Any existing test files in the workspace — understand what's already there

---

## Iteration Structure

Each iteration follows this exact structure:

### Phase 1 — Inventory

Map out what needs testing. For each item, record:

- **Subject**: what is being tested
- **Kind**: `unit` / `integration` / `contract` / `property` / `chaos` / `e2e`
- **Current coverage**: does anything test this today?
- **Risk if untested**: what production bug does missing this test allow?

Cover every layer:

**Aggregate correctness**
- Does every aggregate reject invalid commands with the right error?
- Does every aggregate produce exactly the right events for every valid command?
- Are aggregate invariants enforced across multi-command sequences?
- Does replay from event history always reconstruct the correct state?

**Inbox**
- Does `INSERT ... ON CONFLICT DO NOTHING` actually provide idempotency under concurrent inserts?
- What happens if the same command_id arrives with different payloads?
- Does the PostgreSQL schema enforce all constraints we rely on?

**Queue (RabbitMQ)**
- Is a message that is nacked and redelivered processed exactly once?
- Does the dead-letter exchange receive messages after max retries?
- What happens if the consumer crashes mid-processing — is the ack lost?
- Does the in-memory queue implementation faithfully simulate RabbitMQ's redelivery semantics?

**Command store**
- Is every command lifecycle transition valid? (submitted → processing → complete / failed)
- Are invalid transitions rejected?
- Is the audit trail append-only in practice?

**Event store (Cassandra)**
- Are events written in strict causal order per aggregate?
- Does a read of all events for an aggregate always return a consistent, ordered sequence?
- Does the schema handle the 50-event snapshot boundary correctly during replay?

**Handlers**
- Does fan-out deliver to all handlers even if one panics?
- Does `Oversight` gate correctly for every combination of state?
- Does `Option<CommandEnvelope>` from a handler get correctly routed back through the inbox?
- Are handler dispatch modes (single event vs batched) tested for both modes?

**Snapshots**
- Is a snapshot taken at exactly every 50th event?
- Does replay from snapshot + tail produce identical state to full replay from zero?
- What happens if a snapshot write fails halfway?

**Upcasting**
- Does a v1 event upcast to v2 correctly?
- Is the original v1 event preserved verbatim in storage?
- Does replaying a mixed v1/v2 stream produce correct aggregate state?

**Counterfactual replay**
- Does replaying with a different command produce a diff at the command level?
- Are the before/after states correct?

**Projections**
- Does a projection correctly reflect all events up to a given point?
- Does it handle out-of-order delivery?
- Does it handle duplicate events idempotently?

**Cross-service flows**
- Does a Kafka-published event from navigation-service correctly arrive at station-service?
- Is the full flow from command submission to projection update testable without mocking?

**In-memory fidelity**
- For every in-memory implementation, is there a contract test that runs the same test suite against both the in-memory and real infrastructure version?

### Phase 2 — Research

For each testing kind identified, research deeply:

- How do the best event sourcing frameworks approach this? (Axon Framework testing support, Commanded's test DSL, Marten's test helpers, EventStoreDB testing patterns)
- What does the property-based testing literature say about testing aggregates? (Hypothesis strategies, proptest, quickcheck for Rust)
- What chaos engineering techniques apply? (Toxiproxy for Cassandra/RabbitMQ, killing containers mid-test)
- What does the Rust testing ecosystem offer specifically? (`tokio::test`, `testcontainers-rs`, `proptest`, `fake-rs`, `mockall`, `wiremock`)
- Are there published patterns for testing CQRS/ES systems end-to-end in CI?

Use web search extensively. Fetch primary sources. Do not rely on summaries.

### Phase 3 — Design

For each test kind, write a concrete design:

- **Test structure**: what does the test setup, exercise, and assert
- **Canon-specific DSL**: should Canon provide a test helper crate (`canon-test`)? What would it expose?
- **Example code**: write a representative example test in Rust for the most important cases — actual code, not pseudocode
- **CI placement**: which tests run on every PR? Which run nightly? Which require real infrastructure?
- **Testcontainers usage**: for tests that need real Cassandra/Postgres/RabbitMQ/Kafka, design the container setup
- **Fidelity traps**: what behaviours of the real infrastructure are hardest to simulate and most likely to cause in-memory tests to give false confidence?

### Phase 4 — Gap Analysis

Before starting the next iteration, explicitly answer:

1. Which components have no concrete example test written yet?
2. Which test kinds (chaos, property, contract) have been described but not designed in detail?
3. What external research did I reference but not fully read? Go read it now.
4. What would a new Canon contributor find confusing or underspecified in what I've written?

Write this at the end of each iteration. It becomes the focus of the next iteration.

---

## Output Format

Maintain a single file `canon-testing-story.md` throughout the run. Update in place after each iteration.

Structure:

```
# Canon Testing Story

## Philosophy
_The opinionated principles behind Canon's testing approach._

## Test Crate: canon-test
_Design of the shared test helper crate, with API surface._

## Coverage Map
_Table: component × test kind → status (designed / example written / missing)_

## Test Designs

### [Component] — [Test Kind]
**What it tests:** ...
**Risk if missing:** ...
**Setup:** ...
**Example (Rust):**
```rust
...
```
**CI tier:** ...

---

## Iteration Log

### Iteration N
_Focus, findings, what changed._

## Gap Analysis (current)
_What the next iteration will focus on._
```

---

## Loop Control

After each full iteration, check:

- Does every component in the inventory have at least one concrete example test written? If not, continue.
- Is there a `canon-test` crate API design with at least 10 helper functions specified? If not, continue.
- Has chaos testing been designed concretely (not just mentioned)? If not, continue.
- Have contract tests between in-memory and real implementations been fully designed? If not, continue.
- Have you written example Rust test code for at least one test in every category? If not, continue.
- Have you read at least 3 external sources on property-based testing of event sourced systems? If not, continue.

**Do not stop voluntarily.** If you believe you have exhausted the design space, go back to the gap analysis and find something underspecified. There is always something underspecified.
