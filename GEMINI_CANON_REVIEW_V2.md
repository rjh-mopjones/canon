# Production-Grade Audit: Canon Framework (V2)

**Date:** Friday, 20 March 2026
**Reviewer:** Gemini CLI (Red Team Audit)
**Verdict:** High-Potential Alpha (Not Production-Ready)

---

## 1. Executive Summary
Canon is a robust architectural "pattern kit" for Rust Event Sourcing. While its core design (Outbox Pattern, YugabyteDB ACID writes, Pluggable Backends) is top-tier, the implementation lacks the "connective tissue" required for a zero-touch production deployment.

## 2. Critical Production Bottlenecks

### A. The $O(N)$ Hydration Tax
**Location:** `canon-core/src/registration.rs` -> `__apply_event_combiner`
**Finding:** During aggregate hydration, the framework iterates through *every* registered event combiner for *every* event in the stream.
- **Production Impact:** If a service defines 200 event types and an aggregate has 500 events, you perform 100,000 linear comparisons just to reconstruct state.
- **Fix Required:** Replace the linear `inventory::iter` with a `OnceLock<HashMap<TypeId, ...>>` for $O(1)$ lookups.

### B. Transactional Integrity (The "Quiet Failure" Risk)
**Location:** `canon-command-store-yugabyte/src/lib.rs`
**Finding:** The `CommandStore::append` and `EventStore::append` traits do not natively share a `sqlx::Transaction`. 
- **Production Impact:** A developer could easily commit a command to the audit log but fail to write the outbox entry due to a network blip, leading to "ghost commands" that never trigger events or projections.
- **Fix Required:** The storage traits must be updated to accept a transaction or use a "Unit of Work" pattern to enforce atomicity across Command + Event + Outbox writes.

### C. Missing Orchestration (`ServiceBuilder`)
**Finding:** Throughout the codebase (and `CLAUDE.md`), `ServiceBuilder` is referenced as the mechanism for auto-discovery and wiring. **It does not exist.**
- **Production Impact:** Developers must manually wire Kafka consumers, Yugabyte pools, and outbox processors in `main.rs`, leading to brittle, non-standardized service startup code.
- **Fix Required:** Complete the `ServiceBuilder` implementation to fulfill the "Convention over Configuration" promise.

## 3. Production Strengths

### A. Horizontal Scalability (Outbox)
**Location:** `canon-core/src/outbox.rs`
**Finding:** Correct implementation of `SELECT ... FOR UPDATE SKIP LOCKED`.
- **Why it matters:** This allows you to scale your pods horizontally without multiple instances fighting over the same outbox rows. It is the correct, production-grade way to implement the Outbox pattern.

### B. Idempotency & Failure Recovery
**Location:** `canon-inbox-yugabyte/src/lib.rs`
**Finding:** Robust "At-Least-Once" handling using `processed_windows` and `deduplicate` logic.
- **Why it matters:** Kafka *will* deliver duplicates. The framework's ability to ignore them at the Inbox layer prevents side-effect corruption (e.g., double-billing).

### C. Observability
**Finding:** Deep integration with the `tracing` crate.
- **Why it matters:** Essential for distributed tracing in a microservice environment.

## 4. Prioritized "Must-Fix" List for Production

1.  **[High]** **Optimize Hydration**: Convert the linear registration search to a `HashMap`.
2.  **[High]** **ServiceBuilder**: Finish the orchestration layer to enable standardized service startup.
3.  **[Medium]** **Transaction Safety**: Update traits to enforce atomic writes across the Command/Event/Outbox boundary.
4.  **[Low]** **Namespace Registrations**: Use full type paths in macros to prevent name collisions between different modules using the same event names (e.g., `UpdateStatus`).

---
*End of V2 Audit*
