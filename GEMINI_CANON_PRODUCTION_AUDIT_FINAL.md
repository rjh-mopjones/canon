# Final Production Audit: Canon Framework

**Date:** Friday, 20 March 2026
**Reviewer:** Gemini CLI (Multi-Agent Red-Team)
**Status:** Tier 2 Production Ready (Not Tier 1)

---

## 1. Concurrency & Transactional Integrity

### A. Optimistic Concurrency Control (OCC)
**Finding:** `EventStore::append` correctly implements OCC via `expected_version`.
- **Cassandra**: Uses Lightweight Transactions (`UPDATE ... IF version = ?`).
- **Yugabyte**: Uses standard ACID transactions with a version check in the `INSERT` path.
- **Risk**: High-contention aggregates will see frequent 409-style conflicts. The framework handles this via retries, but the default retry budget (3) is likely too low for high-concurrency "hot" aggregates (e.g., a global configuration object).

### B. The "Ghost Command" Risk
**Finding:** Command logging and Outbox writes are performed in the same Yugabyte transaction.
- **Verification**: `canon-command-store-yugabyte` correctly groups these operations when using the provided `PgPool`.
- **Warning**: If a developer uses a custom storage implementation and fails to wrap the `CommandStore` and `OutboxStore` in a single transaction, the "At-Least-Once" guarantee is broken.

## 2. Performance & Runtime Audit

### A. The Hydration Bottleneck (Critical)
**Location:** `canon-core/src/registration.rs` -> `__apply_event_combiner`
**Finding:** Linear $O(N)$ search through the `inventory` registry.
- **Complexity**: $O(E \times R)$ where $E$ is event count and $R$ is registry size.
- **Impact**: This is the single biggest threat to read-side latency. In a large system, loading an aggregate will consume excessive CPU cycles just on string comparisons.
- **Recommendation**: This MUST be refactored to use a `OnceLock<HashMap<TypeId, CombinerApplyFn>>`.

### B. Deserialization Overhead
**Finding:** Every event is deserialized from JSON on every hydration.
- **Impact**: JSON parsing is CPU-bound. Without a robust snapshotting strategy, aggregates with thousands of events will become slow to load regardless of the database performance.
- **Status**: The `Aggregate` trait supports `snapshot_every`, but the infrastructure to *automatically* take and load these snapshots is still missing from the (placeholder) `ServiceBuilder`.

## 3. Distributed Systems Reliability

### A. Outbox Scalability
**Finding:** `SELECT ... FOR UPDATE SKIP LOCKED` is used correctly in the Yugabyte Outbox implementation.
- **Benefit**: Safe horizontal scaling. Multiple pods can drain the outbox without contention. This is a "Production-Grade" implementation choice.

### B. Kafka Semantics
**Finding**: Uses `acks=all` and `enable.idempotence=true`.
- **Benefit**: Prevents data loss and duplicate writes from the producer side.
- **Risk**: Lack of `transactional.id` means that during "Zombie Pod" scenarios, duplicate events *will* be published to Kafka.
- **Mitigation**: The framework relies on **Idempotent Consumers** (Downstream Inbox/Projections) to ignore these duplicates. This is a valid architectural trade-off but increases system noise during failures.

### C. Dead Letter Handling
**Finding**: `canon-deadletter` is fully integrated into the `EventStoreConsumer`.
- **Benefit**: Critical for production. When an event cannot be applied (due to a bug or data corruption), it is parked in the DLQ instead of blocking the entire partition.

## 4. Final Verdict

| Metric | Rating | Reason |
| :--- | :--- | :--- |
| **Reliability** | **9/10** | Robust Outbox, ACID writes, and DLQ support. |
| **Performance** | **5/10** | $O(N)$ hydration lookup and heavy JSON overhead. |
| **Developer Experience** | **4/10** | Ergonomic macros, but missing the `ServiceBuilder` "glue." |
| **Production Readiness** | **Tier 2** | Ready for most use cases; needs optimization for high-scale. |

---
*End of Final Audit*
