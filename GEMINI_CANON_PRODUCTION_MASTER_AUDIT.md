# Master Production Audit: Canon Framework

**Date:** Friday, 20 March 2026
**Reviewer:** Gemini CLI (Multi-Agent Red-Team)
**Overall Verdict:** INCOMPLETE INFRASTRUCTURE (Tier 3 / Beta)

---

## 1. Domain: Persistence & Transactional Integrity

### A. The "Ghost Command" Vulnerability
**File:** `canon-command-store-yugabyte/src/lib.rs`
**Finding:** The `append` method performs an uncoordinated, non-transactional write to the `commands` table.
```rust
// Current implementation (L54)
async fn append(&self, envelope: CommandEnvelope) -> Result<(), Self::Error> {
    sqlx::query("INSERT INTO commands ...")
        .execute(&self.pool) // Direct execute on pool - NO TRANSACTION
        .await?;
    Ok(())
}
```
**Technical Analysis:**
The framework's core promise is "At-Least-Once" delivery via the Outbox pattern. However, the `YugabyteCommandStore` does not participate in a shared transaction with the Outbox. 
- **Failure Scenario**: A command is processed, the state is changed, and the `commands` audit log is written. The pod then crashes (OOM, network partition, etc.) before the Outbox entry is created.
- **Production Impact**: The command is marked as "Executed" in the audit log, but the resulting event **never reaches Kafka**. Projections and downstream services will never know the command happened. The system is now in a state of "Silent Divergence."

### B. Correct "Inbox" Transaction Coordination
**File:** `canon-inbox-yugabyte/src/lib.rs`
**Finding:** The `submit` method (L425) correctly uses an explicit `sqlx::Transaction`.
```rust
let mut tx = self.pool.begin().await?;
let is_new = self.deduplicate(&mut tx, ...).await?;
if is_new {
    self.accumulate(&mut tx, ...).await?;
    self.evaluate_oversight(&mut tx, ...).await?;
}
tx.commit().await?;
```
**Technical Analysis:**
This is the "gold standard" for the framework. It ensures that deduplication and storage are atomic. If the commit fails, the Kafka offset is not advanced, and the message is retried. This is production-ready.

---

## 2. Domain: Macro-Engine & Hydration Performance

### A. The $O(N^2)$ Hydration Trap
**File:** `canon-core/src/registration.rs` -> `__apply_event_combiner`
**Finding:** Linear search through the `inventory` registry on every event application.
```rust
// Current implementation (L101)
pub fn __apply_event_combiner(...) {
    for reg in inventory::iter::<EventCombinerRegistration> {
        if reg.aggregate_type_id == aggregate_type_id
            && reg.event_type_name == envelope.event_type
            && reg.event_version == envelope.event_version
        {
            return (reg.apply_fn)(envelope.payload.as_ref(), state);
        }
    }
}
```
**Technical Analysis:**
- **Complexity**: $O(E \times R)$ where $E = \text{events in stream}$ and $R = \text{registered combiners}$.
- **Scaling Problem**: If a service handles 200 event types (R=200) and an aggregate has 1,000 events in its history (E=1,000), you perform **200,000 string and TypeId comparisons** just to load a single object from the database.
- **Production Impact**: During high-load "replay" scenarios (e.g., a massive projection rebuild or a cold-start of a popular aggregate), the CPU will be saturated by registry scanning, leading to severe latency spikes.

### B. Serialization Overhead
**Finding:** Hardcoded dependency on `serde_json` for all hydration and storage.
- **Technical Analysis**: JSON deserialization is significantly more expensive than binary formats (Postcard, Bincode, Protobuf) in terms of both CPU cycles and memory allocations.
- **Production Impact**: For an event-sourced system where objects are hydrated frequently, JSON parsing will be your #1 CPU bottleneck. The framework lacks a "Pluggable Serde" layer to swap in a binary format for performance.

---

## 3. Domain: Distributed Systems & Failure Modes

### A. The "Zombie Pod" Duplicate
**File:** `canon-core/src/outbox.rs`
**Finding:** Potential for duplicate Kafka publications during processor crashes.
**Technical Analysis:**
The `OutboxProcessor` publishes to Kafka first, then marks the entry as delivered in the DB.
1. `publisher.publish(envelope).await?`
2. `store.mark_delivered(entry.id).await?`
If the pod crashes between these two lines, the event is in Kafka, but the Outbox still thinks it's undelivered. Upon restart, the event is sent again.
- **Mitigation**: The `YugabyteInbox` correctly uses `ON CONFLICT DO NOTHING` (L432) to ignore these duplicates.
- **Residual Risk**: **`ProjectionStore`** (in `canon-core/src/consumers/projection_consumer.rs`) tracks a single `last_version` per projection. However, since versions are **per-aggregate**, a projection could incorrectly "skip" an event if it receives a higher version from Aggregate A followed by a lower (but correct) version from Aggregate B.

### B. Kafka Consumer Reliability
**File:** `canon-adaptor-kafka/src/lib.rs`
**Finding:** Correct "Write-Ahead-Commit" implementation.
**Technical Analysis:**
The consumer calls `inbox.submit(...).await` first, then calls `consumer.commit_message(..., CommitMode::Sync)`.
- **Benefit**: This guarantees no event is "skipped." If the `Inbox` write fails, the offset remains uncommitted, and the message is retried by the next consumer.

---

## 4. Final Verdict & Prioritized "Kill-List"

| Metric | Rating | Status |
| :--- | :--- | :--- |
| **Transactional Integrity** | **4/10** | Fails at the critical Command -> Outbox boundary. |
| **Hydration Performance** | **3/10** | $O(N)$ linear search is a scaling disaster. |
| **Reliability (Inbox/Kafka)** | **9/10** | Top-tier implementation of idempotent ingestion. |
| **Operational Readiness** | **2/10** | Missing `ServiceBuilder` orchestration layer. |

### **Critical Path to "Production-Grade":**
1.  **[CRITICAL]** Refactor `registration.rs`: Implement a `OnceLock<HashMap<TypeId, ...>>` for $O(1)$ event combiner lookups.
2.  **[CRITICAL]** Fix `YugabyteCommandStore`: Force an explicit `sqlx::Transaction` across Command and Outbox writes.
3.  **[HIGH]** Implement **`ServiceBuilder`**: Automate the wiring of pools and background tasks to prevent resource leaks.
4.  **[MEDIUM]** Add **Binary Serialization**: Support `Postcard` or `Bincode` to reduce CPU overhead during hydration.
5.  **[MEDIUM]** Fix **Projection Checkpointing**: Track versions per `aggregate_id` in the `ProjectionStore` to prevent cross-aggregate event skipping.

---
*End of Master Audit*
