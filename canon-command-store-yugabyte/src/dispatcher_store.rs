//! YugabyteDB-backed dispatcher store (shared implementation).
//!
//! Implements `DispatcherStore` for any service by:
//! - Polling inbox_messages from YugabyteDB
//! - Loading events via a generic `EventStore` implementation
//! - Writing events to the outbox table and marking inbox messages as processed
//!
//! Generic over the event store so that each service can supply its own
//! (e.g., `CassandraEventStore`, `InMemoryEventStore`).

use async_trait::async_trait;
use canon_core::dispatcher::{DispatcherError, DispatcherStore, InboxCommandRow};
use canon_core::traits::EventStore;
use canon_core::{AggregateId, CommandEnvelope, EventEnvelope};
use sqlx::PgPool;
use uuid::Uuid;

/// Dispatcher store backed by YugabyteDB (inbox, outbox, retry_attempts,
/// dead_letters) and a pluggable event store (for aggregate hydration).
///
/// This is the shared implementation used by all demo services. Each service
/// constructs it with its own `handler_id` (aggregate type name) and event
/// store.
///
/// # Example
///
/// ```rust,ignore
/// use std::sync::Arc;
/// use canon_command_store_yugabyte::dispatcher_store::PgDispatcherStore;
///
/// let store = PgDispatcherStore::new(pool, event_store, "Ship");
/// ```
pub struct PgDispatcherStore<ES>
where
    ES: EventStore,
{
    pool: PgPool,
    event_store: ES,
    handler_id: String,
}

impl<ES> Clone for PgDispatcherStore<ES>
where
    ES: EventStore + Clone,
{
    fn clone(&self) -> Self {
        Self {
            pool: self.pool.clone(),
            event_store: self.event_store.clone(),
            handler_id: self.handler_id.clone(),
        }
    }
}

impl<ES> PgDispatcherStore<ES>
where
    ES: EventStore,
{
    /// Create a new PgDispatcherStore.
    ///
    /// `handler_id` is the aggregate type name (e.g., `"Ship"`, `"Station"`)
    /// used to filter inbox messages addressed to this service.
    pub fn new(pool: PgPool, event_store: ES, handler_id: &str) -> Self {
        Self {
            pool,
            event_store,
            handler_id: handler_id.to_owned(),
        }
    }
}

#[async_trait]
impl<ES> DispatcherStore for PgDispatcherStore<ES>
where
    ES: EventStore,
{
    async fn poll_inbox(&self, batch_size: usize) -> Result<Vec<InboxCommandRow>, DispatcherError> {
        let rows: Vec<(String, Uuid, Uuid, Vec<u8>)> = sqlx::query_as(
            "SELECT handler_id, message_id, aggregate_id, payload \
             FROM inbox_messages \
             WHERE handler_id = $1 \
             ORDER BY received_at ASC \
             LIMIT $2 \
             FOR UPDATE SKIP LOCKED",
        )
        .bind(&self.handler_id)
        .bind(batch_size as i64)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| DispatcherError::PollFailed {
            reason: e.to_string(),
        })?;

        let mut result = Vec::with_capacity(rows.len());
        for (handler_id, message_id, aggregate_id, payload) in rows {
            let envelope: CommandEnvelope =
                serde_json::from_slice(&payload).map_err(|e| DispatcherError::PollFailed {
                    reason: format!("failed to deserialize command envelope: {e}"),
                })?;

            result.push(InboxCommandRow {
                handler_id,
                message_id,
                aggregate_id: AggregateId::from_uuid(aggregate_id),
                envelope,
            });
        }

        Ok(result)
    }

    async fn load_events(
        &self,
        aggregate_id: &AggregateId,
    ) -> Result<Vec<EventEnvelope>, DispatcherError> {
        // 1. Load confirmed events from the event store (Cassandra).
        let mut events = self.event_store.load(aggregate_id).await.map_err(|e| {
            DispatcherError::LoadEventsFailed {
                aggregate_id: aggregate_id.clone(),
                reason: e.to_string(),
            }
        })?;

        // 2. Load pending (undelivered) outbox events for this aggregate.
        //    These are events written by previous dispatcher cycles that haven't
        //    yet made it through outbox → Kafka → Cassandra. Without this, a
        //    second command arriving before the first event reaches Cassandra
        //    would see stale state and produce a duplicate version.
        let pending_rows: Vec<(Vec<u8>,)> = sqlx::query_as(
            "SELECT payload FROM outbox \
             WHERE aggregate_id = $1 AND delivered_at IS NULL \
             ORDER BY sequence_number ASC",
        )
        .bind(aggregate_id.as_uuid())
        .fetch_all(&self.pool)
        .await
        .map_err(|e| DispatcherError::LoadEventsFailed {
            aggregate_id: aggregate_id.clone(),
            reason: format!("failed to load pending outbox events: {e}"),
        })?;

        for (payload,) in pending_rows {
            let envelope: EventEnvelope = serde_json::from_slice(&payload).map_err(|e| {
                DispatcherError::LoadEventsFailed {
                    aggregate_id: aggregate_id.clone(),
                    reason: format!("failed to deserialize pending outbox event: {e}"),
                }
            })?;
            events.push(envelope);
        }

        Ok(events)
    }

    async fn write_outbox_and_mark_processed(
        &self,
        message_id: Uuid,
        handler_id: &str,
        envelope: EventEnvelope,
    ) -> Result<(), DispatcherError> {
        let payload =
            serde_json::to_vec(&envelope).map_err(|e| DispatcherError::OutboxWriteFailed {
                reason: format!("failed to serialize event envelope: {e}"),
            })?;

        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| DispatcherError::OutboxWriteFailed {
                reason: format!("failed to begin transaction: {e}"),
            })?;

        // Lock the inbox row with FOR UPDATE SKIP LOCKED to prevent
        // concurrent dispatchers from processing the same message.
        // If the row is already locked or deleted, this returns 0 rows
        // and we skip processing (another dispatcher claimed it).
        let locked: Option<(Uuid,)> = sqlx::query_as(
            "SELECT message_id FROM inbox_messages \
             WHERE handler_id = $1 AND message_id = $2 \
             FOR UPDATE SKIP LOCKED",
        )
        .bind(handler_id)
        .bind(message_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(|e| DispatcherError::MarkProcessedFailed {
            message_id,
            reason: format!("failed to lock inbox message: {e}"),
        })?;

        if locked.is_none() {
            // Another dispatcher already claimed or processed this message.
            tx.rollback()
                .await
                .map_err(|e| DispatcherError::OutboxWriteFailed {
                    reason: format!("failed to rollback transaction: {e}"),
                })?;
            return Ok(());
        }

        // Insert into outbox
        sqlx::query(
            "INSERT INTO outbox (aggregate_id, payload, created_at) \
             VALUES ($1, $2, now())",
        )
        .bind(envelope.aggregate_id.as_uuid())
        .bind(&payload)
        .execute(&mut *tx)
        .await
        .map_err(|e| DispatcherError::OutboxWriteFailed {
            reason: format!("failed to insert into outbox: {e}"),
        })?;

        // Delete from inbox_messages (mark processed)
        sqlx::query(
            "DELETE FROM inbox_messages \
             WHERE handler_id = $1 AND message_id = $2",
        )
        .bind(handler_id)
        .bind(message_id)
        .execute(&mut *tx)
        .await
        .map_err(|e| DispatcherError::MarkProcessedFailed {
            message_id,
            reason: format!("failed to delete inbox message: {e}"),
        })?;

        tx.commit()
            .await
            .map_err(|e| DispatcherError::OutboxWriteFailed {
                reason: format!("failed to commit transaction: {e}"),
            })?;

        Ok(())
    }

    async fn record_failure(
        &self,
        message_id: Uuid,
        handler_id: &str,
        error: &str,
    ) -> Result<u32, DispatcherError> {
        // Upsert into retry_attempts: increment counter, record the error
        // and handler_id for diagnostics.
        let row: (i32,) = sqlx::query_as(
            "INSERT INTO retry_attempts (message_id, handler_id, attempts, last_attempted) \
             VALUES ($1, $2, 1, now()) \
             ON CONFLICT (message_id) \
             DO UPDATE SET attempts = retry_attempts.attempts + 1, \
                           last_attempted = now() \
             RETURNING attempts",
        )
        .bind(message_id)
        .bind(handler_id)
        .fetch_one(&self.pool)
        .await
        .map_err(|e| DispatcherError::RetryRecordFailed {
            message_id,
            reason: format!("failed to upsert retry_attempts: {e}"),
        })?;

        let _ = error; // Error text is logged by the dispatcher; retained for future audit table use.
        Ok(row.0 as u32)
    }

    async fn dead_letter(
        &self,
        row: &InboxCommandRow,
        error: &str,
        attempts: u32,
    ) -> Result<(), DispatcherError> {
        let payload =
            serde_json::to_vec(&row.envelope).map_err(|e| DispatcherError::DeadLetterFailed {
                message_id: row.message_id,
                reason: format!("failed to serialize command envelope: {e}"),
            })?;

        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| DispatcherError::DeadLetterFailed {
                message_id: row.message_id,
                reason: format!("failed to begin transaction: {e}"),
            })?;

        // Insert into dead_letters
        sqlx::query(
            "INSERT INTO dead_letters (message_id, handler_id, aggregate_id, payload, error, attempts, created_at, last_attempted) \
             VALUES ($1, $2, $3, $4, $5, $6, now(), now())",
        )
        .bind(row.message_id)
        .bind(&row.handler_id)
        .bind(row.aggregate_id.as_uuid())
        .bind(&payload)
        .bind(error)
        .bind(attempts as i32)
        .execute(&mut *tx)
        .await
        .map_err(|e| DispatcherError::DeadLetterFailed {
            message_id: row.message_id,
            reason: format!("failed to insert dead letter: {e}"),
        })?;

        // Remove from inbox_messages
        sqlx::query("DELETE FROM inbox_messages WHERE handler_id = $1 AND message_id = $2")
            .bind(&row.handler_id)
            .bind(row.message_id)
            .execute(&mut *tx)
            .await
            .map_err(|e| DispatcherError::DeadLetterFailed {
                message_id: row.message_id,
                reason: format!("failed to delete inbox message: {e}"),
            })?;

        // Clean up retry_attempts
        sqlx::query("DELETE FROM retry_attempts WHERE message_id = $1")
            .bind(row.message_id)
            .execute(&mut *tx)
            .await
            .map_err(|e| DispatcherError::DeadLetterFailed {
                message_id: row.message_id,
                reason: format!("failed to clean up retry_attempts: {e}"),
            })?;

        tx.commit()
            .await
            .map_err(|e| DispatcherError::DeadLetterFailed {
                message_id: row.message_id,
                reason: format!("failed to commit dead letter transaction: {e}"),
            })?;

        Ok(())
    }
}
