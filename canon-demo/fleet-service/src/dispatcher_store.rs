//! YugabyteDB + Cassandra backed dispatcher store.
//!
//! Implements `DispatcherStore` for the fleet service by:
//! - Polling inbox_messages from YugabyteDB
//! - Loading events from Cassandra via the event store
//! - Writing events to the outbox table and marking inbox messages as processed

use std::sync::Arc;

use async_trait::async_trait;
use canon_core::dispatcher::{DispatcherError, DispatcherStore, InboxCommandRow};
use canon_core::{AggregateId, CommandEnvelope, EventEnvelope, Version};
use canon_event_store_cassandra::CassandraEventStore;
use sqlx::PgPool;
use uuid::Uuid;

/// Dispatcher store backed by YugabyteDB (inbox, outbox, commands) and
/// Cassandra (event history for aggregate hydration).
#[derive(Clone)]
pub struct PgDispatcherStore {
    pool: PgPool,
    event_store: Arc<CassandraEventStore>,
    handler_id: String,
}

impl PgDispatcherStore {
    /// Create a new PgDispatcherStore.
    ///
    /// `handler_id` is the aggregate type name (e.g., "Ship") used to filter
    /// inbox messages addressed to this service.
    pub fn new(pool: PgPool, event_store: Arc<CassandraEventStore>, handler_id: &str) -> Self {
        Self {
            pool,
            event_store,
            handler_id: handler_id.to_owned(),
        }
    }
}

#[async_trait]
impl DispatcherStore for PgDispatcherStore {
    async fn poll_inbox(&self, batch_size: usize) -> Result<Vec<InboxCommandRow>, DispatcherError> {
        // Select unprocessed commands from inbox_messages for this handler.
        let rows: Vec<(String, Uuid, Uuid, Vec<u8>)> = sqlx::query_as(
            "SELECT handler_id, message_id, aggregate_id, payload \
             FROM inbox_messages \
             WHERE handler_id = $1 \
             ORDER BY received_at ASC \
             LIMIT $2",
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
        use canon_core::traits::EventStore;
        self.event_store
            .load(aggregate_id)
            .await
            .map_err(|e| DispatcherError::LoadEventsFailed {
                aggregate_id: aggregate_id.clone(),
                reason: e.to_string(),
            })
    }

    async fn current_version(
        &self,
        aggregate_id: &AggregateId,
    ) -> Result<Version, DispatcherError> {
        use canon_core::traits::EventStore;
        self.event_store
            .current_version(aggregate_id)
            .await
            .map_err(|e| DispatcherError::LoadEventsFailed {
                aggregate_id: aggregate_id.clone(),
                reason: e.to_string(),
            })
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
}
