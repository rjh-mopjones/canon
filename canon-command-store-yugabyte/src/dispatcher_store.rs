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
use canon_core::dispatcher::{DispatcherError, DispatcherStore, InboxCommandRow, ReadyWindow};
use canon_core::traits::EventStore;
use canon_core::{AggregateId, CommandEnvelope, EventEnvelope, IncomingMessage};
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

    async fn poll_ready_windows(
        &self,
        batch_size: usize,
    ) -> Result<Vec<ReadyWindow>, DispatcherError> {
        let rows: Vec<(Uuid, String, Uuid, Option<String>, Vec<u8>)> = sqlx::query_as(
            "SELECT window_id, handler_id, aggregate_id, message_type, messages \
             FROM inbox_windows \
             WHERE status = 'ready' \
             ORDER BY updated_at ASC \
             LIMIT $1 \
             FOR UPDATE SKIP LOCKED",
        )
        .bind(batch_size as i64)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| DispatcherError::PollFailed {
            reason: format!("failed to poll ready windows: {e}"),
        })?;

        let mut result = Vec::with_capacity(rows.len());
        for (window_id, handler_id, correlation_key, message_type, messages_json) in rows {
            let message_type = message_type.unwrap_or_else(|| "internal".to_owned());
            let envelopes: Vec<EventEnvelope> =
                serde_json::from_slice(&messages_json).map_err(|e| {
                    DispatcherError::PollFailed {
                        reason: format!("failed to deserialize window envelopes: {e}"),
                    }
                })?;

            let messages: Vec<IncomingMessage> = envelopes
                .into_iter()
                .map(|e| match message_type.as_str() {
                    "external" => IncomingMessage::ExternalEvent(e),
                    _ => IncomingMessage::InternalEvent(e),
                })
                .collect();

            result.push(ReadyWindow {
                window_id,
                handler_id,
                correlation_key,
                messages,
            });
        }

        Ok(result)
    }

    async fn mark_window_dispatched(
        &self,
        window_id: Uuid,
        handler_id: &str,
    ) -> Result<(), DispatcherError> {
        // Delete the window row — the dispatcher has processed it.
        sqlx::query(
            "DELETE FROM inbox_windows \
             WHERE window_id = $1 AND handler_id = $2",
        )
        .bind(window_id)
        .bind(handler_id)
        .execute(&self.pool)
        .await
        .map_err(|e| DispatcherError::WindowDispatchFailed {
            window_id,
            reason: format!("failed to mark window dispatched: {e}"),
        })?;

        Ok(())
    }

    async fn write_command_to_inbox(
        &self,
        handler_id: &str,
        command: CommandEnvelope,
    ) -> Result<(), DispatcherError> {
        let envelope_json =
            serde_json::to_vec(&command).map_err(|e| DispatcherError::CommandReentryFailed {
                reason: format!("failed to serialize command: {e}"),
            })?;

        sqlx::query(
            "INSERT INTO inbox_messages (handler_id, message_id, aggregate_id, message_type, payload) \
             VALUES ($1, $2, $3, 'command', $4) \
             ON CONFLICT DO NOTHING",
        )
        .bind(handler_id)
        .bind(command.command_id)
        .bind(command.aggregate_id.as_uuid())
        .bind(&envelope_json)
        .execute(&self.pool)
        .await
        .map_err(|e| DispatcherError::CommandReentryFailed {
            reason: format!("failed to write command to inbox: {e}"),
        })?;

        Ok(())
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

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use canon_core::memory::event_store::InMemoryEventStore;
    use canon_core::{AggregateId, Version};
    use chrono::Utc;
    use sqlx::PgPool;
    use testcontainers::runners::AsyncRunner;
    use testcontainers::ContainerAsync;
    use testcontainers_modules::postgres::Postgres;

    fn make_command(aggregate_id: &AggregateId) -> CommandEnvelope {
        CommandEnvelope {
            command_id: Uuid::new_v4(),
            aggregate_id: aggregate_id.clone(),
            command_type: "TestCommand".to_string(),
            correlation_id: Uuid::new_v4(),
            causation_id: Uuid::new_v4(),
            timestamp: Utc::now(),
            payload: Bytes::from_static(b"{}"),
            command_version: 1,
        }
    }

    fn make_event(aggregate_id: &AggregateId) -> EventEnvelope {
        EventEnvelope {
            event_id: Uuid::new_v4(),
            aggregate_id: aggregate_id.clone(),
            version: Version::initial().next(),
            event_type: "TestEvent".into(),
            event_version: 1,
            payload: Bytes::from_static(b"{}"),
            correlation_id: Uuid::new_v4(),
            causation_id: Uuid::new_v4(),
            timestamp: Utc::now(),
        }
    }

    async fn setup_container() -> (ContainerAsync<Postgres>, PgPool) {
        let container = Postgres::default()
            .start()
            .await
            .expect("Failed to start postgres container");

        let port = container.get_host_port_ipv4(5432).await.expect("port");
        let url = format!("postgres://postgres:postgres@127.0.0.1:{}/postgres", port);

        let pool = PgPool::connect(&url).await.expect("connect");

        // Enable pgcrypto for gen_random_uuid() support in standard Postgres
        sqlx::query("CREATE EXTENSION IF NOT EXISTS \"pgcrypto\"")
            .execute(&pool)
            .await
            .expect("create pgcrypto extension");

        // Create inbox_messages table
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS inbox_messages (
                handler_id TEXT NOT NULL,
                message_id UUID NOT NULL,
                aggregate_id UUID NOT NULL,
                payload BYTEA NOT NULL,
                received_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                PRIMARY KEY (handler_id, message_id)
            )",
        )
        .execute(&pool)
        .await
        .expect("create inbox_messages");

        // Create outbox table with sequence
        sqlx::query(
            "DO $$ BEGIN CREATE SEQUENCE outbox_seq; EXCEPTION WHEN duplicate_table THEN NULL; END $$",
        )
        .execute(&pool)
        .await
        .expect("create sequence");

        sqlx::query(
            "CREATE TABLE IF NOT EXISTS outbox (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                sequence_number BIGINT DEFAULT nextval('outbox_seq'),
                aggregate_id UUID,
                payload BYTEA,
                created_at TIMESTAMPTZ DEFAULT now(),
                delivered_at TIMESTAMPTZ
            )",
        )
        .execute(&pool)
        .await
        .expect("create outbox");

        // Create retry_attempts table
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS retry_attempts (
                message_id UUID PRIMARY KEY,
                handler_id TEXT NOT NULL,
                attempts INT NOT NULL DEFAULT 0,
                last_attempted TIMESTAMPTZ NOT NULL DEFAULT now()
            )",
        )
        .execute(&pool)
        .await
        .expect("create retry_attempts");

        // Create dead_letters table
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS dead_letters (
                message_id UUID PRIMARY KEY,
                handler_id TEXT NOT NULL,
                aggregate_id UUID NOT NULL,
                payload BYTEA NOT NULL,
                error TEXT NOT NULL,
                attempts INT NOT NULL DEFAULT 0,
                created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                last_attempted TIMESTAMPTZ NOT NULL DEFAULT now()
            )",
        )
        .execute(&pool)
        .await
        .expect("create dead_letters");

        (container, pool)
    }

    /// Insert an inbox_messages row directly (simulating gateway write).
    async fn insert_inbox_message(pool: &PgPool, handler_id: &str, envelope: &CommandEnvelope) {
        let payload = serde_json::to_vec(envelope).expect("serialize command envelope");
        sqlx::query(
            "INSERT INTO inbox_messages (handler_id, message_id, aggregate_id, payload, received_at) \
             VALUES ($1, $2, $3, $4, now())",
        )
        .bind(handler_id)
        .bind(envelope.command_id)
        .bind(envelope.aggregate_id.as_uuid())
        .bind(&payload)
        .execute(pool)
        .await
        .expect("insert inbox message");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn poll_inbox_returns_empty_when_no_messages() {
        let (_container, pool) = setup_container().await;
        let event_store = InMemoryEventStore::new();
        let store = PgDispatcherStore::new(pool, event_store, "Ship");

        let rows = store.poll_inbox(10).await.expect("poll_inbox");
        assert!(rows.is_empty());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn poll_inbox_returns_messages_for_handler() {
        let (_container, pool) = setup_container().await;
        let event_store = InMemoryEventStore::new();
        let store = PgDispatcherStore::new(pool.clone(), event_store, "Ship");

        let agg_id = AggregateId::new();
        let cmd = make_command(&agg_id);
        let cmd_id = cmd.command_id;

        insert_inbox_message(&pool, "Ship", &cmd).await;

        let rows = store.poll_inbox(10).await.expect("poll_inbox");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].handler_id, "Ship");
        assert_eq!(rows[0].message_id, cmd_id);
        assert_eq!(rows[0].aggregate_id, agg_id);
        assert_eq!(rows[0].envelope.command_type, "TestCommand");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn poll_inbox_filters_by_handler_id() {
        let (_container, pool) = setup_container().await;
        let event_store = InMemoryEventStore::new();
        let store = PgDispatcherStore::new(pool.clone(), event_store, "Ship");

        let agg1 = AggregateId::new();
        let agg2 = AggregateId::new();
        let cmd_ship = make_command(&agg1);
        let cmd_station = make_command(&agg2);

        insert_inbox_message(&pool, "Ship", &cmd_ship).await;
        insert_inbox_message(&pool, "Station", &cmd_station).await;

        let rows = store.poll_inbox(10).await.expect("poll_inbox");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].handler_id, "Ship");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn poll_inbox_respects_batch_size() {
        let (_container, pool) = setup_container().await;
        let event_store = InMemoryEventStore::new();
        let store = PgDispatcherStore::new(pool.clone(), event_store, "Ship");

        for _ in 0..5 {
            let agg_id = AggregateId::new();
            let cmd = make_command(&agg_id);
            insert_inbox_message(&pool, "Ship", &cmd).await;
        }

        let rows = store.poll_inbox(3).await.expect("poll_inbox");
        assert_eq!(rows.len(), 3);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn load_events_delegates_to_event_store() {
        let (_container, pool) = setup_container().await;
        let event_store = InMemoryEventStore::new();
        let agg_id = AggregateId::new();

        // Pre-populate the in-memory event store
        let evt = make_event(&agg_id);
        event_store
            .append(&agg_id, Version::initial(), vec![evt.clone()])
            .expect("append event");

        let store = PgDispatcherStore::new(pool, event_store, "Ship");

        let events = store.load_events(&agg_id).await.expect("load_events");
        assert_eq!(events.len(), 1);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn load_events_returns_empty_for_unknown_aggregate() {
        let (_container, pool) = setup_container().await;
        let event_store = InMemoryEventStore::new();
        let store = PgDispatcherStore::new(pool, event_store, "Ship");

        let agg_id = AggregateId::new();
        let events = store.load_events(&agg_id).await.expect("load_events");
        assert!(events.is_empty());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn write_outbox_and_mark_processed_roundtrip() {
        let (_container, pool) = setup_container().await;
        let event_store = InMemoryEventStore::new();
        let store = PgDispatcherStore::new(pool.clone(), event_store, "Ship");

        let agg_id = AggregateId::new();
        let cmd = make_command(&agg_id);
        let message_id = cmd.command_id;

        // Insert an inbox message first
        insert_inbox_message(&pool, "Ship", &cmd).await;

        // Verify the inbox message exists
        let pre_rows = store.poll_inbox(10).await.expect("poll_inbox");
        assert_eq!(pre_rows.len(), 1);

        // Write to outbox and mark processed
        let evt = make_event(&agg_id);
        store
            .write_outbox_and_mark_processed(message_id, "Ship", evt.clone())
            .await
            .expect("write_outbox_and_mark_processed");

        // Inbox message should be gone
        let post_rows = store.poll_inbox(10).await.expect("poll_inbox after");
        assert!(post_rows.is_empty());

        // Outbox should have the event
        let outbox_row: (Uuid,) = sqlx::query_as("SELECT aggregate_id FROM outbox LIMIT 1")
            .fetch_one(&pool)
            .await
            .expect("select outbox");
        assert_eq!(outbox_row.0, *agg_id.as_uuid());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn write_outbox_skips_when_message_already_claimed() {
        let (_container, pool) = setup_container().await;
        let event_store = InMemoryEventStore::new();
        let store = PgDispatcherStore::new(pool.clone(), event_store, "Ship");

        let agg_id = AggregateId::new();
        let evt = make_event(&agg_id);

        // Call with a message_id that does not exist in inbox_messages
        // (simulates another dispatcher already having processed it).
        let nonexistent_id = Uuid::new_v4();
        store
            .write_outbox_and_mark_processed(nonexistent_id, "Ship", evt)
            .await
            .expect("should succeed (skip locked)");

        // No outbox entries should have been written
        let count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM outbox")
            .fetch_one(&pool)
            .await
            .expect("count outbox");
        assert_eq!(count.0, 0);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn record_failure_increments_count() {
        let (_container, pool) = setup_container().await;
        let event_store = InMemoryEventStore::new();
        let store = PgDispatcherStore::new(pool.clone(), event_store, "Ship");

        let message_id = Uuid::new_v4();

        // First failure
        let count1 = store
            .record_failure(message_id, "Ship", "something went wrong")
            .await
            .expect("record_failure 1");
        assert_eq!(count1, 1);

        // Second failure
        let count2 = store
            .record_failure(message_id, "Ship", "still wrong")
            .await
            .expect("record_failure 2");
        assert_eq!(count2, 2);

        // Third failure
        let count3 = store
            .record_failure(message_id, "Ship", "still broken")
            .await
            .expect("record_failure 3");
        assert_eq!(count3, 3);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn dead_letter_moves_message_and_cleans_up() {
        let (_container, pool) = setup_container().await;
        let event_store = InMemoryEventStore::new();
        let store = PgDispatcherStore::new(pool.clone(), event_store, "Ship");

        let agg_id = AggregateId::new();
        let cmd = make_command(&agg_id);
        let message_id = cmd.command_id;

        // Insert inbox message
        insert_inbox_message(&pool, "Ship", &cmd).await;

        // Record some retry attempts
        store
            .record_failure(message_id, "Ship", "error 1")
            .await
            .expect("record_failure");
        store
            .record_failure(message_id, "Ship", "error 2")
            .await
            .expect("record_failure");

        // Build the InboxCommandRow
        let row = InboxCommandRow {
            handler_id: "Ship".to_owned(),
            message_id,
            aggregate_id: agg_id.clone(),
            envelope: cmd,
        };

        // Dead letter it
        store
            .dead_letter(&row, "permanent failure", 2)
            .await
            .expect("dead_letter");

        // Inbox message should be deleted
        let inbox_count: (i64,) =
            sqlx::query_as("SELECT COUNT(*) FROM inbox_messages WHERE message_id = $1")
                .bind(message_id)
                .fetch_one(&pool)
                .await
                .expect("count inbox");
        assert_eq!(inbox_count.0, 0);

        // Dead letter should exist
        let dl_row: (Uuid, String, i32) = sqlx::query_as(
            "SELECT message_id, error, attempts FROM dead_letters WHERE message_id = $1",
        )
        .bind(message_id)
        .fetch_one(&pool)
        .await
        .expect("select dead_letter");
        assert_eq!(dl_row.0, message_id);
        assert_eq!(dl_row.1, "permanent failure");
        assert_eq!(dl_row.2, 2);

        // Retry attempts should be cleaned up
        let retry_count: (i64,) =
            sqlx::query_as("SELECT COUNT(*) FROM retry_attempts WHERE message_id = $1")
                .bind(message_id)
                .fetch_one(&pool)
                .await
                .expect("count retry_attempts");
        assert_eq!(retry_count.0, 0);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn clone_works() {
        let (_container, pool) = setup_container().await;
        let event_store = InMemoryEventStore::new();
        let store = PgDispatcherStore::new(pool, event_store, "Ship");
        let store2 = store.clone();

        // Both should work independently
        let rows = store.poll_inbox(10).await.expect("poll_inbox original");
        assert!(rows.is_empty());
        let rows2 = store2.poll_inbox(10).await.expect("poll_inbox clone");
        assert!(rows2.is_empty());
    }
}
