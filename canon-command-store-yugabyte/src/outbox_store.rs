//! YugabyteDB-backed implementation of [`OutboxStore`].
//!
//! Polls the `outbox` table for uncommitted event envelopes using
//! `SELECT ... FOR UPDATE SKIP LOCKED` to prevent double-processing
//! across replicas, and marks rows as delivered after confirmed publish.

use async_trait::async_trait;
use sqlx::PgPool;
use uuid::Uuid;

use canon_core::outbox::{OutboxEntry, OutboxProcessorError, OutboxStore};
use canon_core::{AggregateId, EventEnvelope};

/// YugabyteDB-backed outbox store.
///
/// Implements the [`OutboxStore`] trait by reading from and writing to the
/// `outbox` table. The event envelope payload is stored as JSON-encoded
/// `BYTEA` and deserialized back into [`EventEnvelope`] on poll.
///
/// # Transactional inserts
///
/// Use [`insert_in_tx`](Self::insert_in_tx) to add outbox entries within
/// an existing database transaction — this is the method the command handler
/// write path **must** use so that the command INSERT and outbox INSERT(s)
/// happen inside a single YugabyteDB ACID transaction.
#[derive(Clone)]
pub struct YugabyteOutboxStore {
    pool: PgPool,
}

impl YugabyteOutboxStore {
    /// Create a new outbox store from an existing connection pool.
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Create a new store from the `YUGABYTE_URL` environment variable.
    pub async fn from_env() -> Result<Self, OutboxStoreError> {
        let url =
            std::env::var("YUGABYTE_URL").map_err(|e| OutboxStoreError::Env(e.to_string()))?;
        let pool = PgPool::connect(&url)
            .await
            .map_err(OutboxStoreError::Database)?;
        Ok(Self { pool })
    }

    /// Returns a reference to the underlying connection pool.
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// Insert an event envelope into the outbox within an existing transaction.
    ///
    /// The caller is responsible for beginning and committing the transaction.
    /// This allows the command store and outbox store to share the same ACID
    /// transaction:
    ///
    /// ```rust,ignore
    /// let mut tx = pool.begin().await?;
    /// command_store.append_in_tx(&mut tx, command).await?;
    /// outbox_store.insert_in_tx(&mut tx, envelope).await?;
    /// tx.commit().await?;
    /// ```
    pub async fn insert_in_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        envelope: &EventEnvelope,
    ) -> Result<(), OutboxStoreError> {
        let payload = serde_json::to_vec(envelope)
            .map_err(|e| OutboxStoreError::Serialization(e.to_string()))?;

        sqlx::query(
            "INSERT INTO outbox (aggregate_id, payload, created_at) \
             VALUES ($1, $2, now())",
        )
        .bind(envelope.aggregate_id.as_uuid())
        .bind(&payload)
        .execute(&mut **tx)
        .await
        .map_err(OutboxStoreError::Database)?;

        Ok(())
    }
}

#[async_trait]
impl OutboxStore for YugabyteOutboxStore {
    async fn poll_undelivered(
        &self,
        batch_size: usize,
    ) -> Result<Vec<OutboxEntry>, OutboxProcessorError> {
        let rows = sqlx::query_as::<_, OutboxRow>(
            "SELECT id, sequence_number, aggregate_id, payload \
             FROM outbox \
             WHERE delivered_at IS NULL \
             ORDER BY sequence_number ASC \
             LIMIT $1 \
             FOR UPDATE SKIP LOCKED",
        )
        .bind(batch_size as i64)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| OutboxProcessorError::PollFailed {
            reason: e.to_string(),
        })?;

        let mut entries = Vec::with_capacity(rows.len());
        for row in rows {
            let envelope: EventEnvelope = serde_json::from_slice(&row.payload).map_err(|e| {
                OutboxProcessorError::PollFailed {
                    reason: format!(
                        "failed to deserialize event envelope for outbox entry {}: {e}",
                        row.id
                    ),
                }
            })?;

            entries.push(OutboxEntry {
                id: row.id,
                sequence_number: row.sequence_number,
                aggregate_id: AggregateId::from_uuid(row.aggregate_id),
                envelope,
            });
        }

        Ok(entries)
    }

    async fn mark_delivered(&self, entry_id: Uuid) -> Result<(), OutboxProcessorError> {
        let result = sqlx::query("UPDATE outbox SET delivered_at = now() WHERE id = $1")
            .bind(entry_id)
            .execute(&self.pool)
            .await
            .map_err(|e| OutboxProcessorError::MarkDeliveredFailed {
                entry_id,
                reason: e.to_string(),
            })?;

        if result.rows_affected() == 0 {
            return Err(OutboxProcessorError::MarkDeliveredFailed {
                entry_id,
                reason: "entry not found".into(),
            });
        }

        Ok(())
    }
}

/// Internal row type for mapping outbox SQL results.
#[derive(sqlx::FromRow)]
struct OutboxRow {
    id: Uuid,
    sequence_number: i64,
    aggregate_id: Uuid,
    payload: Vec<u8>,
}

/// Errors specific to the YugabyteDB outbox store setup and transactional
/// insert operations. Polling and mark-delivered errors use
/// [`OutboxProcessorError`] from canon-core.
#[derive(Debug, thiserror::Error)]
pub enum OutboxStoreError {
    /// A database operation failed.
    #[error("database error: {0}")]
    Database(#[from] sqlx::Error),

    /// An environment variable was missing or invalid.
    #[error("environment error: {0}")]
    Env(String),

    /// Failed to serialize an event envelope to JSON.
    #[error("serialization error: {0}")]
    Serialization(String),
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use canon_core::{AggregateId, Version};
    use chrono::Utc;

    fn make_envelope(aggregate_id: &AggregateId) -> EventEnvelope {
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

    async fn setup_schema(pool: &PgPool) {
        // Create sequence (idempotent via DO block)
        sqlx::query(
            "DO $$ BEGIN CREATE SEQUENCE outbox_seq; EXCEPTION WHEN duplicate_table THEN NULL; END $$",
        )
        .execute(pool)
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
        .execute(pool)
        .await
        .expect("create table");

        sqlx::query(
            "CREATE INDEX IF NOT EXISTS outbox_seq_idx \
             ON outbox (sequence_number) WHERE delivered_at IS NULL",
        )
        .execute(pool)
        .await
        .expect("create index");
    }

    #[sqlx::test(migrations = false)]
    #[ignore = "requires DATABASE_URL"]
    async fn test_insert_and_poll(pool: PgPool) {
        setup_schema(&pool).await;
        let store = YugabyteOutboxStore::new(pool.clone());
        let agg_id = AggregateId::new();
        let env = make_envelope(&agg_id);

        // Insert via transaction
        let mut tx = pool.begin().await.expect("begin tx");
        store
            .insert_in_tx(&mut tx, &env)
            .await
            .expect("insert_in_tx");
        tx.commit().await.expect("commit");

        // Poll should return the entry
        let entries = store.poll_undelivered(10).await.expect("poll_undelivered");
        assert_eq!(entries.len(), 1);
        assert_eq!(*entries[0].aggregate_id.as_uuid(), *agg_id.as_uuid());
        assert_eq!(entries[0].envelope.event_id, env.event_id);
    }

    #[sqlx::test(migrations = false)]
    #[ignore = "requires DATABASE_URL"]
    async fn test_mark_delivered_excludes_from_poll(pool: PgPool) {
        setup_schema(&pool).await;
        let store = YugabyteOutboxStore::new(pool.clone());
        let agg_id = AggregateId::new();
        let env = make_envelope(&agg_id);

        let mut tx = pool.begin().await.expect("begin tx");
        store
            .insert_in_tx(&mut tx, &env)
            .await
            .expect("insert_in_tx");
        tx.commit().await.expect("commit");

        let entries = store.poll_undelivered(10).await.expect("poll_undelivered");
        assert_eq!(entries.len(), 1);
        let entry_id = entries[0].id;

        store
            .mark_delivered(entry_id)
            .await
            .expect("mark_delivered");

        let entries_after = store
            .poll_undelivered(10)
            .await
            .expect("poll_undelivered after mark");
        assert!(entries_after.is_empty());
    }

    #[sqlx::test(migrations = false)]
    #[ignore = "requires DATABASE_URL"]
    async fn test_mark_delivered_unknown_id(pool: PgPool) {
        setup_schema(&pool).await;
        let store = YugabyteOutboxStore::new(pool);
        let result = store.mark_delivered(Uuid::new_v4()).await;
        assert!(result.is_err());
    }

    #[sqlx::test(migrations = false)]
    #[ignore = "requires DATABASE_URL"]
    async fn test_poll_respects_batch_size(pool: PgPool) {
        setup_schema(&pool).await;
        let store = YugabyteOutboxStore::new(pool.clone());
        let agg_id = AggregateId::new();

        for _ in 0..5 {
            let env = make_envelope(&agg_id);
            let mut tx = pool.begin().await.expect("begin tx");
            store
                .insert_in_tx(&mut tx, &env)
                .await
                .expect("insert_in_tx");
            tx.commit().await.expect("commit");
        }

        let entries = store.poll_undelivered(3).await.expect("poll_undelivered");
        assert_eq!(entries.len(), 3);
    }

    #[sqlx::test(migrations = false)]
    #[ignore = "requires DATABASE_URL"]
    async fn test_poll_returns_in_sequence_order(pool: PgPool) {
        setup_schema(&pool).await;
        let store = YugabyteOutboxStore::new(pool.clone());
        let agg_id = AggregateId::new();

        for _ in 0..3 {
            let env = make_envelope(&agg_id);
            let mut tx = pool.begin().await.expect("begin tx");
            store
                .insert_in_tx(&mut tx, &env)
                .await
                .expect("insert_in_tx");
            tx.commit().await.expect("commit");
        }

        let entries = store.poll_undelivered(10).await.expect("poll_undelivered");
        assert_eq!(entries.len(), 3);
        assert!(entries[0].sequence_number < entries[1].sequence_number);
        assert!(entries[1].sequence_number < entries[2].sequence_number);
    }

    #[sqlx::test(migrations = false)]
    #[ignore = "requires DATABASE_URL"]
    async fn test_insert_rollback_not_visible(pool: PgPool) {
        setup_schema(&pool).await;
        let store = YugabyteOutboxStore::new(pool.clone());
        let agg_id = AggregateId::new();
        let env = make_envelope(&agg_id);

        // Insert inside a transaction but do NOT commit
        {
            let mut tx = pool.begin().await.expect("begin tx");
            store
                .insert_in_tx(&mut tx, &env)
                .await
                .expect("insert_in_tx");
            // tx dropped without commit -> implicit rollback
        }

        let entries = store.poll_undelivered(10).await.expect("poll_undelivered");
        assert!(
            entries.is_empty(),
            "rolled-back entry should not be visible"
        );
    }
}
