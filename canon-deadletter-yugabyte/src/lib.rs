//! YugabyteDB-backed implementations for the dead letter subsystem.
//!
//! Provides:
//! - [`YugabyteDeadLetterStore`] — stores dead-lettered messages in the `dead_letters` table.
//! - [`YugabyteRetryTracker`] — crash-safe retry counting via the `retry_attempts` table.

mod retry_tracker;

pub use retry_tracker::{YugabyteRetryTracker, YugabyteRetryTrackerError};

use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use sqlx::PgPool;
use uuid::Uuid;

use canon_core::traits::DeadLetterStore;
use canon_core::{AggregateId, DeadLetter};

// ── Error type ──────────────────────────────────────────────────────────────

#[derive(Debug, thiserror::Error)]
pub enum YugabyteDeadLetterStoreError {
    #[error("database error: {0}")]
    Database(#[from] sqlx::Error),

    #[error("environment error: {0}")]
    Env(#[from] std::env::VarError),

    #[error("dead letter not found: {id}")]
    NotFound { id: Uuid },
}

// ── Store ───────────────────────────────────────────────────────────────────

/// YugabyteDB-backed dead letter store.
///
/// Persists failed messages that have exhausted their retry budget. Provides
/// list, requeue, and discard operations for admin tooling.
#[derive(Clone)]
pub struct YugabyteDeadLetterStore {
    pool: PgPool,
}

impl YugabyteDeadLetterStore {
    /// Create a new store from an existing connection pool.
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Create a new store from the `YUGABYTE_URL` environment variable.
    pub async fn from_env() -> Result<Self, YugabyteDeadLetterStoreError> {
        let url = std::env::var("YUGABYTE_URL")?;
        let pool = PgPool::connect(&url).await?;
        Ok(Self { pool })
    }

    /// Returns a reference to the underlying connection pool.
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }
}

// ── Trait impl ──────────────────────────────────────────────────────────────

#[async_trait]
impl DeadLetterStore for YugabyteDeadLetterStore {
    type Error = YugabyteDeadLetterStoreError;

    async fn store(
        &self,
        message_id: Uuid,
        handler_id: &str,
        aggregate_id: &AggregateId,
        payload: Bytes,
        error: &str,
    ) -> Result<Uuid, Self::Error> {
        let id = Uuid::new_v4();
        let now = Utc::now();

        sqlx::query(
            "INSERT INTO dead_letters \
             (id, message_id, handler_id, aggregate_id, payload, error, attempts, \
              created_at, last_attempted) \
             VALUES ($1, $2, $3, $4, $5, $6, 1, $7, $8) \
             ON CONFLICT (id) DO NOTHING",
        )
        .bind(id)
        .bind(message_id)
        .bind(handler_id)
        .bind(aggregate_id.as_uuid())
        .bind(payload.as_ref())
        .bind(error)
        .bind(now)
        .bind(now)
        .execute(&self.pool)
        .await?;

        Ok(id)
    }

    async fn list(&self, handler_id: Option<&str>) -> Result<Vec<DeadLetter>, Self::Error> {
        let rows = match handler_id {
            Some(hid) => {
                sqlx::query_as::<_, DeadLetterRow>(
                    "SELECT id, message_id, handler_id, aggregate_id, payload, error, \
                     attempts, created_at, last_attempted \
                     FROM dead_letters \
                     WHERE handler_id = $1 \
                     ORDER BY created_at ASC",
                )
                .bind(hid)
                .fetch_all(&self.pool)
                .await?
            }
            None => {
                sqlx::query_as::<_, DeadLetterRow>(
                    "SELECT id, message_id, handler_id, aggregate_id, payload, error, \
                     attempts, created_at, last_attempted \
                     FROM dead_letters \
                     ORDER BY created_at ASC",
                )
                .fetch_all(&self.pool)
                .await?
            }
        };

        Ok(rows.into_iter().map(Into::into).collect())
    }

    async fn requeue(&self, id: Uuid) -> Result<(), Self::Error> {
        let result = sqlx::query("DELETE FROM dead_letters WHERE id = $1")
            .bind(id)
            .execute(&self.pool)
            .await?;

        if result.rows_affected() == 0 {
            return Err(YugabyteDeadLetterStoreError::NotFound { id });
        }

        Ok(())
    }

    async fn discard(&self, id: Uuid) -> Result<(), Self::Error> {
        let result = sqlx::query("DELETE FROM dead_letters WHERE id = $1")
            .bind(id)
            .execute(&self.pool)
            .await?;

        if result.rows_affected() == 0 {
            return Err(YugabyteDeadLetterStoreError::NotFound { id });
        }

        Ok(())
    }
}

// ── Internal row type ───────────────────────────────────────────────────────

/// Internal row type for mapping SQL results to [`DeadLetter`].
#[derive(sqlx::FromRow)]
struct DeadLetterRow {
    id: Uuid,
    message_id: Uuid,
    handler_id: String,
    aggregate_id: Uuid,
    payload: Vec<u8>,
    error: String,
    attempts: i32,
    created_at: DateTime<Utc>,
    last_attempted: DateTime<Utc>,
}

impl From<DeadLetterRow> for DeadLetter {
    fn from(row: DeadLetterRow) -> Self {
        Self {
            id: row.id,
            message_id: row.message_id,
            handler_id: row.handler_id,
            aggregate_id: AggregateId::from_uuid(row.aggregate_id),
            payload: Bytes::from(row.payload),
            error: row.error,
            attempts: row.attempts as u32,
            created_at: row.created_at,
            last_attempted: row.last_attempted,
        }
    }
}

// ── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use canon_core::AggregateId;

    async fn setup_schema(pool: &PgPool) {
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS dead_letters (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                message_id UUID,
                handler_id TEXT,
                aggregate_id UUID,
                payload BYTEA,
                error TEXT,
                attempts INT DEFAULT 1,
                created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                last_attempted TIMESTAMPTZ NOT NULL DEFAULT now()
            )",
        )
        .execute(pool)
        .await
        .expect("create dead_letters table");
    }

    #[sqlx::test(migrations = false)]
    #[ignore = "requires DATABASE_URL"]
    async fn test_store_and_list(pool: PgPool) {
        setup_schema(&pool).await;
        let store = YugabyteDeadLetterStore::new(pool);
        let agg_id = AggregateId::new();
        let msg_id = Uuid::new_v4();

        let dl_id = store
            .store(
                msg_id,
                "handler-1",
                &agg_id,
                Bytes::from_static(b"{}"),
                "boom",
            )
            .await
            .expect("store failed");

        let all = store.list(None).await.expect("list failed");
        assert_eq!(all.len(), 1);
        assert_eq!(all[0].id, dl_id);
        assert_eq!(all[0].message_id, msg_id);
        assert_eq!(all[0].handler_id, "handler-1");
        assert_eq!(all[0].error, "boom");
        assert_eq!(all[0].attempts, 1);
    }

    #[sqlx::test(migrations = false)]
    #[ignore = "requires DATABASE_URL"]
    async fn test_list_filtered_by_handler(pool: PgPool) {
        setup_schema(&pool).await;
        let store = YugabyteDeadLetterStore::new(pool);
        let agg_id = AggregateId::new();

        store
            .store(
                Uuid::new_v4(),
                "h1",
                &agg_id,
                Bytes::from_static(b"{}"),
                "err1",
            )
            .await
            .expect("store h1");
        store
            .store(
                Uuid::new_v4(),
                "h2",
                &agg_id,
                Bytes::from_static(b"{}"),
                "err2",
            )
            .await
            .expect("store h2");

        let all = store.list(None).await.expect("list all");
        assert_eq!(all.len(), 2);

        let h1_only = store.list(Some("h1")).await.expect("list h1");
        assert_eq!(h1_only.len(), 1);
        assert_eq!(h1_only[0].handler_id, "h1");

        let h2_only = store.list(Some("h2")).await.expect("list h2");
        assert_eq!(h2_only.len(), 1);
        assert_eq!(h2_only[0].handler_id, "h2");
    }

    #[sqlx::test(migrations = false)]
    #[ignore = "requires DATABASE_URL"]
    async fn test_requeue_removes_entry(pool: PgPool) {
        setup_schema(&pool).await;
        let store = YugabyteDeadLetterStore::new(pool);
        let agg_id = AggregateId::new();

        let dl_id = store
            .store(
                Uuid::new_v4(),
                "h1",
                &agg_id,
                Bytes::from_static(b"{}"),
                "err",
            )
            .await
            .expect("store");

        store.requeue(dl_id).await.expect("requeue");
        let remaining = store.list(None).await.expect("list");
        assert!(remaining.is_empty());
    }

    #[sqlx::test(migrations = false)]
    #[ignore = "requires DATABASE_URL"]
    async fn test_discard_removes_entry(pool: PgPool) {
        setup_schema(&pool).await;
        let store = YugabyteDeadLetterStore::new(pool);
        let agg_id = AggregateId::new();

        let dl_id = store
            .store(
                Uuid::new_v4(),
                "h1",
                &agg_id,
                Bytes::from_static(b"{}"),
                "err",
            )
            .await
            .expect("store");

        store.discard(dl_id).await.expect("discard");
        let remaining = store.list(None).await.expect("list");
        assert!(remaining.is_empty());
    }

    #[sqlx::test(migrations = false)]
    #[ignore = "requires DATABASE_URL"]
    async fn test_requeue_nonexistent_returns_err(pool: PgPool) {
        setup_schema(&pool).await;
        let store = YugabyteDeadLetterStore::new(pool);
        let result = store.requeue(Uuid::new_v4()).await;
        assert!(matches!(
            result,
            Err(YugabyteDeadLetterStoreError::NotFound { .. })
        ));
    }

    #[sqlx::test(migrations = false)]
    #[ignore = "requires DATABASE_URL"]
    async fn test_discard_nonexistent_returns_err(pool: PgPool) {
        setup_schema(&pool).await;
        let store = YugabyteDeadLetterStore::new(pool);
        let result = store.discard(Uuid::new_v4()).await;
        assert!(matches!(
            result,
            Err(YugabyteDeadLetterStoreError::NotFound { .. })
        ));
    }
}
