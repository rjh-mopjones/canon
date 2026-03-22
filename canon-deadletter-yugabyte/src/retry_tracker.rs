//! YugabyteDB-backed [`RetryTracker`] implementation.
//!
//! Persists retry attempt counts in the `retry_attempts` table so that counts
//! survive process restarts. Uses UPSERT (`INSERT ... ON CONFLICT ... DO UPDATE`)
//! semantics for crash-safe atomic increments.

use chrono::{DateTime, Utc};
use sqlx::PgPool;
use uuid::Uuid;

use canon_core::traits::retry_tracker::{RetryAttempt, RetryTracker};

/// Errors produced by [`YugabyteRetryTracker`].
#[derive(Debug, thiserror::Error)]
pub enum YugabyteRetryTrackerError {
    /// A database operation failed.
    #[error("database error: {0}")]
    Database(#[from] sqlx::Error),

    /// The `YUGABYTE_URL` environment variable was not set.
    #[error("environment error: {0}")]
    Env(#[from] std::env::VarError),
}

/// YugabyteDB-backed [`RetryTracker`].
///
/// Expects the following table:
///
/// ```sql
/// CREATE TABLE retry_attempts (
///     message_id     UUID         PRIMARY KEY,
///     handler_id     TEXT         NOT NULL,
///     attempts       INT          NOT NULL DEFAULT 0,
///     last_attempted TIMESTAMPTZ  NOT NULL DEFAULT now()
/// );
/// ```
///
/// The `increment` method uses an atomic UPSERT: if the row already exists,
/// the attempt count is bumped by one and `last_attempted` is set to `now()`.
/// If the row does not exist, it is inserted with `attempts = 1`.
#[derive(Debug, Clone)]
pub struct YugabyteRetryTracker {
    pool: PgPool,
}

impl YugabyteRetryTracker {
    /// Create a new tracker from an existing connection pool.
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Create a new tracker by connecting to the database at the given URL.
    pub async fn from_url(url: &str) -> Result<Self, YugabyteRetryTrackerError> {
        let pool = PgPool::connect(url).await?;
        Ok(Self { pool })
    }

    /// Create a new tracker from the `YUGABYTE_URL` environment variable.
    pub async fn from_env() -> Result<Self, YugabyteRetryTrackerError> {
        let url = std::env::var("YUGABYTE_URL")?;
        Self::from_url(&url).await
    }

    /// Returns a reference to the underlying connection pool.
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }
}

/// Internal row type for mapping `retry_attempts` query results.
#[derive(sqlx::FromRow)]
struct RetryRow {
    message_id: Uuid,
    handler_id: String,
    attempts: i32,
    last_attempted: DateTime<Utc>,
}

impl From<RetryRow> for RetryAttempt {
    fn from(row: RetryRow) -> Self {
        Self {
            message_id: row.message_id,
            handler_id: row.handler_id,
            attempts: row.attempts as u32,
            last_attempted: row.last_attempted,
        }
    }
}

impl RetryTracker for YugabyteRetryTracker {
    type Error = YugabyteRetryTrackerError;

    fn increment(&self, message_id: Uuid, handler_id: &str) -> Result<u32, Self::Error> {
        // RetryTracker is a synchronous trait (no async). Use `block_in_place` to
        // run the async database call on the current runtime without creating a
        // nested runtime.
        let pool = self.pool.clone();
        let handler_id = handler_id.to_owned();

        let row: (i32,) = tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                sqlx::query_as(
                    "INSERT INTO retry_attempts (message_id, handler_id, attempts, last_attempted) \
                     VALUES ($1, $2, 1, now()) \
                     ON CONFLICT (message_id) DO UPDATE \
                       SET attempts = retry_attempts.attempts + 1, \
                           last_attempted = now() \
                     RETURNING attempts",
                )
                .bind(message_id)
                .bind(&handler_id)
                .fetch_one(&pool)
                .await
            })
        })?;

        Ok(row.0 as u32)
    }

    fn get(&self, message_id: Uuid) -> Result<Option<RetryAttempt>, Self::Error> {
        let pool = self.pool.clone();

        let row: Option<RetryRow> = tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                sqlx::query_as::<_, RetryRow>(
                    "SELECT message_id, handler_id, attempts, last_attempted \
                     FROM retry_attempts \
                     WHERE message_id = $1",
                )
                .bind(message_id)
                .fetch_optional(&pool)
                .await
            })
        })?;

        Ok(row.map(Into::into))
    }

    fn remove(&self, message_id: Uuid) -> Result<(), Self::Error> {
        let pool = self.pool.clone();

        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                sqlx::query("DELETE FROM retry_attempts WHERE message_id = $1")
                    .bind(message_id)
                    .execute(&pool)
                    .await
            })
        })?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use testcontainers::runners::AsyncRunner;
    use testcontainers::ContainerAsync;
    use testcontainers_modules::postgres::Postgres;

    async fn setup_container() -> (ContainerAsync<Postgres>, PgPool) {
        let container = Postgres::default()
            .start()
            .await
            .expect("Failed to start postgres container");

        let port = container.get_host_port_ipv4(5432).await.expect("port");
        let url = format!("postgres://postgres:postgres@127.0.0.1:{}/postgres", port);

        let pool = PgPool::connect(&url).await.expect("connect");

        sqlx::query(
            "CREATE TABLE IF NOT EXISTS retry_attempts (
                message_id     UUID         PRIMARY KEY,
                handler_id     TEXT         NOT NULL,
                attempts       INT          NOT NULL DEFAULT 0,
                last_attempted TIMESTAMPTZ  NOT NULL DEFAULT now()
            )",
        )
        .execute(&pool)
        .await
        .expect("create retry_attempts table");

        (container, pool)
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn increment_creates_entry_on_first_call() {
        let (_container, pool) = setup_container().await;
        let tracker = YugabyteRetryTracker::new(pool);
        let msg_id = Uuid::new_v4();

        let count = tracker
            .increment(msg_id, "handler_a")
            .unwrap_or_else(|e| panic!("increment failed: {e}"));
        assert_eq!(count, 1);

        let attempt = tracker
            .get(msg_id)
            .unwrap_or_else(|e| panic!("get failed: {e}"))
            .unwrap_or_else(|| panic!("expected Some"));
        assert_eq!(attempt.attempts, 1);
        assert_eq!(attempt.handler_id, "handler_a");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn increment_accumulates() {
        let (_container, pool) = setup_container().await;
        let tracker = YugabyteRetryTracker::new(pool);
        let msg_id = Uuid::new_v4();

        assert_eq!(
            tracker
                .increment(msg_id, "h1")
                .unwrap_or_else(|e| panic!("{e}")),
            1
        );
        assert_eq!(
            tracker
                .increment(msg_id, "h1")
                .unwrap_or_else(|e| panic!("{e}")),
            2
        );
        assert_eq!(
            tracker
                .increment(msg_id, "h1")
                .unwrap_or_else(|e| panic!("{e}")),
            3
        );

        let attempt = tracker
            .get(msg_id)
            .unwrap_or_else(|e| panic!("{e}"))
            .unwrap_or_else(|| panic!("expected Some"));
        assert_eq!(attempt.attempts, 3);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn get_returns_none_for_unknown_message() {
        let (_container, pool) = setup_container().await;
        let tracker = YugabyteRetryTracker::new(pool);
        let result = tracker
            .get(Uuid::new_v4())
            .unwrap_or_else(|e| panic!("{e}"));
        assert!(result.is_none());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn remove_deletes_entry() {
        let (_container, pool) = setup_container().await;
        let tracker = YugabyteRetryTracker::new(pool);
        let msg_id = Uuid::new_v4();

        tracker
            .increment(msg_id, "h1")
            .unwrap_or_else(|e| panic!("{e}"));
        assert!(tracker
            .get(msg_id)
            .unwrap_or_else(|e| panic!("{e}"))
            .is_some());

        tracker.remove(msg_id).unwrap_or_else(|e| panic!("{e}"));
        assert!(tracker
            .get(msg_id)
            .unwrap_or_else(|e| panic!("{e}"))
            .is_none());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn remove_nonexistent_is_noop() {
        let (_container, pool) = setup_container().await;
        let tracker = YugabyteRetryTracker::new(pool);
        let result = tracker.remove(Uuid::new_v4());
        assert!(result.is_ok());
    }
}
