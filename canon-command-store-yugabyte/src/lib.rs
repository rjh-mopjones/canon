use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use sqlx::PgPool;
use uuid::Uuid;

use canon_core::traits::CommandStore;
use canon_core::{AggregateId, CommandEnvelope, CommandStatus};

#[derive(Debug, thiserror::Error)]
pub enum YugabyteCommandStoreError {
    #[error("database error: {0}")]
    Database(#[from] sqlx::Error),

    #[error("environment error: {0}")]
    Env(#[from] std::env::VarError),
}

/// YugabyteDB-backed command store.
///
/// Stores every command submitted to the system as an audit trail.
/// Written as part of the single YugabyteDB ACID transaction alongside the outbox.
#[derive(Clone)]
pub struct YugabyteCommandStore {
    pool: PgPool,
}

impl YugabyteCommandStore {
    /// Create a new store from an existing connection pool.
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Create a new store from the `YUGABYTE_URL` environment variable.
    pub async fn from_env() -> Result<Self, YugabyteCommandStoreError> {
        let url = std::env::var("YUGABYTE_URL")?;
        let pool = PgPool::connect(&url).await?;
        Ok(Self { pool })
    }

    /// Returns a reference to the underlying connection pool.
    ///
    /// Useful for callers that need to include command writes in a broader transaction.
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }
}

#[async_trait]
impl CommandStore for YugabyteCommandStore {
    type Error = YugabyteCommandStoreError;

    async fn append(&self, envelope: CommandEnvelope) -> Result<(), Self::Error> {
        sqlx::query(
            "INSERT INTO commands \
             (command_id, aggregate_id, command_type, command_version, payload, \
              correlation_id, causation_id, created_at) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8) \
             ON CONFLICT (command_id) DO NOTHING",
        )
        .bind(envelope.command_id)
        .bind(envelope.aggregate_id.as_uuid())
        .bind(&envelope.command_type)
        .bind(envelope.command_version as i32)
        .bind(envelope.payload.as_ref())
        .bind(envelope.correlation_id)
        .bind(envelope.causation_id)
        .bind(envelope.timestamp)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    async fn load(&self, command_id: Uuid) -> Result<Option<CommandEnvelope>, Self::Error> {
        let row = sqlx::query_as::<_, CommandRow>(
            "SELECT command_id, aggregate_id, command_type, command_version, payload, \
             correlation_id, causation_id, created_at \
             FROM commands WHERE command_id = $1",
        )
        .bind(command_id)
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.map(Into::into))
    }

    async fn load_for_aggregate(
        &self,
        aggregate_id: &AggregateId,
    ) -> Result<Vec<CommandEnvelope>, Self::Error> {
        let rows = sqlx::query_as::<_, CommandRow>(
            "SELECT command_id, aggregate_id, command_type, command_version, payload, \
             correlation_id, causation_id, created_at \
             FROM commands WHERE aggregate_id = $1 ORDER BY created_at ASC",
        )
        .bind(aggregate_id.as_uuid())
        .fetch_all(&self.pool)
        .await?;

        Ok(rows.into_iter().map(Into::into).collect())
    }

    async fn load_range(
        &self,
        aggregate_id: &AggregateId,
        from: Option<DateTime<Utc>>,
        to: Option<DateTime<Utc>>,
    ) -> Result<Vec<CommandEnvelope>, Self::Error> {
        let rows = sqlx::query_as::<_, CommandRow>(
            "SELECT command_id, aggregate_id, command_type, command_version, payload, \
             correlation_id, causation_id, created_at \
             FROM commands \
             WHERE aggregate_id = $1 \
               AND ($2::timestamptz IS NULL OR created_at >= $2) \
               AND ($3::timestamptz IS NULL OR created_at <= $3) \
             ORDER BY created_at ASC",
        )
        .bind(aggregate_id.as_uuid())
        .bind(from)
        .bind(to)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows.into_iter().map(Into::into).collect())
    }

    async fn update_status(
        &self,
        command_id: Uuid,
        status: CommandStatus,
    ) -> Result<(), Self::Error> {
        sqlx::query("UPDATE commands SET status = $1 WHERE command_id = $2")
            .bind(status.as_str())
            .bind(command_id)
            .execute(&self.pool)
            .await?;

        Ok(())
    }
}

/// Internal row type for mapping SQL results.
#[derive(sqlx::FromRow)]
struct CommandRow {
    command_id: Uuid,
    aggregate_id: Uuid,
    command_type: String,
    command_version: i32,
    payload: Vec<u8>,
    correlation_id: Option<Uuid>,
    causation_id: Option<Uuid>,
    created_at: DateTime<Utc>,
}

impl From<CommandRow> for CommandEnvelope {
    fn from(row: CommandRow) -> Self {
        Self {
            command_id: row.command_id,
            aggregate_id: AggregateId::from_uuid(row.aggregate_id),
            command_type: row.command_type,
            correlation_id: row.correlation_id.unwrap_or(Uuid::nil()),
            causation_id: row.causation_id.unwrap_or(Uuid::nil()),
            timestamp: row.created_at,
            payload: Bytes::from(row.payload),
            command_version: row.command_version as u32,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use canon_core::AggregateId;

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

    async fn setup_schema(pool: &PgPool) {
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS commands (
                command_id UUID PRIMARY KEY,
                aggregate_id UUID NOT NULL,
                command_type TEXT NOT NULL DEFAULT '',
                command_version INT NOT NULL DEFAULT 1,
                payload BYTEA NOT NULL,
                correlation_id UUID,
                causation_id UUID,
                status TEXT NOT NULL DEFAULT 'pending',
                created_at TIMESTAMPTZ NOT NULL DEFAULT now()
            )",
        )
        .execute(pool)
        .await
        .expect("create table");

        sqlx::query(
            "CREATE INDEX IF NOT EXISTS commands_aggregate_idx \
             ON commands (aggregate_id, created_at)",
        )
        .execute(pool)
        .await
        .expect("create index");
    }

    #[sqlx::test(migrations = false)]
    #[ignore = "requires DATABASE_URL"]
    async fn test_store_and_load(pool: PgPool) {
        setup_schema(&pool).await;
        let store = YugabyteCommandStore::new(pool);
        let agg_id = AggregateId::new();
        let cmd = make_command(&agg_id);
        let cmd_id = cmd.command_id;

        store.append(cmd).await.expect("append failed");

        let loaded = store.load(cmd_id).await.expect("load failed");
        assert!(loaded.is_some());
        let loaded = loaded.unwrap();
        assert_eq!(loaded.command_id, cmd_id);
        assert_eq!(loaded.aggregate_id, agg_id);
    }

    #[sqlx::test(migrations = false)]
    #[ignore = "requires DATABASE_URL"]
    async fn test_store_idempotent(pool: PgPool) {
        setup_schema(&pool).await;
        let store = YugabyteCommandStore::new(pool);
        let agg_id = AggregateId::new();
        let cmd = make_command(&agg_id);

        store.append(cmd.clone()).await.expect("first append");
        store
            .append(cmd)
            .await
            .expect("duplicate should succeed silently");
    }

    #[sqlx::test(migrations = false)]
    #[ignore = "requires DATABASE_URL"]
    async fn test_load_for_aggregate_ordered(pool: PgPool) {
        setup_schema(&pool).await;
        let store = YugabyteCommandStore::new(pool);
        let agg_id = AggregateId::new();
        let other_agg = AggregateId::new();

        let cmd1 = make_command(&agg_id);
        let cmd2 = make_command(&agg_id);
        let cmd3 = make_command(&other_agg);

        let id1 = cmd1.command_id;
        let id2 = cmd2.command_id;

        store.append(cmd1).await.expect("append cmd1");
        store.append(cmd2).await.expect("append cmd2");
        store.append(cmd3).await.expect("append cmd3");

        let loaded = store
            .load_for_aggregate(&agg_id)
            .await
            .expect("load_for_aggregate");
        assert_eq!(loaded.len(), 2);
        assert_eq!(loaded[0].command_id, id1);
        assert_eq!(loaded[1].command_id, id2);
    }

    #[sqlx::test(migrations = false)]
    #[ignore = "requires DATABASE_URL"]
    async fn test_update_status(pool: PgPool) {
        setup_schema(&pool).await;
        let store = YugabyteCommandStore::new(pool);
        let agg_id = AggregateId::new();
        let cmd = make_command(&agg_id);
        let cmd_id = cmd.command_id;

        store.append(cmd).await.expect("append");
        store
            .update_status(cmd_id, CommandStatus::Executed)
            .await
            .expect("update_status");

        let row: (String,) = sqlx::query_as("SELECT status FROM commands WHERE command_id = $1")
            .bind(cmd_id)
            .fetch_one(store.pool())
            .await
            .expect("select status");
        assert_eq!(row.0, "executed");
    }

    #[sqlx::test(migrations = false)]
    #[ignore = "requires DATABASE_URL"]
    async fn test_load_range_with_bounds(pool: PgPool) {
        setup_schema(&pool).await;
        let store = YugabyteCommandStore::new(pool);
        let agg_id = AggregateId::new();

        let before = Utc::now();
        let cmd1 = make_command(&agg_id);
        store.append(cmd1).await.expect("append cmd1");

        let mid = Utc::now();
        let cmd2 = make_command(&agg_id);
        store.append(cmd2).await.expect("append cmd2");
        let after = Utc::now();

        let all = store
            .load_range(&agg_id, None, None)
            .await
            .expect("load all");
        assert_eq!(all.len(), 2);

        let from_mid = store
            .load_range(&agg_id, Some(mid), None)
            .await
            .expect("from mid");
        assert_eq!(from_mid.len(), 1);

        let to_before = store
            .load_range(&agg_id, None, Some(before))
            .await
            .expect("to before");
        assert_eq!(to_before.len(), 0);

        let bounded = store
            .load_range(&agg_id, Some(before), Some(after))
            .await
            .expect("bounded");
        assert_eq!(bounded.len(), 2);
    }
}
