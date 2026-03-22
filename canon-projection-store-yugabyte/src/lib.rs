use async_trait::async_trait;
use chrono::{DateTime, Utc};
use sqlx::PgPool;

use canon_core::traits::ProjectionCheckpointStore;
use canon_projection_store::{
    AggregateId, Checkpoint, ProjectionStore, ProjectionStoreError, Version,
};

#[derive(Debug, thiserror::Error)]
pub enum YugabyteProjectionStoreError {
    #[error("database error: {0}")]
    Database(#[from] sqlx::Error),

    #[error("environment error: {0}")]
    Env(#[from] std::env::VarError),
}

impl From<YugabyteProjectionStoreError> for ProjectionStoreError {
    fn from(err: YugabyteProjectionStoreError) -> Self {
        ProjectionStoreError::Store(Box::new(err))
    }
}

/// YugabyteDB-backed projection store.
///
/// Persists materialised read models as JSONB keyed by `(projection_id, aggregate_id)`,
/// and tracks event version checkpoints per projection for idempotent apply.
#[derive(Clone)]
pub struct YugabyteProjectionStore {
    pool: PgPool,
}

impl YugabyteProjectionStore {
    /// Create a new store by connecting to the given database URL.
    pub async fn new(url: &str) -> Result<Self, YugabyteProjectionStoreError> {
        let pool = PgPool::connect(url).await?;
        Ok(Self { pool })
    }

    /// Create a new store from an existing connection pool.
    pub fn from_pool(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Create a new store from the `YUGABYTE_URL` environment variable.
    pub async fn from_env() -> Result<Self, YugabyteProjectionStoreError> {
        let url = std::env::var("YUGABYTE_URL")?;
        Self::new(&url).await
    }

    /// Returns a reference to the underlying connection pool.
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }
}

#[async_trait]
impl ProjectionStore for YugabyteProjectionStore {
    async fn upsert(
        &self,
        projection_id: &str,
        aggregate_id: &AggregateId,
        state: &[u8],
    ) -> Result<(), ProjectionStoreError> {
        let json_value: serde_json::Value =
            serde_json::from_slice(state).map_err(|e| ProjectionStoreError::Store(Box::new(e)))?;

        sqlx::query(
            "INSERT INTO projections (projection_id, aggregate_id, state, updated_at) \
             VALUES ($1, $2, $3, now()) \
             ON CONFLICT (projection_id, aggregate_id) \
             DO UPDATE SET state = EXCLUDED.state, updated_at = now()",
        )
        .bind(projection_id)
        .bind(aggregate_id.as_uuid())
        .bind(&json_value)
        .execute(&self.pool)
        .await
        .map_err(YugabyteProjectionStoreError::from)?;

        Ok(())
    }

    async fn load(
        &self,
        projection_id: &str,
        aggregate_id: &AggregateId,
    ) -> Result<Option<Vec<u8>>, ProjectionStoreError> {
        let row: Option<(serde_json::Value,)> = sqlx::query_as(
            "SELECT state FROM projections \
             WHERE projection_id = $1 AND aggregate_id = $2",
        )
        .bind(projection_id)
        .bind(aggregate_id.as_uuid())
        .fetch_optional(&self.pool)
        .await
        .map_err(YugabyteProjectionStoreError::from)?;

        match row {
            Some((value,)) => {
                let bytes = serde_json::to_vec(&value)
                    .map_err(|e| ProjectionStoreError::Store(Box::new(e)))?;
                Ok(Some(bytes))
            }
            None => Ok(None),
        }
    }

    async fn update_last_version(
        &self,
        projection_id: &str,
        version: Version,
    ) -> Result<(), ProjectionStoreError> {
        sqlx::query(
            "INSERT INTO projection_checkpoints (projection_id, last_version, updated_at) \
             VALUES ($1, $2, now()) \
             ON CONFLICT (projection_id) \
             DO UPDATE SET last_version = EXCLUDED.last_version, updated_at = now()",
        )
        .bind(projection_id)
        .bind(version.as_u64() as i64)
        .execute(&self.pool)
        .await
        .map_err(YugabyteProjectionStoreError::from)?;

        Ok(())
    }

    async fn get_last_version(&self, projection_id: &str) -> Result<Version, ProjectionStoreError> {
        let row: Option<(i64,)> = sqlx::query_as(
            "SELECT last_version FROM projection_checkpoints \
             WHERE projection_id = $1",
        )
        .bind(projection_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(YugabyteProjectionStoreError::from)?;

        match row {
            Some((v,)) => Ok(Version::from_u64(v as u64)),
            None => Ok(Version::initial()),
        }
    }

    async fn set_rebuilding(
        &self,
        projection_id: &str,
        rebuilding: bool,
    ) -> Result<(), ProjectionStoreError> {
        sqlx::query(
            "INSERT INTO projection_checkpoints (projection_id, rebuilding, updated_at) \
             VALUES ($1, $2, now()) \
             ON CONFLICT (projection_id) \
             DO UPDATE SET rebuilding = EXCLUDED.rebuilding, updated_at = now()",
        )
        .bind(projection_id)
        .bind(rebuilding)
        .execute(&self.pool)
        .await
        .map_err(YugabyteProjectionStoreError::from)?;

        Ok(())
    }

    async fn is_rebuilding(&self, projection_id: &str) -> Result<bool, ProjectionStoreError> {
        let row: Option<(bool,)> = sqlx::query_as(
            "SELECT rebuilding FROM projection_checkpoints \
             WHERE projection_id = $1",
        )
        .bind(projection_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(YugabyteProjectionStoreError::from)?;

        Ok(row.map(|(r,)| r).unwrap_or(false))
    }

    async fn get_checkpoint(
        &self,
        projection_id: &str,
    ) -> Result<Checkpoint, ProjectionStoreError> {
        let row: Option<(i64, bool, DateTime<Utc>)> = sqlx::query_as(
            "SELECT last_version, rebuilding, updated_at FROM projection_checkpoints \
             WHERE projection_id = $1",
        )
        .bind(projection_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(YugabyteProjectionStoreError::from)?;

        match row {
            Some((version, rebuilding, updated_at)) => Ok(Checkpoint {
                projection_id: projection_id.to_owned(),
                last_version: Version::from_u64(version as u64),
                rebuilding,
                updated_at,
            }),
            None => Ok(Checkpoint {
                projection_id: projection_id.to_owned(),
                last_version: Version::initial(),
                rebuilding: false,
                updated_at: Utc::now(),
            }),
        }
    }

    async fn reset_checkpoint(
        &self,
        projection_id: &str,
        target: Version,
    ) -> Result<(), ProjectionStoreError> {
        sqlx::query(
            "INSERT INTO projection_checkpoints \
                (projection_id, last_version, rebuilding, updated_at) \
             VALUES ($1, $2, true, now()) \
             ON CONFLICT (projection_id) \
             DO UPDATE SET last_version = EXCLUDED.last_version, \
                           rebuilding = true, \
                           updated_at = now()",
        )
        .bind(projection_id)
        .bind(target.as_u64() as i64)
        .execute(&self.pool)
        .await
        .map_err(YugabyteProjectionStoreError::from)?;

        Ok(())
    }
}

/// Implements the canon-core `ProjectionCheckpointStore` trait so that
/// `YugabyteProjectionStore` can be used with `ServiceBuilder`.
#[async_trait]
impl ProjectionCheckpointStore for YugabyteProjectionStore {
    type Error = ProjectionStoreError;

    async fn get_checkpoint(&self, projection_id: &str) -> Result<Version, Self::Error> {
        self.get_last_version(projection_id).await
    }

    async fn set_checkpoint(
        &self,
        projection_id: &str,
        version: Version,
    ) -> Result<(), Self::Error> {
        self.update_last_version(projection_id, version).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use canon_projection_store::AggregateId;
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
            "CREATE TABLE IF NOT EXISTS projections (
                projection_id TEXT        NOT NULL,
                aggregate_id  UUID        NOT NULL,
                state         JSONB       NOT NULL,
                updated_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
                PRIMARY KEY (projection_id, aggregate_id)
            )",
        )
        .execute(&pool)
        .await
        .expect("create projections table");

        sqlx::query(
            "CREATE TABLE IF NOT EXISTS projection_checkpoints (
                projection_id TEXT PRIMARY KEY,
                last_version  BIGINT  NOT NULL DEFAULT 0,
                rebuilding    BOOLEAN NOT NULL DEFAULT false,
                updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
            )",
        )
        .execute(&pool)
        .await
        .expect("create projection_checkpoints table");

        (container, pool)
    }

    #[tokio::test]
    async fn test_upsert_and_load() {
        let (_container, pool) = setup_container().await;
        let store = YugabyteProjectionStore::from_pool(pool);
        let agg_id = AggregateId::new();

        let state = serde_json::json!({"stock": 42}).to_string();
        store
            .upsert("inventory", &agg_id, state.as_bytes())
            .await
            .expect("upsert failed");

        let loaded = store.load("inventory", &agg_id).await.expect("load failed");
        assert!(loaded.is_some());
        let value: serde_json::Value =
            serde_json::from_slice(&loaded.unwrap()).expect("invalid json");
        assert_eq!(value["stock"], 42);

        // Upsert overwrites
        let state2 = serde_json::json!({"stock": 99}).to_string();
        store
            .upsert("inventory", &agg_id, state2.as_bytes())
            .await
            .expect("second upsert failed");

        let loaded2 = store
            .load("inventory", &agg_id)
            .await
            .expect("load after upsert failed");
        let value2: serde_json::Value =
            serde_json::from_slice(&loaded2.unwrap()).expect("invalid json");
        assert_eq!(value2["stock"], 99);

        // Load missing key returns None
        let missing = store
            .load("inventory", &AggregateId::new())
            .await
            .expect("load missing failed");
        assert!(missing.is_none());
    }

    #[tokio::test]
    async fn test_last_version_tracking() {
        let (_container, pool) = setup_container().await;
        let store = YugabyteProjectionStore::from_pool(pool);

        // Default is Version::initial()
        let v = store
            .get_last_version("proj-1")
            .await
            .expect("get_last_version failed");
        assert_eq!(v, Version::initial());

        // Update and read back
        let v3 = Version::initial().next().next().next();
        store
            .update_last_version("proj-1", v3)
            .await
            .expect("update_last_version failed");

        let loaded = store
            .get_last_version("proj-1")
            .await
            .expect("get_last_version after update failed");
        assert_eq!(loaded, v3);

        // Upsert overwrites
        let v5 = v3.next().next();
        store
            .update_last_version("proj-1", v5)
            .await
            .expect("second update_last_version failed");

        let loaded2 = store
            .get_last_version("proj-1")
            .await
            .expect("get_last_version after second update failed");
        assert_eq!(loaded2, v5);
    }

    #[tokio::test]
    async fn test_rebuilding_flag() {
        let (_container, pool) = setup_container().await;
        let store = YugabyteProjectionStore::from_pool(pool);

        // Default is false
        let rebuilding = store
            .is_rebuilding("proj-1")
            .await
            .expect("is_rebuilding failed");
        assert!(!rebuilding);

        // Set to true
        store
            .set_rebuilding("proj-1", true)
            .await
            .expect("set_rebuilding true failed");
        let rebuilding = store
            .is_rebuilding("proj-1")
            .await
            .expect("is_rebuilding after set true failed");
        assert!(rebuilding);

        // Set back to false
        store
            .set_rebuilding("proj-1", false)
            .await
            .expect("set_rebuilding false failed");
        let rebuilding = store
            .is_rebuilding("proj-1")
            .await
            .expect("is_rebuilding after set false failed");
        assert!(!rebuilding);
    }

    #[tokio::test]
    async fn test_get_checkpoint() {
        let (_container, pool) = setup_container().await;
        let store = YugabyteProjectionStore::from_pool(pool);

        // Default checkpoint for unknown projection
        let cp = ProjectionStore::get_checkpoint(&store, "proj-new")
            .await
            .expect("get_checkpoint failed");
        assert_eq!(cp.last_version, Version::initial());
        assert!(!cp.rebuilding);

        // After setting version and rebuilding flag
        store
            .update_last_version("proj-new", Version::from_u64(5))
            .await
            .expect("update_last_version failed");
        store
            .set_rebuilding("proj-new", true)
            .await
            .expect("set_rebuilding failed");

        let cp = ProjectionStore::get_checkpoint(&store, "proj-new")
            .await
            .expect("get_checkpoint after update failed");
        assert_eq!(cp.last_version, Version::from_u64(5));
        assert!(cp.rebuilding);
    }

    #[tokio::test]
    async fn test_reset_checkpoint() {
        let (_container, pool) = setup_container().await;
        let store = YugabyteProjectionStore::from_pool(pool);

        // Set initial checkpoint
        store
            .update_last_version("proj-r", Version::from_u64(10))
            .await
            .expect("update_last_version failed");

        // Reset checkpoint to version 3
        store
            .reset_checkpoint("proj-r", Version::from_u64(3))
            .await
            .expect("reset_checkpoint failed");

        let cp = ProjectionStore::get_checkpoint(&store, "proj-r")
            .await
            .expect("get_checkpoint after reset failed");
        assert_eq!(cp.last_version, Version::from_u64(3));
        assert!(cp.rebuilding);
    }
}
