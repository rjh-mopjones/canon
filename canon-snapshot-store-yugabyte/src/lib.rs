use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use sqlx::PgPool;
use uuid::Uuid;

use canon_core::{AggregateId, Version};
use canon_snapshot_store::{Snapshot, SnapshotStore, SnapshotStoreError};

/// YugabyteDB-backed [`SnapshotStore`] implementation.
///
/// Expects the following table:
///
/// ```sql
/// CREATE TABLE snapshots (
///     aggregate_id UUID NOT NULL,
///     version BIGINT NOT NULL,
///     state BYTEA NOT NULL,
///     taken_at TIMESTAMPTZ NOT NULL DEFAULT now(),
///     PRIMARY KEY (aggregate_id, version)
/// );
/// ```
#[derive(Debug, Clone)]
pub struct YugabyteSnapshotStore {
    pool: PgPool,
}

impl YugabyteSnapshotStore {
    /// Create a new store from an existing connection pool.
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Create a new store by connecting to the database at the given URL.
    pub async fn from_url(url: &str) -> Result<Self, SnapshotStoreError> {
        let pool = PgPool::connect(url)
            .await
            .map_err(|e| SnapshotStoreError::Store(Box::new(e)))?;
        Ok(Self { pool })
    }

    /// Create a new store from the `YUGABYTE_URL` environment variable.
    pub async fn from_env() -> Result<Self, SnapshotStoreError> {
        let url =
            std::env::var("YUGABYTE_URL").map_err(|e| SnapshotStoreError::Store(Box::new(e)))?;
        Self::from_url(&url).await
    }

    /// Returns a reference to the underlying connection pool.
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }
}

#[async_trait]
impl SnapshotStore for YugabyteSnapshotStore {
    async fn save(&self, snapshot: Snapshot) -> Result<(), SnapshotStoreError> {
        sqlx::query(
            "INSERT INTO snapshots (aggregate_id, version, state, taken_at) \
             VALUES ($1, $2, $3, $4) \
             ON CONFLICT (aggregate_id, version) \
             DO UPDATE SET state = EXCLUDED.state, taken_at = EXCLUDED.taken_at",
        )
        .bind(*snapshot.aggregate_id.as_uuid())
        .bind(snapshot.version.as_u64() as i64)
        .bind(snapshot.state.as_ref())
        .bind(snapshot.taken_at)
        .execute(&self.pool)
        .await
        .map_err(|e| SnapshotStoreError::Store(Box::new(e)))?;

        Ok(())
    }

    async fn load(
        &self,
        aggregate_id: &AggregateId,
    ) -> Result<Option<Snapshot>, SnapshotStoreError> {
        let row: Option<(Uuid, i64, Vec<u8>, DateTime<Utc>)> = sqlx::query_as(
            "SELECT aggregate_id, version, state, taken_at \
             FROM snapshots \
             WHERE aggregate_id = $1 \
             ORDER BY version DESC \
             LIMIT 1",
        )
        .bind(*aggregate_id.as_uuid())
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| SnapshotStoreError::Store(Box::new(e)))?;

        Ok(row.map(|(agg_id, version, state, taken_at)| Snapshot {
            aggregate_id: AggregateId::from_uuid(agg_id),
            version: Version::from_u64(version as u64),
            state: Bytes::from(state),
            taken_at,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn setup_schema(pool: &PgPool) {
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS snapshots (
                aggregate_id UUID NOT NULL,
                version BIGINT NOT NULL,
                state BYTEA NOT NULL,
                taken_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                PRIMARY KEY (aggregate_id, version)
            )",
        )
        .execute(pool)
        .await
        .expect("create snapshots table");
    }

    #[sqlx::test(migrations = false)]
    async fn test_save_and_load_latest(pool: PgPool) {
        setup_schema(&pool).await;
        let store = YugabyteSnapshotStore::new(pool);
        let id = AggregateId::new();

        let snapshot = Snapshot {
            aggregate_id: id.clone(),
            version: Version::from_u64(50),
            state: Bytes::from_static(b"serialized-state"),
            taken_at: Utc::now(),
        };

        store.save(snapshot).await.expect("save failed");

        let loaded = store.load(&id).await.expect("load failed").expect("should find snapshot");
        assert_eq!(*loaded.aggregate_id.as_uuid(), *id.as_uuid());
        assert_eq!(loaded.version.as_u64(), 50);
        assert_eq!(loaded.state.as_ref(), b"serialized-state");
    }

    #[sqlx::test(migrations = false)]
    async fn test_multiple_snapshots_returns_latest(pool: PgPool) {
        setup_schema(&pool).await;
        let store = YugabyteSnapshotStore::new(pool);
        let id = AggregateId::new();

        let snap_v50 = Snapshot {
            aggregate_id: id.clone(),
            version: Version::from_u64(50),
            state: Bytes::from_static(b"state-v50"),
            taken_at: Utc::now(),
        };
        let snap_v100 = Snapshot {
            aggregate_id: id.clone(),
            version: Version::from_u64(100),
            state: Bytes::from_static(b"state-v100"),
            taken_at: Utc::now(),
        };

        store.save(snap_v50).await.expect("save v50");
        store.save(snap_v100).await.expect("save v100");

        let loaded = store.load(&id).await.expect("load failed").expect("should find snapshot");
        assert_eq!(loaded.version.as_u64(), 100);
        assert_eq!(loaded.state.as_ref(), b"state-v100");
    }

    #[sqlx::test(migrations = false)]
    async fn test_load_nonexistent_returns_none(pool: PgPool) {
        setup_schema(&pool).await;
        let store = YugabyteSnapshotStore::new(pool);

        let result = store.load(&AggregateId::new()).await.expect("load failed");
        assert!(result.is_none());
    }
}
