use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use sqlx::PgPool;
use uuid::Uuid;

use canon_core::traits::SnapshotStore;
use canon_core::AggregateId;
use canon_snapshot_store::{Snapshot, SnapshotStoreError};

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
    ///
    /// Uses `PgPool::connect` defaults. For production use, prefer [`Self::new`]
    /// with a pool built via `PgPoolOptions` to control max connections, idle timeout, etc.
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
    type Error = SnapshotStoreError;

    async fn save(&self, snapshot: Snapshot) -> Result<(), Self::Error> {
        let v_i64 = i64::try_from(snapshot.version.as_u64()).map_err(|_| {
            SnapshotStoreError::Store(
                format!("version {} overflows i64", snapshot.version.as_u64()).into(),
            )
        })?;

        let result = sqlx::query(
            "INSERT INTO snapshots (aggregate_id, version, state, taken_at) \
             VALUES ($1, $2, $3, $4) \
             ON CONFLICT (aggregate_id, version) DO NOTHING",
        )
        .bind(*snapshot.aggregate_id.as_uuid())
        .bind(v_i64)
        .bind(snapshot.state.as_ref())
        .bind(snapshot.taken_at)
        .execute(&self.pool)
        .await
        .map_err(|e| SnapshotStoreError::Store(Box::new(e)))?;

        if result.rows_affected() == 0 {
            return Err(SnapshotStoreError::Store(
                format!(
                    "snapshot already exists for aggregate {}",
                    snapshot.aggregate_id.as_uuid()
                )
                .into(),
            ));
        }

        Ok(())
    }

    async fn load(&self, aggregate_id: &AggregateId) -> Result<Option<Snapshot>, Self::Error> {
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

        Ok(row.map(|(agg_id, version, state, taken_at)| {
            let v = u64::try_from(version).unwrap_or(0);
            Snapshot {
                aggregate_id: AggregateId::from_uuid(agg_id),
                version: v.into(),
                state: Bytes::from(state),
                taken_at,
            }
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use canon_core::Version;

    static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("./migrations");

    #[ignore]
    #[sqlx::test(migrator = "MIGRATOR")]
    async fn test_save_and_load_latest(pool: PgPool) {
        let store = YugabyteSnapshotStore::new(pool);
        let id = AggregateId::new();

        let snapshot = Snapshot {
            aggregate_id: id.clone(),
            version: Version::from(50),
            state: Bytes::from_static(b"serialized-state"),
            taken_at: Utc::now(),
        };

        store.save(snapshot).await.expect("save failed");

        let loaded = store
            .load(&id)
            .await
            .expect("load failed")
            .expect("should find snapshot");
        assert_eq!(*loaded.aggregate_id.as_uuid(), *id.as_uuid());
        assert_eq!(loaded.version.as_u64(), 50);
        assert_eq!(loaded.state.as_ref(), b"serialized-state");
        assert!(loaded.taken_at.timestamp() > 0);
    }

    #[ignore]
    #[sqlx::test(migrator = "MIGRATOR")]
    async fn test_multiple_snapshots_returns_latest(pool: PgPool) {
        let store = YugabyteSnapshotStore::new(pool);
        let id = AggregateId::new();

        let snap_v50 = Snapshot {
            aggregate_id: id.clone(),
            version: Version::from(50),
            state: Bytes::from_static(b"state-v50"),
            taken_at: Utc::now(),
        };
        let snap_v100 = Snapshot {
            aggregate_id: id.clone(),
            version: Version::from(100),
            state: Bytes::from_static(b"state-v100"),
            taken_at: Utc::now(),
        };

        store.save(snap_v50).await.expect("save v50");
        store.save(snap_v100).await.expect("save v100");

        let loaded = store
            .load(&id)
            .await
            .expect("load failed")
            .expect("should find snapshot");
        assert_eq!(loaded.version.as_u64(), 100);
        assert_eq!(loaded.state.as_ref(), b"state-v100");
    }

    #[ignore]
    #[sqlx::test(migrator = "MIGRATOR")]
    async fn test_load_nonexistent_returns_none(pool: PgPool) {
        let store = YugabyteSnapshotStore::new(pool);

        let result = store.load(&AggregateId::new()).await.expect("load failed");
        assert!(result.is_none());
    }
}
