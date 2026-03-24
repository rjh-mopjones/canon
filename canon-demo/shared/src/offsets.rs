//! Kafka consumer offset persistence for demo services.
//!
//! Stores the last-processed Kafka offset per consumer in YugabyteDB.
//! On service restart, consumers resume from the persisted offset instead
//! of replaying from zero. Application-layer idempotency is still the
//! safety net — this is a performance optimization only.

use std::sync::Arc;

use async_trait::async_trait;
use sqlx::PgPool;
use tokio::sync::Mutex as TokioMutex;
use tracing::warn;

use canon_core::consumers::{ConsumerReceiver, ConsumerReceiverError, ReceivedEnvelope};

/// Load the last persisted offset for a consumer. Returns `None` if never persisted,
/// meaning the consumer should start from offset 0 (first-ever run).
pub async fn load_offset(pool: &PgPool, consumer_id: &str) -> Option<i64> {
    match sqlx::query_scalar::<_, i64>(
        "SELECT last_offset FROM kafka_consumer_offsets WHERE consumer_id = $1",
    )
    .bind(consumer_id)
    .fetch_optional(pool)
    .await
    {
        Ok(row) => row,
        Err(e) => {
            warn!(consumer_id = %consumer_id, error = %e, "failed to load persisted offset, starting from 0");
            None
        }
    }
}

/// Persist the current offset for a consumer (upsert). Non-fatal on failure.
pub async fn save_offset(pool: &PgPool, consumer_id: &str, topic: &str, offset: i64) {
    if let Err(e) = sqlx::query(
        "INSERT INTO kafka_consumer_offsets (consumer_id, topic, partition_id, last_offset, updated_at) \
         VALUES ($1, $2, 0, $3, now()) \
         ON CONFLICT (consumer_id) DO UPDATE SET last_offset = $3, updated_at = now()",
    )
    .bind(consumer_id)
    .bind(topic)
    .bind(offset)
    .execute(pool)
    .await
    {
        warn!(
            consumer_id = %consumer_id,
            offset = offset,
            error = %e,
            "failed to persist Kafka offset (non-fatal)"
        );
    }
}

// ---------------------------------------------------------------------------
// OffsetTrackingReceiver — wraps a ConsumerReceiver with offset persistence
// ---------------------------------------------------------------------------

/// Wraps a `ConsumerReceiver` and persists the last-processed offset to
/// YugabyteDB after each `receive()`. The consumer loop in `canon-core` calls
/// `receive()` in a tight loop, so we batch offset saves by only persisting
/// when the sequence number advances past a threshold.
pub struct OffsetTrackingReceiver<R: ConsumerReceiver> {
    inner: R,
    pool: PgPool,
    consumer_id: String,
    topic: String,
    /// Track the last-persisted offset to avoid redundant writes.
    last_saved: Arc<TokioMutex<i64>>,
    /// Counter to batch saves (persist every N messages).
    receive_count: Arc<TokioMutex<u64>>,
}

/// Persist every N messages to avoid per-message DB writes.
const PERSIST_INTERVAL: u64 = 50;

impl<R: ConsumerReceiver> OffsetTrackingReceiver<R> {
    pub fn new(inner: R, pool: PgPool, consumer_id: String, topic: String) -> Self {
        Self {
            inner,
            pool,
            consumer_id,
            topic,
            last_saved: Arc::new(TokioMutex::new(-1)),
            receive_count: Arc::new(TokioMutex::new(0)),
        }
    }
}

#[async_trait]
impl<R: ConsumerReceiver> ConsumerReceiver for OffsetTrackingReceiver<R> {
    async fn receive(&self) -> Result<Option<ReceivedEnvelope>, ConsumerReceiverError> {
        let result = self.inner.receive().await?;
        if let Some(ref envelope) = result {
            let offset = envelope.sequence_number as i64;
            let mut count = self.receive_count.lock().await;
            *count += 1;
            if *count % PERSIST_INTERVAL == 0 {
                let mut last = self.last_saved.lock().await;
                if offset > *last {
                    save_offset(&self.pool, &self.consumer_id, &self.topic, offset).await;
                    *last = offset;
                }
            }
        }
        Ok(result)
    }

    async fn commit(&self) -> Result<(), ConsumerReceiverError> {
        self.inner.commit().await
    }
}
