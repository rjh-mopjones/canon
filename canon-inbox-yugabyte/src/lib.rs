use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use tokio::sync::RwLock;
use tracing::{debug, info};
use uuid::Uuid;

use canon_core::{
    AggregateId, CommandEnvelope, EventEnvelope, IncomingMessage, Oversight, Version,
};
use canon_inbound_queue::{InboundQueue, InboundQueueError};
use canon_inbox::{HandlerRegistration, Inbox, InboxError};

// ── Error ────────────────────────────────────────────────────────────────────

#[derive(Debug, thiserror::Error)]
pub enum YugabyteInboxError {
    #[error("database error: {0}")]
    Database(#[from] sqlx::Error),

    #[error("serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    #[error("environment error: {0}")]
    Env(#[from] std::env::VarError),

    #[error("handler not registered: {0}")]
    HandlerNotRegistered(String),

    #[error("inbound queue error: {0}")]
    Queue(#[from] InboundQueueError),
}

impl From<YugabyteInboxError> for InboxError {
    fn from(e: YugabyteInboxError) -> Self {
        InboxError::Store(Box::new(e))
    }
}

// ── Serializable message wrapper for JSONB storage ───────────────────────────

/// Serializable representation of an `IncomingMessage` for JSONB column storage.
/// `IncomingMessage` itself is an in-process type without Serialize/Deserialize,
/// so we map through this intermediate form.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredMessage {
    message_type: StoredMessageType,
    #[serde(flatten)]
    envelope: StoredEnvelope,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum StoredMessageType {
    Command,
    InternalEvent,
    ExternalEvent,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredEnvelope {
    // Command fields
    #[serde(skip_serializing_if = "Option::is_none")]
    command_id: Option<Uuid>,
    #[serde(skip_serializing_if = "Option::is_none")]
    command_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    command_version: Option<u32>,

    // Event fields
    #[serde(skip_serializing_if = "Option::is_none")]
    event_id: Option<Uuid>,
    #[serde(skip_serializing_if = "Option::is_none")]
    event_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    event_version: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    version: Option<u64>,

    // Common fields
    aggregate_id: Uuid,
    correlation_id: Uuid,
    causation_id: Uuid,
    timestamp: DateTime<Utc>,
    #[serde(with = "base64_bytes")]
    payload: Vec<u8>,
}

mod base64_bytes {
    use serde::{self, Deserialize, Deserializer, Serializer};

    pub fn serialize<S>(bytes: &[u8], serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_bytes(bytes)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Vec<u8>, D::Error>
    where
        D: Deserializer<'de>,
    {
        // Accept both byte arrays and sequences of integers (JSON arrays)
        let v: serde_json::Value = Deserialize::deserialize(deserializer)?;
        match v {
            serde_json::Value::Array(arr) => arr
                .into_iter()
                .map(|v| {
                    v.as_u64()
                        .and_then(|n| u8::try_from(n).ok())
                        .ok_or_else(|| serde::de::Error::custom("invalid byte value"))
                })
                .collect(),
            _ => Err(serde::de::Error::custom("expected byte array")),
        }
    }
}

impl StoredMessage {
    fn from_incoming(msg: &IncomingMessage) -> Self {
        match msg {
            IncomingMessage::Command(c) => StoredMessage {
                message_type: StoredMessageType::Command,
                envelope: StoredEnvelope {
                    command_id: Some(c.command_id),
                    command_type: Some(c.command_type.clone()),
                    command_version: Some(c.command_version),
                    event_id: None,
                    event_type: None,
                    event_version: None,
                    version: None,
                    aggregate_id: *c.aggregate_id.as_uuid(),
                    correlation_id: c.correlation_id,
                    causation_id: c.causation_id,
                    timestamp: c.timestamp,
                    payload: c.payload.to_vec(),
                },
            },
            IncomingMessage::InternalEvent(e) | IncomingMessage::ExternalEvent(e) => {
                let message_type = if matches!(msg, IncomingMessage::InternalEvent(_)) {
                    StoredMessageType::InternalEvent
                } else {
                    StoredMessageType::ExternalEvent
                };
                StoredMessage {
                    message_type,
                    envelope: StoredEnvelope {
                        command_id: None,
                        command_type: None,
                        command_version: None,
                        event_id: Some(e.event_id),
                        event_type: Some(e.event_type.clone()),
                        event_version: Some(e.event_version),
                        version: Some(e.version.as_u64()),
                        aggregate_id: *e.aggregate_id.as_uuid(),
                        correlation_id: e.correlation_id,
                        causation_id: e.causation_id,
                        timestamp: e.timestamp,
                        payload: e.payload.to_vec(),
                    },
                }
            }
        }
    }

    fn into_incoming(self) -> IncomingMessage {
        let env = self.envelope;
        match self.message_type {
            StoredMessageType::Command => IncomingMessage::Command(CommandEnvelope {
                command_id: env.command_id.unwrap_or(Uuid::nil()),
                aggregate_id: AggregateId::from_uuid(env.aggregate_id),
                command_type: env.command_type.unwrap_or_default(),
                correlation_id: env.correlation_id,
                causation_id: env.causation_id,
                timestamp: env.timestamp,
                payload: Bytes::from(env.payload),
                command_version: env.command_version.unwrap_or(1),
            }),
            StoredMessageType::InternalEvent => IncomingMessage::InternalEvent(EventEnvelope {
                event_id: env.event_id.unwrap_or(Uuid::nil()),
                aggregate_id: AggregateId::from_uuid(env.aggregate_id),
                version: Version::from_u64(env.version.unwrap_or(0)),
                event_type: env.event_type.unwrap_or_default(),
                event_version: env.event_version.unwrap_or(1),
                payload: Bytes::from(env.payload),
                correlation_id: env.correlation_id,
                causation_id: env.causation_id,
                timestamp: env.timestamp,
            }),
            StoredMessageType::ExternalEvent => IncomingMessage::ExternalEvent(EventEnvelope {
                event_id: env.event_id.unwrap_or(Uuid::nil()),
                aggregate_id: AggregateId::from_uuid(env.aggregate_id),
                version: Version::from_u64(env.version.unwrap_or(0)),
                event_type: env.event_type.unwrap_or_default(),
                event_version: env.event_version.unwrap_or(1),
                payload: Bytes::from(env.payload),
                correlation_id: env.correlation_id,
                causation_id: env.causation_id,
                timestamp: env.timestamp,
            }),
        }
    }
}

// ── Oversight function type ──────────────────────────────────────────────────

type OversightFn = Box<dyn Fn(&[IncomingMessage]) -> Oversight + Send + Sync>;

struct HandlerEntry {
    oversight_fn: Option<OversightFn>,
    #[allow(dead_code)]
    registration: HandlerRegistration,
}

// ── YugabyteInbox ────────────────────────────────────────────────────────────

/// YugabyteDB-backed implementation of the [`Inbox`] port.
///
/// Provides idempotent message intake, windowed accumulation per
/// `(handler_id, aggregate_id)`, and oversight-gated dispatch to the
/// inbound queue.
///
/// # Oversight registry
///
/// Oversight functions are held in-memory and must be re-registered on startup.
/// This is the intended pattern: `ServiceBuilder` + `inventory` auto-discovers
/// all handlers and calls `register_handler` / `register_oversight` during
/// service initialization.
#[derive(Clone)]
pub struct YugabyteInbox {
    pool: PgPool,
    handlers: Arc<RwLock<HashMap<String, HandlerEntry>>>,
    queue: Arc<dyn InboundQueue>,
}

impl YugabyteInbox {
    /// Create a new inbox from an existing connection pool and inbound queue.
    pub fn new(pool: PgPool, queue: Arc<dyn InboundQueue>) -> Self {
        Self {
            pool,
            handlers: Arc::new(RwLock::new(HashMap::new())),
            queue,
        }
    }

    /// Create a new inbox from the `YUGABYTE_URL` environment variable.
    pub async fn from_env(queue: Arc<dyn InboundQueue>) -> Result<Self, YugabyteInboxError> {
        let url = std::env::var("YUGABYTE_URL")?;
        let pool = PgPool::connect(&url).await?;
        Ok(Self::new(pool, queue))
    }

    /// Returns a reference to the underlying connection pool.
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// Register an oversight function for a handler.
    ///
    /// The oversight function is called after every non-duplicate submission
    /// to decide whether the accumulated window is ready for dispatch.
    pub async fn register_oversight(
        &self,
        handler_id: &str,
        oversight_fn: impl Fn(&[IncomingMessage]) -> Oversight + Send + Sync + 'static,
    ) {
        let mut handlers = self.handlers.write().await;
        if let Some(entry) = handlers.get_mut(handler_id) {
            entry.oversight_fn = Some(Box::new(oversight_fn));
        }
    }

    /// Sweep expired windows: mark pending windows past their TTL as `expired`.
    ///
    /// This is intended to be called periodically by a background task.
    /// Expired windows can then be moved to the dead letter store by a
    /// separate cleanup process.
    pub async fn sweep_expired_windows(&self) -> Result<u64, YugabyteInboxError> {
        let result = sqlx::query(
            "UPDATE inbox_windows SET status = 'expired', updated_at = now() \
             WHERE expires_at IS NOT NULL AND expires_at < now() AND status = 'pending'",
        )
        .execute(&self.pool)
        .await?;

        let count = result.rows_affected();
        if count > 0 {
            info!(count, "swept expired inbox windows");
        }
        Ok(count)
    }

    /// Deduplicate: insert message, return true if new (not a duplicate).
    async fn deduplicate(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        handler_id: &str,
        message_id: Uuid,
        aggregate_id: &AggregateId,
        message: &IncomingMessage,
    ) -> Result<bool, YugabyteInboxError> {
        let message_type = match message {
            IncomingMessage::Command(_) => "command",
            IncomingMessage::InternalEvent(_) => "internal_event",
            IncomingMessage::ExternalEvent(_) => "external_event",
        };

        let stored = StoredMessage::from_incoming(message);
        let payload = serde_json::to_value(&stored)?;

        let result = sqlx::query(
            "INSERT INTO inbox_messages (handler_id, message_id, aggregate_id, message_type, payload) \
             VALUES ($1, $2, $3, $4, $5) \
             ON CONFLICT (handler_id, message_id) DO NOTHING",
        )
        .bind(handler_id)
        .bind(message_id)
        .bind(aggregate_id.as_uuid())
        .bind(message_type)
        .bind(payload)
        .execute(&mut **tx)
        .await?;

        if result.rows_affected() == 0 {
            debug!(handler_id, %message_id, "duplicate message, skipping");
        } else {
            debug!(handler_id, %message_id, "new message accepted");
        }

        Ok(result.rows_affected() > 0)
    }

    /// Accumulate: upsert the message into the handler's window for this aggregate.
    async fn accumulate(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        handler_id: &str,
        aggregate_id: &AggregateId,
        message: &IncomingMessage,
    ) -> Result<(), YugabyteInboxError> {
        let stored = StoredMessage::from_incoming(message);
        let message_json = serde_json::to_value(&stored)?;
        // Wrap in a JSON array for the || append operator
        let message_array = serde_json::Value::Array(vec![message_json]);

        sqlx::query(
            "INSERT INTO inbox_windows (handler_id, aggregate_id, messages) \
             VALUES ($1, $2, $3) \
             ON CONFLICT (handler_id, aggregate_id) \
             DO UPDATE SET messages = inbox_windows.messages || $3, \
                           updated_at = now()",
        )
        .bind(handler_id)
        .bind(aggregate_id.as_uuid())
        .bind(&message_array)
        .execute(&mut **tx)
        .await?;

        Ok(())
    }

    /// Evaluate oversight and return a pending dispatch if the window is ready.
    ///
    /// Returns `Some((batch, aggregate_id))` when the oversight decision is
    /// `Ready` -- the caller must publish this batch **after** the transaction
    /// commits. Returns `None` for `NotReady` and `Discard`.
    async fn evaluate_oversight(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        handler_id: &str,
        aggregate_id: &AggregateId,
    ) -> Result<Option<(Vec<IncomingMessage>, AggregateId)>, YugabyteInboxError> {
        // Load the current window
        let row: Option<WindowRow> = sqlx::query_as(
            "SELECT handler_id, aggregate_id, window_id, messages, status, expires_at \
             FROM inbox_windows \
             WHERE handler_id = $1 AND aggregate_id = $2 AND status = 'pending'",
        )
        .bind(handler_id)
        .bind(aggregate_id.as_uuid())
        .fetch_optional(&mut **tx)
        .await?;

        let row = match row {
            Some(r) => r,
            None => return Ok(None),
        };

        // Deserialize accumulated messages
        let stored_messages: Vec<StoredMessage> = serde_json::from_value(row.messages)?;
        let incoming_messages: Vec<IncomingMessage> = stored_messages
            .into_iter()
            .map(StoredMessage::into_incoming)
            .collect();

        // Evaluate oversight
        let decision = {
            let handlers = self.handlers.read().await;
            match handlers.get(handler_id) {
                Some(entry) => match &entry.oversight_fn {
                    Some(f) => f(&incoming_messages),
                    None => Oversight::Ready, // No oversight fn = always ready
                },
                None => {
                    return Err(YugabyteInboxError::HandlerNotRegistered(
                        handler_id.to_string(),
                    ))
                }
            }
        };

        match decision {
            Oversight::Ready => {
                debug!(handler_id, "oversight: Ready — dispatching window");

                // Mark as dispatched
                sqlx::query(
                    "UPDATE inbox_windows SET status = 'dispatched', updated_at = now() \
                     WHERE handler_id = $1 AND aggregate_id = $2",
                )
                .bind(handler_id)
                .bind(aggregate_id.as_uuid())
                .execute(&mut **tx)
                .await?;

                // Record in processed_windows
                sqlx::query(
                    "INSERT INTO processed_windows (window_id, handler_id) VALUES ($1, $2) \
                     ON CONFLICT (window_id) DO NOTHING",
                )
                .bind(row.window_id)
                .bind(handler_id)
                .execute(&mut **tx)
                .await?;

                // Delete the window row
                sqlx::query(
                    "DELETE FROM inbox_windows WHERE handler_id = $1 AND aggregate_id = $2",
                )
                .bind(handler_id)
                .bind(aggregate_id.as_uuid())
                .execute(&mut **tx)
                .await?;

                Ok(Some((
                    incoming_messages,
                    AggregateId::from_uuid(row.aggregate_id),
                )))
            }
            Oversight::NotReady => {
                debug!(handler_id, "oversight: NotReady — window stays pending");
                Ok(None)
            }
            Oversight::Discard => {
                debug!(handler_id, "oversight: Discard — deleting window");
                // Delete the window row without dispatch
                sqlx::query(
                    "DELETE FROM inbox_windows WHERE handler_id = $1 AND aggregate_id = $2",
                )
                .bind(handler_id)
                .bind(aggregate_id.as_uuid())
                .execute(&mut **tx)
                .await?;

                Ok(None)
            }
        }
    }
}

#[derive(sqlx::FromRow)]
struct WindowRow {
    #[allow(dead_code)]
    handler_id: String,
    aggregate_id: Uuid,
    window_id: Uuid,
    messages: serde_json::Value,
    #[allow(dead_code)]
    status: String,
    #[allow(dead_code)]
    expires_at: Option<DateTime<Utc>>,
}

// ── Inbox trait impl ─────────────────────────────────────────────────────────

#[async_trait]
impl Inbox for YugabyteInbox {
    async fn register_handler(&self, registration: HandlerRegistration) -> Result<(), InboxError> {
        info!(handler_id = %registration.handler_id, "registering inbox handler");
        let mut handlers = self.handlers.write().await;
        handlers.insert(
            registration.handler_id.clone(),
            HandlerEntry {
                oversight_fn: None,
                registration,
            },
        );
        Ok(())
    }

    async fn submit(
        &self,
        handler_id: &str,
        message_id: Uuid,
        message: IncomingMessage,
    ) -> Result<(), InboxError> {
        let aggregate_id = message.aggregate_id().clone();

        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(YugabyteInboxError::from)
            .map_err(InboxError::from)?;

        // Step 1: Deduplicate
        let is_new = self
            .deduplicate(&mut tx, handler_id, message_id, &aggregate_id, &message)
            .await
            .map_err(InboxError::from)?;

        let pending_dispatch = if is_new {
            // Step 2: Accumulate
            self.accumulate(&mut tx, handler_id, &aggregate_id, &message)
                .await
                .map_err(InboxError::from)?;

            // Step 3: Evaluate oversight
            self.evaluate_oversight(&mut tx, handler_id, &aggregate_id)
                .await
                .map_err(InboxError::from)?
        } else {
            None
        };

        tx.commit()
            .await
            .map_err(YugabyteInboxError::from)
            .map_err(InboxError::from)?;

        if let Some((batch, agg_id)) = pending_dispatch {
            self.queue
                .publish(batch, &agg_id)
                .await
                .map_err(YugabyteInboxError::from)
                .map_err(InboxError::from)?;
        }

        Ok(())
    }
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use canon_core::{AggregateId, IncomingMessage, Oversight};
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// In-memory inbound queue for tests.
    struct TestQueue {
        messages: Arc<RwLock<Vec<Vec<IncomingMessage>>>>,
    }

    impl TestQueue {
        fn new() -> Self {
            Self {
                messages: Arc::new(RwLock::new(Vec::new())),
            }
        }

        async fn published_count(&self) -> usize {
            self.messages.read().await.len()
        }
    }

    #[async_trait]
    impl InboundQueue for TestQueue {
        async fn publish(
            &self,
            batch: Vec<IncomingMessage>,
            _aggregate_id: &AggregateId,
        ) -> Result<(), InboundQueueError> {
            self.messages.write().await.push(batch);
            Ok(())
        }

        async fn receive(&self) -> Result<Option<Vec<IncomingMessage>>, InboundQueueError> {
            Ok(None)
        }

        async fn commit(&self) -> Result<(), InboundQueueError> {
            Ok(())
        }
    }

    fn make_event(aggregate_id: &AggregateId, event_type: &str) -> IncomingMessage {
        IncomingMessage::ExternalEvent(EventEnvelope {
            event_id: Uuid::new_v4(),
            aggregate_id: aggregate_id.clone(),
            version: Version::initial(),
            event_type: event_type.to_string(),
            event_version: 1,
            payload: Bytes::from_static(b"{}"),
            correlation_id: Uuid::new_v4(),
            causation_id: Uuid::new_v4(),
            timestamp: Utc::now(),
        })
    }

    async fn setup_schema(pool: &PgPool) {
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS inbox_messages (
                handler_id   TEXT        NOT NULL,
                message_id   UUID        NOT NULL,
                aggregate_id UUID        NOT NULL,
                message_type TEXT        NOT NULL,
                payload      JSONB       NOT NULL,
                received_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
                PRIMARY KEY (handler_id, message_id)
            )",
        )
        .execute(pool)
        .await
        .expect("create inbox_messages");

        sqlx::query(
            "CREATE TABLE IF NOT EXISTS inbox_windows (
                handler_id   TEXT        NOT NULL,
                aggregate_id UUID        NOT NULL,
                window_id    UUID        NOT NULL DEFAULT gen_random_uuid(),
                messages     JSONB       NOT NULL DEFAULT '[]',
                status       TEXT        NOT NULL DEFAULT 'pending',
                expires_at   TIMESTAMPTZ,
                created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
                updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
                PRIMARY KEY (handler_id, aggregate_id)
            )",
        )
        .execute(pool)
        .await
        .expect("create inbox_windows");

        sqlx::query(
            "CREATE TABLE IF NOT EXISTS processed_windows (
                window_id    UUID        PRIMARY KEY,
                handler_id   TEXT        NOT NULL,
                processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
            )",
        )
        .execute(pool)
        .await
        .expect("create processed_windows");
    }

    #[sqlx::test(migrations = false)]
    #[ignore = "requires DATABASE_URL"]
    async fn test_deduplication(pool: PgPool) {
        setup_schema(&pool).await;
        let queue = Arc::new(TestQueue::new());
        let inbox = YugabyteInbox::new(pool.clone(), queue.clone());

        let handler_id = "dedup-handler";
        inbox
            .register_handler(HandlerRegistration {
                handler_id: handler_id.to_string(),
                event_types: vec!["TestEvent".to_string()],
            })
            .await
            .expect("register_handler");

        let agg_id = AggregateId::new();
        let msg = make_event(&agg_id, "TestEvent");
        let msg_id = msg.message_id();

        // Submit the same message twice
        inbox
            .submit(handler_id, msg_id, msg.clone())
            .await
            .expect("first submit");
        inbox
            .submit(handler_id, msg_id, msg)
            .await
            .expect("second submit (duplicate)");

        // Assert only one row in inbox_messages
        let count: (i64,) =
            sqlx::query_as("SELECT COUNT(*) FROM inbox_messages WHERE handler_id = $1")
                .bind(handler_id)
                .fetch_one(&pool)
                .await
                .expect("count");

        assert_eq!(
            count.0, 1,
            "duplicate message should not create a second row"
        );
    }

    #[sqlx::test(migrations = false)]
    #[ignore = "requires DATABASE_URL"]
    async fn test_oversight_not_ready_then_ready(pool: PgPool) {
        setup_schema(&pool).await;
        let queue = Arc::new(TestQueue::new());
        let inbox = YugabyteInbox::new(pool.clone(), queue.clone());

        let handler_id = "ready-handler";
        inbox
            .register_handler(HandlerRegistration {
                handler_id: handler_id.to_string(),
                event_types: vec!["TestEvent".to_string()],
            })
            .await
            .expect("register_handler");

        // Oversight: Ready when 2+ messages accumulated
        let call_count = Arc::new(AtomicUsize::new(0));
        let counter = call_count.clone();
        inbox
            .register_oversight(handler_id, move |msgs| {
                counter.fetch_add(1, Ordering::SeqCst);
                if msgs.len() >= 2 {
                    Oversight::Ready
                } else {
                    Oversight::NotReady
                }
            })
            .await;

        let agg_id = AggregateId::new();

        // First message: NotReady
        let msg1 = make_event(&agg_id, "TestEvent");
        inbox
            .submit(handler_id, msg1.message_id(), msg1)
            .await
            .expect("submit msg1");

        // Window should still exist (pending)
        let window_count: (i64,) = sqlx::query_as(
            "SELECT COUNT(*) FROM inbox_windows WHERE handler_id = $1 AND aggregate_id = $2",
        )
        .bind(handler_id)
        .bind(agg_id.as_uuid())
        .fetch_one(&pool)
        .await
        .expect("window count");
        assert_eq!(window_count.0, 1, "window should exist after NotReady");

        // Queue should be empty
        assert_eq!(
            queue.published_count().await,
            0,
            "queue should be empty after NotReady"
        );

        // Second message: Ready
        let msg2 = make_event(&agg_id, "TestEvent");
        inbox
            .submit(handler_id, msg2.message_id(), msg2)
            .await
            .expect("submit msg2");

        // Window should be deleted
        let window_count: (i64,) = sqlx::query_as(
            "SELECT COUNT(*) FROM inbox_windows WHERE handler_id = $1 AND aggregate_id = $2",
        )
        .bind(handler_id)
        .bind(agg_id.as_uuid())
        .fetch_one(&pool)
        .await
        .expect("window count after Ready");
        assert_eq!(
            window_count.0, 0,
            "window should be deleted after Ready dispatch"
        );

        // Queue should have one batch
        assert_eq!(
            queue.published_count().await,
            1,
            "queue should have one batch"
        );

        // Oversight should have been called twice
        assert_eq!(call_count.load(Ordering::SeqCst), 2);
    }

    #[sqlx::test(migrations = false)]
    #[ignore = "requires DATABASE_URL"]
    async fn test_oversight_discard(pool: PgPool) {
        setup_schema(&pool).await;
        let queue = Arc::new(TestQueue::new());
        let inbox = YugabyteInbox::new(pool.clone(), queue.clone());

        let handler_id = "discard-handler";
        inbox
            .register_handler(HandlerRegistration {
                handler_id: handler_id.to_string(),
                event_types: vec!["TestEvent".to_string()],
            })
            .await
            .expect("register_handler");

        // Always discard
        inbox
            .register_oversight(handler_id, |_msgs| Oversight::Discard)
            .await;

        let agg_id = AggregateId::new();
        let msg = make_event(&agg_id, "TestEvent");

        inbox
            .submit(handler_id, msg.message_id(), msg)
            .await
            .expect("submit");

        // Window should be deleted
        let window_count: (i64,) =
            sqlx::query_as("SELECT COUNT(*) FROM inbox_windows WHERE handler_id = $1")
                .bind(handler_id)
                .fetch_one(&pool)
                .await
                .expect("window count");
        assert_eq!(window_count.0, 0, "window should be deleted on Discard");

        // Queue should be empty
        assert_eq!(
            queue.published_count().await,
            0,
            "queue should be empty on Discard"
        );
    }

    #[sqlx::test(migrations = false)]
    #[ignore = "requires DATABASE_URL"]
    async fn test_multiple_handlers_independent(pool: PgPool) {
        setup_schema(&pool).await;
        let queue = Arc::new(TestQueue::new());
        let inbox = YugabyteInbox::new(pool.clone(), queue.clone());

        let handler_a = "handler-a";
        let handler_b = "handler-b";

        inbox
            .register_handler(HandlerRegistration {
                handler_id: handler_a.to_string(),
                event_types: vec!["TestEvent".to_string()],
            })
            .await
            .expect("register handler A");

        inbox
            .register_handler(HandlerRegistration {
                handler_id: handler_b.to_string(),
                event_types: vec!["TestEvent".to_string()],
            })
            .await
            .expect("register handler B");

        // Both use NotReady oversight so windows stay open
        inbox
            .register_oversight(handler_a, |_| Oversight::NotReady)
            .await;
        inbox
            .register_oversight(handler_b, |_| Oversight::NotReady)
            .await;

        let agg_id = AggregateId::new();
        let msg_a = make_event(&agg_id, "TestEvent");
        let msg_b = make_event(&agg_id, "TestEvent");

        inbox
            .submit(handler_a, msg_a.message_id(), msg_a)
            .await
            .expect("submit to handler A");
        inbox
            .submit(handler_b, msg_b.message_id(), msg_b)
            .await
            .expect("submit to handler B");

        // Each handler should have its own window row
        let window_count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM inbox_windows")
            .fetch_one(&pool)
            .await
            .expect("window count");
        assert_eq!(window_count.0, 2, "each handler should have its own window");

        // Verify distinct handler_ids
        let handlers: Vec<(String,)> =
            sqlx::query_as("SELECT handler_id FROM inbox_windows ORDER BY handler_id")
                .fetch_all(&pool)
                .await
                .expect("handler ids");
        assert_eq!(handlers[0].0, handler_a);
        assert_eq!(handlers[1].0, handler_b);
    }

    #[sqlx::test(migrations = false)]
    #[ignore = "requires DATABASE_URL"]
    async fn test_window_expiry(pool: PgPool) {
        setup_schema(&pool).await;
        let queue = Arc::new(TestQueue::new());
        let inbox = YugabyteInbox::new(pool.clone(), queue.clone());

        let handler_id = "expiry-handler";
        inbox
            .register_handler(HandlerRegistration {
                handler_id: handler_id.to_string(),
                event_types: vec!["TestEvent".to_string()],
            })
            .await
            .expect("register_handler");

        // NotReady so window stays open
        inbox
            .register_oversight(handler_id, |_| Oversight::NotReady)
            .await;

        let agg_id = AggregateId::new();
        let msg = make_event(&agg_id, "TestEvent");
        inbox
            .submit(handler_id, msg.message_id(), msg)
            .await
            .expect("submit");

        // Simulate short TTL by setting expires_at to the past
        sqlx::query(
            "UPDATE inbox_windows SET expires_at = now() - INTERVAL '1 second' \
             WHERE handler_id = $1 AND aggregate_id = $2",
        )
        .bind(handler_id)
        .bind(agg_id.as_uuid())
        .execute(&pool)
        .await
        .expect("set expired TTL");

        // Use the sweep method instead of raw SQL
        let swept = inbox.sweep_expired_windows().await.expect("sweep");
        assert_eq!(swept, 1, "should have swept one expired window");

        // Verify status is expired
        let status: (String,) = sqlx::query_as(
            "SELECT status FROM inbox_windows WHERE handler_id = $1 AND aggregate_id = $2",
        )
        .bind(handler_id)
        .bind(agg_id.as_uuid())
        .fetch_one(&pool)
        .await
        .expect("fetch status");
        assert_eq!(status.0, "expired", "window should be marked as expired");
    }
}
