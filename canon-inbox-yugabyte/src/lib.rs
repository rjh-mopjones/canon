use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use tokio::sync::RwLock;
use tracing::{debug, info, warn};
use uuid::Uuid;

use canon_core::{
    AggregateId, CommandEnvelope, EventEnvelope, IncomingMessage, Oversight, Version,
};
use canon_inbound_queue::{InboundQueue, InboundQueueError};
use canon_inbox::{ExpiredWindowEntry, HandlerRegistration, Inbox, InboxError};

/// No-op inbound queue used when the inbox is created for event handler
/// dispatch only. The submit() path no longer publishes to the queue (it
/// marks windows as 'ready' for the dispatcher), so this is never called.
struct NoOpInboundQueue;

#[async_trait]
impl InboundQueue for NoOpInboundQueue {
    async fn publish(
        &self,
        _batch: Vec<IncomingMessage>,
        _aggregate_id: &AggregateId,
    ) -> Result<(), InboundQueueError> {
        Ok(())
    }

    async fn receive(&self) -> Result<Option<Vec<IncomingMessage>>, InboundQueueError> {
        Ok(None)
    }

    async fn commit(&self) -> Result<(), InboundQueueError> {
        Ok(())
    }
}

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
            StoredMessageType::Command => {
                if env.command_id.is_none() {
                    tracing::warn!(
                        aggregate_id = %env.aggregate_id,
                        "inbox message (command) missing command_id, defaulting to nil"
                    );
                }
                if env.command_type.is_none() {
                    tracing::warn!(
                        aggregate_id = %env.aggregate_id,
                        "inbox message (command) missing command_type, defaulting to empty"
                    );
                }
                IncomingMessage::Command(CommandEnvelope {
                    command_id: env.command_id.unwrap_or(Uuid::nil()),
                    aggregate_id: AggregateId::from_uuid(env.aggregate_id),
                    command_type: env.command_type.unwrap_or_default(),
                    correlation_id: env.correlation_id,
                    causation_id: env.causation_id,
                    timestamp: env.timestamp,
                    payload: Bytes::from(env.payload),
                    command_version: env.command_version.unwrap_or(1),
                })
            }
            StoredMessageType::InternalEvent => {
                if env.event_id.is_none() {
                    tracing::warn!(
                        aggregate_id = %env.aggregate_id,
                        "inbox message (internal event) missing event_id, defaulting to nil"
                    );
                }
                if env.event_type.is_none() {
                    tracing::warn!(
                        aggregate_id = %env.aggregate_id,
                        "inbox message (internal event) missing event_type, defaulting to empty"
                    );
                }
                IncomingMessage::InternalEvent(EventEnvelope {
                    event_id: env.event_id.unwrap_or(Uuid::nil()),
                    aggregate_id: AggregateId::from_uuid(env.aggregate_id),
                    version: Version::from_u64(env.version.unwrap_or(0)),
                    event_type: env.event_type.unwrap_or_default(),
                    event_version: env.event_version.unwrap_or(1),
                    payload: Bytes::from(env.payload),
                    correlation_id: env.correlation_id,
                    causation_id: env.causation_id,
                    timestamp: env.timestamp,
                })
            }
            StoredMessageType::ExternalEvent => {
                if env.event_id.is_none() {
                    tracing::warn!(
                        aggregate_id = %env.aggregate_id,
                        "inbox message (external event) missing event_id, defaulting to nil"
                    );
                }
                if env.event_type.is_none() {
                    tracing::warn!(
                        aggregate_id = %env.aggregate_id,
                        "inbox message (external event) missing event_type, defaulting to empty"
                    );
                }
                IncomingMessage::ExternalEvent(EventEnvelope {
                    event_id: env.event_id.unwrap_or(Uuid::nil()),
                    aggregate_id: AggregateId::from_uuid(env.aggregate_id),
                    version: Version::from_u64(env.version.unwrap_or(0)),
                    event_type: env.event_type.unwrap_or_default(),
                    event_version: env.event_version.unwrap_or(1),
                    payload: Bytes::from(env.payload),
                    correlation_id: env.correlation_id,
                    causation_id: env.causation_id,
                    timestamp: env.timestamp,
                })
            }
        }
    }
}

// ── Oversight function type ──────────────────────────────────────────────────

type OversightFn = Box<dyn Fn(&[IncomingMessage]) -> Oversight + Send + Sync>;

struct HandlerEntry {
    oversight_fn: Option<OversightFn>,
    registration: HandlerRegistration,
}

// ── YugabyteInbox ────────────────────────────────────────────────────────────

/// YugabyteDB-backed implementation of the [`Inbox`] port.
///
/// Provides idempotent message intake, windowed accumulation per
/// `(handler_id, aggregate_id)`, oversight-gated dispatch to the inbound queue,
/// and window expiry with dead-letter integration.
///
/// # Window expiry
///
/// When a handler is registered with a `window_ttl`, new windows get an
/// `expires_at` timestamp. The [`spawn_cleanup_task`] function starts a
/// background tokio task that periodically sweeps expired windows and collects
/// them for dead lettering.
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
    /// Retained for the `new()` constructor. The event handler dispatch path
    /// marks windows as `ready` for the dispatcher to poll — it does not
    /// publish to this queue.
    _queue: Arc<dyn InboundQueue>,
}

impl YugabyteInbox {
    /// Create a new inbox from an existing connection pool and inbound queue.
    pub fn new(pool: PgPool, queue: Arc<dyn InboundQueue>) -> Self {
        Self {
            pool,
            handlers: Arc::new(RwLock::new(HashMap::new())),
            _queue: queue,
        }
    }

    /// Create a new inbox for event handler dispatch only.
    ///
    /// When oversight returns `Ready`, the window is marked `ready` in the
    /// database for the dispatcher to poll — no inbound queue publish is needed.
    /// This constructor avoids requiring an `InboundQueue` dependency.
    pub fn for_event_handler_dispatch(pool: PgPool) -> Self {
        // Create a no-op queue since submit() no longer publishes to it.
        Self {
            pool,
            handlers: Arc::new(RwLock::new(HashMap::new())),
            _queue: Arc::new(NoOpInboundQueue),
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

    /// Look up the TTL for a given handler, returning `None` if the handler is
    /// not registered or has no TTL.
    async fn handler_ttl(&self, handler_id: &str) -> Option<Duration> {
        let handlers = self.handlers.read().await;
        handlers
            .get(handler_id)
            .and_then(|e| e.registration.window_ttl)
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
        let payload = serde_json::to_vec(&stored)?;

        let result = sqlx::query(
            "INSERT INTO inbox_messages (handler_id, message_id, aggregate_id, message_type, payload) \
             VALUES ($1, $2, $3, $4, $5) \
             ON CONFLICT (handler_id, message_id) DO NOTHING",
        )
        .bind(handler_id)
        .bind(message_id)
        .bind(aggregate_id.as_uuid())
        .bind(message_type)
        .bind(&payload)
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
    ///
    /// When the handler has a `window_ttl`, the initial insert sets `expires_at`.
    /// Subsequent appends to the same window do not reset the expiry.
    async fn accumulate(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        handler_id: &str,
        aggregate_id: &AggregateId,
        message: &IncomingMessage,
    ) -> Result<(), YugabyteInboxError> {
        let stored = StoredMessage::from_incoming(message);
        let envelope_json = serde_json::to_value(&stored.envelope)?;
        let envelope_array = serde_json::Value::Array(vec![envelope_json]);
        let message_type = match message {
            IncomingMessage::Command(_) => "command",
            IncomingMessage::InternalEvent(_) => "internal",
            IncomingMessage::ExternalEvent(_) => "external",
        };

        let expires_at = self.compute_expires_at(handler_id).await;

        sqlx::query(
            "INSERT INTO inbox_windows (handler_id, aggregate_id, messages, message_type, expires_at) \
             VALUES ($1, $2, $3, $4, $5) \
             ON CONFLICT (handler_id, aggregate_id) \
             DO UPDATE SET messages = inbox_windows.messages || $3, \
                           message_type = COALESCE(inbox_windows.message_type, EXCLUDED.message_type), \
                           updated_at = now()",
        )
        .bind(handler_id)
        .bind(aggregate_id.as_uuid())
        .bind(&envelope_array)
        .bind(message_type)
        .bind(expires_at)
        .execute(&mut **tx)
        .await?;

        Ok(())
    }

    /// Compute the `expires_at` timestamp for a new window.
    ///
    /// If the TTL duration overflows `chrono::TimeDelta`, we clamp to 24 hours
    /// and log a warning. This avoids `TimeDelta::MAX` which can cause
    /// arithmetic overflow in timestamp calculations.
    async fn compute_expires_at(&self, handler_id: &str) -> Option<DateTime<Utc>> {
        self.handler_ttl(handler_id).await.map(|ttl| {
            let delta = chrono::Duration::from_std(ttl).unwrap_or_else(|_| {
                warn!(
                    handler_id,
                    ttl_ms = ttl.as_millis() as u64,
                    "inbox TTL overflows chrono::TimeDelta, clamping to 24 hours"
                );
                chrono::TimeDelta::hours(24)
            });
            Utc::now() + delta
        })
    }

    /// Evaluate oversight and return a pending dispatch if the window is ready.
    ///
    /// Returns `Some((batch, aggregate_id, window_id))` when the oversight
    /// decision is `Ready` -- the caller must publish this batch **after** the
    /// transaction commits. The `window_id` travels with the batch so the
    /// consumer can use it for batch-level idempotency via `try_mark_window_processed`.
    /// Returns `None` for `NotReady` and `Discard`.
    async fn evaluate_oversight(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        handler_id: &str,
        aggregate_id: &AggregateId,
    ) -> Result<Option<(Vec<IncomingMessage>, AggregateId, Uuid)>, YugabyteInboxError> {
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

        // Evaluate oversight. Handlers auto-registered via `#[event_handler]`
        // inventory may submit before explicit register_handler — default to
        // Ready so the window dispatches without manual oversight.
        let decision = {
            let handlers = self.handlers.read().await;
            match handlers.get(handler_id) {
                Some(entry) => match &entry.oversight_fn {
                    Some(f) => f(&incoming_messages),
                    None => Oversight::Ready,
                },
                None => Oversight::Ready,
            }
        };

        match decision {
            Oversight::Ready => {
                debug!(
                    handler_id,
                    "oversight: Ready — marking window ready for dispatcher"
                );

                // Mark as ready for the dispatcher to poll via poll_ready_windows().
                // The window row and its messages are preserved — the dispatcher
                // will call mark_window_dispatched() after processing.
                sqlx::query(
                    "UPDATE inbox_windows SET status = 'ready', updated_at = now() \
                     WHERE handler_id = $1 AND aggregate_id = $2",
                )
                .bind(handler_id)
                .bind(aggregate_id.as_uuid())
                .execute(&mut **tx)
                .await?;

                Ok(Some((
                    incoming_messages,
                    AggregateId::from_uuid(row.aggregate_id),
                    row.window_id,
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
        info!(
            handler_id = %registration.handler_id,
            window_ttl = ?registration.window_ttl,
            "registering inbox handler"
        );
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
            // Step 2: Accumulate (now sets expires_at from handler TTL)
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

        // When oversight returns Ready, the window is now marked 'ready' in the
        // database. The dispatcher's poll_ready_windows() picks it up — no need
        // to publish to the inbound queue.
        let _ = pending_dispatch;

        Ok(())
    }

    async fn try_mark_window_processed(
        &self,
        window_id: Uuid,
        handler_id: &str,
    ) -> Result<bool, InboxError> {
        let result = sqlx::query(
            "INSERT INTO processed_windows (window_id, handler_id) VALUES ($1, $2) \
             ON CONFLICT (window_id) DO NOTHING",
        )
        .bind(window_id)
        .bind(handler_id)
        .execute(&self.pool)
        .await
        .map_err(YugabyteInboxError::from)
        .map_err(InboxError::from)?;

        let is_new = result.rows_affected() > 0;
        if !is_new {
            debug!(%window_id, handler_id, "window already processed, skipping batch");
        } else {
            debug!(%window_id, handler_id, "window marked as processed");
        }
        Ok(is_new)
    }

    async fn sweep_expired_windows(&self) -> Result<u64, InboxError> {
        let result = sqlx::query(
            "UPDATE inbox_windows SET status = 'expired', updated_at = now() \
             WHERE expires_at IS NOT NULL AND expires_at < now() AND status = 'pending'",
        )
        .execute(&self.pool)
        .await
        .map_err(YugabyteInboxError::from)
        .map_err(InboxError::from)?;
        let count = result.rows_affected();
        if count > 0 {
            info!(count, "swept expired inbox windows");
        }
        Ok(count)
    }

    async fn collect_expired_windows(&self) -> Result<Vec<ExpiredWindowEntry>, InboxError> {
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(YugabyteInboxError::from)
            .map_err(InboxError::from)?;

        // Select all expired windows and lock them for update
        let rows: Vec<WindowRow> = sqlx::query_as(
            "SELECT handler_id, aggregate_id, window_id, messages, status, expires_at \
             FROM inbox_windows \
             WHERE status = 'expired' \
             FOR UPDATE SKIP LOCKED",
        )
        .fetch_all(&mut *tx)
        .await
        .map_err(YugabyteInboxError::from)
        .map_err(InboxError::from)?;

        let mut entries = Vec::with_capacity(rows.len());

        for row in &rows {
            let stored_messages: Vec<StoredMessage> = serde_json::from_value(row.messages.clone())
                .map_err(YugabyteInboxError::from)
                .map_err(InboxError::from)?;

            let incoming_messages: Vec<IncomingMessage> = stored_messages
                .into_iter()
                .map(StoredMessage::into_incoming)
                .collect();

            entries.push(ExpiredWindowEntry {
                handler_id: row.handler_id.clone(),
                correlation_key: row.aggregate_id,
                aggregate_id: AggregateId::from_uuid(row.aggregate_id),
                messages: incoming_messages,
            });
        }

        // Transition all collected windows to dead_lettered and delete them
        for row in &rows {
            sqlx::query(
                "UPDATE inbox_windows SET status = 'dead_lettered', updated_at = now() \
                 WHERE handler_id = $1 AND aggregate_id = $2 AND status = 'expired'",
            )
            .bind(&row.handler_id)
            .bind(row.aggregate_id)
            .execute(&mut *tx)
            .await
            .map_err(YugabyteInboxError::from)
            .map_err(InboxError::from)?;
        }

        tx.commit()
            .await
            .map_err(YugabyteInboxError::from)
            .map_err(InboxError::from)?;

        if !entries.is_empty() {
            info!(
                count = entries.len(),
                "collected expired windows for dead lettering"
            );
        }

        Ok(entries)
    }

    async fn requeue_expired_window(
        &self,
        handler_id: &str,
        _correlation_key: Uuid,
        messages: Vec<IncomingMessage>,
    ) -> Result<(), InboxError> {
        // Delete old dedup records for these messages so they can be re-processed
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(YugabyteInboxError::from)
            .map_err(InboxError::from)?;

        for msg in &messages {
            sqlx::query("DELETE FROM inbox_messages WHERE handler_id = $1 AND message_id = $2")
                .bind(handler_id)
                .bind(msg.message_id())
                .execute(&mut *tx)
                .await
                .map_err(YugabyteInboxError::from)
                .map_err(InboxError::from)?;
        }

        // Also delete any dead_lettered window row for this handler + aggregate
        if let Some(first_msg) = messages.first() {
            sqlx::query(
                "DELETE FROM inbox_windows \
                 WHERE handler_id = $1 AND aggregate_id = $2 AND status = 'dead_lettered'",
            )
            .bind(handler_id)
            .bind(first_msg.aggregate_id().as_uuid())
            .execute(&mut *tx)
            .await
            .map_err(YugabyteInboxError::from)
            .map_err(InboxError::from)?;
        }

        tx.commit()
            .await
            .map_err(YugabyteInboxError::from)
            .map_err(InboxError::from)?;

        // Re-submit each message through the normal path (dedup + oversight)
        // This gives them fresh expires_at and runs oversight from scratch.
        for msg in messages {
            let msg_id = msg.message_id();
            self.submit(handler_id, msg_id, msg).await?;
        }

        info!(
            handler_id,
            "requeued expired window — oversight runs from scratch"
        );
        Ok(())
    }
}

impl YugabyteInbox {
    /// Delete windows that have been in terminal states (`expired`, `dead_lettered`,
    /// `dispatched`) for longer than `retention`. Returns the number of rows deleted.
    ///
    /// This is a garbage-collection method intended to be called periodically
    /// (e.g. from the cleanup task or an admin endpoint) to prevent the
    /// `inbox_windows` table from growing unboundedly.
    pub async fn cleanup_expired_windows(
        &self,
        retention: std::time::Duration,
    ) -> Result<u64, YugabyteInboxError> {
        let delta = chrono::Duration::from_std(retention).unwrap_or_else(|_| {
            warn!(
                retention_ms = retention.as_millis() as u64,
                "retention duration overflows chrono::TimeDelta, clamping to 7 days"
            );
            chrono::TimeDelta::days(7)
        });
        let cutoff = Utc::now() - delta;

        let result = sqlx::query(
            "DELETE FROM inbox_windows \
             WHERE status IN ('expired', 'dead_lettered', 'dispatched') \
               AND updated_at < $1",
        )
        .bind(cutoff)
        .execute(&self.pool)
        .await?;

        let deleted = result.rows_affected();
        if deleted > 0 {
            info!(deleted, "garbage-collected old inbox windows");
        }
        Ok(deleted)
    }
}

// ── Cleanup task ─────────────────────────────────────────────────────────────

/// Configuration for the inbox cleanup background task.
#[derive(Debug, Clone)]
pub struct CleanupConfig {
    /// How often the cleanup task runs. Defaults to 30 seconds.
    pub interval: Duration,
}

impl Default for CleanupConfig {
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(30),
        }
    }
}

/// Spawn a background tokio task that periodically sweeps expired inbox windows
/// and collects them for dead lettering.
///
/// The returned [`tokio::task::JoinHandle`] can be used to cancel the task
/// (via `handle.abort()`) or await its completion.
///
/// The `dead_letter_fn` callback is invoked for each batch of expired windows.
/// It receives the list of expired window entries and should persist them to the
/// dead letter store. If the callback returns an error, the windows remain in
/// `expired` status and will be retried on the next sweep.
///
/// # Example
///
/// ```rust,ignore
/// let handle = spawn_cleanup_task(
///     inbox.clone(),
///     CleanupConfig::default(),
///     |entries| async move {
///         for entry in entries {
///             dead_letter_store.store(DeadLetter { ... }).await?;
///         }
///         Ok(())
///     },
/// );
/// ```
pub fn spawn_cleanup_task<F, Fut>(
    inbox: YugabyteInbox,
    config: CleanupConfig,
    dead_letter_fn: F,
) -> tokio::task::JoinHandle<()>
where
    F: Fn(Vec<ExpiredWindowEntry>) -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = Result<(), Box<dyn std::error::Error + Send + Sync>>>
        + Send
        + 'static,
{
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(config.interval);
        loop {
            interval.tick().await;

            // Step 1: Sweep — mark pending windows past TTL as expired
            match inbox.sweep_expired_windows().await {
                Ok(count) => {
                    if count > 0 {
                        debug!(count, "cleanup task swept expired windows");
                    }
                }
                Err(e) => {
                    warn!(error = %e, "cleanup task: sweep_expired_windows failed");
                    continue;
                }
            }

            // Step 2: Collect — transition expired → dead_lettered and return entries
            let entries = match inbox.collect_expired_windows().await {
                Ok(entries) => entries,
                Err(e) => {
                    warn!(error = %e, "cleanup task: collect_expired_windows failed");
                    continue;
                }
            };

            if entries.is_empty() {
                continue;
            }

            // Step 3: Persist to dead letter store via user-provided callback
            if let Err(e) = dead_letter_fn(entries).await {
                warn!(error = %e, "cleanup task: dead_letter_fn failed");
            }
        }
    })
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use canon_core::{AggregateId, IncomingMessage, Oversight};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use testcontainers::runners::AsyncRunner;
    use testcontainers::ContainerAsync;
    use testcontainers_modules::postgres::Postgres;

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
        .execute(&pool)
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
        .execute(&pool)
        .await
        .expect("create inbox_windows");

        sqlx::query(
            "CREATE TABLE IF NOT EXISTS processed_windows (
                window_id    UUID        PRIMARY KEY,
                handler_id   TEXT        NOT NULL,
                processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
            )",
        )
        .execute(&pool)
        .await
        .expect("create processed_windows");

        (container, pool)
    }

    fn reg(handler_id: &str) -> HandlerRegistration {
        HandlerRegistration {
            handler_id: handler_id.to_string(),
            event_types: vec!["TestEvent".to_string()],
            window_ttl: None,
        }
    }

    fn reg_with_ttl(handler_id: &str, ttl: Duration) -> HandlerRegistration {
        HandlerRegistration {
            handler_id: handler_id.to_string(),
            event_types: vec!["TestEvent".to_string()],
            window_ttl: Some(ttl),
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_deduplication() {
        let (_container, pool) = setup_container().await;
        let queue = Arc::new(TestQueue::new());
        let inbox = YugabyteInbox::new(pool.clone(), queue.clone());

        let handler_id = "dedup-handler";
        inbox
            .register_handler(reg(handler_id))
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

    #[tokio::test(flavor = "multi_thread")]
    async fn test_oversight_not_ready_then_ready() {
        let (_container, pool) = setup_container().await;
        let queue = Arc::new(TestQueue::new());
        let inbox = YugabyteInbox::new(pool.clone(), queue.clone());

        let handler_id = "ready-handler";
        inbox
            .register_handler(reg(handler_id))
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

        // Window should be marked 'ready' for the dispatcher to poll
        // (no longer deleted — the dispatcher calls mark_window_dispatched).
        let window_status: (String,) = sqlx::query_as(
            "SELECT status FROM inbox_windows WHERE handler_id = $1 AND aggregate_id = $2",
        )
        .bind(handler_id)
        .bind(agg_id.as_uuid())
        .fetch_one(&pool)
        .await
        .expect("window status after Ready");
        assert_eq!(
            window_status.0, "ready",
            "window should be marked ready for dispatcher"
        );

        // Queue should NOT have any batches — the dispatcher polls ready
        // windows directly instead of publishing to the inbound queue.
        assert_eq!(
            queue.published_count().await,
            0,
            "queue should be empty (dispatcher polls ready windows)"
        );

        // Oversight should have been called twice
        assert_eq!(call_count.load(Ordering::SeqCst), 2);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_oversight_discard() {
        let (_container, pool) = setup_container().await;
        let queue = Arc::new(TestQueue::new());
        let inbox = YugabyteInbox::new(pool.clone(), queue.clone());

        let handler_id = "discard-handler";
        inbox
            .register_handler(reg(handler_id))
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

    #[tokio::test(flavor = "multi_thread")]
    async fn test_multiple_handlers_independent() {
        let (_container, pool) = setup_container().await;
        let queue = Arc::new(TestQueue::new());
        let inbox = YugabyteInbox::new(pool.clone(), queue.clone());

        let handler_a = "handler-a";
        let handler_b = "handler-b";

        inbox
            .register_handler(reg(handler_a))
            .await
            .expect("register handler A");

        inbox
            .register_handler(reg(handler_b))
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

    #[tokio::test(flavor = "multi_thread")]
    async fn test_window_expiry_sweep() {
        let (_container, pool) = setup_container().await;
        let queue = Arc::new(TestQueue::new());
        let inbox = YugabyteInbox::new(pool.clone(), queue.clone());

        let handler_id = "expiry-handler";
        inbox
            .register_handler(reg(handler_id))
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

        // Use the sweep method
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

    #[tokio::test(flavor = "multi_thread")]
    async fn test_try_mark_window_processed_new_window() {
        let (_container, pool) = setup_container().await;
        let queue = Arc::new(TestQueue::new());
        let inbox = YugabyteInbox::new(pool.clone(), queue);

        let window_id = Uuid::new_v4();
        let handler_id = "batch-handler";

        // First mark: should return true (new)
        let is_new = inbox
            .try_mark_window_processed(window_id, handler_id)
            .await
            .expect("try_mark_window_processed");
        assert!(is_new, "first mark should return true");

        // Verify row exists in processed_windows
        let count: (i64,) =
            sqlx::query_as("SELECT COUNT(*) FROM processed_windows WHERE window_id = $1")
                .bind(window_id)
                .fetch_one(&pool)
                .await
                .expect("count");
        assert_eq!(count.0, 1, "processed_windows should have one row");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_try_mark_window_processed_duplicate_is_noop() {
        let (_container, pool) = setup_container().await;
        let queue = Arc::new(TestQueue::new());
        let inbox = YugabyteInbox::new(pool.clone(), queue);

        let window_id = Uuid::new_v4();
        let handler_id = "batch-handler";

        // First mark
        let is_new = inbox
            .try_mark_window_processed(window_id, handler_id)
            .await
            .expect("first mark");
        assert!(is_new, "first mark should return true");

        // Second mark: should return false (duplicate / already processed)
        let is_new = inbox
            .try_mark_window_processed(window_id, handler_id)
            .await
            .expect("second mark");
        assert!(!is_new, "second mark should return false (duplicate)");

        // Still only one row
        let count: (i64,) =
            sqlx::query_as("SELECT COUNT(*) FROM processed_windows WHERE window_id = $1")
                .bind(window_id)
                .fetch_one(&pool)
                .await
                .expect("count");
        assert_eq!(
            count.0, 1,
            "ON CONFLICT DO NOTHING should prevent duplicates"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_try_mark_window_processed_different_windows_independent() {
        let (_container, pool) = setup_container().await;
        let queue = Arc::new(TestQueue::new());
        let inbox = YugabyteInbox::new(pool.clone(), queue);

        let w1 = Uuid::new_v4();
        let w2 = Uuid::new_v4();
        let handler_id = "batch-handler";

        // Mark two different windows
        assert!(
            inbox
                .try_mark_window_processed(w1, handler_id)
                .await
                .expect("mark w1"),
            "w1 should be new"
        );
        assert!(
            inbox
                .try_mark_window_processed(w2, handler_id)
                .await
                .expect("mark w2"),
            "w2 should be new"
        );

        // Both should now be duplicates
        assert!(
            !inbox
                .try_mark_window_processed(w1, handler_id)
                .await
                .expect("re-mark w1"),
            "w1 should be duplicate"
        );
        assert!(
            !inbox
                .try_mark_window_processed(w2, handler_id)
                .await
                .expect("re-mark w2"),
            "w2 should be duplicate"
        );

        // Two rows total
        let count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM processed_windows")
            .fetch_one(&pool)
            .await
            .expect("count");
        assert_eq!(count.0, 2, "should have two independent processed windows");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_collect_expired_windows() {
        let (_container, pool) = setup_container().await;
        let queue = Arc::new(TestQueue::new());
        let inbox = YugabyteInbox::new(pool.clone(), queue.clone());

        let handler_id = "collect-handler";
        inbox
            .register_handler(reg(handler_id))
            .await
            .expect("register_handler");

        inbox
            .register_oversight(handler_id, |_| Oversight::NotReady)
            .await;

        let agg_id = AggregateId::new();
        let msg = make_event(&agg_id, "TestEvent");
        inbox
            .submit(handler_id, msg.message_id(), msg)
            .await
            .expect("submit");

        // Set expires_at to past
        sqlx::query(
            "UPDATE inbox_windows SET expires_at = now() - INTERVAL '1 second' \
             WHERE handler_id = $1 AND aggregate_id = $2",
        )
        .bind(handler_id)
        .bind(agg_id.as_uuid())
        .execute(&pool)
        .await
        .expect("set expired TTL");

        // Sweep first
        inbox.sweep_expired_windows().await.expect("sweep");

        // Collect
        let entries = inbox.collect_expired_windows().await.expect("collect");
        assert_eq!(entries.len(), 1, "should have collected one expired window");
        assert_eq!(entries[0].handler_id, handler_id);
        assert_eq!(entries[0].messages.len(), 1);

        // Verify status is now dead_lettered
        let status: (String,) = sqlx::query_as(
            "SELECT status FROM inbox_windows WHERE handler_id = $1 AND aggregate_id = $2",
        )
        .bind(handler_id)
        .bind(agg_id.as_uuid())
        .fetch_one(&pool)
        .await
        .expect("fetch status");
        assert_eq!(
            status.0, "dead_lettered",
            "window should be marked as dead_lettered"
        );

        // Second collect should be empty
        let entries2 = inbox.collect_expired_windows().await.expect("collect2");
        assert!(
            entries2.is_empty(),
            "already collected windows should not appear again"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_requeue_expired_window() {
        let (_container, pool) = setup_container().await;
        let queue = Arc::new(TestQueue::new());
        let inbox = YugabyteInbox::new(pool.clone(), queue.clone());

        let handler_id = "requeue-handler";
        inbox
            .register_handler(reg(handler_id))
            .await
            .expect("register_handler");

        // Start with NotReady oversight
        inbox
            .register_oversight(handler_id, |_| Oversight::NotReady)
            .await;

        let agg_id = AggregateId::new();
        let msg = make_event(&agg_id, "TestEvent");
        inbox
            .submit(handler_id, msg.message_id(), msg)
            .await
            .expect("submit");

        // Expire it
        sqlx::query(
            "UPDATE inbox_windows SET expires_at = now() - INTERVAL '1 second' \
             WHERE handler_id = $1 AND aggregate_id = $2",
        )
        .bind(handler_id)
        .bind(agg_id.as_uuid())
        .execute(&pool)
        .await
        .expect("set expired TTL");

        inbox.sweep_expired_windows().await.expect("sweep");
        let entries = inbox.collect_expired_windows().await.expect("collect");
        assert_eq!(entries.len(), 1);

        // Now switch to Ready oversight
        inbox
            .register_oversight(handler_id, |_| Oversight::Ready)
            .await;

        // Requeue
        let correlation_key = entries[0].correlation_key;
        inbox
            .requeue_expired_window(handler_id, correlation_key, entries[0].messages.clone())
            .await
            .expect("requeue");

        // The message should have been resubmitted — since oversight now returns
        // Ready, the window should be marked 'ready' for the dispatcher to poll.
        // The queue is no longer used for dispatch (dispatcher polls directly).
        assert_eq!(
            queue.published_count().await,
            0,
            "queue should be empty (dispatcher polls ready windows)"
        );

        // dead_lettered window row should be replaced by a 'ready' window
        let window_count: (i64,) = sqlx::query_as(
            "SELECT COUNT(*) FROM inbox_windows \
             WHERE handler_id = $1 AND aggregate_id = $2 AND status = 'dead_lettered'",
        )
        .bind(handler_id)
        .bind(agg_id.as_uuid())
        .fetch_one(&pool)
        .await
        .expect("window count");
        assert_eq!(
            window_count.0, 0,
            "dead_lettered window should be removed after requeue"
        );

        // A 'ready' window should exist for dispatcher polling
        let ready_count: (i64,) = sqlx::query_as(
            "SELECT COUNT(*) FROM inbox_windows \
             WHERE handler_id = $1 AND aggregate_id = $2 AND status = 'ready'",
        )
        .bind(handler_id)
        .bind(agg_id.as_uuid())
        .fetch_one(&pool)
        .await
        .expect("ready window count");
        assert_eq!(
            ready_count.0, 1,
            "requeued window should be marked ready for dispatcher"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_handler_with_ttl_sets_expires_at() {
        let (_container, pool) = setup_container().await;
        let queue = Arc::new(TestQueue::new());
        let inbox = YugabyteInbox::new(pool.clone(), queue.clone());

        let handler_id = "ttl-handler";
        inbox
            .register_handler(reg_with_ttl(handler_id, Duration::from_secs(3600)))
            .await
            .expect("register_handler");

        inbox
            .register_oversight(handler_id, |_| Oversight::NotReady)
            .await;

        let agg_id = AggregateId::new();
        let msg = make_event(&agg_id, "TestEvent");
        inbox
            .submit(handler_id, msg.message_id(), msg)
            .await
            .expect("submit");

        // Verify expires_at is set
        let row: (Option<DateTime<Utc>>,) = sqlx::query_as(
            "SELECT expires_at FROM inbox_windows \
             WHERE handler_id = $1 AND aggregate_id = $2",
        )
        .bind(handler_id)
        .bind(agg_id.as_uuid())
        .fetch_one(&pool)
        .await
        .expect("fetch expires_at");

        assert!(
            row.0.is_some(),
            "expires_at should be set for handler with TTL"
        );

        // expires_at should be roughly 1 hour from now
        let expires_at = row.0.expect("expires_at is Some");
        let diff = expires_at - Utc::now();
        assert!(
            diff.num_seconds() > 3500 && diff.num_seconds() <= 3600,
            "expires_at should be ~1 hour from now, got {} seconds",
            diff.num_seconds()
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_handler_without_ttl_no_expires_at() {
        let (_container, pool) = setup_container().await;
        let queue = Arc::new(TestQueue::new());
        let inbox = YugabyteInbox::new(pool.clone(), queue.clone());

        let handler_id = "no-ttl-handler";
        inbox
            .register_handler(reg(handler_id))
            .await
            .expect("register_handler");

        inbox
            .register_oversight(handler_id, |_| Oversight::NotReady)
            .await;

        let agg_id = AggregateId::new();
        let msg = make_event(&agg_id, "TestEvent");
        inbox
            .submit(handler_id, msg.message_id(), msg)
            .await
            .expect("submit");

        // Verify expires_at is NULL
        let row: (Option<DateTime<Utc>>,) = sqlx::query_as(
            "SELECT expires_at FROM inbox_windows \
             WHERE handler_id = $1 AND aggregate_id = $2",
        )
        .bind(handler_id)
        .bind(agg_id.as_uuid())
        .fetch_one(&pool)
        .await
        .expect("fetch expires_at");

        assert!(
            row.0.is_none(),
            "expires_at should be NULL for handler without TTL"
        );

        // Sweep should not affect this window
        let swept = inbox.sweep_expired_windows().await.expect("sweep");
        assert_eq!(swept, 0, "windows without TTL should not be swept");
    }
}
