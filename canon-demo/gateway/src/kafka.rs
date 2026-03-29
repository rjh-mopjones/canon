//! Kafka consumers that apply events directly to per-session game projections.

use std::sync::atomic::Ordering;
use std::sync::Arc;

use rskafka::client::partition::UnknownTopicHandling;
use rskafka::client::ClientBuilder;
use tokio::sync::RwLock;
use tracing::{error, info, warn};
use uuid::Uuid;

use canon_core::EventEnvelope;

use crate::session::SessionStore;
use crate::state::InfraStatus;

/// Errors that can occur in the gateway Kafka consumer.
#[derive(Debug, thiserror::Error)]
pub enum KafkaConsumerError {
    #[error("kafka error: {0}")]
    Kafka(String),
}

/// Service names for building topic-to-service mappings.
const SERVICES: &[&str] = &["fleet", "cargo", "navigation", "supply", "station"];

/// Spawn one Kafka polling task per topic. Each task deserialises published
/// events and applies them directly to matching session projections.
pub fn spawn_kafka_consumers(
    brokers: &str,
    sessions: SessionStore,
    offset_pool: sqlx::PgPool,
    topic_prefix: &str,
) {
    for service in SERVICES {
        let brokers = brokers.to_owned();
        let topic = format!("{topic_prefix}.{service}.events");
        let sessions = sessions.clone();
        let pool = offset_pool.clone();
        let service = service.to_string();

        tokio::spawn(async move {
            if let Err(e) = consume_topic(&brokers, &topic, &service, &sessions, &pool).await {
                error!(topic = %topic, error = %e, "kafka consumer failed to start");
            }
        });
    }
}

async fn consume_topic(
    brokers: &str,
    topic: &str,
    service: &str,
    sessions: &SessionStore,
    pool: &sqlx::PgPool,
) -> Result<(), KafkaConsumerError> {
    let broker_list: Vec<String> = brokers.split(',').map(|s| s.trim().to_owned()).collect();

    let client = ClientBuilder::new(broker_list)
        .build()
        .await
        .map_err(|e| KafkaConsumerError::Kafka(e.to_string()))?;

    let partition_client = Arc::new(
        client
            .partition_client(topic, 0, UnknownTopicHandling::Retry)
            .await
            .map_err(|e| KafkaConsumerError::Kafka(e.to_string()))?,
    );

    let consumer_id = format!("gateway:{topic}");
    let persisted = canon_demo_shared::offsets::load_offset(pool, &consumer_id).await;
    let mut next_offset: i64 = persisted.map(|o| o + 1).unwrap_or(0);

    info!(topic = %topic, offset = next_offset, "gateway kafka consumer started (rskafka)");

    loop {
        match partition_client
            .fetch_records(next_offset, 1..1_048_576, 100)
            .await
        {
            Ok((records, _watermark)) => {
                if records.is_empty() {
                    continue;
                }

                for record_and_offset in &records {
                    next_offset = record_and_offset.offset + 1;

                    let payload = match record_and_offset.record.value.as_ref() {
                        Some(p) => p,
                        None => continue,
                    };

                    let envelope: EventEnvelope = match serde_json::from_slice(payload) {
                        Ok(e) => e,
                        Err(e) => {
                            warn!(error = %e, topic = %topic, "failed to deserialise event");
                            continue;
                        }
                    };

                    let aggregate_id = *envelope.aggregate_id.as_uuid();
                    let related_ids =
                        serde_json::from_slice::<serde_json::Value>(&envelope.payload)
                            .ok()
                            .map(|value| extract_uuid_strings(&value))
                            .unwrap_or_default();

                    // Apply event to all matching session projections
                    apply_to_sessions(sessions, service, &envelope, aggregate_id, &related_ids)
                        .await;
                }

                canon_demo_shared::offsets::save_offset(pool, &consumer_id, topic, next_offset - 1)
                    .await;
            }
            Err(e) => {
                warn!(error = %e, topic = %topic, "kafka fetch failed, retrying");
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            }
        }
    }
}

/// Apply an event to all sessions whose tracked IDs match.
async fn apply_to_sessions(
    sessions: &SessionStore,
    service: &str,
    envelope: &EventEnvelope,
    aggregate_id: Uuid,
    related_ids: &[Uuid],
) {
    let store = sessions.read().await;
    for session in store.values() {
        let should_apply = {
            let projection = session.projection.read().await;
            projection.tracks_aggregate(aggregate_id, related_ids)
        };

        if should_apply {
            let mut projection = session.projection.write().await;
            projection.apply_event(service, envelope);
            // Mirror game_over to atomic for lock-free reaper access
            session
                .game_over
                .store(projection.game_over, Ordering::Relaxed);
        }
    }
}

/// Spawn a background task that refreshes cached infra health.
pub fn spawn_infra_status_broadcaster(
    yugabyte_pool: sqlx::PgPool,
    cassandra_nodes: String,
    kafka_brokers: String,
    shared_infra: Arc<RwLock<InfraStatus>>,
) {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(10));

        loop {
            interval.tick().await;

            let yugabyte_ok = sqlx::query("SELECT 1")
                .execute(&yugabyte_pool)
                .await
                .is_ok();

            let cassandra_ok = {
                let addr = cassandra_nodes
                    .split(',')
                    .next()
                    .unwrap_or("cassandra:9042")
                    .trim();
                tokio::net::TcpStream::connect(addr).await.is_ok()
            };

            let kafka_ok = {
                let addr = kafka_brokers
                    .split(',')
                    .next()
                    .unwrap_or("kafka:9092")
                    .trim();
                tokio::net::TcpStream::connect(addr).await.is_ok()
            };

            {
                let mut infra = shared_infra.write().await;
                infra.kafka = kafka_ok;
                infra.yugabyte = yugabyte_ok;
                infra.cassandra = cassandra_ok;
            }
        }
    });
}

fn extract_uuid_strings(value: &serde_json::Value) -> Vec<Uuid> {
    let mut ids = Vec::new();
    collect_uuid_strings(value, &mut ids);
    ids.sort_unstable();
    ids.dedup();
    ids
}

fn collect_uuid_strings(value: &serde_json::Value, ids: &mut Vec<Uuid>) {
    match value {
        serde_json::Value::String(s) => {
            if let Ok(id) = Uuid::parse_str(s) {
                ids.push(id);
            }
        }
        serde_json::Value::Array(items) => {
            for item in items {
                collect_uuid_strings(item, ids);
            }
        }
        serde_json::Value::Object(map) => {
            for item in map.values() {
                collect_uuid_strings(item, ids);
            }
        }
        _ => {}
    }
}
