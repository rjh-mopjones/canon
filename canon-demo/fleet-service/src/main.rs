use tracing::info;

use canon_core::{
    EventPayloadSnapshotProvider, InMemoryDeadLetterStore, InMemoryEventStore,
    InMemoryOutboundQueue, InMemoryOutboxPublisher, InMemoryOutboxStore, InMemoryProjectionStore,
    InMemoryPublisher, InMemoryRetryTracker, InMemorySnapshotStore, ServiceBuilder,
};
use fleet_service::aggregate::Ship;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    // Build the fleet service with in-memory infrastructure.
    // In production, these would be replaced with YugabyteDB, Cassandra, and Kafka impls.
    let service = ServiceBuilder::new("fleet")
        .for_aggregate::<Ship>()
        .event_store(InMemoryEventStore::new())
        .snapshot_store(InMemorySnapshotStore::new())
        .dead_letter_store(InMemoryDeadLetterStore::new())
        .retry_tracker(InMemoryRetryTracker::new())
        .snapshot_state_provider(EventPayloadSnapshotProvider)
        .outbox_store(InMemoryOutboxStore::new())
        .outbox_publisher(InMemoryOutboxPublisher::new(InMemoryOutboundQueue::new()))
        .projection_checkpoint_store(InMemoryProjectionStore::new())
        .publisher(InMemoryPublisher::new())
        .topic("canon.fleet.events")
        .build()
        .expect("fleet service failed to build — check inventory registrations");

    info!(service = service.service_name(), "fleet-service ready");

    // Wait for shutdown signal.
    tokio::signal::ctrl_c()
        .await
        .expect("failed to listen for ctrl-c");

    info!("fleet-service shutting down");
}
