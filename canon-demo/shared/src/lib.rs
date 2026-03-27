#[cfg(feature = "db")]
pub mod db;
#[cfg(feature = "db")]
pub mod offsets;
pub mod topics;

/// Namespace UUID for deriving deterministic cross-service command IDs.
/// This is a fixed v4 UUID used as the namespace for v5 UUID generation.
/// Using a stable namespace ensures that the same (source_event_id, command_type)
/// pair always produces the same command_id, enabling inbox deduplication
/// via `ON CONFLICT DO NOTHING` even when the same Kafka event is replayed.
const CROSS_SERVICE_COMMAND_NS: uuid::Uuid = uuid::Uuid::from_bytes([
    0x6b, 0xa7, 0xb8, 0x10, 0x9d, 0xad, 0x11, 0xd1, 0x80, 0xb4, 0x00, 0xc0, 0x4f, 0xd4, 0x30, 0xc8,
]);

/// Derive a deterministic command ID from the source event ID and command type.
///
/// This ensures that if the same Kafka event is consumed twice (e.g., due to
/// offset loss on restart), the second submission will produce the same
/// command_id and be safely deduplicated by the inbox's
/// `ON CONFLICT DO NOTHING` clause.
///
/// Uses UUID v5 (SHA-1 based) with a fixed namespace.
pub fn deterministic_command_id(source_event_id: uuid::Uuid, command_type: &str) -> uuid::Uuid {
    let name = format!("{source_event_id}:{command_type}");
    uuid::Uuid::new_v5(&CROSS_SERVICE_COMMAND_NS, name.as_bytes())
}
