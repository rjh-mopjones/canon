#[cfg(feature = "db")]
pub mod db;
#[cfg(feature = "db")]
pub mod offsets;
pub mod topics;

/// Deterministic command ID from source event + command type.
/// Uses UUID v5 (SHA-1) so replayed Kafka events produce identical
/// command IDs, correctly deduped by the inbox.
pub fn deterministic_command_id(source_event_id: uuid::Uuid, command_type: &str) -> uuid::Uuid {
    use uuid::Uuid;
    const NAMESPACE: Uuid = Uuid::from_bytes([
        0x6b, 0xa7, 0xb8, 0x10, 0x9d, 0xad, 0x11, 0xd1, 0x80, 0xb4, 0x00, 0xc0, 0x4f, 0xd4, 0x30,
        0xc8,
    ]);
    Uuid::new_v5(
        &NAMESPACE,
        format!("{}:{}", source_event_id, command_type).as_bytes(),
    )
}
