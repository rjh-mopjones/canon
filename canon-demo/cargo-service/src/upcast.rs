use bytes::Bytes;
use canon_core::EventEnvelope;

use crate::error::UpcastError;
use crate::events::CargoLoaded;

/// Upcast a raw event envelope's payload from an older schema version to the
/// current version. Currently handles:
///
/// - `CargoLoaded` v1 → v2: adds `description` field with a migration marker.
///
/// Returns `Ok(CargoEvent)` with the upcast payload, or `Err` if deserialization fails.
pub fn upcast_cargo_loaded_v1(raw: &EventEnvelope) -> Result<CargoLoaded, UpcastError> {
    #[derive(serde::Deserialize)]
    struct V1 {
        item_id: uuid::Uuid,
        weight_kg: f32,
    }

    let v1: V1 = serde_json::from_slice(&raw.payload)
        .map_err(|e| UpcastError::Deserialization(e.to_string()))?;

    // Use the aggregate_id from the envelope as the manifest_id
    let manifest_id = *raw.aggregate_id.as_uuid();

    Ok(CargoLoaded {
        manifest_id,
        item_id: v1.item_id,
        weight_kg: v1.weight_kg,
        description: "(migrated from v1)".to_string(),
    })
}

/// Process a raw event envelope and upcast it if needed.
/// For `CargoLoaded` v1, converts the payload to v2 format and bumps event_version.
/// All other events pass through unchanged.
///
/// Returns `Err` if a v1 `CargoLoaded` payload cannot be deserialized or
/// re-serialized, rather than silently passing the corrupt envelope through.
pub fn upcast_envelope(mut env: EventEnvelope) -> Result<EventEnvelope, UpcastError> {
    if env.event_type == "CargoLoaded" && env.event_version == 1 {
        let v2 = upcast_cargo_loaded_v1(&env)?;
        let payload =
            serde_json::to_vec(&v2).map_err(|e| UpcastError::Deserialization(e.to_string()))?;
        env.payload = Bytes::from(payload);
        env.event_version = 2;
    }
    Ok(env)
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use canon_core::{AggregateId, Version};
    use uuid::Uuid;

    fn make_envelope(event_type: &str, event_version: u32, payload: &[u8]) -> EventEnvelope {
        EventEnvelope {
            event_id: Uuid::new_v4(),
            aggregate_id: AggregateId::from_uuid(Uuid::new_v4()),
            version: Version::initial(),
            event_type: event_type.to_string(),
            event_version,
            payload: Bytes::copy_from_slice(payload),
            correlation_id: Uuid::new_v4(),
            causation_id: Uuid::new_v4(),
            timestamp: chrono::Utc::now(),
        }
    }

    #[test]
    fn upcast_cargo_loaded_v1_to_v2() {
        let item_id = Uuid::new_v4();
        let v1_payload = serde_json::to_vec(&serde_json::json!({
            "item_id": item_id,
            "weight_kg": 100.0
        }))
        .expect("test: serialization should not fail");

        let env = make_envelope("CargoLoaded", 1, &v1_payload);
        let agg_id = *env.aggregate_id.as_uuid();
        let result = upcast_cargo_loaded_v1(&env);
        assert!(result.is_ok());
        let loaded = result.expect("test: upcast should succeed");
        assert_eq!(loaded.manifest_id, agg_id);
        assert_eq!(loaded.item_id, item_id);
        assert!((loaded.weight_kg - 100.0).abs() < f32::EPSILON);
        assert_eq!(loaded.description, "(migrated from v1)");
    }

    #[test]
    fn upcast_envelope_converts_v1_to_v2() {
        let item_id = Uuid::new_v4();
        let v1_payload = serde_json::to_vec(&serde_json::json!({
            "item_id": item_id,
            "weight_kg": 50.5
        }))
        .expect("test: serialization should not fail");

        let env = make_envelope("CargoLoaded", 1, &v1_payload);
        let result = upcast_envelope(env).expect("test: upcast should succeed");
        assert_eq!(result.event_version, 2);

        let loaded: CargoLoaded =
            serde_json::from_slice(&result.payload).expect("test: deserialization should not fail");
        assert_eq!(loaded.item_id, item_id);
        assert_eq!(loaded.description, "(migrated from v1)");
    }

    #[test]
    fn upcast_envelope_passes_through_v2() {
        let item_id = Uuid::new_v4();
        let v2_payload = serde_json::to_vec(&serde_json::json!({
            "manifest_id": Uuid::new_v4(),
            "item_id": item_id,
            "weight_kg": 75.0,
            "description": "steel beams"
        }))
        .expect("test: serialization should not fail");

        let env = make_envelope("CargoLoaded", 2, &v2_payload);
        let original_payload = env.payload.clone();
        let result = upcast_envelope(env).expect("test: upcast should succeed");
        assert_eq!(result.event_version, 2);
        assert_eq!(result.payload, original_payload);
    }

    #[test]
    fn upcast_envelope_passes_through_other_events() {
        let payload = serde_json::to_vec(&serde_json::json!({
            "manifest_id": Uuid::new_v4(),
        }))
        .expect("test: serialization should not fail");

        let env = make_envelope("ManifestClosed", 1, &payload);
        let original_payload = env.payload.clone();
        let result = upcast_envelope(env).expect("test: upcast should succeed");
        assert_eq!(result.event_version, 1);
        assert_eq!(result.payload, original_payload);
    }

    #[test]
    fn upcast_cargo_loaded_v1_bad_payload() {
        let env = make_envelope("CargoLoaded", 1, b"not valid json");
        let result = upcast_cargo_loaded_v1(&env);
        assert!(result.is_err());
    }

    #[test]
    fn upcast_envelope_returns_error_on_corrupt_v1_payload() {
        let env = make_envelope("CargoLoaded", 1, b"not valid json");
        let result = upcast_envelope(env);
        assert!(result.is_err());
    }
}
