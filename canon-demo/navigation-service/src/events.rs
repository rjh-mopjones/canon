//! Navigation event types.
//!
//! All navigation events are defined in `canon-demo-shared`. This module
//! re-exports them for convenience and provides the upcast function for
//! version migration.

pub use canon_demo_shared::events::{
    NavigationEvent, PositionUpdated, RoutePlanned, ShipArrivedAtStation,
};

/// Upcast an older event version to the current version.
///
/// v1 pattern: currently a no-op since all events are at version 1.
/// When a v2 event is introduced, this function maps v1 payloads to
/// the v2 schema.
pub fn upcast(event_type: &str, event_version: u32, payload: &[u8]) -> Option<Vec<u8>> {
    match (event_type, event_version) {
        // All events are currently at version 1 — no upcasting needed.
        ("RoutePlanned", 1) | ("PositionUpdated", 1) | ("ShipArrivedAtStation", 1) => None,
        _ => {
            tracing::warn!(
                event_type,
                event_version,
                "unknown event type or version for upcast"
            );
            let _ = payload;
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn upcast_v1_returns_none() {
        assert!(upcast("RoutePlanned", 1, b"{}").is_none());
        assert!(upcast("PositionUpdated", 1, b"{}").is_none());
        assert!(upcast("ShipArrivedAtStation", 1, b"{}").is_none());
    }

    #[test]
    fn upcast_unknown_returns_none() {
        assert!(upcast("UnknownEvent", 1, b"{}").is_none());
        assert!(upcast("RoutePlanned", 99, b"{}").is_none());
    }
}
