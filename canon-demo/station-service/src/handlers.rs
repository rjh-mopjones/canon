//! Event handlers for the station service.
//!
//! Cross-service event handling (e.g., `ShipArrivedAtStation` from navigation,
//! `CargoUnloaded` from cargo) is done via Kafka consumers in `cross_service.rs`,
//! which write commands directly to the station service's inbox. This avoids the
//! need for `#[event_handler]` macro-based handlers for cross-service events.
//!
//! Internal event monitoring (e.g., stock level checks after `CargoReceived`) will
//! be wired once the event handler dispatch pipeline is fully integrated into the
//! service lifecycle.
