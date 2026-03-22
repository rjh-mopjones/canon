//! Event handlers for the navigation service.
//!
//! Cross-service event handling (Fleet:ShipDeparted → PlanRoute, and
//! Nav:RoutePlanned → RecordArrival) is implemented in `cross_service.rs`
//! as direct Kafka consumers rather than via the `EventHandler` trait,
//! because the shared crate owns the event types (Rust orphan rules).
