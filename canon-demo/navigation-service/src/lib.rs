//! Navigation service — owns the Route aggregate.
//!
//! Handles route planning, departure recording, position updates, and arrival
//! recording. Publishes `ShipArrivedAtStation` events that trigger cargo
//! unloading and station docking in downstream services.

pub mod aggregate;
pub mod commands;
pub mod dispatcher_store;
pub mod error;
pub mod events;
pub mod handlers;
pub mod projection;
