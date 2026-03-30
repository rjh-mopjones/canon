//! Navigation service — owns the Route aggregate.
//!
//! Handles route planning, departure recording, position updates, and arrival
//! recording. Publishes `ShipArrivedAtStation` events that trigger cargo
//! unloading and station docking in downstream services.

pub mod aggregate;
pub mod combiners;
pub mod command_handlers;
pub mod commands;
pub mod error;
pub mod event_handlers;
pub mod events;
pub mod handlers;
pub mod inbound;
pub mod projection;
