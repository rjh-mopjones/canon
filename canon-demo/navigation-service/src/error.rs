//! Navigation service error types.
//!
//! One error enum per crate. Each variant is named and descriptive.

/// Errors that can occur during navigation command handling.
#[derive(Debug, thiserror::Error)]
pub enum NavigationError {
    /// Attempted to record departure on a route with no ship assigned.
    #[error("route has no ship assigned")]
    NoShipAssigned,

    /// Attempted to record arrival on a route that has already arrived.
    #[error("route has already arrived at destination")]
    AlreadyArrived,

    /// Attempted to update position on a route that has already arrived.
    #[error("cannot update position after arrival")]
    PositionUpdateAfterArrival,

    /// Attempted to plan a route with no waypoints.
    #[error("route must have at least one waypoint")]
    EmptyWaypoints,

    /// Command ship_id does not match the ship assigned to the route.
    #[error("command ship_id does not match the route's assigned ship")]
    ShipMismatch,
}
