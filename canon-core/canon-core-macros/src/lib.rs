extern crate proc_macro;

mod aggregate;
mod command;
mod command_handler;
mod event;
mod event_combiner;
mod event_handler;
mod projection_macro;
mod projection_handler;
mod util;

use proc_macro::TokenStream;

/// Registers an aggregate with optional snapshot cadence.
///
/// Generates `impl Aggregate`, `Default`, serde derives, and `SNAPSHOT_EVERY` const.
/// Hydration dispatches to version-matched `#[event_combiner]` impls via `inventory`.
///
/// ```ignore
/// #[aggregate(snapshot_every = 50)]
/// pub struct Ship { pub status: ShipStatus, pub fuel_level: f32 }
/// ```
#[proc_macro_attribute]
pub fn aggregate(attr: TokenStream, item: TokenStream) -> TokenStream {
    aggregate::expand(attr.into(), item.into())
        .unwrap_or_else(|err| err.to_compile_error())
        .into()
}

/// Registers a command targeting an aggregate, with version and produces list.
///
/// Generates serde derives, a per-command event enum, marker trait, and `inventory` registration.
///
/// ```ignore
/// #[command(Ship, version = 1, produces = [ShipDeparted, FuelConsumed])]
/// pub struct DepartForStation { pub destination: StationId }
/// ```
#[proc_macro_attribute]
pub fn command(attr: TokenStream, item: TokenStream) -> TokenStream {
    command::expand(attr.into(), item.into())
        .unwrap_or_else(|err| err.to_compile_error())
        .into()
}

/// Registers an event for an aggregate, with version.
///
/// Generates serde derives, marker trait, and `inventory` registration.
///
/// ```ignore
/// #[event(Ship, version = 1)]
/// pub struct ShipDeparted { pub destination: StationId }
/// ```
#[proc_macro_attribute]
pub fn event(attr: TokenStream, item: TokenStream) -> TokenStream {
    event::expand(attr.into(), item.into())
        .unwrap_or_else(|err| err.to_compile_error())
        .into()
}

/// Registers a version-matched event combiner for pure state folding.
///
/// Transforms the inherent `combine` method into an `EventCombiner` trait impl.
/// Generates `inventory` registration with a deserialization + apply function.
///
/// ```ignore
/// #[event_combiner(Ship, version = 1)]
/// impl ShipDeparted {
///     fn combine(&self, state: &mut Ship) {
///         state.status = ShipStatus::InFlight;
///     }
/// }
/// ```
#[proc_macro_attribute]
pub fn event_combiner(attr: TokenStream, item: TokenStream) -> TokenStream {
    event_combiner::expand(attr.into(), item.into())
        .unwrap_or_else(|err| err.to_compile_error())
        .into()
}

/// Registers a versioned command handler.
///
/// Transforms the impl block into a `CommandHandler` trait impl (async).
/// The user writes a sync `handle` method; the macro wraps it in async.
///
/// ```ignore
/// #[command_handler(Ship, version = 1)]
/// impl DepartForStationHandler {
///     type Error = FleetError;
///     fn handle(&self, state: &Ship, cmd: DepartForStation) -> Result<Vec<DepartForStationEvent>, FleetError> {
///         // ...
///     }
/// }
/// ```
#[proc_macro_attribute]
pub fn command_handler(attr: TokenStream, item: TokenStream) -> TokenStream {
    command_handler::expand(attr.into(), item.into())
        .unwrap_or_else(|err| err.to_compile_error())
        .into()
}

/// Registers an event handler with optional `window_ttl`.
///
/// `window_ttl` requires an `oversight` method — compile error without it.
/// The `#[handles(EventType, version = N)]` attribute declares which event this handler processes.
///
/// ```ignore
/// #[event_handler(window_ttl = "30m")]
/// impl ShipArrivedHandler {
///     #[handles(ShipArrivedAtStation, version = 1)]
///     fn handle(&self, events: Vec<ShipArrivedAtStation>) -> Option<CommandEnvelope> { None }
///     fn oversight(&self, accumulated: &[IncomingMessage]) -> Oversight { Oversight::Ready }
/// }
/// ```
#[proc_macro_attribute]
pub fn event_handler(attr: TokenStream, item: TokenStream) -> TokenStream {
    event_handler::expand(attr.into(), item.into())
        .unwrap_or_else(|err| err.to_compile_error())
        .into()
}

/// Registers a projection (read model).
///
/// Generates serde derives, an inherent `projection_id()` method, and `inventory` registration.
///
/// ```ignore
/// #[projection]
/// pub struct StationInventory {
///     pub station_id: StationId,
///     pub stock_levels: HashMap<String, u32>,
/// }
/// ```
#[proc_macro_attribute]
pub fn projection(attr: TokenStream, item: TokenStream) -> TokenStream {
    projection_macro::expand(attr.into(), item.into())
        .unwrap_or_else(|err| err.to_compile_error())
        .into()
}

/// Registers a projection handler that applies events to a read model.
///
/// Transforms the impl block into a `ProjectionHandler` trait impl.
///
/// ```ignore
/// #[projection_handler(StationInventory)]
/// impl CargoReceivedHandler {
///     fn apply(&self, event: &CargoReceived, store: &mut StationInventory) {
///         *store.stock_levels.entry(event.cargo_type.clone()).or_insert(0) += event.quantity;
///     }
/// }
/// ```
#[proc_macro_attribute]
pub fn projection_handler(attr: TokenStream, item: TokenStream) -> TokenStream {
    projection_handler::expand(attr.into(), item.into())
        .unwrap_or_else(|err| err.to_compile_error())
        .into()
}
