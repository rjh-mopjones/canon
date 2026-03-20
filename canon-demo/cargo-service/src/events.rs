// Re-export shared cargo events. These are defined in canon-demo-shared with
// their #[event] macros which generate Serialize/Deserialize, marker traits,
// and inventory registrations.
pub use canon_demo_shared::events::CargoEvent;
pub use canon_demo_shared::events::{
    CargoLoaded, CargoUnloaded, ManifestClosed, ManifestCreated, UnloadingStarted,
};
