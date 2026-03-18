use async_trait::async_trait;
use crate::EventEnvelope;

#[async_trait]
pub trait Aggregate: Sized + Send + Sync + 'static {
    type State: Default + Send + Sync;
    type Command: Send + Sync;
    type Event: Send + Sync;
    type Error: std::error::Error + Send + Sync + 'static;

    /// Apply a single domain event to state. Pure — no side effects.
    fn apply(state: &mut Self::State, event: &Self::Event);

    /// Validate a command against current state and produce events.
    async fn handle(
        state: &Self::State,
        command: Self::Command,
    ) -> Result<Vec<Self::Event>, Self::Error>;

    /// Transform a raw stored EventEnvelope into the current domain event type.
    /// Called during hydration before apply(). Handles schema migrations.
    fn upcast(raw: EventEnvelope) -> Result<Self::Event, Self::Error>;

    /// Default implementation — calls apply() for each event in order.
    fn hydrate(state: &mut Self::State, events: impl Iterator<Item = Self::Event>) {
        for event in events {
            Self::apply(state, &event);
        }
    }
}
