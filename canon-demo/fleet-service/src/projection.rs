use canon_core::{InMemoryProjectionStore, MacroError, Projection, ProjectionCheckpointStore};
use canon_demo_shared::events::FleetEvent;

pub struct ShipReadModel;

#[async_trait::async_trait]
impl Projection for ShipReadModel {
    type Event = FleetEvent;
    type Store = InMemoryProjectionStore;
    type Error = MacroError;

    async fn apply(&self, _event: &Self::Event, store: &Self::Store) -> Result<(), Self::Error> {
        let current = store
            .get_checkpoint(self.projection_id())
            .await
            .map_err(|e| MacroError(e.to_string()))?;
        store
            .set_checkpoint(self.projection_id(), current.next())
            .await
            .map_err(|e| MacroError(e.to_string()))?;
        Ok(())
    }

    async fn rebuild(
        &self,
        events: impl futures::Stream<Item = Self::Event> + Send,
        store: &Self::Store,
    ) -> Result<(), Self::Error> {
        use futures::StreamExt;
        let mut stream = std::pin::pin!(events);
        while let Some(event) = stream.next().await {
            self.apply(&event, store).await?;
        }
        Ok(())
    }

    fn projection_id(&self) -> &str {
        "ship-read-model"
    }
}
