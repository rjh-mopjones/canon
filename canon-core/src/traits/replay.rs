use crate::{CounterfactualRequest, CounterfactualResult};
use async_trait::async_trait;

/// Runs a what-if simulation. Substitutes a command at a branch point,
/// re-runs the full handler chain in dry-run mode (no writes), diffs
/// resulting downstream commands against the originals.
#[async_trait]
pub trait CounterfactualReplay: Send + Sync {
    type Error: std::error::Error + Send + Sync + 'static;

    async fn replay(
        &self,
        request: CounterfactualRequest,
    ) -> Result<CounterfactualResult, Self::Error>;
}
