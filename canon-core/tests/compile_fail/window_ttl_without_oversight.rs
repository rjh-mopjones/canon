use canon_core::*;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SomeEvent {
    pub data: String,
}

// This should fail to compile: window_ttl requires an oversight method
#[event_handler(window_ttl = "30m")]
impl BadHandler {
    #[handles(SomeEvent, version = 1)]
    fn handle(&self, events: Vec<SomeEvent>) -> Option<CommandEnvelope> {
        let _ = events;
        None
    }
    // Missing oversight method!
}

fn main() {}
