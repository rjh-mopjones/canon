use canon_core::*;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct TestEvent {
    pub data: String,
}

// This should fail to compile: window_ttl requires an oversight method
#[event_handler(window_ttl = "30m")]
impl BadHandler {
    #[handles(TestEvent, version = 1)]
    fn handle(&self, events: Vec<TestEvent>) -> Option<CommandEnvelope> {
        let _ = events;
        None
    }
    // Missing oversight method!
}

fn main() {}
