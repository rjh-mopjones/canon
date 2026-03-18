use uuid::Uuid;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use serde::{Serialize, Deserialize};

/// Unique identifier for an aggregate instance.
/// Always a Uuid newtype. Never use a plain Uuid in domain code.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct AggregateId(Uuid);

impl AggregateId {
    pub fn new() -> Self { Self(Uuid::new_v4()) }
    pub fn from_uuid(id: Uuid) -> Self { Self(id) }
    pub fn as_uuid(&self) -> &Uuid { &self.0 }
}
impl Default for AggregateId { fn default() -> Self { Self::new() } }

/// Monotonically increasing event version. Used for optimistic concurrency.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Version(u64);

impl Version {
    pub fn initial() -> Self { Self(0) }
    pub fn next(self) -> Self { Self(self.0 + 1) }
    pub fn as_u64(&self) -> u64 { self.0 }
}

/// Every event written to the store is wrapped in this envelope.
/// The payload is opaque bytes — the aggregate's upcast() decodes it.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventEnvelope {
    pub event_id: Uuid,
    pub aggregate_id: AggregateId,
    pub version: Version,
    pub event_type: String,
    pub event_version: u32,      // schema version — used by upcast()
    pub payload: Bytes,
    pub correlation_id: Uuid,    // traces the full causal chain end to end
    pub causation_id: Uuid,      // the immediate cause (command_id or event_id)
    pub timestamp: DateTime<Utc>,
}

/// Every command dispatched through the system is wrapped in this envelope.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommandEnvelope {
    pub command_id: Uuid,
    pub aggregate_id: AggregateId,
    pub correlation_id: Uuid,
    pub causation_id: Uuid,
    pub timestamp: DateTime<Utc>,
    pub payload: Bytes,    // opaque — the command handler decodes it
}

/// All message types that flow into the inbox.
/// Does NOT implement Serialize/Deserialize — it is an in-process type only.
#[derive(Debug, Clone)]
pub enum IncomingMessage {
    Command(CommandEnvelope),
    InternalEvent(EventEnvelope),   // produced by this service
    ExternalEvent(EventEnvelope),   // arrived via canon-adaptor
}

/// Return value of a handler's oversight() function.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Oversight {
    Ready,      // dispatch accumulated batch to queue now
    NotReady,   // wait for more messages
    Discard,    // abandon this accumulation window entirely
}

// ── Counterfactual replay ─────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct CounterfactualRequest {
    pub aggregate_id: AggregateId,
    pub branch_version: Version,              // replay unchanged up to here
    pub substituted_command: CommandEnvelope, // replace the command at this point
}

#[derive(Debug, Clone)]
pub struct CounterfactualResult {
    pub original_commands: Vec<CommandEnvelope>,
    pub counterfactual_commands: Vec<CommandEnvelope>,
    pub diff: CommandDiff,
}

#[derive(Debug, Clone)]
pub struct CommandDiff {
    pub added: Vec<CommandEnvelope>,
    pub removed: Vec<CommandEnvelope>,
    pub unchanged: Vec<CommandEnvelope>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn version_initial_is_zero() {
        let v = Version::initial();
        assert_eq!(v.as_u64(), 0);
    }

    #[test]
    fn version_next_increments() {
        let v = Version::initial();
        let v1 = v.next();
        assert_eq!(v1.as_u64(), 1);
        let v2 = v1.next();
        assert_eq!(v2.as_u64(), 2);
    }

    #[test]
    fn aggregate_id_new_is_unique() {
        let a = AggregateId::new();
        let b = AggregateId::new();
        assert_ne!(a, b);
    }

    #[test]
    fn aggregate_id_from_uuid_roundtrips() {
        let uuid = Uuid::new_v4();
        let id = AggregateId::from_uuid(uuid);
        assert_eq!(*id.as_uuid(), uuid);
    }
}
