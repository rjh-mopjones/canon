use std::sync::{Arc, Mutex};

use crate::{AggregateId, CommandEnvelope};

#[derive(Debug, thiserror::Error)]
pub enum CommandStoreError {
    #[error("lock poisoned")]
    Poisoned,
}

#[derive(Clone)]
pub struct InMemoryCommandStore {
    inner: Arc<Mutex<Vec<CommandEnvelope>>>,
}

impl InMemoryCommandStore {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Append a command to the audit trail.
    pub fn append(&self, envelope: CommandEnvelope) -> Result<(), CommandStoreError> {
        let mut store = self.inner.lock().map_err(|_| CommandStoreError::Poisoned)?;
        store.push(envelope);
        Ok(())
    }

    /// Load commands for an aggregate, optionally filtered by timestamp range.
    pub fn load_range(
        &self,
        aggregate_id: &AggregateId,
        from: Option<chrono::DateTime<chrono::Utc>>,
        to: Option<chrono::DateTime<chrono::Utc>>,
    ) -> Result<Vec<CommandEnvelope>, CommandStoreError> {
        let store = self.inner.lock().map_err(|_| CommandStoreError::Poisoned)?;
        Ok(store
            .iter()
            .filter(|c| {
                c.aggregate_id == *aggregate_id
                    && from.is_none_or(|f| c.timestamp >= f)
                    && to.is_none_or(|t| c.timestamp <= t)
            })
            .cloned()
            .collect())
    }
}

impl Default for InMemoryCommandStore {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use chrono::Utc;
    use uuid::Uuid;

    fn make_command(aggregate_id: &AggregateId) -> CommandEnvelope {
        CommandEnvelope {
            command_id: Uuid::new_v4(),
            aggregate_id: aggregate_id.clone(),
            correlation_id: Uuid::new_v4(),
            causation_id: Uuid::new_v4(),
            timestamp: Utc::now(),
            payload: Bytes::from_static(b"{}"),
            command_version: 1,
        }
    }

    #[test]
    fn append_and_load() {
        let store = InMemoryCommandStore::new();
        let id = AggregateId::new();
        store.append(make_command(&id)).unwrap();
        store.append(make_command(&id)).unwrap();

        let loaded = store.load_range(&id, None, None).unwrap();
        assert_eq!(loaded.len(), 2);
    }

    #[test]
    fn load_range_filters_by_aggregate() {
        let store = InMemoryCommandStore::new();
        let id_a = AggregateId::new();
        let id_b = AggregateId::new();
        store.append(make_command(&id_a)).unwrap();
        store.append(make_command(&id_b)).unwrap();

        let loaded = store.load_range(&id_a, None, None).unwrap();
        assert_eq!(loaded.len(), 1);
    }
}
