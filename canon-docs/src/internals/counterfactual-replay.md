# Counterfactual Replay

Counterfactual replay is Canon's mechanism for what-if analysis. Given an aggregate's
command history, it answers the question: *"What would have happened if a different
command had been issued at a specific point in time?"*

The replay engine substitutes a command at a chosen branch point, replays the command
sequence forward, and produces a diff that shows which downstream commands would have
changed, been added, or been removed. This operates on **commands, not events** --
the diff captures divergence in intent rather than raw event data.

This chapter covers the replay architecture, the core types involved, the step-by-step
replay process, the role of the ReplayEventStore, and practical use cases.

---

## Why operate on commands?

In an event-sourced system, commands represent intent ("depart for station Beta") and
events represent facts ("ship departed for Beta"). Commands are the inputs to the
system; events are the outputs.

Counterfactual analysis asks "what if the input had been different?" -- so the natural
unit of analysis is the command. By substituting a command and replaying the handler
chain, Canon captures:

- Which downstream commands would have been produced differently.
- Which commands would not have been produced at all.
- Which new commands would have appeared.

Diffing at the command level is more meaningful than diffing events. A command diff
tells you *"this decision would have been different"* rather than *"this field in this
event would have had a different value"*.

This is also why Canon persists commands in the command store (via `CommandStore`) --
command history is essential for counterfactual analysis. Without stored commands, there
is nothing to substitute.

---

## Core types

All counterfactual types are defined in `canon-core/src/types.rs`.

### CounterfactualRequest

The input to a replay operation:

```rust
pub struct CounterfactualRequest {
    pub aggregate_id: AggregateId,
    pub branch_version: Version,
    pub substituted_command: CommandEnvelope,
}
```

| Field                 | Type              | Description                                            |
|-----------------------|-------------------|--------------------------------------------------------|
| `aggregate_id`        | `AggregateId`     | The aggregate to replay.                               |
| `branch_version`      | `Version`         | The position in the command history to substitute at.  |
| `substituted_command` | `CommandEnvelope`  | The replacement command to insert at the branch point. |

The `branch_version` identifies which command in the history to replace. If the
aggregate has commands at positions [0, 1, 2, 3, 4], a `branch_version` of 2 means
"replace the command at position 2 and see what changes."

If `branch_version` equals the length of the command history (i.e., one past the end),
the substituted command is appended rather than replacing an existing one.

### CounterfactualResult

The output of a replay operation:

```rust
pub struct CounterfactualResult {
    pub original_commands: Vec<CommandEnvelope>,
    pub counterfactual_commands: Vec<CommandEnvelope>,
    pub diff: CommandDiff,
}
```

| Field                     | Type                    | Description                                    |
|---------------------------|-------------------------|------------------------------------------------|
| `original_commands`       | `Vec<CommandEnvelope>`  | The actual command history as stored.           |
| `counterfactual_commands` | `Vec<CommandEnvelope>`  | The hypothetical command sequence after substitution. |
| `diff`                    | `CommandDiff`           | Positional comparison of original vs counterfactual. |

### CommandDiff

The structural diff between two command sequences:

```rust
pub struct CommandDiff {
    pub added: Vec<CommandEnvelope>,
    pub removed: Vec<CommandEnvelope>,
    pub unchanged: Vec<CommandEnvelope>,
}
```

| Field       | Description                                                        |
|-------------|--------------------------------------------------------------------|
| `added`     | Commands that exist in the counterfactual sequence but not the original. |
| `removed`   | Commands that exist in the original sequence but not the counterfactual. |
| `unchanged` | Commands that are identical at the same position in both sequences. |

Two commands at the same position are considered "unchanged" when all three of these
match: `command_type`, `command_version`, and `payload`. If any of these differ, the
original command goes into `removed` and the counterfactual command goes into `added`.

This matching rule ensures that commands routed to different version-matched handlers
are detected as changes even if the payload bytes happen to be identical.

---

## The CounterfactualReplay trait

Defined in `canon-core/src/traits/replay.rs`:

```rust
#[async_trait]
pub trait CounterfactualReplay: Send + Sync {
    type Error: std::error::Error + Send + Sync + 'static;

    async fn replay(
        &self,
        request: CounterfactualRequest,
    ) -> Result<CounterfactualResult, Self::Error>;
}
```

The trait has a single method: `replay`. It takes a `CounterfactualRequest` and returns
a `CounterfactualResult` or an error. The trait is async because loading command
history and events from the stores requires I/O.

Implementations are generic over `CommandStore` and `ReplayEventStore`, allowing the
same replay logic to work with both in-memory stores (for testing) and production
infrastructure.

---

## The replay process step by step

The default implementation lives in
`canon-core/src/memory/counterfactual_replay.rs` as `DefaultCounterfactualReplay`.

### Construction

```rust
pub struct DefaultCounterfactualReplay<C: CommandStore, R: ReplayEventStore> {
    pub command_store: C,
    pub replay_event_store: R,
}

impl<C: CommandStore, R: ReplayEventStore> DefaultCounterfactualReplay<C, R> {
    pub fn new(command_store: C, replay_event_store: R) -> Self {
        Self {
            command_store,
            replay_event_store,
        }
    }
}
```

The engine takes two dependencies:

- **CommandStore**: provides the original command history.
- **ReplayEventStore**: provides events for hydrating aggregate state to the branch
  point. This is a separate store from the live event store (see the ReplayEventStore
  section below).

### Step 1: Load command history

```rust
let original_commands = self.command_store
    .load_for_aggregate(&request.aggregate_id)
    .await?;
```

The engine loads the full command history for the target aggregate from the command
store. Commands are returned in timestamp order. This history is the baseline for
comparison.

### Step 2: Validate the branch version

```rust
let branch_idx = request.branch_version.as_u64() as usize;

if branch_idx > original_commands.len() {
    return Err(CounterfactualReplayError::BranchVersionOutOfRange {
        branch_version: request.branch_version.as_u64(),
        history_len: original_commands.len(),
    });
}
```

The branch version must be within the command history (0 to N) or exactly at the end
(N, for appending). If it exceeds the history length, the engine returns
`BranchVersionOutOfRange`.

### Step 3: Build the counterfactual command list

```rust
let mut counterfactual_commands = Vec::with_capacity(original_commands.len());
for (i, cmd) in original_commands.iter().enumerate() {
    if i == branch_idx {
        counterfactual_commands.push(request.substituted_command.clone());
    } else {
        counterfactual_commands.push(cmd.clone());
    }
}
// If branch_idx == original_commands.len(), append instead of replace.
if branch_idx == original_commands.len() {
    counterfactual_commands.push(request.substituted_command.clone());
}
```

The engine constructs the hypothetical command list by copying the original commands
and replacing (or appending) the substituted command at the branch point. All other
commands remain unchanged.

### Step 4: Positional diff

```rust
let max_len = original_commands.len().max(counterfactual_commands.len());
for i in 0..max_len {
    match (original_commands.get(i), counterfactual_commands.get(i)) {
        (Some(orig), Some(cf))
            if orig.command_type == cf.command_type
                && orig.command_version == cf.command_version
                && orig.payload == cf.payload =>
        {
            unchanged.push(orig.clone());
        }
        (Some(orig), Some(cf)) => {
            removed.push(orig.clone());
            added.push(cf.clone());
        }
        (Some(orig), None) => {
            removed.push(orig.clone());
        }
        (None, Some(cf)) => {
            added.push(cf.clone());
        }
        (None, None) => {}
    }
}
```

The diff walks both command lists position by position:

- **Same position, matching content**: the command is `unchanged`.
- **Same position, different content**: the original is `removed`, the counterfactual
  is `added`.
- **Original exists, no counterfactual**: the command is `removed` (the counterfactual
  sequence is shorter).
- **No original, counterfactual exists**: the command is `added` (the counterfactual
  sequence is longer, which happens when the branch point is at the end).

### Step 5: Return the result

```rust
Ok(CounterfactualResult {
    original_commands,
    counterfactual_commands,
    diff: CommandDiff {
        added,
        removed,
        unchanged,
    },
})
```

The result includes both the full original and counterfactual command lists, plus the
structural diff. Consumers can inspect either the full sequences or just the diff,
depending on their needs.

---

## Error handling

The `CounterfactualReplayError` enum covers three failure modes:

```rust
#[derive(Debug, thiserror::Error)]
pub enum CounterfactualReplayError {
    #[error("command store error: {0}")]
    CommandStore(String),

    #[error("replay event store error: {0}")]
    ReplayEventStore(String),

    #[error("branch version {branch_version} exceeds command history length {history_len}")]
    BranchVersionOutOfRange {
        branch_version: u64,
        history_len: usize,
    },
}
```

| Variant                    | Cause                                              |
|----------------------------|----------------------------------------------------|
| `CommandStore`             | Failed to load command history from the store.      |
| `ReplayEventStore`        | Failed to load events from the replay store.        |
| `BranchVersionOutOfRange` | The requested branch point is beyond the end of the command history. |

---

## The ReplayEventStore

Counterfactual replay reads events from a dedicated `ReplayEventStore` -- not the live
`EventStore`. This separation is a deliberate design choice.

### Trait definition

Defined in `canon-core/src/traits/replay.rs`:

```rust
#[async_trait]
pub trait ReplayEventStore: Send + Sync + 'static {
    type Error: std::error::Error + Send + Sync + 'static;

    /// Load all events for an aggregate in ascending version order.
    async fn load(
        &self,
        aggregate_id: &AggregateId,
    ) -> Result<Vec<EventEnvelope>, Self::Error>;

    /// Load events for an aggregate where version >= `from_version`.
    async fn load_from_version(
        &self,
        aggregate_id: &AggregateId,
        from_version: Version,
    ) -> Result<Vec<EventEnvelope>, Self::Error>;
}
```

The interface mirrors `EventStore` but is read-only -- there is no `append` method.
This enforces the invariant that replay never writes to any event store.

### Why a separate store?

Three reasons:

1. **Read isolation**: replay queries can be expensive (loading the full event history
   for an aggregate). Sending these queries to a read replica avoids impacting the live
   write path.

2. **Consistency**: replay runs against a consistent snapshot of the event stream. The
   read replica may lag slightly behind the primary, but this is acceptable for what-if
   analysis.

3. **Safety**: by using a separate trait that lacks `append`, there is no way for the
   replay engine to accidentally write events. The type system enforces read-only
   access.

### In-memory implementation

`InMemoryReplayEventStore` wraps an `InMemoryEventStore` and delegates all reads to it:

```rust
pub struct InMemoryReplayEventStore {
    inner: InMemoryEventStore,
}
```

It provides two constructors:

```rust
// Empty replay store
let replay_store = InMemoryReplayEventStore::new();

// Replay store backed by an existing event store (shared data)
let replay_store = InMemoryReplayEventStore::from_event_store(event_store.clone());
```

The `from_event_store` constructor is useful in tests where the replay store should see
the same events as the live store. The `new` constructor creates an independent, empty
store.

### Production implementation

In production, the `ReplayEventStore` would be a Cassandra client pointing at a read
replica, separate from the primary Cassandra cluster. It is injected via
`ServiceBuilder`:

```rust
ServiceBuilder::new("fleet")
    .for_aggregate::<Ship>()
    .with_replay_event_store(read_replica_store)
    // ... other infrastructure
    .build()?;
```

---

## Version-matched routing during replay

When the replay engine hydrates aggregate state to the branch point, it uses the same
version-matched routing as normal hydration:

```
Events: [v0, v1, v2, ..., v_branch]
         |    |    |         |
         v    v    v         v
     combiner matching each event_version
         |
         v
     Aggregate state at branch point
```

Each event's `event_version` field determines which `#[event_combiner]` is called. No
casting or upcasting occurs. If an event was stored with `event_version = 1`, it is
processed by the combiner registered at version 1, even if version 2 exists.

When commands are re-executed forward from the branch point, each stored command's
`command_version` field routes it to the `#[command_handler]` registered at that exact
version. This ensures that the replay produces the same results as the original
execution, except at the substituted branch point.

```
Branch point state (with substituted command applied)
         |
         v
Command at position N+1  ->  handler at command_version  ->  counterfactual result
Command at position N+2  ->  handler at command_version  ->  counterfactual result
...
Command at position last ->  handler at command_version  ->  counterfactual result
```

This version-matched routing is essential for correctness. If the system has evolved
and newer command handler versions exist, replay still uses the original handler version
because the stored `command_version` governs routing.

---

## The CommandStore dependency

Counterfactual replay requires the command store to contain the full command history for
the aggregate. The `CommandStore` trait provides the required method:

```rust
#[async_trait]
pub trait CommandStore: Send + Sync + 'static {
    type Error: std::error::Error + Send + Sync + 'static;

    async fn load_for_aggregate(
        &self,
        aggregate_id: &AggregateId,
    ) -> Result<Vec<CommandEnvelope>, Self::Error>;

    // ... other methods
}
```

Commands are loaded in timestamp order. Each `CommandEnvelope` carries:

```rust
pub struct CommandEnvelope {
    pub command_id: Uuid,
    pub aggregate_id: AggregateId,
    pub command_type: String,
    pub correlation_id: Uuid,
    pub causation_id: Uuid,
    pub timestamp: DateTime<Utc>,
    pub payload: Bytes,
    pub command_version: u32,
}
```

The `command_type` and `command_version` fields are used by the diff algorithm to
determine whether two commands at the same position are the same. The `payload` is
compared byte-for-byte.

---

## Gateway API

The demo gateway exposes counterfactual replay via a REST endpoint:

```
GET /replay/counterfactual
    ?aggregate_id=<uuid>
    &branch_version=<u64>
    &command=<json-encoded-substituted-command>
```

Example request:

```bash
curl "http://localhost:8080/replay/counterfactual?\
aggregate_id=550e8400-e29b-41d4-a716-446655440000&\
branch_version=2&\
command=%7B%22command_type%22%3A%22DepartForStation%22%2C%22payload%22%3A%7B%22destination%22%3A%22Beta%22%7D%7D"
```

Response:

```json
{
  "original_commands": [
    { "command_id": "...", "command_type": "RegisterShip", "command_version": 1, ... },
    { "command_id": "...", "command_type": "AssignRoute", "command_version": 1, ... },
    { "command_id": "...", "command_type": "DepartForStation", "command_version": 1, ... },
    { "command_id": "...", "command_type": "DepartForStation", "command_version": 1, ... }
  ],
  "counterfactual_commands": [
    { "command_id": "...", "command_type": "RegisterShip", "command_version": 1, ... },
    { "command_id": "...", "command_type": "AssignRoute", "command_version": 1, ... },
    { "command_id": "...", "command_type": "DepartForStation", "command_version": 1, ... },
    { "command_id": "...", "command_type": "DepartForStation", "command_version": 1, ... }
  ],
  "diff": {
    "added": [ { "command_type": "DepartForStation", "payload": "{\"destination\":\"Beta\"}", ... } ],
    "removed": [ { "command_type": "DepartForStation", "payload": "{\"destination\":\"Alpha\"}", ... } ],
    "unchanged": [
      { "command_type": "RegisterShip", ... },
      { "command_type": "AssignRoute", ... },
      { "command_type": "DepartForStation", ... }
    ]
  }
}
```

In this example, the third command (position 2) was a `DepartForStation` to Alpha. The
counterfactual substitutes it with a departure to Beta. The diff shows one command
removed (the original Alpha departure) and one added (the Beta departure). The other
three commands are unchanged.

---

## Use cases

### Debugging

*"What would have happened if we sent a different route assignment?"*

After a production incident, an operator can replay the aggregate with a corrected
command at the branch point to understand the impact of the original mistake.

### Impact analysis

*"How many downstream commands change if we modify this input?"*

Before deploying a new command handler version, replay existing aggregates with the new
handler to see how many commands would differ. If the diff is empty, the new handler
is backward-compatible.

### Auditing

*"Show me the alternative timeline for this aggregate."*

Compliance teams can use counterfactual replay to document what would have happened
under different conditions, without modifying any stored data.

### Testing

*"Validate that command handlers produce expected results under different inputs."*

Integration tests can replay aggregates with known substitutions and assert that the
diff matches expectations.

---

## Requirements and prerequisites

Counterfactual replay requires four things to be in place:

1. **Command store**: the full command history for the aggregate must be persisted. If
   commands are not stored, there is nothing to substitute or diff.

2. **Event store (or read replica)**: events up to the branch point must be available
   for hydrating aggregate state.

3. **All command handlers at their original versions**: the handlers registered at
   `command_version = 1`, `command_version = 2`, etc. must still be present in the
   codebase. If a handler version is removed, replay cannot process commands stored at
   that version.

4. **All event combiners at their original versions**: similarly, event combiners for
   all stored event versions must be present for hydration to succeed.

This is why Canon uses version-matched routing rather than schema migration: old
handler and combiner versions coexist with new ones. Removing an old version is a
breaking change that disables replay for aggregates that used it.

---

## Limitations and considerations

### Positional diffing

The current implementation performs positional payload diffing. Two commands at the same
position are compared by `command_type`, `command_version`, and `payload` bytes. This is
sufficient for detecting changes at the substitution point and for commands that remain
identical.

However, if a substituted command causes a cascade where later commands in the sequence
would be different (because the aggregate state at those later points would differ),
the current implementation does not capture this. Full state hydration and re-execution
through version-matched handlers is the planned extension.

### Read-only operation

Counterfactual replay is strictly read-only. It never writes events, commands, or
snapshots. The `ReplayEventStore` trait has no write methods, and the replay engine
produces a result struct but does not persist it anywhere.

### No event-level diff

The diff operates at the command level, not the event level. If you need to know which
events would have been different, you would need to run the command handlers on both
command sequences and compare the resulting events. The current API does not provide this
directly.

### Branch version semantics

The `branch_version` is an index into the command list, not an event version. Command
position 0 is the first command ever issued to the aggregate. This may not correspond
to event version 0, since a single command can produce one event, and event versions
are assigned sequentially by the event store.

### Aggregate scope

Counterfactual replay operates on a single aggregate. It does not trace cross-service
effects. If a substituted command would produce a different event, and that event would
trigger a different cross-service handler, the cross-service impact is not captured.
The diff shows only the commands within the target aggregate's history.

---

## Worked example

Consider a `Ship` aggregate with this command history:

| Position | command_type       | command_version | payload                              |
|----------|--------------------|-----------------|--------------------------------------|
| 0        | RegisterShip       | 1               | `{"name": "VSS Meridian"}`           |
| 1        | DepartForStation   | 1               | `{"destination": "Alpha Depot"}`     |
| 2        | DepartForStation   | 1               | `{"destination": "Beta Relay"}`      |
| 3        | DepartForStation   | 1               | `{"destination": "Gamma Outpost"}`   |

### Scenario: What if the second departure went to Delta Prime instead?

Request:

```rust
CounterfactualRequest {
    aggregate_id: ship_id,
    branch_version: Version::from_u64(2),  // position 2
    substituted_command: CommandEnvelope {
        command_type: "DepartForStation".into(),
        command_version: 1,
        payload: Bytes::from(r#"{"destination": "Delta Prime"}"#),
        // ... other fields
    },
}
```

Result:

```rust
CounterfactualResult {
    original_commands: [RegisterShip, DepartAlpha, DepartBeta, DepartGamma],
    counterfactual_commands: [RegisterShip, DepartAlpha, DepartDelta, DepartGamma],
    diff: CommandDiff {
        added: [DepartDelta],     // new command at position 2
        removed: [DepartBeta],    // original command at position 2
        unchanged: [RegisterShip, DepartAlpha, DepartGamma],  // positions 0, 1, 3
    },
}
```

The diff clearly shows: position 2 changed from Beta Relay to Delta Prime. Positions
0, 1, and 3 are unchanged.

### Scenario: What if a fourth departure was appended?

Request with `branch_version = 4` (one past the end):

```rust
CounterfactualRequest {
    aggregate_id: ship_id,
    branch_version: Version::from_u64(4),  // append position
    substituted_command: CommandEnvelope {
        command_type: "DepartForStation".into(),
        command_version: 1,
        payload: Bytes::from(r#"{"destination": "Alpha Depot"}"#),
        // ... other fields
    },
}
```

Result:

```rust
CounterfactualResult {
    original_commands: [RegisterShip, DepartAlpha, DepartBeta, DepartGamma],
    counterfactual_commands: [RegisterShip, DepartAlpha, DepartBeta, DepartGamma, DepartAlpha],
    diff: CommandDiff {
        added: [DepartAlpha],    // appended command at position 4
        removed: [],              // nothing removed
        unchanged: [RegisterShip, DepartAlpha, DepartBeta, DepartGamma],
    },
}
```

### Scenario: What if the same command is substituted?

If the substituted command has the same `command_type`, `command_version`, and `payload`
as the original, the diff shows no changes:

```rust
diff: CommandDiff {
    added: [],
    removed: [],
    unchanged: [RegisterShip, DepartAlpha, DepartBeta, DepartGamma],
}
```

This is useful for verifying that a proposed change would have no effect.

---

## Testing counterfactual replay

The `DefaultCounterfactualReplay` is thoroughly tested with in-memory stores. The test
suite covers:

- Same payload produces all `unchanged`.
- Different payload at branch point produces `added` and `removed`.
- Branch at end appends a command.
- Branch beyond history returns `BranchVersionOutOfRange`.
- No change when substituting an identical command.
- Replay uses the replay event store, not the live event store.
- Empty history with branch at zero appends.

Example test:

```rust
#[tokio::test]
async fn different_payload_produces_added_and_removed() {
    let (command_store, replay_event_store) = setup_stores();
    let id = AggregateId::new();

    command_store.append(make_command(&id, b"place")).unwrap();
    command_store.append(make_command(&id, b"cancel")).unwrap();

    let replay = DefaultCounterfactualReplay::new(
        command_store,
        replay_event_store,
    );
    let substitute = make_command(&id, b"different");

    let result = replay
        .replay(CounterfactualRequest {
            aggregate_id: id,
            branch_version: Version::initial(),
            substituted_command: substitute,
        })
        .await
        .unwrap();

    assert_eq!(result.diff.added.len(), 1);
    assert_eq!(result.diff.removed.len(), 1);
    assert_eq!(result.diff.unchanged.len(), 1);
}
```

---

## Summary

Counterfactual replay in Canon is:

- **Command-level**: operates on commands, not events. Diffs capture intent.
- **Read-only**: never writes events, commands, or snapshots.
- **Version-matched**: routes commands through the handler at their stored `command_version`.
- **Isolated**: reads from a dedicated `ReplayEventStore`, not the live event store.
- **Requires command history**: depends on the `CommandStore` having the full history.
- **Single-aggregate scope**: does not trace cross-service effects.
- **Exposed via REST**: the gateway provides a `/replay/counterfactual` endpoint for
  interactive exploration.
