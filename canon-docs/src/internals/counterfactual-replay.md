# Counterfactual Replay

Counterfactual replay is a first-class feature in Canon. It answers the question:
*"What downstream commands would have been produced if a given command had been different?"*

## How it works

The replay engine operates on **commands, not events**:

1. Read command history from the command store for the aggregate
2. Use events only to hydrate aggregate state up to the branch point (via version-matched
   `#[event_combiner]` dispatch)
3. Substitute the specified command at the branch point
4. Re-run the command handler chain forward from the branch point
5. Diff at the command level -- `CommandDiff` captures divergence in intent

## Core types

```rust
pub struct CounterfactualRequest {
    pub aggregate_id: AggregateId,
    pub branch_version: Version,
    pub substituted_command: CommandEnvelope,
}

pub struct CounterfactualResult {
    pub original_commands: Vec<CommandEnvelope>,
    pub counterfactual_commands: Vec<CommandEnvelope>,
    pub diff: CommandDiff,
}

pub struct CommandDiff {
    pub added: Vec<CommandEnvelope>,
    pub removed: Vec<CommandEnvelope>,
    pub unchanged: Vec<CommandEnvelope>,
}
```

## The replay process

### Step 1: Hydrate to branch point

The engine loads events for the aggregate up to `branch_version` and hydrates the
state using version-matched combiners:

```
Events: [v0, v1, v2, ..., v_branch]
         |    |    |         |
         v    v    v         v
     combiner matching each event_version
         |
         v
     Aggregate state at branch point
```

### Step 2: Substitute the command

At the branch point, the engine replaces the original command with
`substituted_command`. It then runs the command handler registered at the command's
`command_version`:

```rust
// Original: DepartForStation { destination: Alpha }
// Substituted: DepartForStation { destination: Beta }
```

### Step 3: Replay forward

From the branch point forward, the engine re-runs the stored command sequence through
their version-matched handlers, using the counterfactual state:

```
Branch point state (with substituted command applied)
         |
         v
Command at v_branch+1  ->  handler(v_branch+1)  ->  counterfactual event
Command at v_branch+2  ->  handler(v_branch+2)  ->  counterfactual event
...
Command at v_latest    ->  handler(v_latest)     ->  counterfactual event
```

Each stored command carries a `command_version` field. The framework routes it to the
`#[command_handler]` registered at that exact version. No casting, no upcasting --
version-matched routing throughout.

### Step 4: Diff

The engine compares the original command sequence against the counterfactual command
sequence:

```rust
CommandDiff {
    added: [commands that exist in counterfactual but not original],
    removed: [commands that exist in original but not counterfactual],
    unchanged: [commands identical in both sequences],
}
```

The diff is at the **command level** -- it captures divergence in *intent* rather than
raw event data. This is more meaningful for analysis: "this command would have been
different" vs "this event field changed".

## The CounterfactualReplay trait

```rust
pub trait CounterfactualReplay: Send + Sync {
    type Error: std::error::Error + Send + Sync + 'static;

    async fn replay(
        &self,
        request: CounterfactualRequest,
    ) -> Result<CounterfactualResult, Self::Error>;
}
```

## ReplayEventStore

Counterfactual replay reads from a dedicated `ReplayEventStore` port that points at a
Cassandra read replica, separate from the live `EventStore`. This ensures:

- Replay reads do not impact live write performance
- Replay can run against a consistent snapshot of the event stream
- The live event store is not burdened with replay queries

The `ReplayEventStore` is injected via `ServiceBuilder` independently:

```rust
ServiceBuilder::new()
    .for_aggregate::<Ship>()
    .with_replay_event_store(read_replica_store)
    .build()
```

## Gateway API

The demo gateway exposes counterfactual replay via a REST endpoint:

```
GET /replay/counterfactual
    ?aggregate_id=<uuid>
    &branch_version=<u64>
    &command=<json-encoded-substituted-command>
```

Response:

```json
{
  "original_commands": [...],
  "counterfactual_commands": [...],
  "diff": {
    "added": [...],
    "removed": [...],
    "unchanged": [...]
  }
}
```

## Use cases

- **Debugging** -- "what would have happened if we sent a different route assignment?"
- **Impact analysis** -- "how many downstream commands would change if we modify this input?"
- **Auditing** -- "show me the alternative timeline for this aggregate"
- **Testing** -- validate that command handlers produce expected results under different inputs

## Requirements

Counterfactual replay requires:

1. The **command store** must have the full command history for the aggregate
2. The **event store** (or read replica) must have events up to the branch point
3. All **command handlers** must be registered at their original versions
4. All **event combiners** must be registered at their original versions

This is why Canon persists commands in the command store (not just events) -- command
history is essential for counterfactual analysis.
