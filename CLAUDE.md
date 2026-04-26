# Canon — Claude Code guide

Rust event sourcing framework. Design settled. Job: implementation.

Demo specifics: `canon-demo/CLAUDE.md`. Frontend: `canon-demo/frontend/CLAUDE.md`.

---

## Pipeline

```
External → canon-adaptor-kafka → canon-inbox-yugabyte → canon-inbound-queue-kafka
  → Dispatcher (command | internal events | external events)
  → YugabyteDB txn (commands + outbox)
  → Outbox processor → canon-outbound-queue-kafka
  → 4 consumers: event store (Cassandra+snapshots) | projections | publisher | internal-event re-entry
  → canon-publisher-kafka → canon.{service}.events
```

All Kafka topics partitioned by `aggregate_id`. Every stage must be wired and covered by an e2e test. A component that compiles but is never called is not implemented.

---

## Workspace

```
canon-core (traits + types + in-memory + canon-core-macros)
  ↓
canon-{event-store,command-store,snapshot-store,inbox,inbound-queue,
       outbound-queue,projection-store,publisher,adaptor,deadletter}
  ↓
canon-{event-store-cassandra,*-yugabyte,*-kafka}
canon-test           ← integration tests (in-memory + testcontainers)
canon-demo/          ← services, gateway, frontend
canon-site/, canon-docs/
```

Strict DAG: impl crates depend on their trait crate + `canon-core` only. No cross-impl deps.

Run `codemap --diff` before any task. Run `codemap handoff` at session boundaries.

---

## Non-negotiable rules

- **tokio + async_trait only**. No async-std. No manual `Pin<Box<dyn Future>>`.
- **`thiserror`** per crate. No `anyhow`. No god enum.
- **`AggregateId(Uuid)`** newtype. Never plain `Uuid`.
- **Proc-macros** in the crate that owns the concept. No `canon-macros` umbrella.
- **In-memory impls** for every trait — the test harness lives in `canon-core`.
- **Outbox pattern**: events + command in one YugabyteDB ACID txn. Outbox is the commit point.
- **Outbox processor** drains outbox → outbound queue. Nothing else.
- **No direct Cassandra writes** from command path.
- **Snapshots** are written by the event store consumer when `version % N == 0`.
- **Idempotency**: every event handler and projection must be safe to call twice.
- **Optimistic concurrency**: event store rejects version mismatches.
- **Macros generate all impls**. Users never hand-write `Aggregate`/`CommandHandler`/`EventHandler`/`Projection`.
- **Exhaustiveness** (compile errors): `#[command(X,v=N)]` ↔ `#[command_handler(X,v=N)]`; `#[event(X,v=N)]` ↔ `#[event_combiner(X,v=N)]`.
- **Version-matched routing, no casting**: framework reads `event_version`/`command_version` and dispatches to the exact matching handler.
- **Event handlers are aggregate-agnostic** — `#[event_handler]` has no aggregate type parameter.
- **`window_ttl` requires `oversight`** (compile error otherwise).
- **Window key = `(handler_id, correlation_key)`** — from `correlate` fn or fallback to envelope `correlation_id`. Never `aggregate_id`.
- **Auto-registration via `inventory`**. `ServiceBuilder` discovers everything.
- **No hand-wired event routing in services**. No manual Kafka consumers, no hand-built `CommandEnvelope`s, no raw inbox SQL, no `cross_service.rs`. Use `#[event_handler]` + `ServiceBuilder` topic subscriptions.
- **Per-service storage isolation**: each demo service owns its YugabyteDB schema (`canon_{fleet,cargo,navigation,supply,station}`) and Cassandra keyspace. Never share inbox/outbox/commands/events tables. Use `canon_demo_shared::db::create_service_pool()` and `CassandraEventStore::new_with_keyspace()`.
- **`rskafka` only**. No `rdkafka`, no C deps. Pure Rust, cross-compilable. Offsets in-memory, restart from zero — application-layer idempotency is the safety net.
- **READMEs stay current**. Touch a crate's public API → update its README in the same PR.

---

## Core types

Defined in `canon-core/src/`. Treat as frozen contracts:

`AggregateId(Uuid)` · `Version(u64)` · `EventEnvelope` · `CommandEnvelope` · `IncomingMessage { Command | InternalEvent | ExternalEvent }` · `Oversight { Ready | NotReady | Discard }` · `CounterfactualRequest`/`Result`/`CommandDiff`.

## Core traits

Defined in `canon-core/src/`. Do not modify signatures.

`Aggregate` (hydrate dispatches to version-matched combiners; `type State = Self`) · `CommandHandler<A>` (one per command/version, returns single event or Err) · `EventHandler` (no aggregate param, optional `oversight`/`correlate`) · `Projection` (idempotent apply + rebuild) · `EventCombiner<A>` (sync state fold, generated) · `ProjectionHandler<P>` (generated) · `CounterfactualReplay`.

## Macros

All in `canon-core/canon-core-macros/`, re-exported from `canon-core`. Eight macros:

`#[aggregate(snapshot_every = N)]` — generates `impl Aggregate`, hydration dispatch, `Default`, serde, `inventory` reg.
`#[command(Aggregate, version = N, produces = [Event])]` — `produces` is metadata only; one event or Err.
`#[event(Aggregate, version = N)]` — versions coexist as distinct types.
`#[event_combiner(Aggregate, version = N)]` — sync, pure fold.
`#[command_handler(Aggregate, version = N)]` — return type must match `produces`.
`#[event_handler(window_ttl = "..."?)]` with `#[handles(EventType, version = N)]` per method.
`#[projection]` / `#[projection_handler(ProjectionName)]`.

`ServiceBuilder::new().for_aggregate::<Ship>().build()` — discovers everything via `inventory`.

For full signatures and examples: read the macros crate and `canon-test/`.

---

## Operational details (framework responsibilities)

**Dispatcher**: polls inbox. Command path → hydrate → version-matched handler → ACID txn (commands + outbox). Event-handler path → ready window → call `handle()` → if `Some(CommandEnvelope)` returned, write to inbox via `InboxPort`.

**Cross-service + internal event routing**: adaptor (external) or 4th outbound consumer (internal) checks `EventHandlerRegistration` inventory for matching `#[handles]`, calls `Inbox::submit`. Inbox dedups, accumulates per `(handler_id, correlation_key)`, evaluates oversight. Service authors only write `#[event_handler]`.

**Outbox processor**: `SELECT … FOR UPDATE SKIP LOCKED` → publish → `delivered_at`. Bounded channel (default 1024).

**Service lifecycle**: `ServiceBuilder::build()` → `Service`. `service.start()` spawns 5 background tasks (outbox, event store, projection, publisher, internal-event consumer) with watch-channel shutdown.

**Outbound consumers** (4): event store (Cassandra + snapshot at `version % N == 0`, retry 3 → DLQ) · projections (idempotent apply, `last_version`, rebuild via offset reset) · publisher (`canon.{service}.events`) · internal-event re-entry. All restart from offset 0; rely on downstream idempotency.

**Kafka pattern (rskafka)**: `ClientBuilder::new(brokers).build().await` → `client.partition_client(topic, 0, UnknownTopicHandling::Retry)`. Produce: `produce(vec![record], Compression::NoCompression)`. Consume: `fetch_records(offset, 1..1_048_576, timeout_ms)` polled, `Mutex<i64>` offset. No consumer groups. Commit = no-op. Errors: per-crate `thiserror`.

**Inbox**: idempotent on `(handler_id, message_id)`. `processed_windows(window_id)` for batch idempotency. Window TTL → `expired` → DLQ with `window_expired`.

**InboxPort**: local re-entry only. Cross-service = REST or framework Kafka routing.

**Counterfactual replay**: operates on commands, not events. Hydrates to branch, routes stored commands by `command_version`, diffs at command level. `ReplayEventStore` points at read replica.

**Projection rebuild**: `rebuilding=true` → reads fall back to read-through → reset consumer offset → replay → `rebuilding=false`.

**Dead letters**: `retry_attempts` table (crash-safe). Max → `dead_letters`. Admin requeue re-enters inbox with fresh `expires_at`.

---

## Schemas

Per-service YugabyteDB schemas (`canon_{fleet,cargo,navigation,supply,station}`) and Cassandra keyspaces (same names). Tables per schema: `inbox_messages`, `inbox_windows`, `processed_windows`, `commands`, `outbox` (+ `outbox_seq`), `snapshots`, `projection_checkpoints`, `dead_letters`, `retry_attempts`. Cassandra: `events(aggregate_id UUID, version BIGINT, …, PK (aggregate_id, version))`.

Full DDL: `canon-demo/init-schema/`. Env: `CASSANDRA_NODES`, `YUGABYTE_URL`, `KAFKA_BROKERS`.

---

## Testing strategy

Three tiers, all run by default — no `#[ignore]`:

1. **In-memory e2e** (`canon-test`): `InMemory*` stores wired through real `Service`/`ServiceBuilder`. Sub-second, no Docker. Logic coverage of the full pipeline.
2. **Testcontainers e2e**: real YugabyteDB + Cassandra + Kafka per test module via `testcontainers` crate. Catches SQL/CQL/serialisation/protocol bugs.
3. **Playwright** (`canon-demo/e2e/`): browser smoke tests against running cluster. `make k8s-test-e2e` after `make k8s-up`, or via `/test-demo`. Local only.

**Never `#[ignore]` pipeline tests** — use testcontainers. `#[ignore]` rots silently. Existing ignored tests in infra crates migrate to testcontainers in #251.

**Never implement a pipeline component without an e2e test exercising it.**

---

## Codebase exploration

Use **LSP first** (go-to-def, find-refs, hover, workspace symbols). Fall back to grep/glob only when LSP can't.

`codemap`:
```
codemap --diff             # before every task
codemap --deps .
codemap blast-radius .
codemap handoff .          # at session start/end
```
Hub-file warnings = high blast radius. Be conservative.

---

## When stuck

1. Re-read this file.
2. Check the trait — the signature is the contract.
3. Check the dependency graph — wrong dep = wrong approach.
4. Ask the user. Do not invent.

## Never

- Add unlisted dependencies without asking.
- Change trait signatures.
- Put business logic in infra crates, or infra in `canon-core`.
- `unwrap()`/`expect()` in library code.
- `clone()` to dodge the borrow checker without flagging it.
- `// TODO` — implement it or ask.
- Checkout other branches in the main working directory. Use git worktrees under `~/worktrees/`. Main stays on `main`.
- Use `InMemory*` stores in demo `main.rs` — wire real impls. In-memory is for `canon-test`.
- Add C deps to Kafka crates (`rdkafka`/`librdkafka-sys` banned).
- Store Kafka offsets externally for correctness — that's a perf optimisation only.
- Commit secrets: GCP keys, `~/.canon-debug-key`, `~/.canon-auth-password`, GitHub tokens, kubeconfigs. Use env vars / K8s Secrets.
- Nuke prod data to debug — diagnose first, reproduce in disposable env, smallest safe fix.
- Blame infra before checking the API. `curl /game/$SID` first.
