# work-issues — Canon issue triage and parallel implementation swarm

You are the Canon issue orchestrator. You will read all open GitHub issues that
have no associated PR, work out which can be implemented in parallel right now,
then spawn an agent swarm to implement them — one agent per issue, each raising
a PR when done.

Read `CLAUDE.md` in full before doing anything else. The dependency graph and
implementation phases in CLAUDE.md are the source of truth for what blocks what:

```bash
cat CLAUDE.md
```

---

## Phase 0 — Discover unimplemented issues

### 0a. Fetch all open issues

```bash
gh issue list --state open --limit 100 \
  --json number,title,body,labels,assignees \
  > /tmp/all_issues.json

cat /tmp/all_issues.json
```

### 0b. Fetch all open PRs to find which issues are already in progress

```bash
gh pr list --state open --limit 100 \
  --json number,title,body,headRefName \
  > /tmp/all_prs.json
```

### 0c. Cross-reference to find unimplemented issues

```bash
python3 << 'EOF'
import json, re

issues = json.load(open('/tmp/all_issues.json'))
prs    = json.load(open('/tmp/all_prs.json'))

# Extract issue numbers mentioned in open PR bodies/branches
claimed = set()
for pr in prs:
    text = (pr.get('body') or '') + ' ' + pr.get('headRefName', '')
    for m in re.findall(r'#(\d+)|issue[- ](\d+)', text, re.IGNORECASE):
        num = m[0] or m[1]
        if num:
            claimed.add(int(num))

unimplemented = []
for issue in issues:
    if issue['number'] not in claimed:
        unimplemented.append(issue)

print(f"Open issues:       {len(issues)}")
print(f"Already in PR:     {len(claimed)}")
print(f"Unimplemented:     {len(unimplemented)}")
print()
for i in unimplemented:
    labels = [l['name'] for l in i.get('labels', [])]
    print(f"  #{i['number']}: {i['title']}  [{', '.join(labels)}]")

with open('/tmp/unimplemented_issues.json', 'w') as f:
    json.dump(unimplemented, f, indent=2)
EOF
```

If there are no unimplemented issues, print "All issues have open PRs." and exit.

---

## Phase 1 — Read and understand every unimplemented issue

For each issue in `/tmp/unimplemented_issues.json`, fetch its full body:

```bash
python3 << 'EOF'
import json, subprocess

issues = json.load(open('/tmp/unimplemented_issues.json'))

enriched = []
for issue in issues:
    num = issue['number']
    result = subprocess.run(
        ['gh', 'issue', 'view', str(num), '--json',
         'number,title,body,labels,comments'],
        capture_output=True, text=True
    )
    data = json.loads(result.stdout) if result.stdout.strip() else issue
    enriched.append(data)
    print(f"\n{'='*60}")
    print(f"Issue #{num}: {issue['title']}")
    print(f"{'='*60}")
    print(data.get('body', '(no body)'))

with open('/tmp/issues_enriched.json', 'w') as f:
    json.dump(enriched, f, indent=2)
EOF
```

---

## Phase 2 — Build the dependency graph

Read the enriched issues and the current codebase state, then determine what
blocks what. Think through this carefully — getting the dependency graph wrong
means agents will try to implement something whose dependencies don't exist yet.

**CRITICAL — inter-issue dependencies:** Two issues that are both individually
unblocked by `main` may still depend on each other. For example, if issue #A
implements `canon-publisher` (trait crate) and issue #B implements
`canon-publisher-kafka` (infra crate), then B depends on A — they cannot run
in parallel even though neither is blocked by something already on `main`.
You MUST detect and respect these transitive chains.

### 2a. Inventory the current codebase state

```bash
# Check which crates are fully implemented (non-empty lib.rs/main.rs)
python3 << 'EOF'
import os, pathlib

workspace_root = pathlib.Path('.')

status = {}
for toml in sorted(workspace_root.glob('*/Cargo.toml')):
    crate_dir = toml.parent
    name = crate_dir.name
    src = crate_dir / 'src'

    impl_files = list(src.glob('*.rs')) if src.exists() else []
    total_lines = sum(
        len(open(f).readlines())
        for f in impl_files
        if f.name not in ('mod.rs',)
    )

    if total_lines == 0:
        status[name] = 'EMPTY'
    elif total_lines < 20:
        status[name] = 'STUB'
    else:
        status[name] = f'IMPL ({total_lines} lines)'

for name, state in sorted(status.items()):
    print(f"  {name:<45} {state}")
EOF
```

### 2b. Map each issue to its codebase dependencies

For each unimplemented issue, determine:

1. **What crate does it implement?** (from issue title/body)
2. **What crates must already be implemented for this to compile?**
   - Check the dependency chain in `CLAUDE.md` implementation phases
   - Check the relevant `Cargo.toml` `[dependencies]` section
3. **Is everything it depends on already merged to `main`?**
   - Check `git log origin/main --oneline` for merged crates
   - Check open PRs — a dep that's in an open PR is NOT available yet
4. **Is any dependency provided by ANOTHER unimplemented issue in this batch?**
   - If issue #A produces crate X, and issue #B needs crate X, then B depends on A
   - This is an **inter-issue dependency** — B cannot start until A's PR is merged

```bash
git fetch origin
git log origin/main --oneline --name-only | grep "Cargo.toml\|/src/lib.rs" | head -40
```

### 2c. Build the full dependency graph (including inter-issue edges)

```bash
python3 << 'EOF'
import json

issues = json.load(open('/tmp/issues_enriched.json'))

# You will populate this dict as you analyse each issue.
# Structure: { issue_number: {
#   "crate": str,
#   "deps": [str],                  # crate names this issue needs
#   "blocked_by_main": [str],       # crates missing from main (not in any issue)
#   "blocked_by_issues": [int],     # issue numbers that produce a dep this issue needs
#   "blocked_by_prs": [int],        # open PR numbers that produce a dep this issue needs
#   "can_start": bool,
#   "wave": int,                    # 0 = can start now, 1 = unblocked after wave 0, etc.
#   "reason": str
# }}
analysis = {}

# STEP 1: For each issue, identify what crate it PRODUCES and what crates it NEEDS.
#
# EXAMPLE:
# analysis[10] = {
#     "crate": "canon-publisher",
#     "deps": ["canon-core"],
#     "blocked_by_main": [],
#     "blocked_by_issues": [],
#     "blocked_by_prs": [],
#     "can_start": True,
#     "wave": 0,
#     "reason": "canon-core is on main, no other deps"
# }
# analysis[25] = {
#     "crate": "canon-publisher-kafka",
#     "deps": ["canon-publisher", "canon-core"],
#     "blocked_by_main": [],
#     "blocked_by_issues": [10],        # issue #10 produces canon-publisher
#     "blocked_by_prs": [],
#     "can_start": False,
#     "wave": 1,                        # can start after wave 0 (issue #10) merges
#     "reason": "needs canon-publisher which is produced by issue #10 (not yet on main)"
# }

# STEP 2: Build a map of { crate_name -> issue_number } for all issues in this batch.
# Use this to detect inter-issue dependencies: if issue B needs crate X and issue A
# produces crate X, then B is blocked by A.

# STEP 3: Assign waves. Wave 0 = no blockers at all. Wave N = all blockers are in
# wave < N. Issues whose blockers include open PRs or crates not in any issue are
# blocked indefinitely (wave = -1).

# STEP 4: Print the full dependency graph.
print("\n=== WAVE 0 — CAN START NOW (no blocking deps) ===")
for num, info in sorted(analysis.items()):
    if info['wave'] == 0:
        print(f"  Issue #{num}: {info['crate']}")
        print(f"    Reason: {info['reason']}")

print("\n=== WAVE 1+ — BLOCKED BY OTHER ISSUES IN THIS BATCH ===")
for num, info in sorted(analysis.items()):
    if info['wave'] > 0:
        print(f"  Issue #{num}: {info['crate']}  [wave {info['wave']}]")
        print(f"    Depends on issues: {info['blocked_by_issues']}")
        print(f"    Reason: {info['reason']}")

print("\n=== BLOCKED INDEFINITELY (waiting for open PRs or missing crates) ===")
for num, info in sorted(analysis.items()):
    if info['wave'] == -1:
        print(f"  Issue #{num}: {info['crate']}")
        if info['blocked_by_prs']:
            print(f"    Waiting for PRs: {info['blocked_by_prs']}")
        if info['blocked_by_main']:
            print(f"    Missing from main: {info['blocked_by_main']}")
        print(f"    Reason: {info['reason']}")

# Save wave 0 as the ready set
ready = [{'number': num, **info} for num, info in sorted(analysis.items()) if info['wave'] == 0]
with open('/tmp/issues_ready.json', 'w') as f:
    json.dump(ready, f, indent=2)

# Save the full graph for the confirmation gate
with open('/tmp/issues_all_waves.json', 'w') as f:
    json.dump(analysis, f, indent=2, default=str)

print(f"\n{len(ready)} issues ready to implement now (wave 0).")
wave_counts = {}
for info in analysis.values():
    w = info['wave']
    wave_counts[w] = wave_counts.get(w, 0) + 1
for w in sorted(wave_counts):
    if w > 0:
        print(f"{wave_counts[w]} issues in wave {w} (unblocked after wave {w-1} merges).")
if -1 in wave_counts:
    print(f"{wave_counts[-1]} issues blocked indefinitely.")
EOF
```

If `/tmp/issues_ready.json` is empty, print which PRs/issues are blocking everything and exit.

---

## Confirmation gate — STOP HERE and ask the user before proceeding

After Phase 2 completes, present a clear summary and ask for explicit confirmation
before spawning any agents. Do not proceed to Phase 3 automatically under any
circumstances.

Present the summary in this exact format:

```
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  READY TO LAUNCH — work-issues swarm
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

  Wave 0 — launching now: N agents

  Issue  Crate                           Why it can start now
  ─────  ──────────────────────────────  ────────────────────────────────────
  #NN    canon-<name>                    <one-line reason>
  #NN    canon-<name>                    <one-line reason>
  ...

  Wave 1+ — blocked by issues in this batch (run /work-issues again after wave 0 PRs merge):
  #NN    canon-<name>   [wave 1]  — depends on: #NN (canon-<dep>)
  #NN    canon-<name>   [wave 2]  — depends on: #NN (canon-<dep>)
  ...

  Blocked indefinitely (waiting for open PRs not in this batch):
  #NN    canon-<name>   — waiting for: PR #NN (canon-<dep>)
  ...

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

  Each agent will: implement the crate → cargo check/clippy/test → raise PR.

  ⚠ Only wave 0 issues will be launched. Wave 1+ issues depend on wave 0
  outputs and must wait until those PRs are merged. Re-run /work-issues
  after merging to pick up the next wave.

  Proceed? (yes / no / adjust)
    yes    — launch all N wave-0 agents now
    no     — abort, nothing will be run
    adjust — tell me which issues to include or exclude before launching

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
```

Wait for the user's response.

- If **"yes"** (or "y", "go", "launch", "do it") — proceed to Phase 3.
- If **"no"** (or "n", "stop", "abort", "cancel") — print "Aborted. No agents were spawned." and exit.
- If **"adjust"** (or any message describing changes) — parse the user's intent:
  - "skip #NN" or "exclude #NN" → remove that issue from `/tmp/issues_ready.json` and re-display the summary
  - "only #NN and #NN" → keep only those issues, re-display the summary
  - "add #NN" → check if issue #NN is in the blocked list; if it can actually start (user is overriding the dep check), add it and warn; if it genuinely can't compile without a missing dep, explain why and decline
  - After applying the adjustment, re-display the updated summary and ask again

Do not move on until you have received a clear "yes" equivalent.

---

## Phase 3 — Spawn parallel implementation agents

One agent per ready issue. All agents run concurrently. Each agent runs in its
own git worktree for isolation.

For each ready issue, spawn an Agent with `isolation: "worktree"` and
`run_in_background: true`. The agent prompt for each issue should be:

```
You are a Canon implementation agent. Your task is to implement issue #<NUMBER>
and raise a pull request.

Issue title: <title>
Crate to implement: <crate>

Issue description:
<body>

---

## Step 0 — Read context

Read CLAUDE.md in full — it is the authoritative spec.

Read the trait crate that this implementation crate depends on.

Read any existing reference implementations for patterns to follow:
- If implementing a *-kafka crate, read another *-kafka crate as a pattern
- If implementing a *-yugabyte crate, read another *-yugabyte crate as a pattern
- If implementing a demo service, read canon-demo/fleet-service

---

## Step 1 — Create the branch

```bash
git checkout -b issue-<NUMBER>/<short-slug>
```

---

## Step 2 — Implement the crate

Follow the Canon rules from CLAUDE.md exactly:

**Non-negotiable:**
- `thiserror` for all error types — one error enum per crate, each variant named
- `AggregateId(Uuid)` newtype at all API boundaries — never plain `Uuid`
- `async_trait` on all trait impls
- No `unwrap()` or `expect()` in library code — all errors propagate via `?`
- No business logic in infrastructure crates
- README.md in every crate
- `tracing::` calls at key paths (dispatch, error, init)

**For YugabyteDB crates:**
- Multi-step operations wrapped in `sqlx::Transaction`
- `ON CONFLICT DO NOTHING` for idempotent inserts
- `migrations/001_<name>.sql` alongside `src/`
- Error type: `Yugabyte<Name>Error` with `Database(#[from] sqlx::Error)` + `Env(#[from] std::env::VarError)`
- All integration tests: `#[ignore = "requires YUGABYTE_URL"]`

**For Kafka crates:**
- `manual.offset.commit` = false always
- Producer and consumer as separable concerns
- Error type: `Kafka<Name>Error` with `thiserror` variants
- All integration tests: `#[ignore = "requires running Kafka broker"]`

**For demo services:**
- Follow the exact domain in CLAUDE.md's domain table
- Use `#[aggregate(snapshot_every = 50)]` unless otherwise specified
- Every command has a matching `#[command_handler]`
- Every event has a matching `#[event_combiner]`
- Every cross-service flow has an `#[event_handler]`

---

## Step 3 — Add tests

**Infrastructure crates:** Integration tests with `#[ignore]`, plus unit-testable
logic (serialisation roundtrips, error conversions, etc.)

**Demo services:** `canon-test` integration tests using `TestHarness` — in-memory only.

---

## Step 4 — Check against CI requirements

```bash
cargo check -p <crate> 2>&1
cargo clippy -p <crate> -- -D warnings 2>&1
cargo test -p <crate> 2>&1
```

Fix every error and warning before continuing.

---

## Step 5 — Commit and push

```bash
git add -A
git commit -m "feat(<crate-slug>): implement <title>

Closes #<NUMBER>."

git push origin issue-<NUMBER>/<short-slug>
```

---

## Step 6 — Raise the PR with labels

Before creating the PR, fetch the issue's labels and determine the correct PR labels:

```bash
# Get the issue's wave label (if any)
gh issue view <NUMBER> --json labels --jq '.labels[].name'
```

Choose labels from this table — apply **all** that match:

| Signal | Label |
|---|---|
| Changes to `.claude/commands/`, `.claude/settings.*`, hooks | `claude-improvement` |
| Updates to `CLAUDE.md`, `README.md`, design docs only | `documentation` |
| Bug fix | `bug` |
| New feature or capability | `enhancement` |
| Work in `canon-core/` | `canon-core` |
| Thin trait/port crate (`canon-event-store`, `canon-inbox`, etc.) | `trait-crate` |
| Infrastructure impl crate (`*-yugabyte`, `*-cassandra`, `*-kafka`) | `infrastructure` |
| Anything under `canon-demo/` | `canon-demo` |
| Leptos frontend (`canon-demo/frontend/`) | `frontend` |
| Issue has a `wave-N` label | copy the same `wave-N` label to the PR |

```bash
gh pr create \
  --title "feat(<crate-slug>): <title>" \
  --label "<label1>" --label "<label2>" \
  --body "$(cat <<'PREOF'
## Summary

Implements `<crate>` — closes #<NUMBER>.

## What's included

- <list what was implemented>
- Tests: <describe test coverage>

## Canon rules compliance

- [ ] `thiserror` error type with named variants
- [ ] No `unwrap()`/`expect()` in library code
- [ ] Migrations file (if DB-backed)
- [ ] All integration tests `#[ignore]`
- [ ] README.md added/updated
- [ ] `cargo check`, `cargo clippy -- -D warnings`, `cargo test` all pass

🤖 Generated with [Claude Code](https://claude.ai/claude-code)
PREOF
)" \
  --base main
```

Print: "Issue #<NUMBER> done — PR raised."
```

After all agents complete, proceed to Phase 4.

---

## Phase 4 — Collect results

After all background agents have completed, collect their results and present
a summary:

```
══════════════════════════════════════════════════════════════
  WORK-ISSUES SUMMARY
══════════════════════════════════════════════════════════════

  Wave 0 PRs raised:
    #NN  canon-<name>  → PR #<pr-number> <url>
    #NN  canon-<name>  → PR #<pr-number> <url>

  Failed:
    #NN  canon-<name>  — <reason>

  Next wave — unblocked once wave 0 PRs merge (re-run /work-issues):
    #NN  canon-<name>  [wave 1]  — depends on: #NN (canon-<dep>)
    #NN  canon-<name>  [wave 2]  — depends on: #NN → #NN (chain)

  Blocked indefinitely:
    #NN  canon-<name>  — waiting for: PR #NN

══════════════════════════════════════════════════════════════
```

For any failed agents, print the last few lines of their output for diagnosis.

---

## Dependency resolution rules for this Canon workspace

When determining whether an issue can start, apply these rules in order:

### Infrastructure crates — what they need on main

| Crate | Requires on main |
|---|---|
| `canon-publisher-kafka` | `canon-publisher` (trait), `canon-core` |
| `canon-deadletter-yugabyte` | `canon-deadletter` (trait), `canon-core` |
| Any `*-yugabyte` crate | Its trait crate, `canon-core`, `sqlx` (workspace) |
| Any `*-kafka` crate | Its trait crate, `canon-core`, `rdkafka` |

### Demo services — what they need on main

All demo services need ALL infrastructure crates on main before they can be
fully wired. However, the **domain layer** (aggregate, commands, events,
combiners, handlers) only needs `canon-core` — it can be implemented and
tested with the in-memory harness without any infrastructure.

A demo service issue CAN start if:
- `canon-core` is on main ✓ (it is)
- `canon-test` is on main ✓ (it is)
- `canon-demo/shared` is on main (check!)

A demo service issue CANNOT be fully wired (main.rs + ServiceBuilder) until:
- All infra crates its service uses are on main

Strategy: implement the domain layer + tests, stub out the `main.rs`,
raise the PR. The ServiceBuilder wiring can be added in a follow-up PR
or as a commit on the same branch once the infra PRs land.

### Never start without

- `canon-core` (always required — always on main)
- The trait crate for the impl crate being built
- For demo services: `canon-demo/shared` for cross-service type references

---

## Rules for all agents

- **Read CLAUDE.md fully.** It is the authoritative spec. If the issue body
  conflicts with CLAUDE.md, CLAUDE.md wins — note the discrepancy in the PR.
- **One issue per agent.** Never implement more than what the issue asks for.
- **Never modify trait crates.** Trait signatures are frozen. If an impl
  needs a different trait, raise it in the PR as a discussion item.
- **`cargo check` must pass** before raising the PR. A PR that doesn't
  compile will not be merged and wastes CI time.
- **Raise the PR even if tests are incomplete** — stub with `#[ignore]` and
  document what's missing rather than blocking the PR indefinitely.
- **If an issue is ambiguous**, implement the most conservative interpretation
  and document the ambiguity in the PR description. Do not invent features.
