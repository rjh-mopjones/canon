# fix-prs — Canon PR conflict and CI failure fixer

You are the Canon PR health agent. You will check every open PR for merge conflicts
and CI failures, fix them all, and push clean branches.

Read `CLAUDE.md` before doing anything else:
```bash
cat CLAUDE.md
```

**IMPORTANT:** Use the LSP tool (rust-analyzer) throughout this process. Before and after every code fix, use `LSP hover`, `LSP goToDefinition`, and `LSP documentSymbol` to verify types, signatures, and symbol existence. Never guess at type signatures — ask the LSP.

---

## Phase 0 — Discover PR state

```bash
# Get all open PRs with their CI status and mergeability
gh pr list --state open --limit 100 \
  --json number,title,headRefName,headRefOid,mergeable,mergeStateStatus,statusCheckRollup \
  > /tmp/pr_states.json

cat /tmp/pr_states.json
```

Categorise every PR:

```bash
python3 << 'EOF'
import json

prs = json.load(open('/tmp/pr_states.json'))

blocked = []
clean   = []

for pr in prs:
    num    = pr['number']
    title  = pr['title']
    branch = pr['headRefName']
    sha    = pr['headRefOid']
    merge  = pr.get('mergeable', 'UNKNOWN')          # MERGEABLE | CONFLICTING | UNKNOWN
    state  = pr.get('mergeStateStatus', 'UNKNOWN')   # CLEAN | DIRTY | BLOCKED | BEHIND | UNKNOWN

    # Collect failing checks
    checks      = pr.get('statusCheckRollup') or []
    failing     = [c for c in checks if c.get('conclusion') in ('FAILURE', 'ERROR', 'TIMED_OUT')]
    pending     = [c for c in checks if c.get('conclusion') is None and c.get('status') != 'COMPLETED']
    check_names = [c.get('name', c.get('context', '?')) for c in failing]

    has_conflict = merge == 'CONFLICTING'
    has_failures = bool(failing)

    if has_conflict or has_failures:
        blocked.append({
            'number':        num,
            'title':         title,
            'branch':        branch,
            'sha':           sha,
            'has_conflict':  has_conflict,
            'failing_checks': check_names,
            'merge_state':   state,
        })
    else:
        clean.append((num, title))

print("=== CLEAN (no action needed) ===")
for num, title in clean:
    print(f"  PR #{num}: {title}")

print(f"\n=== NEED FIXING ({len(blocked)}) ===")
for pr in blocked:
    issues = []
    if pr['has_conflict']:  issues.append('CONFLICT')
    if pr['failing_checks']: issues.append('CI:' + ','.join(pr['failing_checks']))
    print(f"  PR #{pr['number']}: {pr['title']} [{pr['branch']}] — {' | '.join(issues)}")

with open('/tmp/prs_to_fix.json', 'w') as f:
    json.dump(blocked, f, indent=2)

print(f"\n{len(blocked)} PRs need fixing.")
EOF
```

If nothing needs fixing, print "All PRs are clean." and exit.

---

## Phase 1 — Triage CI failures (before touching any branch)

For each PR with failing checks, fetch the actual failure output:

```bash
python3 << 'EOF'
import json, subprocess

prs = json.load(open('/tmp/prs_to_fix.json'))

for pr in prs:
    if not pr['failing_checks']:
        continue
    num = pr['number']
    print(f"\n=== PR #{num} CI failures ===")
    # Get the check run logs
    result = subprocess.run(
        ['gh', 'run', 'list', '--branch', pr['branch'], '--limit', '3',
         '--json', 'databaseId,status,conclusion,name'],
        capture_output=True, text=True
    )
    print(result.stdout)

    # Get the most recent failed run log
    runs = json.loads(result.stdout) if result.stdout.strip() else []
    failed_runs = [r for r in runs if r.get('conclusion') in ('failure', 'timed_out')]
    if failed_runs:
        run_id = failed_runs[0]['databaseId']
        log = subprocess.run(
            ['gh', 'run', 'view', str(run_id), '--log-failed'],
            capture_output=True, text=True
        )
        # Print the relevant error lines
        lines = log.stdout.split('\n')
        error_lines = [l for l in lines if any(
            kw in l for kw in ['error[', 'error:', 'FAILED', 'warning[', 'could not compile']
        )]
        print('\n'.join(error_lines[:50]))
EOF
```

Classify every CI failure into one of these known Canon patterns:

| Pattern | Signature | Fix |
|---|---|---|
| **MISSING_DEP** | `libssl`, `libcurl`, `rdkafka-sys` link error | Add system dep install to `ci.yml` |
| **COMPILE_ERROR** | `error[E...]` in `cargo check` | Fix the Rust error in the crate |
| **CLIPPY** | `error: ... [-D warnings]` in `cargo clippy` | Fix the clippy lint |
| **TEST_FAIL** | `FAILED` in `cargo test` | Fix the failing test |
| **MISSING_MEMBER** | `error: no such package` | Add crate to workspace `Cargo.toml` |
| **LOCK_CONFLICT** | `Cargo.lock ... conflicts` | Regenerate `Cargo.lock` |

Save the classified failures:

```bash
python3 << 'EOF'
import json

# Load prs_to_fix and annotate with classified failure types
# (you will fill this in as you read the failure output above)
# Write /tmp/pr_failures.json with structure:
# { "number": N, "failures": [{"type": "COMPILE_ERROR", "crate": "...", "detail": "..."}] }
EOF
```

---

## Phase 2 — Fix conflicts and CI failures (parallel agents, one per PR)

Spawn one agent per PR in `/tmp/prs_to_fix.json`. All agents run concurrently.

```bash
python3 << 'ORCHESTRATOR'
import json, subprocess, os

prs    = json.load(open('/tmp/prs_to_fix.json'))
main   = subprocess.run(['git', 'rev-parse', 'origin/main'],
                        capture_output=True, text=True).stdout.strip()

AGENT_PROMPT = """
You are the Canon PR fix agent for PR #{number} — "{title}".
Branch: {branch}
Current HEAD: {sha}
Problems: {problems}

Your job: fix every problem, push a clean branch, and report what you did.

**IMPORTANT:** Use the LSP tool (rust-analyzer) throughout. After every code fix:
- Use `LSP hover` on changed symbols to verify their types
- Use `LSP goToDefinition` to confirm method/trait references resolve correctly
- Use `LSP documentSymbol` on edited files to verify structure
- Use `LSP findReferences` when renaming or removing items to catch all usages
Never guess at type signatures or method existence — ask the LSP first.

---

## Setup

```bash
git fetch origin
git checkout {branch}
git status
```

---

## Step 1 — Fix merge conflicts

Check for conflicts against main:
```bash
git merge-base HEAD origin/main
git diff HEAD...origin/main --name-only
```

If conflicts exist, rebase onto main:
```bash
git rebase origin/main
```

**If the rebase hits conflicts, resolve them file by file using these rules:**

### Cargo.toml conflicts (workspace members)
The `members` array in the root `Cargo.toml` is the most common conflict.
Both sides added different crates — the correct resolution is ALWAYS to include
ALL entries from both sides, in alphabetical order within each logical group.

When you see:
```
<<<<<<< HEAD
    "canon-inbox-yugabyte",
=======
    "canon-snapshot-store-yugabyte",
>>>>>>> origin/main
```

Resolve to include both:
```toml
    "canon-inbox-yugabyte",
    "canon-snapshot-store-yugabyte",
```

### Cargo.lock conflicts
Never manually resolve Cargo.lock conflicts. After resolving Cargo.toml:
```bash
rm Cargo.lock
cargo generate-lockfile 2>&1 | tail -5
```

### ci.yml conflicts
The canonical ci.yml includes the `libcurl4-openssl-dev` system dependency install.
If one side has it and the other doesn't, keep the version that has it:

```yaml
jobs:
  check:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - name: Install system dependencies
        run: sudo apt-get update && sudo apt-get install -y libcurl4-openssl-dev
      - uses: dtolnay/rust-toolchain@stable
        with:
          components: clippy
      - run: cargo check --workspace
      - run: cargo clippy --workspace -- -D warnings
      - run: cargo test --workspace
```

### canon-core/src/types.rs conflicts
If both sides add `Version::from_u64`, keep exactly one copy:
```rust
pub fn from_u64(v: u64) -> Self {{ Self(v) }}
```
Remove the duplicate, keep one.

### README.md / CLAUDE.md conflicts
These are documentation files. Read both sides, merge the content manually —
keep all new sections from both sides. Never drop content added by either side.

### After resolving each conflict file:
```bash
git add <resolved-file>
```

Continue the rebase:
```bash
git rebase --continue
```

If the rebase cannot be completed cleanly, abort and use merge instead:
```bash
git rebase --abort
git merge origin/main -m "merge: sync with main for PR #{number}"
# Then resolve conflicts as above
```

---

## Step 2 — Fix CI failures

After the conflict resolution, check what actually fails:

```bash
cargo check --workspace 2>&1 | grep -E "^error" | head -30
```

Work through each class of failure:

### COMPILE_ERROR — fix the Rust error

Read the full error:
```bash
cargo check --workspace 2>&1
```

Identify the crate and file. Read the file, understand the error, fix it.

**Use the LSP to diagnose:** Before editing, use `LSP hover` on the problematic symbol
to see what the compiler thinks its type is. Use `LSP goToDefinition` to find where
traits and methods are actually defined. This avoids guessing.

Common Canon compile errors:
- `Version::from_u64` called but not defined — add to `canon-core/src/types.rs`:
  ```rust
  pub fn from_u64(v: u64) -> Self {{ Self(v) }}
  ```
- Missing trait impl — check the trait crate and implement correctly
- Wrong error type — check the `From<>` impl chain
- `debug_assert!` on a type that requires `Result` — convert to `?` propagation

After each fix: `cargo check -p <crate>` to verify before moving on.

### CLIPPY — fix the lint

```bash
cargo clippy --workspace 2>&1 | grep "^error" | head -20
```

Common Canon clippy issues:
- Unused import — remove it
- `unwrap()` in library code — convert to `?` or `.map_err()`
- Redundant clone — remove it
- `dead_code` on test helpers — delete the helper or add `#[cfg(test)]`

After fixes: `cargo clippy --workspace -- -D warnings 2>&1 | grep "^error"`

### TEST_FAIL — fix the failing test

```bash
cargo test --workspace 2>&1 | grep -A 20 "FAILED\\|failures:"
```

Read the test, understand why it fails, fix either the implementation or the test.
Never delete a test to make CI pass.

### MISSING_DEP — fix the system dependency

If the failure is a linker error for `libssl`, `libcurl`, or `libsasl`:
```bash
# Ensure ci.yml has the system dep install step
cat .github/workflows/ci.yml
```

If the install step is missing, add it:
```bash
# Edit .github/workflows/ci.yml to add:
#   - name: Install system dependencies
#     run: sudo apt-get update && sudo apt-get install -y libcurl4-openssl-dev
```

---

## Step 3 — Final verification

**Use the LSP for final validation:** Run `LSP documentSymbol` on every file you
changed to verify the structure is correct. Use `LSP hover` on any symbol you're
unsure about.

```bash
# Full workspace check
cargo check --workspace 2>&1 | tail -5

# Clippy clean
cargo clippy --workspace -- -D warnings 2>&1 | grep "^error" | wc -l
# Must be 0

# Tests pass (unit tests only — integration tests are #[ignore])
cargo test --workspace 2>&1 | tail -10
```

If anything still fails, iterate on Step 2 until clean.

---

## Step 4 — Commit and push

```bash
git add -A

# Build a descriptive commit message
FIXES=""
if git diff HEAD~ --name-only 2>/dev/null | grep -q "Cargo.toml"; then
  FIXES="$FIXES\\n- resolve Cargo.toml workspace member conflicts"
fi
if git diff HEAD~ --name-only 2>/dev/null | grep -q "ci.yml"; then
  FIXES="$FIXES\\n- fix ci.yml system dependency install"
fi
if git diff HEAD~ --name-only 2>/dev/null | grep -q "types.rs"; then
  FIXES="$FIXES\\n- deduplicate Version::from_u64 in canon-core"
fi

git commit -m "fix: resolve conflicts and CI failures for PR #{number}

$(echo -e $FIXES)

Branch synced with main at $(git rev-parse --short origin/main)."

git push --force-with-lease origin {branch}
```

---

## Step 5 — Report

Post a comment on the PR:
```bash
FIX_SHA=$(git rev-parse HEAD)
gh pr comment {number} --body "## fix-prs bot

Branch has been rebased onto main and all CI failures resolved.

**Changes made:**
$(git log --oneline HEAD~1..HEAD)

**CI status before fix:** {problems}

**Commit:** \`$FIX_SHA\`

_Run \`/review-prs\` to re-review code quality after this sync._"
```

Print: "PR #{number} done — $FIX_SHA"
"""

for pr in prs:
    problems_parts = []
    if pr['has_conflict']:
        problems_parts.append('MERGE CONFLICT')
    if pr['failing_checks']:
        problems_parts.append('CI FAILURES: ' + ', '.join(pr['failing_checks']))
    problems = ' | '.join(problems_parts) or 'unknown'

    prompt = AGENT_PROMPT.format(
        number=pr['number'],
        title=pr['title'],
        branch=pr['branch'],
        sha=pr['sha'],
        problems=problems,
    )

    log_file = f"/tmp/fix_agent_pr{pr['number']}.log"
    proc = subprocess.Popen(
        ['claude', '--print', '--dangerously-skip-permissions'],
        stdin=subprocess.PIPE,
        stdout=open(log_file, 'w'),
        stderr=subprocess.STDOUT,
        text=True
    )
    proc.stdin.write(prompt)
    proc.stdin.close()
    print(f"Spawned fix agent for PR #{pr['number']} ({pr['branch']}) → {log_file}")

print(f"\nAll {len(prs)} agents running...")
ORCHESTRATOR

wait
echo "All fix agents finished."
```

---

## Phase 3 — Verify and report

```bash
python3 << 'EOF'
import json, subprocess, os

prs = json.load(open('/tmp/prs_to_fix.json'))

print("=" * 60)
print("FIX-PRS SUMMARY")
print("=" * 60)

all_ok = True
for pr in prs:
    num = pr['number']
    log = f"/tmp/fix_agent_pr{num}.log"
    print(f"\nPR #{num}: {pr['title']}")

    if not os.path.exists(log):
        print("  ERROR: no log found")
        all_ok = False
        continue

    content = open(log).read()
    lines = content.strip().split('\n')

    # Find the final status line
    done_lines = [l for l in lines if f'PR #{num} done' in l or 'error' in l.lower()]
    for l in done_lines[-3:]:
        print(f"  {l}")

    # Check CI status of the updated branch
    result = subprocess.run(
        ['gh', 'pr', 'view', str(num), '--json', 'mergeStateStatus,statusCheckRollup'],
        capture_output=True, text=True
    )
    if result.stdout.strip():
        try:
            data = json.loads(result.stdout)
            state = data.get('mergeStateStatus', '?')
            checks = data.get('statusCheckRollup') or []
            failing = [c for c in checks if c.get('conclusion') in ('FAILURE', 'ERROR')]
            print(f"  Merge state: {state} | Failing checks: {len(failing)}")
        except:
            pass

print("\n" + "=" * 60)
if all_ok:
    print("All fix agents completed. Check GitHub for updated CI status.")
    print("Note: CI takes ~2-5 minutes to re-run after push.")
else:
    print("Some agents had issues. Review logs in /tmp/fix_agent_pr*.log")
EOF
```

---

## Conflict resolution reference

These are the specific conflict patterns in this Canon workspace:

### Root `Cargo.toml` — workspace members
Each PR adds one crate to the `members` array. When PRs diverge from the same base,
the members list conflicts. Resolution: **include all entries, alphabetically sorted
within each group** (core → trait crates → impl crates → test → demo).

Canonical member order for impl crates:
```toml
"canon-adaptor-kafka",
"canon-command-store-pg",
"canon-command-store-yugabyte",
"canon-deadletter-pg",
"canon-event-store-cassandra",
"canon-inbox-pg",
"canon-inbox-yugabyte",
"canon-inbound-queue-kafka",
"canon-outbound-queue-kafka",
"canon-projection-store-pg",
"canon-projection-store-yugabyte",
"canon-publisher-kafka",
"canon-queue-rabbitmq",
"canon-snapshot-store-pg",
"canon-snapshot-store-yugabyte",
```

### `.github/workflows/ci.yml` — system dependencies
Kafka PRs require `libcurl4-openssl-dev`. The canonical ci.yml always includes it.
Never have two versions of ci.yml on different branches — the version with
`libcurl4-openssl-dev` is always correct.

### `canon-core/src/types.rs` — `Version::from_u64`
PRs #72 and #73 both add this method. The correct resolution is exactly one copy
in the `impl Version` block, immediately after `as_u64`.

### `Cargo.lock`
Never manually resolve. Delete and regenerate after fixing `Cargo.toml`.

---

## Rules

- **Rebase over merge** where possible — keeps the history linear. Fall back to merge
  only if rebase produces unresolvable conflicts.
- **Never drop content** from either side of a documentation conflict (CLAUDE.md, README.md).
- **Never delete tests** to make CI pass — fix the implementation.
- **`cargo check` before `cargo clippy` before `cargo test`** — earlier failures mask later ones.
- **One agent per PR** — agents do not coordinate. If two PRs both fix the same shared file
  (e.g. `canon-core/src/types.rs`), each branch will have the fix independently after rebasing
  onto main. That's correct.
- **`--force-with-lease`** only — never `--force`. If the push is rejected because the remote
  was updated by someone else since the agent started, abort and report rather than overwriting.
- **Use the LSP** — always verify types, definitions, and references via rust-analyzer before
  and after making code changes. Never guess at method signatures or trait bounds.
