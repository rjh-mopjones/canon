# review-prs — Canon PR review, fix, and health agent

You are the Canon PR review orchestrator. Your job is to:

1. Discover all open PRs (review state, merge conflicts, CI status)
2. For each PR, check whether it has already been reviewed by this command
3. Spawn parallel review agents — one per unreviewed (or changed) PR
4. Each agent fixes merge conflicts, fixes CI failures, reviews the PR, posts inline GitHub comments, applies all fixes, and commits

Read `CLAUDE.md` before doing anything else:
```bash
cat CLAUDE.md
```

---

## Phase 0 — Discover open PRs and their review state

```bash
# Get all open PRs with metadata, merge status, CI status, and changed files
gh pr list --state open --limit 100 \
  --json number,title,headRefName,headRefOid,body,comments,mergeable,mergeStateStatus,statusCheckRollup \
  > /tmp/open_prs.json

# For each PR, also get the list of changed files to classify rust vs non-rust
for num in $(cat /tmp/open_prs.json | python3 -c "import sys,json; [print(p['number']) for p in json.load(sys.stdin)]"); do
  gh pr diff "$num" --name-only > "/tmp/pr_${num}_files.txt" 2>/dev/null
done
```

For each PR, check whether a `review-prs` bot comment already exists, classify as rust/non-rust, and report merge conflict / CI status:

```bash
python3 << 'EOF'
import json, subprocess, sys, os

prs = json.load(open('/tmp/open_prs.json'))
SENTINEL = '<!-- review-prs-bot -->'

# Rust file extensions that require compilation
RUST_EXTENSIONS = {'.rs', '.toml'}
# Files that are always non-rust even with .toml extension
NON_RUST_PATHS = {'canon-site/', 'canon-docs/', 'canon-demo/frontend/reference/',
                  'canon-demo/e2e/', 'canon-demo/frontend/style/',
                  'canon-demo/frontend/dist/', 'canon-demo/k8s/'}

def classify_pr(num):
    """Classify PR as 'rust' (needs cargo) or 'non-rust' (review-only)."""
    files_path = f'/tmp/pr_{num}_files.txt'
    if not os.path.exists(files_path):
        return 'rust', []  # default to rust if we can't read files

    with open(files_path) as f:
        files = [line.strip() for line in f if line.strip()]

    # Find which Rust crates are affected
    rust_crates = set()
    has_rust = False

    for fpath in files:
        # Skip known non-rust paths
        if any(fpath.startswith(p) for p in NON_RUST_PATHS):
            continue
        if fpath.endswith('.rs') or (fpath.endswith('.toml') and 'Cargo' in fpath):
            has_rust = True
            # Extract crate name from path (first directory component)
            parts = fpath.split('/')
            if parts[0] in ('canon-core', 'canon-test') or parts[0].startswith('canon-'):
                rust_crates.add(parts[0])
            elif parts[0] == 'canon-demo' and len(parts) > 1:
                rust_crates.add(f'{parts[0]}/{parts[1]}')

    if not has_rust:
        return 'non-rust', files

    return 'rust', list(rust_crates)

needs_review = []
already_reviewed = []

for pr in prs:
    num = pr['number']
    merge = pr.get('mergeable', 'UNKNOWN')
    state = pr.get('mergeStateStatus', 'UNKNOWN')

    checks      = pr.get('statusCheckRollup') or []
    failing     = [c for c in checks if c.get('conclusion') in ('FAILURE', 'ERROR', 'TIMED_OUT')]
    check_names = [c.get('name', c.get('context', '?')) for c in failing]

    has_conflict = merge == 'CONFLICTING'
    has_failures = bool(failing)

    health_notes = []
    if has_conflict:  health_notes.append('CONFLICT')
    if has_failures:  health_notes.append('CI:' + ','.join(check_names))
    health_str = ' | '.join(health_notes) if health_notes else 'healthy'

    pr_type, crates_or_files = classify_pr(num)

    result = subprocess.run(
        ['gh', 'pr', 'view', str(num), '--json', 'comments', '--jq', '.comments[].body'],
        capture_output=True, text=True
    )
    comments = result.stdout
    if SENTINEL in comments:
        review_lines = [l for l in comments.split('\n') if 'review-prs-sha:' in l]
        if review_lines:
            last_sha = review_lines[-1].split('review-prs-sha:')[-1].strip()
            current_sha = pr['headRefOid']
            if last_sha == current_sha and not has_conflict and not has_failures:
                already_reviewed.append((num, pr['title'], 'no new commits, healthy'))
            else:
                reason_parts = []
                if last_sha != current_sha:
                    reason_parts.append(f're-review: HEAD changed {last_sha[:7]}→{current_sha[:7]}')
                if has_conflict or has_failures:
                    reason_parts.append(health_str)
                needs_review.append((num, pr['title'], pr['headRefName'], current_sha,
                                     ' + '.join(reason_parts) if reason_parts else 'initial review',
                                     has_conflict, check_names, pr_type, crates_or_files))
        else:
            needs_review.append((num, pr['title'], pr['headRefName'], pr['headRefOid'],
                                 f'initial review ({health_str})', has_conflict, check_names,
                                 pr_type, crates_or_files))
    else:
        needs_review.append((num, pr['title'], pr['headRefName'], pr['headRefOid'],
                             f'initial review ({health_str})', has_conflict, check_names,
                             pr_type, crates_or_files))

print("=== SKIPPING (already reviewed, no new commits, healthy) ===")
for num, title, reason in already_reviewed:
    print(f"  PR #{num}: {title} — {reason}")

print("\n=== WILL REVIEW ===")
for num, title, branch, sha, reason, conflict, ci_fails, pr_type, crates in needs_review:
    crate_str = ', '.join(crates[:5]) if crates else 'none'
    print(f"  PR #{num}: {title} [{branch}] @ {sha[:7]} — {reason} [{pr_type}: {crate_str}]")

with open('/tmp/prs_to_review.json', 'w') as f:
    json.dump([
        {'number': num, 'title': title, 'branch': branch, 'sha': sha, 'reason': reason,
         'has_conflict': conflict, 'failing_checks': ci_fails,
         'pr_type': pr_type, 'affected_crates': crates}
        for num, title, branch, sha, reason, conflict, ci_fails, pr_type, crates in needs_review
    ], f, indent=2)

print(f"\n{len(needs_review)} PRs to review, {len(already_reviewed)} skipped.")
EOF
```

If `/tmp/prs_to_review.json` is empty, print "All PRs are up to date." and exit.

---

## Phase 1 — Spawn one review+fix agent per PR (all in parallel)

Read the work list and spawn agents. Use the Agent tool with `isolation: "worktree"` for
each PR. Pass `pr_type` and `affected_crates` so agents know whether to compile.

**Performance rules for agents:**

- **Non-Rust PRs** (HTML, CSS, JS, docs, K8s manifests, e2e tests): skip ALL cargo
  commands. Review the diff, post comments, fix issues, commit. Should complete in < 2 min.
- **Rust PRs**: use `CARGO_TARGET_DIR` pointing to the main repo's target dir to share
  cached artifacts. Only compile affected crates, not the full workspace.
- **Targeted compilation**: use `cargo clippy -p <crate>` not `cargo clippy --workspace`.
  Only fall back to `--workspace` if the PR touches `canon-core` or root `Cargo.toml`.
- **Targeted testing**: use `cargo test -p <crate>` not `cargo test --workspace`.
  Only run `--workspace` tests if the PR touches `canon-core`.
- **Never retry a full workspace build**. If a specific crate fails, fix it and recheck
  that crate only. Run `--workspace` exactly once at the end as a final gate.
- **LSP is optional for review agents**. Reading the diff + `cargo clippy` output is
  sufficient for most reviews. Only use LSP for ambiguous type questions.

For each PR in the work list, spawn an Agent with `isolation: "worktree"` and this prompt
template (with variables filled in):

```
You are a Canon PR review, fix, and health agent for PR #<number> (<title>).
Branch: <branch>
Current HEAD: <sha>
Review reason: <reason>
Has merge conflict: <has_conflict>
Failing CI checks: <failing_checks>
PR type: <pr_type>
Affected crates: <affected_crates>

Your job: READ → MERGE-FIX → CI-FIX → REVIEW → COMMENT → FIX.

## Performance constraints

<IF pr_type == "non-rust">
This PR has NO Rust changes. Do NOT run cargo check, cargo clippy, or cargo test.
Skip PHASE M (merge) and PHASE I (CI) entirely unless there are merge conflicts.
Go straight to PHASE V (review), PHASE C (comment), PHASE F (fix).
</IF>

<IF pr_type == "rust">
Share the build cache: export CARGO_TARGET_DIR=/path/to/main/repo/target
Only compile affected crates:
  cargo clippy -p <crate1> -p <crate2> -- -D warnings
  cargo test -p <crate1> -p <crate2>
Only use --workspace if the PR touches canon-core or root Cargo.toml.
Never run cargo check + cargo clippy + cargo test sequentially on --workspace.
Instead: cargo clippy (which includes check) then cargo test. Two commands, not three.
</IF>

Budget: complete in under 25 tool uses. If you're at 20 tool uses and not done,
post what you have and stop.

## PHASE R — Read context

Read CLAUDE.md, fetch PR diff, read existing comments. Identify new vs already-raised issues.

## PHASE M — Fix merge conflicts (skip for non-rust if no conflicts)

If has_conflict: rebase onto main, resolve conflicts per CLAUDE.md rules.
If no conflicts: skip entirely.

## PHASE I — Fix CI failures (skip for non-rust)

<IF pr_type == "rust">
Run cargo clippy on ONLY the affected crates:
  cargo clippy -p <affected_crate> -- -D warnings 2>&1 | tail -20
Fix any warnings. Then test ONLY the affected crates:
  cargo test -p <affected_crate> 2>&1 | tail -20
Only if the PR touches canon-core, run --workspace as a final check.
</IF>

<IF pr_type == "non-rust">
Skip this phase entirely.
</IF>

## PHASE V — Review the code

Read the diff via `gh pr diff <number>`. Review against Canon rules from CLAUDE.md.

## PHASE C — Post GitHub comments

Post a summary comment with severity counts. Post inline comments for new issues only.

## PHASE F — Fix ALL issues and commit

Fix every issue, commit, push with --force-with-lease. Post fix commit comment.
```

---

## Phase 2 — Collect and print results

After all agents complete, print a summary table:

```
| PR | Title | Type | Issues | Fixed | Time |
```

---

## Re-review behaviour

When `/review-prs` is run again on a PR that has already been reviewed:

- **Same HEAD SHA + healthy (no conflicts, no CI failures):** Agent is skipped entirely — "no new commits, healthy".
- **Same HEAD SHA but has conflicts or CI failures:** Agent runs to fix conflicts/CI even though code hasn't changed.
- **New HEAD SHA:** Agent runs but reads existing comments first. It will:
  - Fix any merge conflicts with main
  - Fix any CI failures
  - Skip any issue that already has a comment (whether fixed or not)
  - Only raise issues that are genuinely new in the changed code
  - Note in its summary how many previously-raised issues are still open vs. resolved
  - Post a new top-level summary with the new SHA so future runs track correctly

This means: **comments accumulate on the PR over time but are never duplicated.**
Each review pass is scoped to what's new since the last reviewed SHA.

---

## Conflict resolution reference

These are the specific conflict patterns in this Canon workspace:

### Root `Cargo.toml` — workspace members
Each PR adds one crate to the `members` array. When PRs diverge from the same base,
the members list conflicts. Resolution: **include all entries, alphabetically sorted
within each group** (core -> trait crates -> impl crates -> test -> demo).

Canonical member order for impl crates:
```toml
"canon-adaptor-kafka",
"canon-command-store-yugabyte",
"canon-event-store-cassandra",
"canon-inbox-yugabyte",
"canon-inbound-queue-kafka",
"canon-outbound-queue-kafka",
"canon-projection-store-yugabyte",
"canon-publisher-kafka",
"canon-queue-rabbitmq",
"canon-snapshot-store-yugabyte",
```

### `.github/workflows/ci.yml` — system dependencies
Kafka PRs require `libcurl4-openssl-dev`. The canonical ci.yml always includes it.
Never have two versions of ci.yml on different branches — the version with
`libcurl4-openssl-dev` is always correct.

### `canon-core/src/types.rs` — `Version::from_u64`
Multiple PRs may add this method. The correct resolution is exactly one copy
in the `impl Version` block, immediately after `as_u64`.

### `Cargo.lock`
Never manually resolve. Delete and regenerate after fixing `Cargo.toml`.

### `README.md` / `CLAUDE.md`
Documentation files. Read both sides, merge the content manually —
keep all new sections from both sides. Never drop content added by either side.
