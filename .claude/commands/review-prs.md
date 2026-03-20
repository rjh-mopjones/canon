# review-prs — Canon PR review and fix agent

You are the Canon PR review orchestrator. Your job is to:

1. Discover all open PRs
2. For each PR, check whether it has already been reviewed by this command
3. Spawn parallel review agents — one per unreviewed (or changed) PR
4. Each agent reviews the PR, posts inline GitHub comments, applies all fixes, and commits

Read `CLAUDE.md` before doing anything else:
```bash
cat CLAUDE.md
```

---

## Phase 0 — Discover open PRs and their review state

```bash
# Get all open PRs with metadata
gh pr list --state open --limit 100 \
  --json number,title,headRefName,headRefOid,body,comments \
  > /tmp/open_prs.json

cat /tmp/open_prs.json
```

For each PR, check whether a `review-prs` bot comment already exists:

```bash
python3 << 'EOF'
import json, subprocess, sys

prs = json.load(open('/tmp/open_prs.json'))
SENTINEL = '<!-- review-prs-bot -->'

needs_review = []
already_reviewed = []

for pr in prs:
    num = pr['number']
    # Fetch all comments on this PR
    result = subprocess.run(
        ['gh', 'pr', 'view', str(num), '--json', 'comments', '--jq', '.comments[].body'],
        capture_output=True, text=True
    )
    comments = result.stdout
    if SENTINEL in comments:
        # Check if HEAD sha has changed since last review
        review_lines = [l for l in comments.split('\n') if 'review-prs-sha:' in l]
        if review_lines:
            last_sha = review_lines[-1].split('review-prs-sha:')[-1].strip()
            current_sha = pr['headRefOid']
            if last_sha == current_sha:
                already_reviewed.append((num, pr['title'], 'no new commits'))
            else:
                needs_review.append((num, pr['title'], pr['headRefName'], current_sha,
                                     f're-review: HEAD changed {last_sha[:7]}→{current_sha[:7]}'))
        else:
            needs_review.append((num, pr['title'], pr['headRefName'], pr['headRefOid'], 'initial review'))
    else:
        needs_review.append((num, pr['title'], pr['headRefName'], pr['headRefOid'], 'initial review'))

print("=== SKIPPING (already reviewed, no new commits) ===")
for num, title, reason in already_reviewed:
    print(f"  PR #{num}: {title} — {reason}")

print("\n=== WILL REVIEW ===")
for num, title, branch, sha, reason in needs_review:
    print(f"  PR #{num}: {title} [{branch}] @ {sha[:7]} — {reason}")

# Write the work list
with open('/tmp/prs_to_review.json', 'w') as f:
    json.dump([
        {'number': num, 'title': title, 'branch': branch, 'sha': sha, 'reason': reason}
        for num, title, branch, sha, reason in needs_review
    ], f, indent=2)

print(f"\n{len(needs_review)} PRs to review, {len(already_reviewed)} skipped.")
EOF
```

If `/tmp/prs_to_review.json` is empty, print "All PRs are up to date." and exit.

---

## Phase 1 — Spawn one review+fix agent per PR (all in parallel)

Read the work list and spawn agents:

```bash
python3 << 'ORCHESTRATOR'
import json, subprocess, os

prs = json.load(open('/tmp/prs_to_review.json'))

AGENT_PROMPT_TEMPLATE = """
You are a Canon PR review and fix agent. You are responsible for PR #{number} ({title}).
Branch: {branch}
Current HEAD: {sha}
Review reason: {reason}

Your job has four phases: READ, REVIEW, COMMENT, FIX.
Complete all four before exiting.

---

## PHASE R — Read context

Read the authoritative project guide:
```bash
cat CLAUDE.md
```

Fetch the PR metadata, existing review comments, and diff:
```bash
# PR description and existing comments
gh pr view {number} --json title,body,comments,files

# Full diff of what this PR changes
gh pr diff {number}

# Check out the branch so you can compile and edit
git fetch origin {branch}
git checkout {branch}
```

Read ALL existing comments on this PR and build a list of issues already raised.
This is critical — do not re-raise issues that are already commented and not yet fixed.

```bash
gh pr view {number} --json comments --jq '.comments[] | "--- \\(.author.login) ---\\n\\(.body)"'
```

Identify:
- Issues already raised and FIXED (comment exists, fix appears in subsequent commits)
- Issues already raised but NOT YET FIXED (comment exists, no fix in code)
- Issues that are NEW (not yet commented on at all)

---

## PHASE V — Review the code

Read every changed file in the PR:
```bash
gh pr diff {number} --name-only | while read f; do
  echo "=== $f ==="
  cat "$f" 2>/dev/null || echo "(deleted)"
done
```

Review against these Canon-specific criteria (in addition to general Rust quality):

**Compile correctness**
- Are all called methods actually defined? (Common: `Version::from_u64`, `IncomingMessage::message_id`)
- Do all error types implement the required `From<>` conversions?
- Are all trait bounds satisfied?

**Canon architecture rules (from CLAUDE.md)**
- `thiserror` in every crate — no `anyhow`, no raw `Box<dyn Error>` without a named type
- `AggregateId(Uuid)` newtype always — never plain `Uuid` at API boundaries
- Impl crates depend on their trait crate + `canon-core` only
- No `unwrap()`/`expect()` in library code
- No business logic in infrastructure crates
- Snapshots triggered by event store consumer after confirmed write, not by command handler
- Outbox pattern: events + command in single YugabyteDB ACID txn
- All event handlers and projections must be idempotent
- READMEs required in every crate

**Infrastructure-specific checks**
- YugabyteDB crates: multi-step operations must be wrapped in `sqlx::Transaction`
- Kafka crates: manual offset commit only, no `enable.auto.commit = true`
- Kafka crates: producer and consumer should be separable (different consumer groups)
- Cassandra: `IF NOT EXISTS` LWT for optimistic concurrency on events table
- All `debug_assert!` guards in library code must be proper `Result` paths in release
- `tracing` dependency must have actual `tracing::` calls — or be removed
- Test helpers annotated `#[allow(dead_code)]` must be removed or used
- Integration tests that skip via `return` must use `#[ignore]` instead
- Migration SQL files must exist for every database-backed crate (not just inline in tests)
- `sqlx` must not appear in both `[dependencies]` and `[dev-dependencies]`

**Cross-cutting (check against all other open PRs)**
- Does this PR define something that other open PRs depend on?
  (e.g. `Version::from_u64` — if this PR adds it, note other PRs will need rebasing)
- Does this PR duplicate a definition that another open PR also adds?

---

## PHASE C — Post GitHub comments

### 3a. Categorise every issue you found

For each issue, determine:
- SEVERITY: 🔴 blocker (won't compile / data corruption) | 🟡 should-fix | 🟢 nice-to-have
- STATUS:
  - `new` — not commented before
  - `unresolved` — already commented, not yet fixed
  - `fixed` — already commented and fixed in a later commit

Only comment on `new` issues. Do not re-raise `unresolved` issues (they already have comments).
Do not comment on `fixed` issues at all.

### 3b. Post inline comments for `new` issues

For each `new` issue, post an inline comment at the exact file and line:

```bash
gh api --method POST /repos/rjh-mopjones/canon/pulls/{number}/reviews \\
  --field commit_id='{sha}' \\
  --field event='COMMENT' \\
  --field body='<!-- review-prs-bot -->\\nreview-prs-sha: {sha}\\n\\n**Review summary:** N new issues found.' \\
  --field 'comments[][path]=<file>' \\
  --field 'comments[][line]=<line>' \\
  --field 'comments[][body]=<severity emoji> **<issue title>**\\n\\n<detailed explanation>\\n\\n<code suggestion if applicable>'
  # Repeat --field pairs for each inline comment in the same API call
```

Rules for comment bodies:
- Start with the severity emoji and a bold title
- Explain WHY it is a problem, not just WHAT it is
- For compile errors: include the exact fix as a code block
- For architecture violations: cite the specific CLAUDE.md rule being violated
- For schema/migration issues: show the corrected SQL
- Keep each comment self-contained — the author must be able to fix it without context

### 3c. Post a top-level summary comment

After all inline comments:

```bash
gh pr comment {number} --body '<!-- review-prs-bot -->
review-prs-sha: {sha}

## Canon automated review

| | Count |
|---|---|
| 🔴 Blockers | N |
| 🟡 Should fix | N |
| 🟢 Nice to have | N |
| ✅ Already resolved | N |
| ⏭️ Previously raised, still open | N |

_New issues are posted as inline comments above._
_Previously raised issues that are still open are not re-commented — see earlier review comments._

<!-- review-prs-bot-end -->'
```

---

## PHASE F — Fix ALL issues and commit

Fix every issue you found — 🔴 blockers, 🟡 should-fix, AND 🟢 nice-to-have. Every single issue raised in the review must be resolved. Apply fixes directly to the checked-out branch.

Work through fixes in dependency order:
1. Fixes to `canon-core` first (type additions, trait changes)
2. Fixes to trait crates second
3. Fixes to impl crates last

After every logical group of fixes:
```bash
cargo check -p <affected-crate> 2>&1 | head -30
```

Fix any errors before continuing.

Once all fixes are applied and `cargo check` is clean:

```bash
git add -A
git commit -m "fix(<crate>): address review comments

- <bullet per fix>
- ...

Fixes raised by /review-prs bot."

git push origin {branch}
```

Then post a follow-up comment linking the fix commit:

```bash
FIX_SHA=$(git rev-parse HEAD)
gh pr comment {number} --body "<!-- review-prs-bot -->
review-prs-sha: $FIX_SHA

## Fix commit

Applied fixes for all issues (🔴, 🟡, and 🟢) from the review above.
Commit: \`$FIX_SHA\`

Changes:
$(git show --stat HEAD | tail -n +2)"
```

---

## Rules for this agent

- **CLAUDE.md is truth.** All architectural judgements defer to it.
- **Do not re-raise already-commented issues** — check existing comments first, always.
- **Re-reviews are incremental.** On a re-review, only comment on issues introduced since the last reviewed SHA, or issues that were raised and are confirmed still present in the current code.
- **One review API call per PR** — batch all inline comments into a single `gh api` call.
- **Fix order matters** — `canon-core` changes before impl crate changes.
- **No `TODO` in fixes** — if you can't fully fix something, explain in the comment why and what the author needs to do.
- **`cargo check` must pass** after your fix commit.
- **Do not touch files outside the PR's changed set** unless a canon-core fix is strictly required and the PR description says it was intended.
"""

for pr in prs:
    prompt = AGENT_PROMPT_TEMPLATE.format(**pr)
    log_file = f"/tmp/review_agent_pr{pr['number']}.log"

    proc = subprocess.Popen(
        ['claude', '--print', '--dangerously-skip-permissions'],
        stdin=subprocess.PIPE,
        stdout=open(log_file, 'w'),
        stderr=subprocess.STDOUT,
        text=True
    )
    proc.stdin.write(prompt)
    proc.stdin.close()

    print(f"Spawned agent for PR #{pr['number']} ({pr['title']}) → {log_file}")

print(f"\nAll {len(prs)} agents running. Waiting for completion...")
ORCHESTRATOR
```

Wait for all agents to complete:
```bash
wait
echo "All review agents finished."
```

---

## Phase 2 — Collect and print results

```bash
python3 << 'EOF'
import json, os, re

prs = json.load(open('/tmp/prs_to_review.json'))

print("=" * 60)
print("REVIEW SUMMARY")
print("=" * 60)

for pr in prs:
    log = f"/tmp/review_agent_pr{pr['number']}.log"
    print(f"\n--- PR #{pr['number']}: {pr['title']} ---")
    if os.path.exists(log):
        content = open(log).read()
        # Print last 40 lines (the summary/result)
        lines = content.strip().split('\n')
        for line in lines[-40:]:
            print(line)
    else:
        print("  (no log found)")

print("\n" + "=" * 60)
print("Done. Check each PR on GitHub for posted comments and fix commits.")
EOF
```

---

## Re-review behaviour

When `/review-prs` is run again on a PR that has already been reviewed:

- **Same HEAD SHA:** Agent is skipped entirely — "no new commits".
- **New HEAD SHA:** Agent runs but reads existing comments first. It will:
  - Skip any issue that already has a comment (whether fixed or not)
  - Only raise issues that are genuinely new in the changed code
  - Note in its summary how many previously-raised issues are still open vs. resolved
  - Post a new top-level summary with the new SHA so future runs track correctly

This means: **comments accumulate on the PR over time but are never duplicated.**
Each review pass is scoped to what's new since the last reviewed SHA.
