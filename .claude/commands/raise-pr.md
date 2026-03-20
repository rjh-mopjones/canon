# raise-pr

Implement a GitHub issue and open a pull request — always in a git worktree.

**Usage:** `/raise-pr $ARGUMENTS`
- `/raise-pr 42`
- `/raise-pr https://github.com/rory/canon/issues/42`

## Step 1 — Fetch the issue

Use `gh` to load the full issue, including body, labels, comments, and any linked issues.

```bash
# From a number
gh issue view $ARGUMENTS --json number,title,body,labels,comments,assignees

# From a URL — extract the number first, then run the same command
```

Read every comment. Later comments often clarify, contradict, or supersede the original description. The most recent maintainer comment wins.

If the issue references other issues ("blocked by #38", "see also #12"), fetch those too.

## Step 2 — Classify the issue

Determine the type from labels or content:

| Label / signal | Type | Branch prefix |
|---|---|---|
| `bug` | Fix broken behaviour | `fix/` |
| `feature` / `enhancement` | New capability | `feat/` |
| `refactor` | Internal restructure, no behaviour change | `refactor/` |
| `test` | Tests only | `test/` |
| `chore` / `docs` | Housekeeping | `chore/` |

If unlabelled, infer from the title verb: "Add…" → feat, "Fix…" → fix, "Extract…" → refactor.

## Step 3 — Understand the codebase before touching it

- Read `CLAUDE.md` — non-negotiable rules take precedence over everything
- Find every file relevant to the issue using `rg` / `fd`, not guessing
- Read the tests covering the affected area — they define the existing contract
- Understand conventions in play: error handling (`thiserror`, no `anyhow`), module layout, naming
- Confirm the dependency graph — never violate the strict DAG in `CLAUDE.md`
- Check if a related crate has an in-memory impl that also needs updating

## Step 4 — Identify the minimal change

Write a brief internal plan:
- What is the exact acceptance criterion?
- What is the smallest change that satisfies it without scope creep?
- Are there edge cases the issue doesn't mention but clearly implies?
- Does this touch a public trait? If so, every impl crate is affected.

If the issue is ambiguous or contradictory, **state your assumption explicitly** — it will go in the PR body. Never silently pick one interpretation.

## Step 5 — Check for existing work

```bash
gh pr list --search "closes #<NUMBER>" --state open
git branch -r | grep issue-<NUMBER>
```

If a branch exists, check it out and continue from there rather than creating a duplicate.

## Step 6 — Create a worktree and branch

**Always use a git worktree.** This isolates the implementation from the main working directory and prevents interference with other in-progress work.

```bash
# Ensure main is up to date
git fetch origin

# Create the worktree with a new branch
BRANCH_NAME="issue-<NUMBER>/<short-slug>"
WORKTREE_PATH=".claude/worktrees/issue-<NUMBER>"
git worktree add -b "$BRANCH_NAME" "$WORKTREE_PATH" origin/main
```

Slug: kebab-case, ≤5 words, imperative verb first — e.g. `issue-42/add-snapshot-trigger`, `issue-17/fix-inbox-idempotency`.

**All subsequent steps (7–11) run inside the worktree directory:**

```bash
cd "$WORKTREE_PATH"
```

## Step 7 — Implement

- No `unwrap()` / `expect()` in library code — propagate errors
- No `clone()` to dodge the borrow checker without flagging it in the PR
- No new dependencies without listing them explicitly in the PR body
- No `// TODO` — implement it or surface it as a follow-up issue
- No behaviour changes in a `refactor` PR; no refactoring in a `fix` PR
- If touching a trait, update every impl: real impl **and** the in-memory impl in `canon-core`
- If adding a new proc-macro attribute or variant, update exhaustiveness checks

## Step 8 — Verify

```bash
cargo check --workspace
cargo test --workspace
cargo clippy --workspace -- -D warnings
cargo fmt --check
```

All four must pass before opening the PR. If new behaviour is introduced, add tests that would have caught the bug or validated the feature. Tests go in the same crate as the code they test, or in `canon-test` for cross-crate integration scenarios.

## Step 9 — Commit

```bash
git add -p   # stage hunks deliberately, not git add .
git commit -m "<type>(<scope>): <imperative summary under 72 chars>

Closes #<NUMBER>

<optional body: why this approach, trade-offs, anything non-obvious>"
```

- Scope is the crate or subsystem: `canon-core`, `inbox`, `outbox-processor`, `macros`, etc.
- Summary is imperative mood: "add", "fix", "extract" — not "added", "fixes"
- Body explains **why**, not what — the diff already shows what

## Step 10 — Push and open the PR

```bash
git push origin "$BRANCH_NAME"

gh pr create \
  --title "<same as commit subject>" \
  --body "$(cat <<'EOF'
## Summary
<one paragraph: what changed and why — written for a reviewer who hasn't read the issue>

## Changes
- <one bullet per logical change, crate-scoped where helpful>

## Testing
- `cargo test --workspace` passes
- <any specific test added or scenario manually verified>

## Assumptions
<state any assumptions made where the issue was ambiguous — delete if none>

## Follow-up
<out-of-scope work this surfaced that should become new issues — delete if none>

Closes #<NUMBER>
EOF
)" \
  --base main
```

## Step 11 — Self-review and clean up

```bash
gh pr diff
```

Read every line:
- No `dbg!`, no stray `println!`, no debug output
- No unrelated formatting changes mixed in
- No accidental `Cargo.lock` churn from switching toolchains
- PR title matches the commit subject exactly
- `Closes #<NUMBER>` is in the PR body

**Clean up the worktree** after the PR is opened:

```bash
cd /Users/roryhedderman/Documents/IdeaProjects/Rust/canon
git worktree remove "$WORKTREE_PATH"
```

If you need to keep working on the PR later, leave the worktree in place — it can be re-entered with `cd "$WORKTREE_PATH"`.

---

## When stuck

1. Re-read `CLAUDE.md` — the answer is almost always there
2. Check the trait definition — the signature is the contract, do not change it
3. Check the dependency graph — wrong dependency = wrong approach
4. **Ask rather than invent** — surface the blocker in the PR description; never silently resolve ambiguity
