# Research Overnight

Launch both Canon research agents in parallel and run them until quota is exhausted. Produces two deep research documents by morning.

## What this runs

| Agent | Command | Output |
|---|---|---|
| Stress Test | `/stress-test-canon` | `canon-stress-test.md` |
| Testing Story | `/canon-testing-story` | `canon-testing-story.md` |

Both agents are independent and run simultaneously. Each iterates in a deepening loop — they will not stop voluntarily.

## Steps

### 1. Confirm the workspace is readable

Check that `CLAUDE.md` and `canon-design.md` exist at the workspace root. If either is missing, abort and tell the user — the agents depend on them as their primary source material.

### 2. Create a branch and worktree for the research output

```bash
BRANCH="research/overnight-$(date '+%Y-%m-%d')"
WORKTREE_PATH="../canon-research-overnight"

# Create the branch off current HEAD
git branch "$BRANCH"

# Create a worktree pointing at that branch
git worktree add "$WORKTREE_PATH" "$BRANCH"

echo "Worktree created at $WORKTREE_PATH on branch $BRANCH"
```

If the worktree or branch already exists (re-running after a crash), reuse them — do not abort.

Copy the command files into the worktree so agents can read them:
```bash
cp -r .claude "$WORKTREE_PATH/"
```

### 3. Launch both agents in parallel inside the worktree

Both agents must run with the worktree as their working directory so output documents are written to the branch, not the main tree.

**Agent 1 — Stress Test:**
```bash
(cd "$WORKTREE_PATH" && claude --dangerously-skip-permissions \
  "$(cat .claude/commands/stress-test-canon.md)" \
  > /tmp/stress-test-canon.log 2>&1) &
STRESS_PID=$!
echo "Stress test agent launched (PID $STRESS_PID)"
```

**Agent 2 — Testing Story:**
```bash
(cd "$WORKTREE_PATH" && claude --dangerously-skip-permissions \
  "$(cat .claude/commands/canon-testing-story.md)" \
  > /tmp/canon-testing-story.log 2>&1) &
TESTING_PID=$!
echo "Testing story agent launched (PID $TESTING_PID)"
```

### 4. Monitor progress

Print the following instructions for the user:

```
Both agents are running on branch: research/overnight-YYYY-MM-DD
Worktree: ../canon-research-overnight

To monitor progress:
  tail -f /tmp/stress-test-canon.log
  tail -f /tmp/canon-testing-story.log

Output documents (updated live):
  ../canon-research-overnight/canon-stress-test.md
  ../canon-research-overnight/canon-testing-story.md

To check agents are still running:
  ps aux | grep claude

To commit progress at any point:
  cd ../canon-research-overnight
  git add canon-stress-test.md canon-testing-story.md
  git commit -m "research: overnight progress checkpoint"

Both agents will run until quota is exhausted or they are killed.
Good night.
```

### 5. Wait

Run `wait $STRESS_PID $TESTING_PID` so this command does not exit immediately. If either agent exits (quota hit or error), report which one finished and its exit code. The other continues running.

### 6. Auto-commit on completion

When each agent finishes, immediately commit its output:

```bash
# Called when stress test agent finishes
(cd "$WORKTREE_PATH" && \
  git add canon-stress-test.md && \
  git commit -m "research: stress test complete — $(grep '### Iteration' canon-stress-test.md | wc -l) iterations")

# Called when testing story agent finishes
(cd "$WORKTREE_PATH" && \
  git add canon-testing-story.md && \
  git commit -m "research: testing story complete — $(grep '### Iteration' canon-testing-story.md | wc -l) iterations")
```

### 7. Final summary

When both agents have finished, print:

```
=== OVERNIGHT RESEARCH COMPLETE ===

Branch:  research/overnight-YYYY-MM-DD
Worktree: ../canon-research-overnight

canon-stress-test.md    — [line count] lines
canon-testing-story.md  — [line count] lines

Stress test iterations completed:    [grep "### Iteration" canon-stress-test.md | wc -l]
Testing story iterations completed:  [grep "### Iteration" canon-testing-story.md | wc -l]
Critical findings (stress test):     [grep "^\### \[CRITICAL\]" canon-stress-test.md | wc -l]

To merge into main when ready:
  git -C ../canon-research-overnight push origin research/overnight-YYYY-MM-DD
  # then open a PR, or:
  git merge research/overnight-YYYY-MM-DD
```
