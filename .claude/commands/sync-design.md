# sync-design — Canon design propagation agent

You are the Canon design sync orchestrator. A design change has been described below.
Your job is to propagate it across CLAUDE.md, every crate README, open GitHub issues,
and then raise a PR — using a multi-agent swarm for the parallel work.

---

## DESIGN CHANGE

<!-- PASTE DESIGN CHANGE HERE -->

---

## Phase 1 — Update CLAUDE.md (do this yourself, do not delegate)

CLAUDE.md is the single authoritative design document. All other docs derive from it.

1. Read CLAUDE.md in full:
   ```bash
   cat CLAUDE.md
   ```

2. Make surgical edits. Only touch sections the design change affects. Do not rewrite
   unaffected sections. Apply changes directly with str_replace or a heredoc patch.

3. Verify the result:
   ```bash
   cat CLAUDE.md
   ```

4. Capture the diff for use by downstream agents:
   ```bash
   git diff CLAUDE.md > /tmp/design_change.diff
   cat /tmp/design_change.diff
   ```

Do not proceed to Phase 2 until CLAUDE.md is updated, verified, and the diff is saved.

---

## Phase 2 — Spawn all agents in parallel

Spawn every agent below simultaneously. Pass each agent its prompt via stdin.
Background all of them with & and collect results with wait.

The full list of crates that may have READMEs to update:

  canon-core
  canon-event-store
  canon-event-store-cassandra
  canon-command-store
  canon-command-store-yugabyte
  canon-snapshot-store
  canon-snapshot-store-yugabyte
  canon-inbox
  canon-inbox-yugabyte
  canon-inbound-queue
  canon-inbound-queue-kafka
  canon-outbound-queue
  canon-outbound-queue-kafka
  canon-projection-store
  canon-projection-store-yugabyte
  canon-publisher
  canon-publisher-kafka
  canon-adaptor
  canon-adaptor-kafka
  canon-deadletter
  canon-deadletter-yugabyte
  canon-test
  canon-demo/shared
  canon-demo/fleet-service
  canon-demo/cargo-service
  canon-demo/navigation-service
  canon-demo/supply-service
  canon-demo/station-service
  canon-demo/gateway
  canon-demo/frontend


### Agent A — README updater (one per crate, all in parallel)

Spawn one agent per crate. Each agent independently decides whether the design
change is applicable to its crate before making any edits.

Template prompt — substitute CRATE_PATH and CRATE_NAME for each crate:

---
You are updating the README for the Canon crate at CRATE_PATH.

A design change has been made. Read the diff to understand what changed:

$(cat /tmp/design_change.diff)

Read the current CLAUDE.md for full authoritative context:

$(cat CLAUDE.md)

Read the current README for this crate (if it exists):

$(cat CRATE_PATH/README.md 2>/dev/null || echo "NO README EXISTS")

Your task:

1. DECIDE: Is this design change relevant to CRATE_NAME?

   Ask yourself:
   - Does this crate implement, depend on, or document anything the diff touches?
   - Would a developer reading this crate's README be misled by the old design?
   - Is this crate mentioned in the diff?

   If the answer to all three is NO — print "CRATE_NAME: not applicable" and exit.
   Do not create or modify any file.

2. If relevant — make bespoke edits to CRATE_PATH/README.md that are specific to
   this crate's role in the changed design. Do not copy-paste generic descriptions
   from CLAUDE.md verbatim. Write from the perspective of a developer working
   inside this specific crate.

   If no README exists and the change is relevant, create a focused one covering
   only what a developer needs to know about this crate in light of the change.

3. Print a one-line summary: "CRATE_NAME: updated — <what changed>"
---

Spawn all crate agents in parallel:

```bash
CRATES=(
  "canon-core"
  "canon-event-store"
  "canon-event-store-cassandra"
  "canon-command-store"
  "canon-command-store-yugabyte"
  "canon-snapshot-store"
  "canon-snapshot-store-yugabyte"
  "canon-inbox"
  "canon-inbox-yugabyte"
  "canon-inbound-queue"
  "canon-inbound-queue-kafka"
  "canon-outbound-queue"
  "canon-outbound-queue-kafka"
  "canon-projection-store"
  "canon-projection-store-yugabyte"
  "canon-publisher"
  "canon-publisher-kafka"
  "canon-adaptor"
  "canon-adaptor-kafka"
  "canon-deadletter"
  "canon-deadletter-yugabyte"
  "canon-test"
  "canon-demo/shared"
  "canon-demo/fleet-service"
  "canon-demo/cargo-service"
  "canon-demo/navigation-service"
  "canon-demo/supply-service"
  "canon-demo/station-service"
  "canon-demo/gateway"
  "canon-demo/frontend"
)

for CRATE in "${CRATES[@]}"; do
  PROMPT="You are updating the README for the Canon crate at ${CRATE}.

A design change has been made. Read the diff to understand what changed:

$(cat /tmp/design_change.diff)

Read the current CLAUDE.md for full authoritative context:

$(cat CLAUDE.md)

Read the current README for this crate (if it exists):

$(cat ${CRATE}/README.md 2>/dev/null || echo 'NO README EXISTS')

Your task:

1. DECIDE: Is this design change relevant to ${CRATE}?

   Ask yourself:
   - Does this crate implement, depend on, or document anything the diff touches?
   - Would a developer reading this crate README be misled by the old design?
   - Is this crate mentioned in the diff?

   If the answer to all three is NO — print '${CRATE}: not applicable' and exit.
   Do not create or modify any file.

2. If relevant — make bespoke edits to ${CRATE}/README.md specific to this crate's
   role in the changed design. Do not copy-paste from CLAUDE.md verbatim. Write from
   the perspective of a developer working inside this specific crate.

   If no README exists and the change is relevant, create a focused one covering only
   what a developer needs to know about this crate in light of the change.

3. Print a one-line summary: '${CRATE}: updated — <what changed>'"

  echo "$PROMPT" | claude --print > /tmp/readme_agent_$(echo $CRATE | tr '/' '_').log 2>&1 &
done
```


### Agent B — GitHub issues and PRs updater (spawn in parallel with README agents)

```bash
ISSUE_PR_PROMPT="You are reviewing all open GitHub issues and pull requests for the Canon
project and posting bespoke comments where a design change affects them.

The design change diff:

$(cat /tmp/design_change.diff)

The authoritative updated design (CLAUDE.md):

$(cat CLAUDE.md)

Your tasks:

1. Fetch all open issues:
   gh issue list --state open --limit 100 --json number,title,body,labels

2. Fetch all open PRs, including their descriptions and the diff of changed files:
   gh pr list --state open --limit 100 --json number,title,body,labels,files

   For each open PR, also fetch the full diff so you can see what it is implementing:
   gh pr diff <number>

3. For each issue, decide whether the design change affects it. Ask:
   - Does the issue implement, reference, or depend on anything touched by the diff?
   - Would an implementer picking up this issue be working from a wrong assumption?

4. For each open PR, decide whether the design change affects it. Ask:
   - Does the PR implement or touch anything the diff changes?
   - Would the code in this PR be wrong or incomplete given the new design?
   - Does the PR description reference concepts that have changed?

5. For each affected issue, post a bespoke comment that:
   - States what changed and why
   - Explains specifically how it affects the work described in that issue
   - Provides the updated spec or constraints the implementer needs
   - Does NOT just paste the diff — write from the perspective of the issue

   gh issue comment <number> --body '<your bespoke comment>'

6. For each affected PR, post a bespoke review comment that:
   - States what design change has landed
   - Explains specifically what in this PR needs to change as a result
   - Calls out the exact files or functions that are affected if identifiable
   - Is written from the perspective of a reviewer who wants the PR to land correctly
   - Does NOT just paste the diff — be concrete about what the author needs to do

   gh pr comment <number> --body '<your bespoke comment>'

7. Print a one-line summary for every issue and PR processed:
   - 'Issue #N (<title>): commented — <reason>'
   - 'Issue #N (<title>): not applicable — <reason>'
   - 'PR #N (<title>): commented — <reason>'
   - 'PR #N (<title>): not applicable — <reason>'"

echo "$ISSUE_PR_PROMPT" | claude --print > /tmp/issue_pr_agent.log 2>&1 &
```

---

## Phase 3 — Raise a sync-design PR (do this yourself after all agents complete)

Wait for all agents:
```bash
wait
```

Print all agent logs to review what was done:
```bash
for f in /tmp/readme_agent_*.log /tmp/issue_pr_agent.log; do
  echo "=== $f ==="
  cat "$f"
done
```

Then commit and raise the PR:
```bash
BRANCH="sync-design/$(date +%Y%m%d-%H%M%S)"
git checkout -b "$BRANCH"
git add -A
git commit -m "design sync: <one line summary of the change>"

git push origin "$BRANCH"

CHANGED_FILES=$(git diff HEAD~1 --name-only | sed 's/^/- /')
ISSUE_SUMMARY=$(cat /tmp/issue_pr_agent.log | grep -E "^Issue #")
PR_SUMMARY=$(cat /tmp/issue_pr_agent.log | grep -E "^PR #")

gh pr create \
  --title "design sync: <one line summary>" \
  --body "## What changed

$(cat /tmp/design_change.diff)

## Files updated

$CHANGED_FILES

## Issues commented

$ISSUE_SUMMARY

## PRs commented

$PR_SUMMARY" \
  --label "design-sync"
```

---

## Rules for all agents

- **CLAUDE.md is truth.** When in doubt about the design, read CLAUDE.md, not the old README.
- **Bespoke updates only.** Never copy-paste from CLAUDE.md verbatim into a crate README.
  Write from the perspective of someone working inside that specific crate.
- **Surgical edits.** Do not rewrite sections unaffected by the change.
- **No Rust source changes.** Documentation only. Implementation work belongs in issues.
- **If a README does not exist and the crate is not affected — do not create one.**
