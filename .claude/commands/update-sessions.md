# update-sessions — Refresh the Canon session log in Obsidian

Scan all Claude Code session files for this project and update the Obsidian session log.

## Step 1 — Extract all sessions

Run this to extract every session's UUID, timestamp, first user message, and whether it was
an orchestrator (launched by the user via CLI) or a sub-agent (spawned programmatically via SDK):

```bash
cd /Users/roryhedderman/.claude/projects/-Users-roryhedderman-Documents-IdeaProjects-Rust-canon && python3 << 'SCRIPT'
import json, glob
from datetime import datetime

results = []

for f in sorted(glob.glob("*.jsonl")):
    uuid = f.replace(".jsonl", "")
    first_timestamp = None
    entrypoint = None
    all_user_texts = []

    with open(f) as fh:
        for line in fh:
            try:
                obj = json.loads(line)
            except:
                continue

            ts = obj.get("timestamp")
            if ts and first_timestamp is None:
                first_timestamp = ts

            ep = obj.get("entrypoint")
            if ep and entrypoint is None:
                entrypoint = ep

            if obj.get("type") == "user":
                msg = obj.get("message", {})
                content = msg.get("content", "")
                if isinstance(content, list):
                    for c in content:
                        if isinstance(c, dict) and c.get("type") == "text":
                            text = c.get("text", "").strip()
                            if text and not text.startswith("<") and not text.startswith("[") and len(text) > 5:
                                all_user_texts.append(text[:200])
                                break
                    if all_user_texts:
                        continue
                elif isinstance(content, str):
                    text = content.strip()
                    if text and not text.startswith("<") and not text.startswith("[") and len(text) > 5:
                        all_user_texts.append(text[:200])

    if first_timestamp and all_user_texts:
        # entrypoint "cli" = user launched directly (orchestrator)
        # entrypoint "sdk-cli" = spawned by another agent (sub-agent)
        role = "sub-agent" if entrypoint == "sdk-cli" else "orchestrator"
        results.append({
            "uuid": uuid,
            "timestamp": first_timestamp,
            "role": role,
            "first_msg": all_user_texts[0] if all_user_texts else "(no message)"
        })

results.sort(key=lambda x: x["timestamp"])
print(json.dumps(results, indent=2))
SCRIPT
```

## Step 2 — Read the existing session log

```
Read /Users/roryhedderman/Documents/mop-jones-brain/Notes/canon-claude-sessions.md
```

Extract all UUIDs already present in the file so you know which sessions are new.

## Step 3 — Classify each session

For every session from Step 1, determine:

- **Already logged?** — UUID appears in the existing file → skip
- **New?** — UUID not in file → needs a "What we did" summary

For each new session, write a concise "What we did" summary (5–15 words) based on the first user message. Use these patterns:

| First message pattern | Summary style |
|---|---|
| `Read CLAUDE.md … issue #N` | `Issue #N — <crate or feature name>` |
| `You are implementing canon-<name>` | `canon-<name> implementation` |
| `fix conflicts / CI on PR #N` | `Fixed conflicts/CI on PR #N` |
| `/review-prs` or `/fix-prs` or `/work-issues` | `Ran /command-name — <brief outcome>` |
| `/raise-pr #N` or `/raise-pr URL` | `/raise-pr for issue #N (<crate>)` |
| `/sync-design` | `/sync-design — <what changed>` |
| Command/config creation | `Created /command-name command` |
| Multiple parallel agents (same timestamp) | Group them under a heading |

## Step 4 — Update the file

Read the existing file, then edit it to append any new sessions. Rules:

- Preserve all existing content exactly — do not rewrite old entries
- Add new sessions at the bottom, continuing the numbering
- If multiple new sessions share the same timestamp (parallel agents), group them under a `### <descriptive heading>` just like the existing file does
- Keep the same table column format: `| # | UUID | Time (UTC) | Role | What we did |`
- Role column: use a short emoji tag — `🎛️ orch` for orchestrator (user-launched, `entrypoint=cli`) or `🤖 sub` for sub-agent (spawned, `entrypoint=sdk-cli`)
- Use full UUIDs (not truncated)
- Times in `Mon DD HH:MM` UTC format

Write the updated file back to `/Users/roryhedderman/Documents/mop-jones-brain/Notes/canon-claude-sessions.md`.

## Step 5 — Print summary

Print how many sessions were already logged vs newly added, e.g.:

```
Session log updated: 70 existing, 3 new → 73 total.
New sessions added:
  #71  742a9712-...  Mar 20 12:52  Raising PRs for frontend + demo issues
  #72  abcd1234-...  Mar 20 14:10  /work-issues swarm
  #73  efgh5678-...  Mar 20 14:15  Fixed CI on PR #95
```
