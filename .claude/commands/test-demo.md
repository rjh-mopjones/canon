# test-demo — Canon demo end-to-end browser test

You are verifying that the Canon demo ("canon-demo") works end-to-end in the browser. Follow every step below precisely. Do NOT skip steps or assume anything is healthy without checking.

## Phase 1 — Infrastructure

1. Start the backing infrastructure via Docker Compose:
   ```
   docker compose up -d
   ```
2. Wait for all containers to report healthy. Run `docker compose ps` in a loop (max 60s, poll every 5s) until PostgreSQL, Cassandra, RabbitMQ, and Kafka all show "healthy" or "running". If any container is stuck or restarting after 60s, report the failure and stop.
3. Verify connectivity:
   - `pg_isready` or a test query against PostgreSQL
   - A quick `cqlsh -e "DESCRIBE KEYSPACES"` against Cassandra (retry up to 30s — Cassandra is slow to boot)
   - `rabbitmqctl status` or the management API health endpoint
   - `kafka-topics.sh --bootstrap-server localhost:9092 --list` or equivalent

If any infra check fails, print the docker logs for that container and stop.

## Phase 2 — Build & Launch Services

4. Build the entire workspace in release mode:
   ```
   cargo build --release
   ```
   If the build fails, report the compiler errors and stop.

5. Launch all 5 domain services in the background, capturing their PIDs and log files:
   - fleet-service
   - cargo-service
   - navigation-service
   - station-service
   - supply-service

   For each service, run something like:
   ```
   ./target/release/<service-name> > /tmp/<service-name>.log 2>&1 &
   echo $!
   ```
   Store every PID so you can clean up later.

6. Launch the **gateway** (axum REST + WebSocket):
   ```
   ./target/release/canon-demo-gateway > /tmp/gateway.log 2>&1 &
   ```

7. Build and serve the **Leptos WASM frontend**. This may use `trunk serve`, `leptos watch`, or a similar tool — check the canon-demo frontend crate's README, Makefile, or Trunk.toml for the correct command. Launch it in the background and store the PID.

8. Wait for all processes to be listening:
   - Poll the gateway health endpoint (e.g. `curl -sf http://localhost:3000/health` or whatever port is configured) — retry up to 30s.
   - Poll the frontend dev server (e.g. `curl -sf http://localhost:8080` or the configured port) — retry up to 30s.
   - If any service process has exited, cat its log file, report the failure, and skip to cleanup.

## Phase 3 — Functional Smoke Tests (API level)

9. Run a quick API-level smoke test through the gateway before touching the browser. The exact endpoints depend on the gateway's routes, so inspect the gateway code first (look for axum Router definitions). Then:
   a. **Create a Ship** — POST to the fleet endpoint. Expect 200/201/202.
   b. **Create a Manifest** — POST to the cargo endpoint for that ship. Expect success.
   c. **Query Station Inventory** — GET the station/inventory projection endpoint. Expect 200.
   d. If any of these fail, print the response body and the relevant service log tail, then skip to cleanup.

## Phase 4 — Visual Browser Verification

10. Take a **screenshot** of the frontend landing page:
    ```
    npx playwright screenshot http://localhost:8080 /tmp/canon-landing.png --wait-for-timeout=3000
    ```
    (Install playwright first if needed: `npx playwright install chromium`)

11. **Look at the screenshot yourself.** Describe what you see. Verify:
    - The page renders (not blank, not an error page, not a Trunk/Vite "compiling" spinner stuck forever).
    - There is a meaningful UI — navigation, headings, or dashboard content related to the spaceship logistics demo.
    - No giant red error banners or stack traces visible in the page.

12. Interact with the UI to trigger a real flow:
    - Use Playwright to click "Create Ship" (or whatever the primary action button is). Take a screenshot after.
    - Use Playwright to navigate to the inventory/station view. Take a screenshot.
    - **Look at each screenshot.** Confirm the UI updated — a new ship appeared in a list, inventory data is visible, etc.

    If you can't determine the right selectors, open the page source via `curl` and inspect the HTML structure, or use Playwright's `page.content()` equivalent.

13. Check the **WebSocket** connection if the frontend uses one:
    - Use `websocat ws://localhost:3000/ws` (or the correct WS path) to confirm the handshake succeeds.
    - If events are flowing, you should see JSON messages. Print the first one or two.

## Phase 5 — Verdict

14. Summarise your findings:
    - Infra status: pass or fail per component
    - Build: pass or fail
    - Services running: pass or fail per service
    - API smoke tests: pass or fail per test
    - Frontend renders: pass or fail (with description of what you saw)
    - UI interaction works: pass or fail
    - WebSocket: pass or fail (or N/A)

    Give an overall **PASS** or **FAIL** with a clear explanation of what broke if anything did.

## Phase 6 — Issue Filing

15. If the overall verdict is **FAIL**, or if any individual check failed:

    a. Compile a list of **distinct issues** found. Each issue should have:
       - A clear, concise **title** (e.g. "cargo-service panics on startup: missing CASSANDRA_CONTACT_POINTS env var")
       - The **phase** where it was detected
       - **Reproduction steps** (the exact command that failed and what happened)
       - **Relevant logs** (truncated to the key error — not the full log dump)
       - A suggested **label** (e.g. `bug`, `infra`, `frontend`, `dx`)

    b. Present the full list of issues to the user in a numbered summary. For example:

       ```
       Found 2 issues:

       1. [bug][infra] Cassandra container never reaches healthy — times out after 60s
          -> docker compose logs cassandra shows: "CommitLog.java — Exiting due to error"

       2. [bug][frontend] Landing page renders blank white screen
          -> Screenshot shows empty body, browser console (via Playwright) logs: "TypeError: Cannot read properties of undefined (reading 'ship_list')"
       ```

    c. **Ask the user**: "Would you like me to create GitHub issues for any or all of these? (yes all / pick numbers / no)"

    d. If the user says **yes all** or picks specific numbers:
       - For each selected issue, run:
         ```
         gh issue create \
           --title "<issue title>" \
           --body "<formatted body with reproduction steps, logs, phase, and environment context>" \
           --label "<label>"
         ```
       - The issue body should follow this template:
         ```
         ## Summary
         <one-line description>

         ## Detected During
         E2E verification — Phase <N> (<phase name>)

         ## Steps to Reproduce
         1. `docker compose up -d`
         2. `cargo build --release`
         3. <specific command or action that failed>

         ## Observed Behaviour
         <what happened, with key log lines>

         ## Expected Behaviour
         <what should have happened>

         ## Environment
         - OS: <detect via uname>
         - Rust: <detect via rustc --version>
         - Docker: <detect via docker --version>

         ## Logs
         <truncated relevant logs, max ~50 lines>
         ```
       - After creating each issue, print the issue URL so the user can see it.
       - If `gh` CLI is not authenticated or not installed, tell the user and offer to print the issue bodies so they can file them manually.

    e. If the user says **no**, acknowledge and move on to cleanup.

16. If the overall verdict is **PASS**, congratulate the user and skip issue filing.

## Phase 7 — Cleanup

17. Always run cleanup, even if earlier phases failed, and regardless of issue-filing outcome:
    - Kill all PIDs you stored (services, gateway, frontend dev server).
    - `docker compose down` to tear down infra containers.
    - Print "Cleanup complete."

## Important Notes

- Adapt binary names, ports, and endpoints to whatever you find in the actual code. The names above (fleet-service, port 3000, etc.) are best guesses — read Cargo.toml workspace members, gateway router code, and docker-compose.yml to get the real values.
- If `trunk` or `leptos` CLI tooling isn't installed, install it first (`cargo install trunk` or `cargo install cargo-leptos`).
- If `npx playwright` isn't available, install Node.js tooling as needed. Prefer Playwright for screenshots because it handles WASM apps well.
- You have full autonomy to read any file in the repo to figure out the correct commands, ports, and endpoints. Do not guess when you can grep.
