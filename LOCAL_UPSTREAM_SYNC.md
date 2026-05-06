# Local Upstream Sync Protocol

This repo carries a local Warp OSS runtime patch set that is intentionally not
upstream-compatible yet. Use this protocol whenever syncing with
`warpdotdev/warp`.

## Goals

- Keep the local Pi agent runtime isolated from upstream changes whenever
  possible.
- Preserve a small, reviewable local commit stack on top of upstream `master`.
- Avoid resolving the same merge conflicts from scratch on every sync.
- Verify the local runtime before treating a sync as complete.

## Local Patch Set

Current local commits are expected to sit on top of upstream:

1. `Add Pi local agent runtime`
   - Adds `app/src/ai/agent/api/pi_local.rs`.
   - Routes Agent Mode to Pi when `WARP_AGENT_RUNTIME=pi_local`.
   - Adds OSS no-auth/test-credentials bypass.
   - Adds the first Pi runtime docs and tool gate.
   - Adjusts terminal sizing for active CLI agent input.
2. `Stream Pi local agent output`
   - Streams Pi assistant deltas into Warp messages.
   - Streams Pi read-only tool activity.
3. `Expand Pi local runtime bridge`
   - Adds richer Warp context serialization.
   - Adds `ActionResult` continuation prompts.
   - Adds safe Pi extension UI fallback handling.
4. `Proxy Pi bash through Warp actions`
   - Converts Pi `bash` tool starts into native Warp `RunShellCommand` actions.
   - Keeps `edit`/`write` blocked until they have native Warp action bridges.

Treat these commits as the local feature stack. Prefer keeping new local work in
small commits after these, instead of squashing everything into one large patch.

## Remotes

Recommended remote layout:

```powershell
git remote -v
```

If `origin` points directly to `https://github.com/warpdotdev/warp.git`, then
`origin` is upstream. If a personal fork is added later, use:

```powershell
git remote rename origin upstream
git remote add origin <your-fork-url>
git fetch upstream
```

The rest of this document assumes the upstream remote is named `origin` unless
you rename it to `upstream`.

## Sync Workflow

Start clean:

```powershell
git status --short
```

If there are local modifications, commit or stash them before syncing. Do not
start a sync with unrelated dirty files.

Fetch upstream:

```powershell
git fetch origin
```

Create a safety branch before rewriting or merging:

```powershell
git branch backup/local-pi-before-sync
```

Preferred approach: rebase the local patch stack onto upstream `master`.

```powershell
git rebase origin/master
```

Use merge only if the rebase becomes too noisy or if preserving exact local
history matters more than a clean stack:

```powershell
git merge origin/master
```

## Conflict Priorities

Resolve conflicts in this order:

1. Agent API routing
   - Preserve the `pi_local` module and the early runtime route in
     `app/src/ai/agent/api.rs` / `app/src/ai/agent/api/impl.rs`.
   - If upstream changes request/response stream types, adapt `pi_local.rs` to
     upstream rather than preserving old adapter assumptions.
2. Agent action pipeline
   - Preserve native Warp execution for proxied `bash`.
   - Do not emit display-only `ToolCall` messages for Pi read-only tools,
     because Warp queues tool calls for execution after stream finish.
3. Auth and OSS startup
   - Preserve OSS no-auth behavior only for `Channel::Oss` / local test
     credentials.
   - Avoid weakening auth paths for non-OSS channels.
4. Terminal layout
   - Preserve terminal-size correction for active CLI agent input, but prefer
     upstream layout APIs if they changed.
5. Pi runtime resources
   - Preserve `app/resources/pi-runtime/warp-tool-gate.ts`.
   - Keep `bash` allowed only when the Rust action proxy is active.
   - Keep `edit`/`write` blocked until native action bridges exist.

## Validation

Minimum validation after every sync:

```powershell
$env:PROTOC = "$env:APPDATA\npm\protoc.cmd"
cargo test -p warp pi_local --lib --features gui
cargo check -p warp --bin warp-oss --features gui
git diff --check
```

Functional smoke test:

1. Start Warp with `WARP_AGENT_RUNTIME=pi_local`.
2. Send a plain prompt and confirm Pi streams a response.
3. Send a follow-up in the same conversation and confirm the session resumes.
4. Ask it to run a harmless command such as `git status --short`.
5. Confirm Warp renders a native command action card and returns the result.
6. Confirm Pi does not execute `edit` or `write` directly.

## When Upstream Changes Agent Internals

If upstream changes the agent stream, task, action, or conversation model:

- First update the adapter to compile against upstream types.
- Then preserve the behavioral contract:
  - Pi text deltas become Warp agent output messages.
  - Pi read-only tool activity is display-only.
  - Pi `bash` becomes a native Warp action and ends the stream.
  - Warp `ActionResult` is serialized back to the Pi session.
- Add or update tests in `pi_local.rs` for any changed contract.

## Commit Rules

After a successful sync:

```powershell
git status --short
git log --oneline --decorate -8
```

Commit conflict resolutions separately from new feature work. Use a message
like:

```text
Sync local Pi runtime with upstream
```

Do not mix unrelated experiments into a sync commit.

## Current Runtime Defaults

The desktop launcher should set:

```powershell
WARP_AGENT_RUNTIME=pi_local
WARP_PI_COMMAND=pi.cmd
WARP_PI_PROVIDER=openai-codex
WARP_PI_MODEL=gpt-5.5
WARP_PI_THINKING=high
WARP_PI_TOOLS=read,grep,find,ls,bash
```

Use `WARP_PI_DISABLE_ACTION_PROXY=1` together with
`WARP_PI_ALLOW_UNBRIDGED_MUTATING_TOOLS=1` only for deliberate raw Pi testing.
