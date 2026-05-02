# Pi local agent runtime

This directory documents the first integration point between Warp's in-app agent
UI and the Pi coding agent runtime.

The Rust side does not embed Pi directly. It starts Pi in RPC mode and adapts Pi's
JSONL events into `warp_multi_agent_api::ResponseEvent` values consumed by the
existing Warp UI.

## Enable locally

Install Pi:

```powershell
npm install -g @mariozechner/pi-coding-agent
```

Run Warp with the local runtime:

```powershell
$env:WARP_AGENT_RUNTIME = "pi_local"
cargo run --bin warp-oss --features gui
```

By default, Warp starts Pi with OpenAI Codex OAuth:

```powershell
pi
```

Then type `/login`, select `ChatGPT Plus/Pro (Codex)`, and finish the browser
login.

After login succeeds:

```powershell

$env:WARP_AGENT_RUNTIME = "pi_local"
cargo run --bin warp-oss --features gui
```

Pi stores the OAuth credential in `~/.pi/agent/auth.json` and refreshes it when
needed. This is the path for ChatGPT Plus/Pro Codex access. If login has not
been completed yet, the Warp agent request returns an actionable local-runtime
error instead of falling back to Warp's hosted agent endpoint.

Optional overrides:

```powershell
$env:WARP_PI_COMMAND = "pi.cmd"
$env:WARP_PI_PROVIDER = "openai-codex"
$env:WARP_PI_MODEL = "gpt-5.5"
$env:WARP_PI_THINKING = "high"
$env:WARP_PI_SESSION_DIR = "$env:LOCALAPPDATA\Warp\data\pi-agent-sessions"
$env:WARP_PI_TOOLS = "read,grep,find,ls,bash"
```

If `WARP_PI_SESSION_DIR` is unset, Warp stores Pi session files under its
channel-aware data directory in `pi-agent-sessions`. Each local Warp
conversation gets a deterministic `pi-local-*.jsonl` session file, so normal
follow-up prompts in the same conversation resume the same Pi context.

Pi read-only tools are enabled by default so the local agent can inspect the
workspace without depending on Warp's hosted runtime:

```powershell
$env:WARP_PI_TOOLS = "readonly"
```

`readonly` expands to `read,grep,find,ls`. For the closest Warp-like local UX,
use `read,grep,find,ls,bash`: Pi can inspect files directly, while `bash` tool
calls are proxied into Warp's native `RunShellCommand` action instead of running
inside Pi. `all` expands to `read,grep,find,ls,bash,edit,write`, but the bundled
`warp-tool-gate.ts` extension still blocks `edit`/`write` unless
`WARP_PI_ALLOW_UNBRIDGED_MUTATING_TOOLS=1` is set. This prevents local Pi from
editing files outside Warp's action approval pipeline. Set `WARP_PI_TOOLS` to
`none` to disable Pi tool execution, or set `WARP_PI_DISABLE_TOOL_GATE=1` only
when intentionally testing raw Pi tool behavior.

Set both `WARP_PI_DISABLE_ACTION_PROXY=1` and
`WARP_PI_ALLOW_UNBRIDGED_MUTATING_TOOLS=1` only if you intentionally want Pi's
own local `bash` backend to run without Warp's native approval/action flow.

The adapter also forwards BYO API keys already configured in Warp to Pi as
provider environment variables when those variables are not already set.

## Current scope

The adapter maps a Warp Agent Mode prompt to `pi --mode rpc`, streams assistant
text deltas into the existing Warp agent conversation UI, and uses Pi's session
file support to preserve conversation history across normal follow-up prompts in
the same local Warp conversation.

The prompt sent to Pi includes Warp context for current directory, home
directory, execution environment, current time, indexed codebase, git head,
branch, selected text, selected terminal blocks, file/project-rule context,
running command snapshots, images as metadata, text/document/diff attachments,
and available skill names/descriptions when Warp provides them.

The adapter mirrors Pi read-only tool lifecycle events into a compact streamed
"Pi tool activity" message. This exposes which local tools ran without asking
Warp to execute the same tool call a second time. Pi `bash` starts are converted
to native Warp `RunShellCommand` tool calls and the stream is finished so Warp
can show its normal action card, approval flow, execution, and `ActionResult`
continuation. Native Warp tool-call cards are not emitted for Pi-executed
read-only tools because Warp treats tool-call messages as actions to queue after
the stream finishes; emitting them only for display would double-run the tool.

Warp `ActionResult` continuations are converted back into a Pi prompt frame in
the same `pi-local-*` session. This is the first half of the native action
bridge: once a future controller layer emits a Warp action and finishes the
stream, the resulting action output can be fed back to Pi as the next frame.

Pi extension UI requests are handled in RPC mode. Fire-and-forget requests such
as notifications/status updates are mirrored into the tool activity stream.
Blocking dialog requests (`select`, `confirm`, `input`, `editor`) receive a
safe cancellation response so extensions cannot deadlock the local runtime until
Warp has a real dialog bridge.

Resumed/forked server conversations, native Warp tool approval/execution for
Pi `edit`/`write`, and first-class extension dialogs are intentionally left for
the next controller/action-model integration layer. Until that richer bridge
exists, `edit`/`write` remain blocked by `warp-tool-gate.ts` unless explicitly
enabled for raw Pi testing.
