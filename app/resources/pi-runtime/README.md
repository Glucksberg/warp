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
$env:WARP_PI_TOOLS = "read,grep,find,ls,bash,edit,write,warp_mcp_call,warp_mcp_read_resource,warp_lrc_write,warp_lrc_read,warp_lrc_transfer"
```

If `WARP_PI_SESSION_DIR` is unset, Warp stores Pi session files under its
channel-aware data directory in `pi-agent-sessions`. Each local Warp
conversation gets a deterministic `pi-local-*.jsonl` session file, so normal
follow-up prompts in the same conversation resume the same Pi context.

Pi read-only tools can be selected when you want the local agent to inspect the
workspace without mutating it:

```powershell
$env:WARP_PI_TOOLS = "readonly"
```

`readonly` expands to `read,grep,find,ls`. By default, Warp enables the
read-only Pi tools plus Warp-native bridge tools:

- `bash` is proxied into Warp's native `RunShellCommand` action.
- `edit`/`write` are proxied into Warp's native `ApplyFileDiffs` action so the
  existing diff preview and accept/reject flow owns writes.
- `warp_mcp_call` and `warp_mcp_read_resource` are extension tools that proxy
  MCP requests into Warp's native MCP actions.
- `warp_lrc_write`, `warp_lrc_read`, and `warp_lrc_transfer` proxy
  long-running shell command interaction into Warp's native terminal actions.

`warp-native` expands to only the Warp bridge tools, while `all` expands to the
default tool set. Set `WARP_PI_TOOLS` to `none` to disable Pi tool execution, or
set `WARP_PI_DISABLE_TOOL_GATE=1` only when intentionally testing raw Pi tool
behavior.

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
Warp to execute the same tool call a second time. Pi `bash`, `edit`, `write`,
MCP bridge, and long-running command bridge starts are converted to native Warp
tool calls and the stream is finished so Warp can show its normal action card,
approval flow, execution, and `ActionResult` continuation. Native Warp tool-call
cards are not emitted for Pi-executed read-only tools because Warp treats
tool-call messages as actions to queue after the stream finishes; emitting them
only for display would double-run the tool.

Warp `ActionResult` continuations are converted back into a Pi prompt frame in
the same `pi-local-*` session, so results from native shell, file edit, MCP, and
long-running command actions are fed back to Pi on the next frame.

Pi extension UI requests are handled in RPC mode. Fire-and-forget requests such
as notifications/status updates are mirrored into the tool activity stream.
Blocking dialog requests (`select`, `confirm`, `input`, `editor`) receive a
safe cancellation response so extensions cannot deadlock the local runtime until
Warp has a real dialog bridge.

Resumed/forked server conversations, first-class extension dialogs, and raw Pi
mutating tool execution are intentionally left for later layers. By default,
mutating Pi tools are allowed only when they are proxied through Warp's native
action flow.
