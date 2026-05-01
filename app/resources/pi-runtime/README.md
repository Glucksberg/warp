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
$env:WARP_PI_TOOLS = "readonly"
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

`readonly` expands to `read,grep,find,ls`. `all` expands to
`read,grep,find,ls,bash,edit,write`, but the bundled
`warp-tool-gate.ts` extension still blocks mutating tools unless
`WARP_PI_ALLOW_UNBRIDGED_MUTATING_TOOLS=1` is set. This prevents local Pi from
running shell commands or editing files outside Warp's action approval pipeline.
Set `WARP_PI_TOOLS` to `none` to disable Pi tool execution, or set
`WARP_PI_DISABLE_TOOL_GATE=1` only when intentionally testing raw Pi tool
behavior.

The adapter also forwards BYO API keys already configured in Warp to Pi as
provider environment variables when those variables are not already set.

## Current scope

The adapter maps a Warp Agent Mode prompt to `pi --mode rpc`, collects the
assistant text, and renders it in the existing Warp agent conversation UI. It
uses Pi's session file support to preserve conversation history across normal
follow-up prompts in the same local Warp conversation.

The prompt sent to Pi includes a small safe context block for current directory,
home directory, execution environment, current time, indexed codebase, git head,
branch, and available skill names/descriptions when Warp provides them.

The adapter mirrors Pi tool lifecycle events into a compact "Pi tool activity"
message before the final assistant answer. This exposes which local tools ran
without asking Warp to execute the same tool call a second time.

Resumed/forked server conversations, action-result continuations, native Warp
tool approval/execution for mutating tools, live deltas, file diffs, and
extension UI requests are intentionally left for the next integration layer.
Until that richer bridge exists, the adapter rejects context-dependent requests
instead of sending an incomplete prompt to Pi.
