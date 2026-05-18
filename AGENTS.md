# Agent Instructions

## Downstream Overlay

This repository is not a plain mirror of `warpdotdev/warp`. It maintains a
downstream overlay: a local feature stack rebased on top of upstream Warp.

The overlay currently includes the local Pi agent runtime, OSS/local onboarding
behavior, Pi model selection, native Warp action proxying for Pi tools, and other
fork-specific defaults. Treat these commits as intentional product behavior for
this fork, not temporary patch noise.

When making fork-specific changes:

- Prefer small, reviewable commits on top of the local feature stack.
- Keep upstream-compatible fixes separate from downstream-only overlay behavior.
- Preserve the Pi local runtime contract unless the user explicitly asks to
  remove or replace it.
- Do not squash the overlay into one large sync commit.

## Upstream Sync

Use `LOCAL_UPSTREAM_SYNC.md` as the detailed sync protocol. The current remote
layout for this checkout is:

- `origin`: `https://github.com/Glucksberg/warp.git`
- `upstream`: `https://github.com/warpdotdev/warp.git`

Before syncing:

```powershell
git status --short --branch
```

Start from a clean worktree. If there are local modifications, either commit
them into the downstream overlay when they are intentional, or stash them with a
clear name and reapply them after the sync.

The preferred sync flow is:

```powershell
git fetch upstream
git branch backup/local-pi-before-sync-$(Get-Date -Format yyyyMMdd-HHmmss)
git rebase upstream/master
```

Resolve conflicts by preserving the downstream overlay and adapting it to the
new upstream APIs:

- Keep `app/src/ai/agent/api/pi_local.rs` and the early `pi_local` routing.
- Preserve native Warp actions for Pi `bash`, `edit`, `write`, MCP bridge, and
  long-running command bridge tools.
- Preserve OSS/local no-auth behavior only for OSS/local paths; do not weaken
  non-OSS auth.
- Preserve Pi runtime resources under `app/resources/pi-runtime/`.
- Prefer upstream APIs and types when they changed, then update the Pi adapter
  and tests to match.

After every sync, run:

```powershell
.\script\local\check_pi_runtime.ps1
```

Only use `-SkipCargoCheck` while iterating on a known failure, not for final
validation. When the sync is complete, update the fork remote with:

```powershell
git push --force-with-lease origin master
```

Use a force push only after a rebase rewrites the local feature stack, and only
with `--force-with-lease`.
