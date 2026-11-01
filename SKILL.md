---
name: codex-connector
description: Full-control bridge to a persistent Codex app-server, and the default first choice for any Codex operation in ki. Use to create, list, read, resume, fork, archive, delete, and search Codex threads; start, steer, interrupt, and inspect turns; stream token-level events; and answer approval or tool-input requests. Use when the user says "ask Codex", "let Codex do it", "get a second opinion", "have Codex review this", wants independent implementation work with complete session control, or asks for the status, progress, or results of an existing Codex session or of a tmux pane running Codex.
---

# Codex session control

This skill owns one long-lived `codex app-server` process through a local daemon.

## Use this first

This is the default path for **any** Codex operation, including read-only
inspection. Before parsing `~/.codex/state_*.sqlite`, walking
`sessions/**/rollout-*.jsonl`, or running `codex exec`, try `thread list`,
`thread read`, `events`, and `turn status`. Hand-rolled reads duplicate this
interface and cannot be reproduced by anyone else.

Read-only subcommands: `daemon status`, `thread list`, `thread read`,
`thread turns`, `thread items`, `thread loaded`, `turn status`, `events`,
`approvals list`, `models`, `account read`. They never modify a thread and are
safe against a thread that another agent or a TUI is actively driving.

Do **not** use `send`, `turn steer`, or `turn interrupt` on a thread that
another agent is driving: you would inject into or cancel its live turn. To
branch a conversation, fork it and work on the fork.

One protocol limit matters for status questions: `thread/*` is a Codex-level
view and cannot say which `tmux` pane or OS process owns a thread. To bind a
pane to a thread, use process inspection — `tmux list-panes -F
'#{pane_index} #{pane_pid}'`, then that shell's child `codex` pid, then the
`rollout-*.jsonl` being appended whose `session_meta.session_id` equals the
thread id. `cwd` and `created_at` from `thread list` are the cheap cross-check.
Every command below talks to that same process, so a turn started by one ki
tool call can be steered, interrupted, inspected, or approved by another.

Do not use `codex exec` for this skill. `exec` is a one-process-per-call wrapper
and cannot list, fork, steer, interrupt, or answer approvals across calls.

## Architecture

`scripts/codex_session.py` auto-starts a detached daemon on first use. The
daemon:

- owns one `codex app-server --listen stdio://` child;
- speaks the full JSON-RPC protocol (`thread/*`, `turn/*`, approvals, events);
- records relevant notifications and every server request to `events.jsonl`;
- exposes a token-authenticated localhost TCP control socket;
- keeps thread, turn, usage, and pending-approval state in memory.

State lives in `KI_CODEX_HOME` or the platform cache (`~/.cache/ki-codex` on
Linux/macOS, `%LOCALAPPDATA%\ki-codex` on Windows). `daemon.json` is mode 0600
and contains the control token; do not edit or print it.

## Quick start

```bash
python3 scripts/codex_session.py daemon start
python3 scripts/codex_session.py thread list
python3 scripts/codex_session.py thread start --cwd "$PWD"
python3 scripts/codex_session.py thread read <thread-id> --turns
```

Run a turn and stream it:

```bash
python3 scripts/codex_session.py send --thread <thread-id> \
  'Implement the fix. Do not commit. Run the relevant tests.'
```

Defaults are fully open: `danger-full-access` and `approval never`. Codex can
read and write any path and run any command. `read-only` and `workspace-write`
remain available as explicit opt-ins.

`send` creates a thread when `--thread` is omitted and defaults its `cwd` to
the caller's current directory. Agent text goes to stdout; progress, commands,
files, approvals, and usage go to stderr. `--json` emits raw protocol events.
`--no-wait` returns immediately after `turn/start`. Use `--prompt-file FILE`
for long prompts that would be awkward as a shell argument.

## Full control surface

### Threads

```bash
thread start  [--cwd DIR] [--model M] [--sandbox MODE] [--approval MODE] [--name NAME] [--ephemeral]
               (defaults: danger-full-access, approval never)
thread list   [--limit N] [--cursor C] [--archived] [--cwd DIR] [--search TERM]
thread read   <id> [--turns]
thread resume <id> [--turns]
thread fork   <id> [--last-turn ID] [--name NAME] [--ephemeral]
thread archive|unarchive|delete <id>
thread name   <id> <name>
thread compact <id>
thread revert <id> --before-turn <turn-id>
thread settings <id> [--model M] [--effort E] [--cwd DIR] [--sandbox MODE] [--approval MODE]
thread turns  <id> [--limit N] [--cursor C]
thread items  <id> [--turn ID] [--limit N] [--cursor C]
thread loaded [--limit N]
thread search <term> [--limit N]
```

`thread fork` is the safe way to branch a conversation. Two `work` turns must
not edit the same working tree concurrently; fork and use separate worktrees
for parallel writers.

### Turns

```bash
turn start     <thread-id> <prompt> [--sandbox MODE] [--approval MODE] [--model M] [--effort E] [--output-schema FILE]
               (defaults: danger-full-access, approval never)
turn steer     <thread-id> <prompt> [--turn ID]
turn interrupt <thread-id> [--turn ID]
turn status    <thread-id> [--turn ID]
```

`turn steer` appends guidance to the active turn; it is not a new turn. If
`--turn` is omitted, the daemon uses the thread's active turn. `turn interrupt`
cancels the active turn and leaves the thread resumable.

### Events

```bash
events --thread <id> [--after SEQ] [--limit N]
events --thread <id> --follow [--timeout SECONDS]
```

The event log contains `thread/*`, `turn/*`, every `item/*` event, token usage,
approval requests, and approvals resolved. `--follow` first replays the backlog
after `--after`, then streams live events.

### Approvals and raw server requests

When a turn uses `--approval ask`, a command or file-change approval arrives as
a `serverRequest` event. The `send` stream prints:

```
[codex] approval required id=0 method=item/commandExecution/requestApproval /bin/bash -lc '...'
```

Answer it from another command:

```bash
approvals list
approvals respond <request-id> --decision accept
approvals respond <request-id> --decision decline
approvals respond <request-id> --decision acceptForSession --scope session
```

For `item/tool/requestUserInput`, pass JSON answers:

```bash
approvals respond <id> --decision accept --answers '{"question-id":{"answers":["yes"]}}'
```

For any other server request, use the raw form:

```bash
requests list
requests respond <request-id> --result '{"decision":"accept"}'
```

`--approval accept|acceptForSession|decline|cancel` auto-responds; `never`
disables approvals; `ask` leaves them pending for explicit control. The default
is `never`. When the server offers only `accept`/`cancel` for a command, a
`decline` decision is sent as `cancel` because it is the only available
negative choice.

### Raw protocol

Every app-server method is reachable even when this CLI has no convenience
command:

```bash
raw thread/list --params '{"limit":5}'
raw thread/queue/add --params '{"threadId":"...","clientUserMessageId":"...","input":[{"type":"text","text":"..."}]}'
raw thread/queue/start --params '{"threadId":"..."}'
raw thread/metadata/update --params '{"threadId":"...","projectId":null}'
raw account/rateLimits/read
```

Use `raw <method> --notification` for notification-only methods.

### Models and account

```bash
models [--include-hidden]
account read
account usage
account rate-limits
```

## Sandbox and approval

Defaults:

| Setting | Default |
| --- | --- |
| Sandbox | `danger-full-access` |
| Approval | `never` |

Codex can modify any file and run any command. The previous restrictions are
now explicit opt-ins, not defaults:

| Value | Meaning |
| --- | --- |
| `danger-full-access` / `danger` | no Codex sandbox (default) |
| `read-only` | inspect only, when explicitly requested |
| `workspace-write` | edit the working tree only, when explicitly requested |

`--approval` values: `ask`, `accept`, `acceptForSession`, `decline`, `cancel`,
`never`, `untrusted`. `never` is the default. Use `ask` when you want each
escalation surfaced as an `approval required` event and answered manually.

**Host caveat:** containers and some CI hosts cannot initialize Codex's
bubblewrap sandbox (`bwrap: loopback: Failed RTM_NEWADDR: Operation not
permitted`). This only affects explicit `read-only` / `workspace-write` runs;
the default `danger-full-access` does not use that sandbox.

## Operating rules

- Codex uses its own auth (`~/.codex/auth.json` or `CODEX_HOME`), not ki's
  `credentials.json`. Check with `codex login status`; if logged out, the user
  must run `codex login`.
- Codex reads the repository's `AGENTS.md`; that is expected. It does not see
  ki's system prompt or this conversation. Put the full task, paths,
  constraints, acceptance criteria, and verification commands in the prompt.
- In write tasks, always inspect `git diff` and run the tests yourself. Codex's
  final message is a claim, not verification.
- Do not let Codex commit, push, or change credentials unless the user asked.
- One active turn per thread. Use `turn steer` for corrections during a turn,
  and a new turn after it completes.
- The daemon is shared across ki sessions. Do not start a second `codex
  app-server` or edit `daemon.json` by hand.
- `daemon stop` interrupts active turns and stops the shared daemon. Prefer
  `daemon restart` after updating this script.

## Verification workflow

1. `thread read <id> --turns` or `events --thread <id>` to inspect what Codex did.
2. `turn status <id>` for the latest status, usage, and final message.
3. For write tasks: `git diff --stat`, `git diff`, then run the project tests.
4. Continue with `send --thread <id>`; branch with `thread fork <id>`.
5. Archive or delete test threads when they are no longer needed.

## Troubleshooting

| Symptom | Action |
| --- | --- |
| `daemon is not running` | `daemon start` (or let a command auto-start it) |
| `app-server exited` | `daemon restart`; active turns fail and can be resumed |
| approval never arrives | `approvals list`; the request may be for a different thread |
| `no active turn` | pass `--turn <id>` or start a turn first |
| sandbox/bwrap warning | explicit `read-only`/`workspace-write` failed; use the default `danger-full-access` |
| auth failure | `codex login` |
| stale daemon after code change | `daemon restart` |
| port/token issue | inspect `daemon status` and `daemon logs` |

The wrapper needs Python 3.9+ and the `codex` binary on `PATH` (or `CODEX_BIN`).
