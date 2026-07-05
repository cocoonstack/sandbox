# silkd

silkd is the in-guest product daemon: a Rust binary baked into every
template image, listening on guest vsock port 2048, started at sysinit in
parallel with boot. It is what actually "runs something" inside a sandbox;
sandboxd relays SDK connections to it byte-for-byte. Reaching silkd is the
claim-readiness signal — a claim returns only once silkd answers.

Most users never speak the protocol directly — the [Go SDK](sdk.md) covers
the full surface. This page is the wire reference for other clients and for
debugging.

## Protocol

Newline-delimited JSON frames, **one connection per RPC**. The first frame
is the request, tagged `"op"`; the server streams response frames tagged
`"type"` until a terminal one (`exit`, `done`, or `error`). Binary payloads
ride base64 in `data` fields. Frames are capped at 8 MiB; requests carry
`"v": 1` and unknown fields are ignored (the forward-compatibility story).

Sessions and processes are server-side state addressed by id — a dropped
connection loses nothing (`attach` resumes). The only connection-bound verb
is `fs_watch`.

The authoritative wire contract is the shared fixture corpus in
`protocol/fixtures/v1`, round-tripped by both the Rust and Go test suites —
a frame only one side can parse fails CI.

## Verbs

| group | ops | response flow |
|---|---|---|
| exec | `exec {argv, cwd?, env?, user?, detach?, session?}` | `started{pid}` → `stdout/stderr{data}`… → `exit{code}`; the client may stream `stdin{data}` / `stdin_close`. `detach` returns after `started`; the process keeps a bounded output ring for later `logs`/`attach` |
| procs | `ps` / `kill {pid, signal?}` / `attach {pid}` / `logs {pid}` | handles are guest pids; any connection can list, signal, replay, or re-attach live |
| sessions | `session_create {id?, cwd?, env?}` / `session_list` / `session_rm {id}` | a session is a real persistent bash; `exec` with `session` runs inside it. Idle sessions are reaped after 30 minutes |
| fs | `fs_write {path, mode?}` (+`data`/`data_end` frames) / `fs_read` / `fs_list` / `fs_stat` / `fs_mkdir {parents?}` / `fs_rm {recursive?}` / `fs_rename {from, to}` | streaming both directions; write commits atomically via temp+rename and inherits an overwritten file's mode; `fs_list` streams 4096-entry batches |
| tree | `fs_push {dest}` (+tar as `data` frames) / `fs_pull {path}` | whole trees as tar streams through the guest tar |
| search | `fs_find {path, pattern, glob?}` → `match{file, line, content}`… → `done` / `fs_replace {files, pattern, replacement}` → `replaced{file, replacements}`… → `done` | regex as data, no shell quoting; `glob` is anchored `*`/`?` wildcards over file names; find skips binary and >8 MiB files |
| watch | `fs_watch {path, recursive?}` | `ready` once armed (events after it are guaranteed captured), then `event{kind, path}` until the client disconnects; watcher errors arrive as a terminal `error` |
| pty | `pty_open {cols, rows, cwd?, env?, user?}` / `pty_resize {pid, cols, rows}` | a shell under a pseudo-terminal; output as `stdout` frames, input as `stdin` frames, exit terminal. PTYs register in the proc table like any exec |
| git | `git_clone {url, path, branch?, depth?, auth?}` / `git_status {path}` / `git_add {path, files}` / `git_commit {path, message, author}` / `git_push {path, auth?}` / `git_pull {path, auth?}` / `git_branch {path, action, name?}` | structured results (porcelain-v2 status, commit hash, branch list). `auth` is injected as an in-memory header, never written to guest disk |
| misc | `info` | `{version, proto, uptime_secs, procs, sessions}` — the readiness probe |

## Error kinds

A failed verb terminates with `error {kind, message}`:

| kind | meaning |
|---|---|
| `bad_request` | malformed frame, empty argv, invalid pattern |
| `not_found` | unknown pid/session/path |
| `unimplemented` | verb unavailable on this lane — notably git clone/push/pull on the no-network lane, whose message points at `fs_push` |
| `internal` | everything else (spawn failures, git errors, I/O) |

## Network lanes

git clone/push/pull consult lane detection: an interface under
`/sys/class/net` counts only when it has a `device` backing (a virtio or
physical NIC). Name-based checks are not enough — the all-builtin sandbox
kernel auto-creates virtual tunnels (`sit0` and friends) even on the no-NIC
lane. `SILKD_NET=none|egress` overrides the probe for tests and operators.

## Limits

- 8 MiB frame cap (a malformed peer cannot OOM the daemon)
- foreground exec output is backpressured: a slow client paces the child
  rather than losing output; detached observers ride a best-effort broadcast
- no fsync durability on writes (guest page cache semantics)
- `fs_push` extraction is streaming, not atomic — a truncated stream errors
  but may leave partially extracted files
