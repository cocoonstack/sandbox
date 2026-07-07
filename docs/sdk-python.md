# Python SDK

```python
from cocoonsandbox import Client

client = Client("10.0.0.5:7777", api_token="...")
with client.new("ghcr.io/cocoonstack/sandbox/rt:24.04") as sb:
    print(sb.exec("echo", "hello"))          # "hello\n"
```

stdlib-only and synchronous: `pip install cocoonstack-sandbox` brings no
dependencies. The surface mirrors the [Go SDK](sdk.md); this page lists the
Python spellings — semantics (redirect follow, transparent wake, lanes,
tokens) are identical and documented there.

## Client

| call | meaning |
|---|---|
| `Client(addr, api_token="", timeout=120.0)` | one entry node; cluster redirects are followed transparently |
| `client.new(template, net="", size="", ttl_seconds=0)` | claim a sandbox (context-manager friendly) |
| `client.checkpoints()` | node's checkpoints, newest first |
| `client.delete_template(name, net="", size="")` | name-based template delete, gossip-routed |
| `client.lookup(id, token)` | relocate a handle from id + token (asks the entry node, then each mesh peer) |
| `client.info()` | pool/claim counters |

## Sandbox

Commands: `sb.exec(*argv, cwd=, env=, user=, session=, stdin=)` returns
stdout and raises `ExitError(code, stderr)` on a non-zero exit;
`sb.run(argv, on_stdout=, on_stderr=, ...)` streams and returns the code.

Files: `write_file` (atomic), `read_file`, `list_dir`, `stat`, `mkdir`,
`remove`, `rename`. Trees: `push(dest, tar_bytes)` (atomic against a
truncated stream), `pull(path)` → tar bytes. Search: `find`, `replace`.
Git: `git_clone/status/add/commit/push/pull/branches/checkout/
create_branch/delete_branch` (network verbs are egress-lane only — the none lane raises a typed
`SilkdError(kind="unimplemented")`).

Sessions: `sb.session(cwd=, env=)` returns a persistent shell whose state
survives across `session.exec(...)` calls; `sb.sessions()` lists the live
session ids. Watching: `sb.watch(path, recursive=)` yields event dicts
until closed.

Terminals: `sb.open_pty(cols=80, rows=24, cwd=, env=, user=)` returns a
`Pty` (`read`/`write`/`resize`/`close`, context-manager) — a real shell
under a pseudo-terminal for agents that drive interactive programs.

Ports: `sb.dial_port(port)` returns a byte stream (`send`/`recv`/
`close_write`/`close`) to the guest port, working on the no-network lane;
`sb.proxy_port("127.0.0.1:0", port)` serves it on a local listener for
unmodified local tools (returns the listening socket — close it to stop);
`sb.preview_url(port, ttl_seconds=0)` mints a signed, shareable URL served
by the node's preview listener, clamped to the claim's lease.

Language servers: `sb.start_lsp(language, root="")` spawns the flavor
image's server (base images raise the typed `not_found`) and returns an
`Lsp`; `lsp.request()` opens the JSON-RPC byte stream (the caller frames
Content-Length and correlates ids — agents already speak LSP); a server
serves one request stream for its lifetime, and `lsp.stop()` kills it
early. The python flavor bakes `pylsp`.

Lifecycle: `fork(count, ttl_seconds=0)` → children; `hibernate()` (any
later call wakes transparently); `checkpoint(name="")` → `Checkpoint`;
`promote(template)` → `Template`; `close()` releases.

## Checkpoints — branching and time travel

```python
sb.write_file("/root/state.txt", b"v1")
ckpt = sb.checkpoint("after-setup")       # source keeps running
sb.write_file("/root/state.txt", b"v2")

branch = ckpt.new()                        # a fresh sandbox at the captured moment
branch.read_file("/root/state.txt")        # b"v1"
sb.read_file("/root/state.txt")            # b"v2" — source unaffected
```

A checkpoint captures memory, disk, and running processes without stopping
the sandbox. `ckpt.new()` branches any number of independent sandboxes from
that exact moment; successive checkpoints of sources and branches form a
tree. `ckpt.delete()` removes it. Checkpoints are node-local; handles are
bound to their node, and `client.checkpoints()` lists the connected node's.

## Errors

`SandboxError` is the base; `APIError` (control plane, carries HTTP
status), `SilkdError` (guest daemon, carries the typed `kind`),
`ExitError` (non-zero exit), `ProtocolError` (broken stream).
