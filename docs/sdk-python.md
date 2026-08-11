# Python SDK

```python
from cocoonsandbox import Client

client = Client("10.0.0.5:7777", api_token="...")
with client.new("ghcr.io/cocoonstack/sandbox/rt:24.04") as sb:
    print(sb.exec("echo", "hello"))          # "hello\n"
```

`pip install cocoonstack-sandbox` — stdlib-only, no dependencies, and
synchronous by design (agent frameworks that need async wrap calls in
`asyncio.to_thread`, exactly like the [OpenAI adapter](openai-adapter.md)
does). The surface is at parity with the [Go SDK](sdk.md); wire fidelity is
pinned by the shared protocol fixture corpus that the Rust guest, Go, and
Python all round-trip in CI.

## A complete example

Claim a sandbox, push a project in, run a build, then freeze the built state
and fan out two independent workers from that exact moment:

```python
import io
import os
import tarfile

from cocoonsandbox import Client, ExitError


def project_tar() -> bytes:
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w") as tar:
        body = b"build:\n\techo built > out.txt\n"
        info = tarfile.TarInfo("Makefile")
        info.size = len(body)
        tar.addfile(info, io.BytesIO(body))
    return buf.getvalue()


def main() -> None:
    client = Client(os.environ["SANDBOXD_ADDR"], api_token=os.environ["SANDBOXD_TOKEN"])
    with client.new("rt:24.04", size="medium", ttl_seconds=600) as sb:
        print(f"claimed {sb.id} on {sb.owner}")

        sb.push("/work", project_tar())  # only ingestion path on the no-network lane
        try:
            print(sb.exec("sh", "-c", "cd /work && make build 2>&1"), end="")
        except ExitError as e:
            raise SystemExit(f"build failed (rc={e.code}): {e.stderr}")

        ckpt = sb.checkpoint("built")  # freeze the built state
        for i in range(2):
            with ckpt.new() as worker:  # each branch is fully independent
                print(worker.exec("sh", "-c", f"echo worker {i} && ls /work"), end="")


if __name__ == "__main__":
    main()
```

The rest of this guide is the per-method reference.

## Connecting

```python
client = Client("10.0.0.5:7777", api_token="...", timeout=120.0)
```

`api_token` is the node token — a root `api_token` (full access) or a
tenant token (resource-creating verbs only; operator surfaces answer it
403). On a cluster every node shares the root token and the same tenants
set. `timeout` bounds every socket operation.

**Clusters need nothing extra**: dial any node. On a warm miss the entry
node answers with a redirect and `new` follows it transparently; the
returned handle is bound to the owning node and all further calls go there
directly. To recover a handle when only `id` + `token` survived (say,
across a process restart):

```python
sb = client.lookup(id, token)   # asks the entry node, then each mesh peer
```

## Claiming

```python
sb = client.new("ghcr.io/cocoonstack/sandbox/rt:24.04",
                net="egress", size="medium", ttl_seconds=600,
                volumes=["imagenet", ("weights", "/models")])
```

| parameter | values | default | meaning |
|---|---|---|---|
| `net` | `"none"`, `"egress"` | `"none"` | Cloud Hypervisor network shape: `none` disables the NIC and uses vsock-only I/O; `egress` attaches a bridge/CNI NIC |
| `size` | `"small"`, `"medium"`, `"large"`, `"xlarge"` | `"small"` | resource tier: 1cpu/512M, 2cpu/1G, 4cpu/4G, 4cpu/8G |
| `volumes` | bare names or `(name, mount)` pairs | `None` | attach and mount up to eight unique read-only dataset disks; an omitted mount defaults to `/volumes/<name>`; accepted by `Client.new` and `Template.new` |
| `ttl_seconds` | int | server default 5m | sandbox TTL, server-capped at 24h. The node reaps the sandbox after the TTL even if the client vanishes |

`new` returns when the sandbox's silkd answers: a warm hit is milliseconds,
a cold key can take the full boot. A volume claim always provisions and returns
only after every requested disk is mounted; `sb.volumes` contains dictionaries
with the finalized name and effective mount. Custom mounts must be absolute and
clean, stay outside the guest OS tree, and cannot duplicate or nest. The handle
exposes `sb.id`, `sb.token`,
`sb.owner`, `sb.deadline`, and `sb.from_checkpoint` (the lineage edge when
branched). `sb.template_digest` is the exact content identity when the claim
cloned a promoted template; it is empty for other sources. `Sandbox` is a
context manager; `sb.close()` releases it (releasing one already gone is not
an error — double-release and reap races stay silent).
Volume sandboxes cannot hibernate, fork, checkpoint, or promote. Checkpoint
branches do not accept volumes in this version.

The four caller-visible constraints are deliberate: volume claims never take a
warm VM, remain non-capturable, mount read-only, and require Cloud Hypervisor.

`client.volumes()` returns the fleet entries this token may use:

```python
for volume in client.volumes():
    print(volume["name"], volume["default_mount"],
          volume["size_bytes"], volume["available"], volume["nodes"])
```

Discovery returns the gossiped union and holder count; availability and size
describe the connected node. Placement redirects to a node advertising every
requested name. A promoted-template claim prefers a peer advertising both the
template and every volume; when that intersection is empty, one volume holder
may self-verify access to a shared template store before provisioning.

## Hibernating

```python
sb.hibernate()                        # snapshot + stop atomically; memory freed
sb.exec("cat", "/tmp/state")          # any later call wakes it transparently
```

`hibernate` snapshots the VM and stops it in one atomic step. The handle
stays valid: the first call that reaches the guest restores the VM (roughly
a restore's latency, tens of milliseconds on bare metal). The TTL keeps
running — a hibernated sandbox is still reaped at its deadline, so claim
with a `ttl_seconds` that covers the idle period. When to hibernate is your
policy, unless the deployment opts into `idle_hibernate_seconds`
([deploy](deploy.md#configuration)), which hibernates idle claims
automatically with the same transparent wake.

## Forking

```python
children = sb.fork(2, ttl_seconds=600)   # list[Sandbox], own leases
```

Clones the sandbox into fresh, fully independent claims: memory, disk, and
guest state (sessions, processes, tmpfs) duplicate at the fork point, and
each child gets a distinct machine identity. `ttl_seconds` bounds every
child's lifetime (0 = server default) — children never inherit the parent's
remaining lease. A running parent pauses briefly for the snapshot; a
hibernated parent forks from its memory image without waking.
All-or-nothing: on error no child survived. Count is capped at the node's
`max_fork_count` (default 16).

## Promoting to a template

```python
tpl = sb.promote("myproj:v1")     # publish current state
child = tpl.new()                 # clones the promoted state
assert tpl.content_digest and tpl.content_digest == child.template_digest
tpl.delete()                      # caller owns the lifecycle
```

Templates are keyed by (name, the sandbox's network lane, its size); on
the default local-disk backend they live on the owning node (a shared
store makes every node resolve them); the returned `Template` handle is
bound there. Its `delete` and volume-less `new` reach that node;
`new(volumes=...)` may follow one volume-placement redirect. The name-based calls
(`client.new("myproj:v1")`, `client.delete_template(...)`) route
cluster-wide via template gossip and lag a promote/delete by about a gossip
tick — prefer the handle right after promoting (see
[Templates on a cluster](cluster.md#templates-on-a-cluster)).
`tpl.content_digest` identifies the published export bytes. A caller pinning
the mutable name can compare a claim's `template_digest` with its expected
value and close/refuse a mismatch.
Templates published by an older sandboxd have empty digests until they are
re-promoted after the node is upgraded.

## Checkpoints — branching and time travel

```python
sb.write_file("/root/state.txt", b"v1")
ckpt = sb.checkpoint("after-setup")       # source keeps running
sb.write_file("/root/state.txt", b"v2")

branch = ckpt.new()                        # a fresh sandbox at the captured moment
branch.read_file("/root/state.txt")        # b"v1"
sb.read_file("/root/state.txt")            # b"v2" — source unaffected
ckpt.delete()
client.checkpoints()                       # node's checkpoints, newest first
```

A checkpoint captures memory, disk, and running processes without stopping
the sandbox (the same brief pause a fork takes); `ckpt.new(ttl_seconds=0)`
branches any number of independent sandboxes from that exact moment, and
successive checkpoints of sources and branches form a tree. Checkpoints
live in the node's checkpoint store — a shared FUSE mount or
`checkpoint_store: s3` lets any node branch them; handles stay owner-bound
like templates.

## Language servers (LSP)

```python
lsp = sb.start_lsp("python", "/work")   # flavor image provides the server
stream = lsp.request()                  # JSON-RPC byte stream (frame it yourself)
# ... speak Content-Length-framed JSON-RPC over stream.send()/recv() ...
stream.close()                          # ends the session and reaps the server
```

`start_lsp` spawns the language server the flavor image ships for the
language (the python flavor bakes `pylsp`); the base image has none, so it
raises the typed `not_found`. silkd is a broker — it pipes JSON-RPC bytes
without parsing LSP semantics, so the caller frames (Content-Length) and
correlates by request id. A server serves one `request()` stream for its
lifetime: closing the stream reaps it (start a new one to keep working);
`lsp.stop()` kills it early.

## Reaching guest ports

```python
conn = sb.dial_port(8080)                        # byte stream to 127.0.0.1:8080 in the guest
listener = sb.proxy_port("127.0.0.1:0", 8080)    # local listener piping to it
url = sb.preview_url(8080, ttl_seconds=1800)     # signed, shareable browser URL
```

`dial_port` returns a `PortConn` (`send`/`recv`/`close_write`/`close`,
context-manager) relayed over the silkd protocol — it works on the
no-network lane, where the vsock relay is the only way in. A dead port
raises silkd's `not_found`. `proxy_port` serves the port on a local
listener for unmodified local tools (browsers, curl); close the returned
socket to stop. `preview_url` mints a signed URL served by the node's
preview listener, clamped to the claim's remaining lease — the URL dies
with the sandbox, and a node without `preview_listen` answers 501.

## Node info

```python
client.info()   # {"pools": [...], "claimed": n, "hibernated": n, "peers": [...]}
```

## Running commands

```python
out = sb.exec("python3", "script.py")            # stdout; ExitError on rc != 0

code = sb.run(["bash", "-c", "make test"],
              cwd="/work", env={"CI": "1"}, user="ubuntu",
              stdin=input_bytes,
              on_stdout=lambda b: sys.stdout.buffer.write(b),
              on_stderr=lambda b: sys.stderr.buffer.write(b))
```

`exec` returns stdout and raises `ExitError(code, stderr)` on a non-zero
exit. `run` streams raw bytes through the callbacks (chunk boundaries may
split multi-byte sequences) and returns the exit code. `user` de-escalates
inside the guest; `session=` routes the command into a persistent session.

## Background processes

```python
pid = sb.spawn("sh", "-c", "make build")     # returns immediately
sb.ps()                                       # [{pid, argv, detached, state, exit_code?, ...}]
code = sb.logs(pid, on_stdout=out.append)     # replay the bounded ring; None while running
code = sb.attach(pid, on_stdout=out.append)   # replay, then follow live until exit
sb.kill(pid)                                  # default SIGKILL
```

`spawn` starts the command detached with a bounded output ring; `logs`
replays it, `attach` follows live output until exit (replay and live stream
hand off atomically). Killing an already-exited process is a no-op success.

## Sessions

A session is a real persistent shell: cwd, env and shell state survive
across calls.

```python
sess = sb.session(cwd="/work", env={"PATH": "..."})
sess.exec("export", "MARK=1")            # persists
sess.exec("sh", "-c", "echo $MARK")      # "1\n"
sess.close()

sb.sessions()                            # live session ids
```

Idle sessions are reaped guest-side after 30 minutes.

## Files

```python
sb.write_file("/work/a.txt", b"data")     # atomic; mode=0o755 optional
data = sb.read_file("/work/a.txt")
ents = sb.list_dir("/work")               # [{"name", "kind", "size"}]
info = sb.stat("/work/a.txt")             # {"kind", "size", "mode", "mtime_epoch_secs"}
sb.mkdir("/work/sub", parents=True)
sb.remove("/work/sub", recursive=True)
sb.rename("/a", "/b")
```

Writes stream any size and commit via temp-file rename: a mid-stream
failure never leaves a truncated destination, and overwriting an executable
keeps its exec bit.

## Project trees

```python
sb.push("/work", tar_bytes)    # extract a tar stream under /work (atomic)
tar = sb.pull("/work")         # /work back as tar bytes
```

`push` is atomic against a truncated stream and the only project-ingestion
path on the no-network lane.

## Search

```python
matches = sb.find("/work", r"TODO|FIXME", glob="*.py")
# [{"file", "line", "content"}]; glob is anchored *? wildcards on the file name

results = sb.replace(["/work/main.py"], r"foo", "bar")
# [{"file", "replacements"}]; per-file atomic
```

Patterns are regular expressions evaluated in the guest — no shell quoting.

## Watching

```python
w = sb.watch("/work", recursive=True)
for ev in w:                       # {"kind", "path"}
    print(ev["kind"], ev["path"])  # created|modified|deleted|renamed
w.close()
```

`watch` returns once the guest acknowledges the watch is armed — events
caused after it returns are guaranteed captured. A bad path fails
synchronously.

## Git

```python
sb.git_clone(url, "/work/repo", branch="main", depth=1, auth=token)  # egress lane only
st = sb.git_status("/work/repo")          # {"branch", "ahead", "behind", "files"}
sb.git_add("/work/repo", ["a.txt"])
sha = sb.git_commit("/work/repo", "message", "Dev <dev@example.com>")
sb.git_push("/work/repo", auth=token)     # egress lane only
sb.git_pull("/work/repo", auth=token)     # egress lane only
br = sb.git_branches("/work/repo")        # {"current", "branches"}
sb.git_create_branch("/work/repo", "feature")
sb.git_checkout("/work/repo", "feature")
sb.git_delete_branch("/work/repo", "feature")
```

Results are structured (porcelain v2 under the hood), never scraped stdout.
Auth tokens travel as an in-memory header, never touching guest disk. On
the no-network lane, clone/push/pull raise a typed `unimplemented` error
pointing at `push`.

## Terminals

```python
pty = sb.open_pty(cols=120, rows=40)      # context-manager; pty.pid is the guest process
pty.write(b"make test\n")
data = pty.read()                         # b"" when the shell exits
pty.resize(200, 50)
pty.close()
```

## Errors

`SandboxError` is the base; catch the narrowest type you handle:

- `APIError(verb, status, message)` — control plane (HTTP status)
- `SilkdError(kind, message)` — typed guest failure; `kind` is
  `bad_request` / `not_found` / `unimplemented` / `internal`
- `ExitError(code, stderr)` — non-zero exit from `exec`
- `ProtocolError` — broken stream

```python
try:
    sb.git_clone(url, "/work/repo")
except SilkdError as e:
    if e.kind == "unimplemented":   # no-network lane: fall back to push
        sb.push("/work/repo", tar_bytes)
```
