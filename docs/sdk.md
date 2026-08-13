# Go SDK

```go
import sandbox "github.com/cocoonstack/sandbox/sdk/go"
```

The SDK is stdlib-only. One `Client` talks to one entry node; sandbox
handles dial their owning node directly, so a client works unchanged against
a single node or a cluster.

## A complete example

Claim a sandbox, push a project into it, run a build, then freeze the built
state and fan out two independent workers from that exact moment:

```go
package main

import (
	"archive/tar"
	"bytes"
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"time"

	sandbox "github.com/cocoonstack/sandbox/sdk/go"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	client, err := sandbox.Connect(os.Getenv("SANDBOXD_ADDR"),
		sandbox.WithAPIToken(os.Getenv("SANDBOXD_TOKEN")))
	if err != nil {
		log.Fatal(err)
	}

	sb, err := client.New(ctx, "rt:24.04",
		sandbox.WithSize(sandbox.Medium),
		sandbox.WithTimeout(10*time.Minute))
	if err != nil {
		log.Fatal(err)
	}
	defer sb.Close()
	fmt.Printf("claimed %s on %s\n", sb.ID, sb.Owner())

	// Push is the only ingestion path on the no-network lane.
	if err := sb.Push(ctx, "/work", projectTar()); err != nil {
		log.Fatal(err)
	}
	out, err := sb.Exec(ctx, "sh", "-c", "cd /work && make build 2>&1")
	var exit *sandbox.ExitError
	switch {
	case errors.As(err, &exit):
		log.Fatalf("build failed (rc=%d): %s", exit.Code, exit.Stderr)
	case err != nil:
		log.Fatal(err)
	}
	fmt.Print(out)

	// Freeze the built state; each branch is a fully independent sandbox.
	ckpt, err := sb.Checkpoint(ctx, "built")
	if err != nil {
		log.Fatal(err)
	}
	for i := range 2 {
		worker, err := ckpt.New(ctx)
		if err != nil {
			log.Fatal(err)
		}
		got, err := worker.Exec(ctx, "sh", "-c", fmt.Sprintf("echo worker %d && ls /work", i))
		if err != nil {
			log.Fatal(err)
		}
		fmt.Print(got)
		_ = worker.Close()
	}
}

func projectTar() *bytes.Reader {
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	body := "build:\n\techo built > out.txt\n"
	_ = tw.WriteHeader(&tar.Header{Name: "Makefile", Mode: 0o644, Size: int64(len(body))})
	_, _ = tw.Write([]byte(body))
	_ = tw.Close()
	return bytes.NewReader(buf.Bytes())
}
```

The rest of this guide is the per-method reference.

## Connecting

```go
client, err := sandbox.Connect("10.0.0.5:7777",
    sandbox.WithAPIToken(os.Getenv("SANDBOXD_TOKEN")))
```

- `Connect(addr, opts...)` — `addr` accepts a comma-separated seed list for
  forward compatibility; the current version uses the first entry.
- `WithAPIToken(token)` — the node token: a root `api_token` (full access)
  or a tenant token (resource-creating verbs only; operator surfaces like
  `Info` answer it 403). On a cluster every node shares the same root token
  and the same tenants set.

### Connecting to clusters

Nothing extra: dial any node. On a warm miss the entry node answers with a
redirect and `New` follows it transparently (trying every candidate, so one
dead peer never fails a claim); the returned handle is bound to the owning
node's `owner_addr` and all further calls go there directly.

To recover a handle when only `id` + `token` survived (say, across a process
restart):

```go
sb, err := client.Lookup(ctx, id, token)
```

`Lookup` asks the entry node, then queries all mesh peers concurrently and
binds to whichever confirms ownership first.

## Claiming

```go
sb, err := client.New(ctx, "base:24.04",
    sandbox.WithNetwork(sandbox.NetEgress),
    sandbox.WithSize(sandbox.Medium),
    sandbox.WithVolumes(
        sandbox.Volume{Name: "imagenet"},
        sandbox.Volume{Name: "weights", Mount: "/models"},
        sandbox.Volume{Name: "scratch-db", Mode: "rw"}),
    sandbox.WithTimeout(10*time.Minute))
defer sb.Close()
```

| option | values | default | meaning |
|---|---|---|---|
| `WithNetwork(n)` | `NetNone`, `NetEgress` | `NetNone` | Cloud Hypervisor network shape: `NetNone` disables the NIC and uses vsock-only I/O; `NetEgress` attaches a bridge/CNI NIC |
| `WithSize(s)` | `Small`, `Medium`, `Large`, `XLarge` | `Small` | resource tier: 1cpu/512M, 2cpu/1G, 4cpu/4G, 4cpu/8G |
| `WithVolumes(volumes...)` | `Volume{Name, Mount?, Mode?}` entries | none | attach and mount up to eight unique catalog dataset disks; `Mount` defaults to `/volumes/<name>`; `Mode` is `"ro"` (default) or `"rw"` — `"rw"` requires the catalog entry's `writable: true`; supported by `Client.New` and `Template.New` |
| `WithVolumesAttachOnly()` | — | mount | attach the requested volumes without mounting them; the workload finds each device and owns the mount. Rejects a `Volume.Mount` locally |
| `WithTimeout(d)` | duration | server default 5m | sandbox TTL, rounded up to seconds, server-capped at 24h. The node reaps the sandbox after the TTL even if the client vanishes |

`New` returns when the sandbox's silkd answers: a warm hit is milliseconds,
a cold key can take the full boot. A volume claim may consume an ordinary warm
VM and returns only after every requested disk is mounted; the finalized
name, effective mount, and (for `rw` entries) mode are available in
`Sandbox.Volumes`. Custom mounts must be absolute and clean, stay outside the
guest OS tree, and cannot duplicate or nest.
`Sandbox.ID`, `Sandbox.Deadline`, and
`Sandbox.FromCheckpoint` (the lineage edge when branched) are exported.
`Sandbox.TemplateDigest` is the exact content identity when the claim cloned a
promoted template; it is empty for other sources. `Owner()` names the owning
node, and `Token()` returns the per-sandbox bearer to persist with `ID` for a
later `Lookup`; `Close()` releases the sandbox (releasing one already gone is
not an error, and `Close` is bounded internally so it stays defer-friendly).
Volume sandboxes cannot hibernate, fork, checkpoint, or promote. Passing
`WithVolumes` to `Checkpoint.New` returns a local error because checkpoint
branches do not support volumes of either mode in this version.

`WithVolumesAttachOnly()` claims the same volumes without mounting them: the
entries in `Sandbox.Volumes` carry an empty `Mount`, and the workload finds
each device by polling `/sys/block/*/serial` for the catalog name — not
guaranteed present when the claim returns, typically within ~100ms — then
confirms the `/dev/<blk>` node itself exists before mounting. Everything
above describes the default and is unchanged by this option. What changes is
that the mount and its consistency are entirely yours: sandboxd writes and
clears no dirty marker for an attach-only `rw` claim, because it cannot verify
your unmount, so releasing without unmounting cleanly leaves the image as a
crash would — see
[sandboxd-api](sandboxd-api.md#attach-only-volumes) for the full contract.

The caller-visible constraints are deliberate: volume claims may consume a
warm VM, remain non-capturable, mount read-only by default, and require Cloud
Hypervisor.

Discover the fleet entries this token may use before planning a claim:

```go
catalog, err := client.Volumes(ctx) // []sandbox.VolumeInfo
for _, volume := range catalog {
    fmt.Println(volume.Name, volume.DefaultMount, volume.SizeBytes, volume.Available, volume.Nodes, volume.Writable)
}
```

Discovery returns the gossiped union and holder count; availability and size
describe the connected node. Warm candidates retain normal ranking, filtered to
nodes advertising every requested name. A promoted-template claim prefers a
peer advertising both the template and every volume; when that intersection is
empty, one volume holder may self-verify access to a shared template store
before provisioning.

## Hibernating

```go
err := sb.Hibernate(ctx)   // snapshot + stop atomically; memory freed
// ... any later call wakes it transparently:
out, err := sb.Exec(ctx, "cat", "/tmp/state")   // sessions & memory intact
```

`Hibernate` snapshots the VM and stops it in one atomic step — nothing the
guest does can fall between the snapshot point and the stop. The handle
stays valid: the first call that reaches the guest restores the VM (adding
roughly a restore's latency, tens of milliseconds on bare metal). The TTL
keeps running — a hibernated sandbox is still reaped at its deadline, so
claim with a `WithTimeout` that covers the idle period. When to hibernate
is your policy; the node only provides the transition — unless the
deployment opts into `idle_hibernate_seconds` (see
[deploy](deploy.md#configuration)), which hibernates idle claims
automatically with the same transparent wake.

## Forking

```go
children, err := sb.Fork(ctx, 2, 10*time.Minute)   // []*Sandbox, own leases
```

`Fork` clones the sandbox into fresh, fully independent claims: memory,
disk, and guest state (sessions, processes, tmpfs) duplicate at the fork
point, and each child gets a distinct machine identity. The ttl bounds every
child's lifetime (zero = server default) — children never inherit the
parent's remaining lease. A running parent pauses briefly for the snapshot;
a hibernated parent forks from its memory image without waking.
All-or-nothing: on error no child survived. Count is capped at the node's
`max_fork_count` (default 16).
Fork and Promote create node resources, so on a token-guarded node the
client needs `WithAPIToken` — a sandbox handle alone cannot amplify.

## Promoting to a template

```go
tpl, err := sb.Promote(ctx, "myproj:v1")  // publish current state
child, err := tpl.New(ctx)                // clones the promoted state
fmt.Println(tpl.ContentDigest != "" && tpl.ContentDigest == child.TemplateDigest) // true
err = tpl.Delete(ctx)                     // caller owns the lifecycle
```

`Promote` publishes the sandbox's state as a template on its owning node,
keyed by (name, the sandbox's network lane, its size). Claims clone on
demand (~a golden-clone's latency); there is no warm pool for promoted
templates unless the node's config adds one. Re-promoting to the same name
replaces the template. `Template.ContentDigest` identifies the published
export bytes; a claim from that exact generation carries the same value in
`Sandbox.TemplateDigest`. A caller pinning a mutable template name can compare
the claim's value with its expected digest and close/refuse a mismatch.
Templates published by an older sandboxd have empty digests until they are
re-promoted after the node is upgraded.

**On the default local-disk backend templates live on one node**, and on a
cluster the parent claim may have been redirected — the returned `Template`
handle is bound to the owning node. Its `Delete` and volume-less `New` reach
that node; `New(WithVolumes(...))` may follow one volume-placement redirect.
The name-based calls
(`client.New("myproj:v1")`, `client.DeleteTemplate(...)` with
`WithNetwork`/`WithSize` when non-default) route cluster-wide via the
mesh's template gossip; they lag a promote or delete by about a gossip
tick, so prefer the handle right after promoting (see
[Templates on a cluster](cluster.md#templates-on-a-cluster)).

## Checkpoints — branching and time travel

```go
ckpt, err := sb.Checkpoint(ctx, "after-setup")  // source keeps running
branch, err := ckpt.New(ctx)                     // fresh sandbox at the captured moment
err = ckpt.Delete(ctx)
ckpts, err := client.Checkpoints(ctx)            // node's checkpoints, newest first
```

`Checkpoint` captures the sandbox's full state — memory, disk, running
processes — without stopping it (the same brief pause a fork takes), and
`ckpt.New` branches any number of independent sandboxes from that exact
moment; the checkpoint's key axes apply and `WithTimeout` may set each
branch's TTL. Successive checkpoints of sources and branches form a tree.
Checkpoints live in the node's checkpoint store — a shared FUSE mount or
`checkpoint_store: s3` object storage lets any node branch them; handles
stay owner-bound like templates; `client.Checkpoints` lists the connected
node's. Checkpoint creation is resource-creating and takes the api token,
like fork.

## Language servers (LSP)

```go
lsp, err := sb.StartLsp(ctx, "python", "/workspace")  // flavor image provides the server
conn, err := lsp.Request(ctx)                          // JSON-RPC byte stream (frame it yourself)
// ... speak LSP over conn (Content-Length framed JSON-RPC) ...
err = lsp.Stop(ctx)
```

`StartLsp` spawns the language server the flavor image ships for the
language (its argv in `/etc/silkd/lsp.d/<language>`; the python flavor bakes
`pylsp`); the base image has none, so it returns silkd's typed `not_found`.
silkd is a broker — it pipes JSON-RPC bytes between your `Request` stream
and the server's stdio without parsing LSP semantics, so the caller frames
(Content-Length) and correlates by request id. A server serves one
`Request` stream for its lifetime: closing the stream ends the session and
reaps the server (start a new one to keep working); `Stop` kills it early.

## Reaching guest ports

```go
conn, err := sb.DialPort(ctx, 8080)          // net.Conn to 127.0.0.1:8080 in the guest
l, err := sb.ProxyPort(ctx, "127.0.0.1:0", 8080)  // local listener piping to it
```

`DialPort` opens a TCP connection to a port inside the sandbox, relayed over
the silkd protocol — it works on the no-network lane, where the vsock relay
is the only way in. The returned `net.Conn` supports half-close
(`CloseWrite`) but not deadlines; bound the ctx instead. A dead port fails
with silkd's `not_found`. `ProxyPort` serves it to unmodified local tools
(browsers, curl) via a local listener.

### Preview URLs

```go
url, err := sb.PreviewURL(ctx, 8080, 30*time.Minute)
```

Mints a signed, shareable URL serving the guest HTTP port from a plain
browser via the node's preview listener. The TTL is clamped to the claim's
remaining lease, and the URL dies with the sandbox — release or reap
revokes it with no extra state. Answers 501 when the node has no
`preview_listen` configured; see [deploy](deploy.md#preview-urls).

## Node info

```go
info, err := client.Info(ctx)   // *NodeInfo: Pools, Claimed, Hibernated, Archived, Peers
```

## Running commands

```go
out, err := sb.Exec(ctx, "python3", "script.py")   // stdout; *ExitError on rc != 0

code, err := sb.Run(ctx, sandbox.Cmd{
    Argv:   []string{"bash", "-c", "make test"},
    Cwd:    "/work",
    Env:    map[string]string{"CI": "1"},
    User:   "ubuntu",
    Stdin:  strings.NewReader(input),
    Stdout: os.Stdout,
    Stderr: os.Stderr,
})
```

`Cmd` fields: `Argv` (required), `Cwd`, `Env`, `User` (de-escalation inside
the guest), `Session` (run inside a persistent session, below), `Stdin`
(nil closes the child's stdin immediately; do not share one blocking reader
across Runs), `Stdout`/`Stderr` (nil discards).

Non-zero exits surface as `*sandbox.ExitError{Code, Stderr}` from `Exec`
(alongside partial stdout); `Run` returns the code directly.

## Background processes

```go
pid, err := sb.Spawn(ctx, sandbox.Cmd{Argv: []string{"sh", "-c", "make build"}})
procs, err := sb.Ps(ctx)                          // []wire.ProcInfo{PID, Argv, Detached, State, ExitCode, ...}
code, exited, err := sb.Logs(ctx, pid, w, nil)    // replay the bounded output ring
code, exited, err = sb.Attach(ctx, pid, w, nil)   // replay, then follow live until exit
err = sb.Kill(ctx, pid, 0)                        // 0 = SIGKILL
```

`Spawn` returns as soon as the process starts; it keeps running with a
bounded output ring. `Logs` replays the ring and reports the exit code once
the process has ended (`exited=false` while running); `Attach` follows live
output until exit — the replay and the live stream hand off atomically, so
no chunk is lost or doubled between them. Killing an already-exited process
is a no-op success (its OS pid may be recycled; silkd never signals a
reaped child).

## Sessions

A session is a real persistent shell: cwd, env and shell state survive
across calls.

```go
sess, err := sb.NewSession(ctx,
    sandbox.WithSessionCwd("/work"),
    sandbox.WithSessionEnv(map[string]string{"PATH": "…"}))
out, err := sess.Exec(ctx, "export", "MARK=1")     // persists
out, err  = sess.Exec(ctx, "sh", "-c", "echo $MARK")
err = sess.Close(ctx)

ids, err := sb.Sessions(ctx)                       // live session ids
```

Idle sessions are reaped guest-side after 30 minutes.

## Files

```go
err  := sb.WriteFile(ctx, "/work/a.txt", data, nil)   // atomic; *uint32 mode optional
data, err := sb.ReadFile(ctx, "/work/a.txt")
ents, err := sb.ListDir(ctx, "/work")                  // []wire.DirEntry{Name,Kind,Size}
info, err := sb.Stat(ctx, "/work/a.txt")               // wire.FileInfo{Kind,Size,Mode,MtimeEpochSecs}
err  = sb.Mkdir(ctx, "/work/sub", true)                // parents
err  = sb.Remove(ctx, "/work/sub", true)               // recursive
err  = sb.Rename(ctx, "/a", "/b")
```

Writes stream any size and commit via temp-file rename: a mid-stream
failure never leaves a truncated destination, and overwriting an executable
keeps its exec bit.

## Project trees

```go
err = sb.Push(ctx, "/work", tarReader)   // extract a tar stream under /work
err = sb.Pull(ctx, "/work", tarWriter)   // stream /work back as a tar
```

`Push` is the only project-ingestion path on the no-network lane.

## Search

```go
matches, err := sb.Find(ctx, "/work", `TODO|FIXME`, "*.go")
// []wire.Match{File, Line, Content}; glob is anchored *? wildcards on the file name

results, err := sb.Replace(ctx, []string{"/work/main.go"}, `foo`, "bar")
// []wire.Replaced{File, Replacements}; per-file atomic
```

Patterns are regular expressions evaluated in the guest — no shell quoting.

## Watching

```go
w, err := sb.Watch(ctx, "/work", true)
defer w.Close()
for ev := range w.Events() {           // wire.Event{Kind, Path}
    fmt.Println(ev.Kind, ev.Path)      // created|modified|deleted|renamed
}
err = w.Err()                          // why the stream ended; nil after Close
```

`Watch` returns once the guest acknowledges the watch is armed — events
caused after it returns are guaranteed captured. A bad path fails
synchronously; if the consumer falls too far behind, `Err` reports a terminal
overflow instead of the stream silently dropping events.

## Git

```go
err  = sb.GitClone(ctx, url, "/work/repo", "main", 0, token) // egress lane only; depth > 0 = shallow
st,  err := sb.GitStatus(ctx, "/work/repo")   // Branch, Ahead, Behind, Files[]
err  = sb.GitAdd(ctx, "/work/repo", "a.txt")
hash, err := sb.GitCommit(ctx, "/work/repo", "message", "Dev <dev@example.com>")
err  = sb.GitPush(ctx, "/work/repo", token)   // egress lane only
err  = sb.GitPull(ctx, "/work/repo", token)   // egress lane only
br,  err := sb.GitBranches(ctx, "/work/repo") // Current + Branches
err  = sb.GitCreateBranch(ctx, "/work/repo", "feature")
err  = sb.GitCheckout(ctx, "/work/repo", "feature")
err  = sb.GitDeleteBranch(ctx, "/work/repo", "feature")
```

Results are structured (porcelain v2 under the hood), never scraped stdout.
Auth tokens travel as an in-memory header, never touching guest disk. On the
no-network lane, clone/push/pull fail fast with a typed `unimplemented`
error pointing at `Push`.

## Terminals

```go
pty, err := sb.OpenPty(ctx, sandbox.PtyOpts{Cols: 120, Rows: 40})
defer pty.Close()
pty.Write([]byte("make test\n"))
io.Copy(os.Stdout, pty)                   // EOF when the shell exits
code, ok := pty.ExitCode()
err = pty.Resize(ctx, 200, 50)
```

A PTY is a tracked guest process (`pty.PID`); closing the handle (or the
ctx) tears the shell down.

## Node operations

Root-token verbs for operating a node, plus the reference the aggregated
apiserver claims under:

```go
sb, _ := client.New(ctx, "rt:24.04", sandbox.WithClaimRef("ns/workload"))
list, _ := client.Sandboxes(ctx)          // id, key, deadline, claim_ref — never tokens
info, _ := client.Drain(ctx)              // cordon: refuse new claims, run leases out
info, _ = client.Uncordon(ctx)
info, _ = client.SetPools(ctx, pools)     // retune warm targets without a restart
info, _ = client.SetPoolsCluster(ctx, pools)
sb = client.Attach(ownerAddr, id, token)  // bind a known handle, no lookup round-trip
```

`Sandboxes` is scoped to the calling token, so a tenant sees only its own
claims. `Drain` leaves live claims alone — poll `Info` until `Claimed` is zero.

## Error handling

- `*sandbox.ExitError` — non-zero exit from `Exec` (`Code`, `Stderr`)
- `*wire.ErrorResp` — a typed guest-side failure; `Kind` is one of
  `wire.KindBadRequest`, `KindNotFound`, `KindUnimplemented`,
  `KindInternal` (import `github.com/cocoonstack/sandbox/protocol/wire`)

```go
var e *wire.ErrorResp
if errors.As(err, &e) && e.Kind == wire.KindUnimplemented {
    // no-network lane: fall back to sb.Push
}
```

Context cancellation is honored on every call: canceling the ctx closes the
underlying connection and the call returns `ctx.Err()`.
