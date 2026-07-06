# Go SDK

```go
import sandbox "github.com/cocoonstack/sandbox/sdk"
```

The SDK is stdlib-only. One `Client` talks to one entry node; sandbox
handles dial their owning node directly, so a client works unchanged against
a single node or a cluster.

## Connecting

```go
client, err := sandbox.Connect("10.0.0.5:7777",
    sandbox.WithAPIToken(os.Getenv("SANDBOXD_TOKEN")))
```

- `Connect(addr, opts...)` — `addr` accepts a comma-separated seed list for
  forward compatibility; the current version uses the first entry.
- `WithAPIToken(token)` — authenticates node-level calls (claim, info).
  Required when the nodes are configured with `api_token`; on a cluster all
  nodes share one token.

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
    sandbox.WithTimeout(10*time.Minute))
defer sb.Close()
```

| option | values | default | meaning |
|---|---|---|---|
| `WithNetwork(n)` | `NetNone`, `NetEgress` | `NetNone` | `NetNone`: no NIC at all, vsock-only I/O (hardened lane, Firecracker). `NetEgress`: bridge/CNI NIC (Cloud Hypervisor) |
| `WithSize(s)` | `Small`, `Medium`, `Large` | `Small` | resource tier: 1cpu/512M, 2cpu/1G, 4cpu/4G |
| `WithTimeout(d)` | duration | server default 5m | sandbox TTL, rounded up to seconds, server-capped at 24h. The node reaps the sandbox after the TTL even if the client vanishes |

`New` returns when the sandbox's silkd answers: a warm hit is milliseconds,
a cold key can take the full boot. `Sandbox.ID` and `Sandbox.Deadline` are
exported; `Close()` releases the sandbox (releasing one already gone is not
an error, and `Close` is bounded internally so it stays defer-friendly).

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
is your policy; the node only provides the transition.

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
All-or-nothing: on error no child survived. Count is capped at 16 per call.
Fork and Promote create node resources, so on a token-guarded node the
client needs `WithAPIToken` — a sandbox handle alone cannot amplify.

## Promoting to a template

```go
err := sb.Promote(ctx, "myproj:v1")            // publish current state
tpl, err := client.New(ctx, "myproj:v1")       // clones the promoted state
err = client.DeleteTemplate(ctx, "myproj:v1")  // caller owns the lifecycle
```

`Promote` publishes the sandbox's state as a template on its owning node,
keyed by (name, the sandbox's network lane, its size) — claims must use the
same lane and size. Claims clone on demand (~a golden-clone's latency);
there is no warm pool for promoted templates unless the node's config adds
one. Templates are node-local; re-promoting replaces, `DeleteTemplate`
(with `WithNetwork`/`WithSize` when non-default) removes.

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

## Node info

```go
info, err := client.Info(ctx)   // *NodeInfo: Pools, Claimed, Hibernated, Peers
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
ents, err := sb.ListDir(ctx, "/work")                  // []silkd.DirEntry{Name,Kind,Size}
info, err := sb.Stat(ctx, "/work/a.txt")               // silkd.FileInfo{Kind,Size,Mode,MtimeEpochSecs}
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
// []silkd.Match{File, Line, Content}; glob is anchored *? wildcards on the file name

results, err := sb.Replace(ctx, []string{"/work/main.go"}, `foo`, "bar")
// []silkd.Replaced{File, Replacements}; per-file atomic
```

Patterns are regular expressions evaluated in the guest — no shell quoting.

## Watching

```go
w, err := sb.Watch(ctx, "/work", true)
defer w.Close()
for ev := range w.Events() {           // silkd.Event{Kind, Path}
    fmt.Println(ev.Kind, ev.Path)      // created|modified|deleted|renamed
}
err = w.Err()                          // why the stream ended; nil after Close
```

`Watch` returns once the guest acknowledges the watch is armed — events
caused after it returns are guaranteed captured. A bad path fails
synchronously.

## Git

```go
err  = sb.GitClone(ctx, url, "/work/repo", "main", token) // egress lane only
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

## Error handling

- `*sandbox.ExitError` — non-zero exit from `Exec` (`Code`, `Stderr`)
- `*silkd.ErrorResp` — a typed guest-side failure; `Kind` is one of
  `silkd.KindBadRequest`, `KindNotFound`, `KindUnimplemented`,
  `KindInternal` (import `github.com/cocoonstack/sandbox/sdk/silkd`)

```go
var e *silkd.ErrorResp
if errors.As(err, &e) && e.Kind == silkd.KindUnimplemented {
    // no-network lane: fall back to sb.Push
}
```

Context cancellation is honored on every call: canceling the ctx closes the
underlying connection and the call returns `ctx.Err()`.
