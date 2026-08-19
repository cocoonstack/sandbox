# filecache

Node-local write-back cache layer for CTO-consistent NAS workspaces
(ByteNAS et al.). Single static Go binary, no external dependencies.

## Problem

Small-file workloads pay one synchronous NAS round trip per metadata
operation (measured 1.4–2.0 ms/op on ByteNAS; ~4 SETATTR + OPEN + WRITE +
CLOSE per file for an unpack). A 100k-file install runs 100–240x slower
than on a local disk. Mount options do not change this; the NAS serializes
mutations per directory.

## Approach

Session-granular close-to-open consistency:

- **attach**: acquire a lease file on the shared workspace (O_EXCL, atomic
  on NFSv4; heartbeat refresh, TTL takeover), then mount an overlayfs with
  upper/work on local NVMe and the NAS workspace as lower. Apps use the
  merged view at local latency (kernel overlay, no FUSE).
- The overlay upper **is** the dirty set: creates, copy-ups, whiteouts
  (deletions) and opaque dirs record every change without scanning.
- **detach** (barrier): unmount, replay the delta onto the NAS — deletions
  first, then directories level-parallel, then file bodies per-directory
  parallel (default 20 workers; files within one directory stay serial to
  match server-side per-directory serialization), symlinks last. Files land
  via temp name + rename (atomic visibility per file); umask 0 applies the
  final mode at CREATE, avoiding a SETATTR per file. Release the lease.
- After the barrier, any other client's open sees the final state through
  the NAS's native CTO semantics. During the session the lease excludes
  other writers, so the invisible intermediate states have no legal reader.
- Crash on the node: the upper persists on local disk; `attach` of the same
  workspace id on the same node reclaims its own lease and resumes. Losing
  the node loses at most the un-published delta (RPO = last barrier).

## Usage

```bash
M=$(filecache attach --id ws1 --shared /mnt/nas/<fs>/<workspace>)  # prints merged path
# ... run the workload against $M ...
filecache sync   --id ws1        # optional live checkpoint (reduces RPO)
filecache detach --id ws1        # barrier: publish delta, release lease
filecache status                 # dirty-set stats per workspace
```

Flags: `--state-root` (default /data00/filecache), `--workers` (20),
`--preserve-mtime` (true), `--lease-ttl` (90s), `--merged`, `--no-sync`,
`--no-resume`.

## Measured (safebox-test, ByteNAS mycisb, 2026-08-12)

| Metric | direct ByteNAS | filecache |
|---|---|---|
| yarn install, 78,256-file node_modules | 212.6 s | **11.6 s** (local-disk parity) |
| barrier publish of 90,012 entries | — | 37.6 s (~2,400 entries/s, the NAS 20-way ceiling) |
| cross-client visibility after barrier | CTO | CTO (verified: content, modes, symlinks, dotfiles, whiteout deletions, exact file count) |

## Known limits

- Hard links are published as independent copies.
- overlay metacopy is rejected (mount without metacopy; default on this fleet).
- Concurrent cross-node writers to one workspace are excluded by design
  (lease); workloads needing that must bypass the cache layer.
- fsync inside the session is local-durable only; use `sync` for a
  NAS-durable checkpoint.

## Build

```bash
GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o filecache .
```
