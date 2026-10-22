# Tenant managed persistent volumes

Managed volumes keep a tenant's data disk when a sandbox claim is released or
replaced with a different template. The node-local API supports create, inspect,
list, and conditional delete. Claims can attach managed disks to warm VMs with
durable single-writer exclusion. This builds on the existing
[read-only live volume API](live-volumes.md).

This implements [issue #300](https://github.com/cocoonstack/sandbox/issues/300)
with these limits:

- Explicit hibernate and wake support one mounted writable managed volume when
  the node enables a workload hook. Idle hibernation skips volume claims, and
  attach-only or multi-volume claims cannot sleep.
- Growth extends a volume that no sandbox holds. Growing a held volume answers
  409 naming the holder; online growth waits for cocoon to wrap Cloud
  Hypervisor's disk resize.
- A claim naming a volume on another mesh node is redirected to that node.
- Archive, checkpoint, fork and promote refuse claims that hold volumes.

## Configure one node

Set a nonempty `api_token` and an absolute, clean `managed_volume_dir` in the
sandboxd configuration. The managed directory must be private to the daemon,
on persistent storage, and outside disposable VM working directories. Every
existing ancestor must be owned by root or the daemon user. The daemon creates
private `disks/`, `registry.json` and `lock` entries; it exclusively locks the
registry directory.

Startup rejects managed stores inside `data_dir/readers`, `data_dir/goldens`
and top-level `data_dir/fork-*` trees before creating files. The check includes
existing symbolic links at `data_dir`, `readers` and `goldens`. A store such as
`data_dir/managed-volumes` remains valid. Keep these directories under trusted
host control; validation does not detect bind-mount aliases or prevent a
concurrent pathname swap.

```json
{
  "api_token": "replace-with-your-root-token",
  "managed_volume_dir": "/var/lib/sandboxd/managed-volumes",
  "sync_claims": true
}
```

Volumes are raw sparse files. Put `managed_volume_dir` on xfs or btrfs where
possible, so later copies can use reflinks. `disk_reserve` does not watch this
directory when it is on another filesystem. Set `sync_claims`: the claim
journal is what keeps a writer's lock across a power loss.

Keep the registry and backing files together. Do not resize, rename, replace or
hardlink a backing file by hand. Each managed resource records its immutable
ID, owner, node, capacity and backing device/inode/size. An identity mismatch
requires operator recovery and fails closed. The volume then reports state
`recovery_required`, and PUT and claims return `503` with code
`volume_recovery_required` and no `Retry-After`. If the backing file is gone,
DELETE with the identity `If-Match` drops the record of the unheld volume. A backing that
is present but replaced keeps refusing DELETE, so the daemon never removes a
file it did not create.

The existing static catalog remains operator-owned. Its files and every path
component must be controlled by trusted operator or daemon principals. Static
catalog paths must not alias private managed storage, and a held name must
continue to identify the same backing. Stable symlink aliases and replacements
before attachment are checked. These checks precede the runtime's later path
open; they cannot defeat a concurrent host pathname swap. An untrusted actor
must not be able to change a catalog path component.

## Manage a resource

Use an explicit root or tenant management token. A sandbox token alone cannot
manage resources or call the existing live volume endpoints. Tenant tokens can
create and attach only their own managed resources; root must name an existing
tenant owner when creating one.

| Request | Success | Refusal or retry behavior |
| --- | --- | --- |
| `PUT /v1/volumes/work` with `{"size_gib":20}` | `201`, `volume`, identity `ETag` | Identical retry returns `200` and the same identity; changed owner or capacity returns `409`. |
| `GET /v1/volumes` | Catalog and managed summaries visible to caller | Managed summaries include identity and owner; use individual GET for holders. |
| `GET /v1/volumes/work` | Managed detail with public holder IDs, modes and phases | Foreign and unknown names share `404`; host paths and process IDs stay private. |
| `POST /v1/volumes/work/grow` with identity `If-Match` and `{"size_gib":40}` | `200` with `volume` at the new size | The current size returns `200` unchanged; a smaller size or a held volume returns `409`; wrong identity returns `412`. |
| `DELETE /v1/volumes/work` with `If-Match: "vol_…"` | `204` | Missing precondition `428`; wrong identity `412`; held resource `409`. |

Capacity is an integer from 1 through 1024 GiB. The backing is a raw sparse file;
logical capacity is not reserved physical space. Storage exhaustion can still
occur while guests write. Creation does not format or mount a filesystem.
Deletion keeps no record. Replaying a completed deletion returns `404`.
Replaying an old identity after a same-name resource is recreated returns `412`
and leaves the new resource in place.

The Go SDK exposes `CreateVolume`, `GetVolume`, `GrowVolume`, and `DeleteVolume`. Managed detail
uses `ManagedVolumeInfo`; existing `VolumeInfo` remains comparable. Structured
`ManagedVolumeError` unwraps to `APIError`. Python exposes `create_volume`,
`get_volume`, `grow_volume`, and `delete_volume`; structured fields remain on `APIError`.
Both SDKs send names, sizes and IDs as given. The daemon is the only validator,
and its `400` comes back as `APIError`.

## Format once, reuse across templates

The initial raw disk requires explicit filesystem setup. This Python example
uses an attach-only claim, waits for the disk serial, formats it once and
unmounts it before release. Rerunning the example against an already formatted
resource deliberately fails the blank-disk assertion.

```python
from cocoonsandbox import Client

client = Client("127.0.0.1:7777", api_token="tenant-management-token")
volume = client.create_volume("work", 20)
with client.new("rt:24.04", volumes=[{"name": "work", "mode": "rw"}], mount=False) as sb:
    device = sb.exec(
        "sh", "-c",
        'for i in $(seq 1 200); do for p in /sys/block/*/serial; do '
        '[ "$(cat "$p")" = work ] && { echo "/dev/$(basename "$(dirname "$p")")"; exit 0; }; '
        'done; sleep 0.01; done; exit 1',
    ).strip()
    sb.exec("sh", "-c", 'blkid "$1" >/dev/null 2>&1; [ "$?" = 2 ]', "probe", device)
    sb.exec("mkfs.ext4", "-q", "-F", device)
    sb.exec("mkdir", "-p", "/workspace")
    sb.exec("mount", device, "/workspace")
    sb.write_file("/workspace/marker", b"persistent")
    sb.exec("sync", "-f", "/workspace/marker")
    sb.exec("umount", "/workspace")

with client.new("new-template:local", volumes=[{"name": "work", "mode": "rw", "mount": "/workspace"}]) as sb:
    assert sb.read_file("/workspace/marker") == b"persistent"

client.delete_volume("work", volume["id"])
```

A reader excludes a writer, and a writer excludes both readers and other
writers. Multiple readers may share a ready disk. Live additions and removal
of managed entries are deferred, including mounted read-only managed entries.
A running managed claim can still change mounted read-only catalog entries
through the existing `GET/PUT/PATCH /v1/sandboxes/{id}/volumes` interface.
Attach-only managed entries retain their empty mount across those changes and
across daemon restart.

## Grow an unheld volume

Growth extends the raw backing file of a volume that no sandbox holds. Release
every claim of the volume first; a held volume returns `409` with code
`volume_busy` and its holder list. The target is an absolute size from 1
through 1024 GiB.

```python
volume = client.get_volume("work")
client.grow_volume("work", volume["id"], 40)
```

The equivalent HTTP request is:

```http
POST /v1/volumes/work/grow
Authorization: Bearer tenant-management-token
If-Match: "vol_resource_identity"
Content-Type: application/json

{"size_gib":40}
```

`200` returns the `volume` envelope at the new `size_bytes`. The identity ETag
does not change. Repeating the current size returns `200` and changes nothing,
so the same request is safe to retry after a lost response. A smaller size
returns `409`, and so does a volume whose growth is still in progress. A wrong
resource ID returns `412`, a missing `If-Match` returns `428`, and invalid JSON
or size returns `400`. Foreign and unknown volumes both return `404`.

Growth does not extend a partition or filesystem. Extend the guest filesystem
after the next claim attaches the disk.

The daemon records the target in the registry before it extends the file, and
commits the new size once the file is extended and synced. A daemon that stops
in between settles the volume at next start to whichever size the file has. A
failed final registry write returns `503` with `Retry-After`; the daemon keeps
the new size and retries the write.

## Hibernate one mounted managed volume

The node can enable explicit Hibernate and Wake for one managed writable disk
on a `net: none` claim. The applied, desired, and held volume sets must match.
Attach-only volumes, additional disks, catalog mixtures, egress, replication,
and archive remain unsupported. Idle hibernation continues to skip all volume
claims. Existing API and SDK methods take no new arguments.

Install the same workload hook at an absolute path in every supported guest
image, together with GNU `timeout` and util-linux `flock` at `/usr/bin`.
Enable the hook in node configuration and restart sandboxd.

```json
{
  "managed_volume_dir": "/var/lib/sandboxd/managed-volumes",
  "managed_volume_hibernate": {
    "hook_argv": ["/usr/local/bin/workload-volume"],
    "hook_timeout_seconds": 10
  }
}
```

`hook_timeout_seconds` accepts 1 through 300. The daemon appends five separate
arguments to `hook_argv`.

```text
prepare|resume operation_id volume_id volume_name exact_mount_path
```

`prepare` must stop workload writers and close their filesystem references
before returning success. The daemon then verifies the mount, unmounts without
force, detaches the disk, and captures VM memory. `resume` restarts writers
after the exact backing is attached and mounted. It also compensates for a
failed preparation. Both actions must be idempotent for the same operation ID.
The hook must not detach from its supervised process group or close inherited
lock descriptors while descendants continue working.

Hook calls execute as structured argv under a guest lock at
`/run/sandboxd-volume-hook.lock`. GNU `timeout` kills the hook's process group
at its execution limit. Lock acquisition has the same time limit, and the host
RPC allows both limits plus a drain interval. A failed `prepare` fails the
hibernate: the daemon reattaches and remounts the disk if needed, runs `resume`
once, and the claim keeps running. Once the host has verified the mount, a
failed `resume` is logged and not retried, so the guest stays awake with its
writers stopped. A hook that deliberately escapes supervision is outside this
contract.

A successful hibernate keeps the managed hold and backing file. Another claim
or a volume deletion receives 409 while the owner sleeps. Wake verifies the
backing's path, device, inode, and size before restoring memory. A missing or
replaced backing leaves the claim paused. Restore the original backing before
retrying wake. A restore that fails before any guest runs leaves the claim
hibernated, and the next wake restores from the same snapshot. A partial wake
keeps the restored VM alive and retries attach, mount, and resume on that same
VM, which cocoon identifies by VM ID and a transition generation past the one
recorded at hibernate. It never restores old memory over a running VM. If that
VM dies after its snapshot was consumed, recovery refuses rather than replaying
stale memory against changed disk contents.

Existing data streams end when capture stops the VM. Explicit and data-plane
wake publish access only after remount and resume. Passive operator access
stays paused during incomplete transitions. Readers never serve the detached
snapshot.

An effective outbound policy, including a proxy on the `none` lane, blocks managed
hibernation before it changes the running guest. Wake never consults outbound
policy, so a policy reload while the claim sleeps cannot strand it. Release and
lease expiry never wait for a hibernate or wake in flight: they remove the VM
and snapshot, the in-flight step fails at its next VM call, and the hold is
freed once cocoon confirms the VM gone. The managed disk is preserved. A claim
released or expired after prepare detached its disk leaves the disk clean, so
read-only claims can attach it at once; one torn down mid-prepare or mid-wake
needs a writable claim to replay it first.

The sleep record, including the selected hook and operation ID, lives on the
claim's `claims.json` row. Retries use them even after a configuration change
or disabling new managed sleeps. Each step's intent lands in `claims.json`
before its effect, so a daemon crash resumes from the last step with either
value of `sync_claims`. With `sync_claims` off, host power loss can leave a
stale record; the claim may then be release-only. Once wake has verified the
remount, a failed final write is retried in the background and wake still
succeeds; a crash before it lands runs `resume` once more on the same VM.

## Placement

A volume lives on one node. Each node advertises a tenant-scoped key for each
of its managed volumes, never the name, so peers cannot list or probe another
tenant's volumes. A tenant claim that names a volume on a peer is redirected to
that peer, even from an entry node without `managed_volume_dir`. Another tenant
naming it gets the same 400 as an unknown name. When
the volume is local but no node holds both the volume and the requested
template, the claim answers 409. Root claims are not redirected to managed
volumes; claim through the owner tenant or on the owner node.

PUT refuses with 409 when another mesh node already advertises the same owner's
name, and the message names that node's client address. The check reads the
gossiped view, so two PUTs for one name on different nodes within about one
gossip interval can both succeed. Create each volume from one place, and
delete the duplicate if that happens.

## Ownership and recovery

`claims.json` is the only journal of who holds a volume. The registry records
resources only: per volume its ID, owner, node, size, pending growth, backing
identity and state. A claim holds its volumes from admission until cocoon
confirms its VM gone, and restart derives the hold from the claim row, as it
does for catalog volumes. Release, lease expiry and egress quarantine follow
the ordinary claim paths and never write the registry. A VM that survives
removal stays queued for the reap tick and keeps its volumes held; a refused
claim reports that holder with state `releasing`.

At startup, an unclaimed running `sbx-` VM that cannot be removed is queued the
same way, holding the disks cocoon reports attached. If its disks cannot be
inspected, it holds every managed name until a later tick inspects or removes
it. A claim whose VM is gone is dropped and its volumes freed. A failed VM
listing prevents startup and leaves claims, VMs and files untouched.

A registry that cannot be parsed, or that disagrees with the backing
directory, prevents startup. A failed registry write keeps the new state in
memory and retries in the background. Until it lands, create, delete, growth
and new managed claims answer 503 with `Retry-After`, while running claims keep
their sockets and release normally. GET, listing, and placement checks do not
persist recovery state, so a temporary backing inspection error cannot
permanently disable a healthy volume.

An interrupted allocation with no recorded backing inode stays `creating`. No
VM has seen that file, so repeating the same PUT replaces it with a fresh one,
and DELETE removes it. The registry has no format version.
Unknown keys are ignored and dropped on the next write; unknown lifecycle states
prevent startup.

## Upgrade

The registry and claim formats changed during development and have no
migration. Drain nodes that ran earlier builds of this branch (release every
claim that holds a managed volume) before upgrading.

## Follow-up order

1. Online growth of a held volume, once cocoon wraps Cloud Hypervisor's disk resize.
2. Archive export and restore of external data disks.
3. Idle hibernation and attach-only sleep of volume claims.
4. Kubernetes lifecycle and status ownership once the daemon contracts are stable.
