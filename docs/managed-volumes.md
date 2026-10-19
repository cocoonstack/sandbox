# Tenant managed persistent volumes

Managed volumes keep a tenant's data disk when a sandbox claim is released or
replaced with a different template. This phase adds node-local create, inspect,
list and conditional delete, claim-time attachment to warm VMs, and durable
single-writer exclusion. It builds on the existing
[read-only live volume API](live-volumes.md).

This is a subset of [issue #300](https://github.com/cocoonstack/sandbox/issues/300).
Hibernate/wake, online growth, placement by volume and archive export remain
follow-up work. Attached volumes still block capture. Do not use this branch to
claim that all of #300 is complete.

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
  "managed_volume_dir": "/var/lib/sandboxd/managed-volumes"
}
```

Keep the registry and backing files together. Do not resize, rename, replace or
hardlink a backing file by hand. Each managed resource records its immutable
ID, owner, node, capacity and backing device/inode/size. An identity mismatch
requires operator recovery and fails closed.

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
| `DELETE /v1/volumes/work` with `If-Match: "vol_…"` | `204` | Missing precondition `428`; wrong identity `412`; held resource `409`. |

Capacity is an integer from 1 through 1024 GiB. The backing is a raw sparse file;
logical capacity is not reserved physical space. Storage exhaustion can still
occur while guests write. Creation does not format or mount a filesystem.
Deletion tombstones preserve the outcome of an earlier deletion. Replaying that
old identity returns `204` without deleting a newly created same-name resource.

The Go SDK exposes `CreateVolume`, `GetVolume` and `DeleteVolume`. Managed detail
uses `ManagedVolumeInfo`; existing `VolumeInfo` remains comparable. Structured
`ManagedVolumeError` unwraps to `APIError`. Python exposes `create_volume`,
`get_volume` and `delete_volume`; structured fields remain on `APIError`.

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

## Ownership and recovery

The managed registry supplements the existing flat `claims.json` journal.
Admission durably reserves the resource, then binds its VM process generation
before any disk effect. A live operation records the union of applied, desired
and possibly attached entries before changing a device. A confirmed retry can
trim that union only after device cleanup and claim-journal commit succeed.

Release durably marks a hold retiring before cancelling the existing
`volumeRun` and dropping the claim. A successful runtime removal command alone
does not free the hold. Fresh runtime inspection must report absence and the
recorded boot ID/PID/start generation must be gone. Failed writes, uncertain
attachments and delayed removal retain ownership. These rules also apply when
`sync_claims` is disabled.

A failed egress recovery fences the claim and attempts VM removal even if
managed retirement persistence fails. A stubborn VM or unavailable registry
can keep the hold for operator recovery. This phase does not promise that an
uncooperative VMM can always be killed.

After restart, the persisted egress-quarantine failure marker resumes retirement
and removal. Ordinary failed claims keep their existing ownership until release.
Unclaimed stopped VM records with no live process are removed before startup
continues. Unreadable process metadata and unconfirmed removal still block
startup. The stale-create collector and recorded process-generation checks
remain in force.

## Follow-up order

1. Add quiesce/unmount/detach/capture and restore/reattach/remount with failure
   rollback. Keep single-writer ownership while a claim is hibernated.
2. Add online growth with explicit block-device and guest-filesystem outcomes.
3. Add owner-node placement and a clear unavailable-owner response.
4. Add archive/export and restore of external data disks.
5. Add Kubernetes lifecycle/status ownership once the daemon contracts are stable.

These are proposed next steps. None is implemented or qualified by this phase.
