# Replace a dataset in a running sandbox

This example replaces `dataset-v1` with `dataset-v2` at `/datasets/current`
while preserving the sandbox, session, guest boot, and independent process.

Register both read-only catalog entries on the owning node and grant the
sandbox's tenant access to them. Put `v1` and `v2` in each image's `/version`
file. Keep each name bound to the same immutable image while held. Use Cocoon
v0.7.2 or newer and set `SANDBOX_ADDR` and `SANDBOX_API_TOKEN` for the client.
A sandbox token alone cannot manage volumes.

A configured node root token can [register backing paths](sandboxd-api.md#put-v1volumesname)
without editing static config. Registration persists only node-local catalog
metadata and does not manage disk files. An empty runtime `tenants` list allows
only root to initiate attachment, including attachment to a tenant-owned claim.
Keep `api_token` configured. List tenants explicitly to grant self-service;
that permission also allows reattachment to another claim after detach. For a sleep and wake workflow, an
external controller owns quiescence and unmount before detach, a reservation
through sleep, and access gating until wake, attach, and mount complete. That
gate also covers business requests that wake the sandbox. Wake establishes
only silkd readiness. The API does not include a controller implementation or
verify that composed workflow. Live changes support mounted and attach-only
entries in read-only or writable mode.

Pause application reads before replacement. Close files, mmap mappings, cwd,
and bind mount references on the old disk. Replacement unmounts the old disk
before mounting the new one, so the path has a short unavailable interval.
The independent process below uses `/tmp` and can continue running.

```python
import os

from cocoonsandbox import Client


def process_identity(sb, session, pid):
    stat = sb.exec("cat", f"/proc/{pid}/stat")
    starttime = stat.rsplit(") ", 1)[1].split()[19]
    boot = sb.exec("cat", "/proc/sys/kernel/random/boot_id").strip()
    return sb.id, session.id, boot, pid, starttime


client = Client(os.environ["SANDBOX_ADDR"], api_token=os.environ["SANDBOX_API_TOKEN"])
v1 = [{"name": "dataset-v1", "mount": "/datasets/current"}]
v2 = [{"name": "dataset-v2", "mount": "/datasets/current"}]

with client.new("rt:24.04", ttl_seconds=900) as sb:
    with sb.session(cwd="/tmp", env={"DATASET_SESSION": "same"}) as session:
        pid = sb.spawn("sleep", "3600", cwd="/tmp")
        try:
            identity = process_identity(sb, session, pid)
            sb.set_volumes(v1)
            assert sb.exec("cat", "/datasets/current/version").strip() == "v1"

            sb.patch_volumes(remove=["dataset-v1"], add=v2)
            sb.set_volumes(v2)

            assert sb.exec("cat", "/datasets/current/version").strip() == "v2"
            assert process_identity(sb, session, pid) == identity
            assert session.id in sb.sessions()
            assert session.exec("printenv", "DATASET_SESSION").strip() == "same"
            view = sb.get_volumes()
            assert view["volumes_want"] == view["volumes"] == v2
            assert view["converging"] is False

            sb.set_volumes([])
            assert process_identity(sb, session, pid) == identity
        finally:
            sb.kill(pid)
```

A successful run reads `v1`, then `v2`, and preserves every identity assertion.
The repeated full target verifies that a settled mount can be retained.
This example starts without other volumes. A full PUT replaces the entire
desired set, including writable and attach-only entries. Retain other entries
in that list, or use PATCH to replace only the dataset names. `None`, a nil Go slice, and a missing `volumes` field do not
mean an empty target.

Go uses the same owning handle and client API token.

```go
view, err := sb.PatchVolumes(ctx, sandbox.VolumePatch{
	Remove: []string{"dataset-v1"},
	Add: []sandbox.Volume{{Name: "dataset-v2", Mount: "/datasets/current"}},
})
if err != nil {
	return err
}
if view.Converging {
	return errors.New("volume request still running")
}
```

For a 409 mount conflict, use the guest path in `states[].error` to close
references or remove unexpected mounts, then retry. For a transport failure or
timeout, GET the current target and confirmed set before retrying the target.
A failed call can already have changed devices. `VolumeError` retains a
complete failure view in Go's `Response` or Python's `.response`.

`pending` or `detaching` with `converging: false` needs a caller retry. GET does
not run device repair. After sandboxd restarts with the original VM still
alive, unfinished entries report an interrupted operation and keep their
admission holds. Detailed errors survive only within one daemon lifetime.
If the VMM exits, a sandbox holding volumes is release-only. Release it and
create a new sandbox; neither PUT with an empty target nor daemon restart
restores the original processes.

## Writable and attach-only entries

Use `mode: "rw"` for a catalog entry that permits writing. Set per-entry
`attach_only: true` when the caller manages the device. PUT and PATCH accept
all four modes in a mixed list:

```json
{"volumes":[{"name":"dataset"},{"name":"scratch","mode":"rw"},{"name":"raw-data","attach_only":true},{"name":"raw-work","mode":"rw","attach_only":true}]}
```

Omitting a mount still selects `/volumes/<name>` for a new mounted entry. An
attach-only entry cannot supply a nonempty mount. HTTP responses keep the
legacy `{name, mount?, mode?}` fields and omit `attach_only`. The current SDKs
infer the flag from a mountless response. Echoing that response to the same
sandbox retains the held device-only entry. An omitted or false flag does not
change that held shape. A mount or mode change requires a successful removal
before adding the name again.

The existing Go claim option `WithVolumesAttachOnly()` and Python `mount=False`
remain claim-wide aliases. The current source SDKs also encode an all-attach-only
initial list with the legacy global flag. They reject mixed mounted and
attach-only initial claims before network I/O. Claim without volumes, as in the
example above, then use the live API for a mixed list. Direct HTTP mixed initial
claims require matching daemons across the serving fleet and redirect targets.
Older claim decoders can silently ignore per-entry flags. See
[version compatibility](sandboxd-api.md#volume-version-compatibility). These
per-entry SDK features are unreleased. Published Python 0.1.19 can still echo a
same-sandbox GET response but cannot construct the new mixed request mappings.

Before removal, stop application access and close references. For attach-only
devices, unmount the caller-managed filesystem first. sandboxd checks its guest
mount namespace, but cannot prove that raw block users, partitions, or other
mount namespaces are idle. Keep the control-plane reservation across any sleep
and gate access until wake, attach, and mount complete.

For mounted writable entries, sandboxd clears its dirty marker only after
identifying the expected device at the managed root mount with writable mount
and superblock options, completing a normal unmount, confirming no residual
device mounts, and confirming host detach. The same removal must supply all
that evidence before the old hold is released.
External pre-unmount, an absent device, or a lost result leaves the
marker conservatively in place. Detach can succeed while a subsequent read-only
attach still requires recovery. Attach-only writers have no daemon-managed
marker, including after a guest crash during writes. The caller owns recovery
and must establish consistency before read-only publication. See the [API
contract](sandboxd-api.md#patch-v1sandboxesidvolumes) for failure and retry
semantics.
