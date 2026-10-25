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
metadata and does not manage disk files. Omitted or empty runtime `tenants`
lists permit every authenticated scope. List tenants explicitly to restrict
access. Live changes remain limited to mounted read-only entries.

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
This example starts without other volumes. If your sandbox has writable or
attach-only volumes, retain them in a full PUT or use PATCH to replace only
the dataset names. `None`, a nil Go slice, and a missing `volumes` field do not
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
