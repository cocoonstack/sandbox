# Hibernate a sandbox with an external volume controller

Remove a data disk before hibernation and restore it after wake, keeping the same
claim and catalog registration. The controller owns reservation and access policy.

## Prepare the claim and controller

Use a `net: none` claim on its owning node. Egress-lane claims cannot hibernate.
Keep its TTL valid across sleep. Set `replicate_seconds: 0` and
`archive_after_seconds: 0` on its pool, and `on_expire: "destroy"` on the claim.
Keep that pool configured to avoid the node-wide archive fallback. An archived
or archiving claim refuses renew, so renewal cannot replace this configuration.
Keep replication disabled even while detached, when the claim holds no volume.
Changing replication requires a daemon restart. When migrating replicated claims
or changing failover, follow [disabling archive failover](deploy.md#disabling-archive-failover)
for retained replica or archive intent and recovery risks.

Use Cloud Hypervisor, Cocoon v0.7.2 or newer, and the current Python SDK with
[per-entry attach-only support](sdk-python.md#changing-live-volumes).
Prepare and format the backing outside sandboxd. Follow the
[registration and backing requirements](sandboxd-api.md#put-v1volumesname),
including stable paths outside daemon cleanup directories. Register on the
claim's node with `writable: true`, the configured root token, and an explicit
[tenant access list](sandboxd-api.md#volume-access-lists).

Hold a durable external reservation throughout use, detached sleep, and recovery.
Prevent other root or tenant callers from attaching the backing while detached.
A tenant access list is not a reservation. On owner change, keep access closed
and fence the old node before attaching elsewhere. Node-local admission is not
a distributed single-writer lock.

## Supply the application hooks

Copy the [tested example](https://github.com/cocoonstack/sandbox/blob/main/sdk/python/examples/volume_controller.py) and implement
its `VolumeHooks` contracts. Gate and drain API and preview traffic on every node,
including guest API access and direct ports. Issued preview URLs can wake the
claim. Stopping new URL issuance does not close access.
Stop writers, flush state, and ordinarily unmount before detach. Never use lazy
or forced unmount. Keep the reservation and access gate through every step.

Use one attach-only writable volume. The controller owns filesystem consistency
and crash recovery. Attach-only RW writes no `.dirty` marker. An automatically
mounted RW volume that the controller unmounts externally retains its marker,
because sandboxd cannot confirm the clean unmount. See the
[dirty-marker rules](sandboxd-api.md#patch-v1sandboxesidvolumes).

Bind a root client to the existing tenant claim. The root token authorizes
volume changes. The sandbox token authorizes guest commands and hibernation.

```python
from cocoonsandbox import Client

root = Client(owner_addr, api_token=root_node_token)
operator = root.attach(owner_addr, sandbox_id, sandbox_token)
```

Call `resume_volume` for initial attachment to an empty running claim or after
sleep. It wakes silkd, attaches, mounts, and verifies before opening access.
Call `hibernate_volume` after use. Do not use a context manager that releases
the claim when the sleep function returns. Neither function releases the reservation.

## Recover with access closed

For attach or detach failures, follow [partial failure and recovery](sandboxd-api.md#partial-failure-and-recovery).
Require a successful remove response before hibernation. An empty GET view does
not acknowledge durable retirement. After a failed, timed-out, or lost response,
retry the same remove target until acknowledged. Any remaining desired, applied,
or per-volume state, or ongoing convergence, prevents sleep.

On any failure or cancellation, retain the reservation and keep access closed.
Inspect the confirmed claim state and resume from that stage. Make guest cleanup
safe to retry after unmount. Do not blindly repeat writes or formatting.
`exec("true")` proves silkd readiness, not disk attachment or mount readiness.
Attach, mount, and data checks must succeed before business traffic resumes.
Recover an uncertain gate operation externally, including a partial reopen.

After a cycle, verify the same claim, backing, serial, filesystem, and payload.
The backing is external to the memory snapshot. The mocked HTTP tests check
SDK compatibility and failure ordering. They do not prove guest unmount, disk
integrity, external gates, or fencing. Qualify those on the real deployment.
