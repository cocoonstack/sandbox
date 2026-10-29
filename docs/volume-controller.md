# Hibernate a sandbox with an external volume controller

Remove a data disk before hibernation and restore it after wake, keeping the same
claim and catalog registration. The controller owns reservation and access policy.

## Prepare the claim and controller

Use a `net: none` claim on its owning node, since egress-lane claims cannot
hibernate, and keep its TTL valid across sleep. Its pool sets
`replicate_seconds: 0` and `archive_after_seconds: 0`, and the claim uses
`on_expire: "destroy"`: a detached sleeping claim holds no volume, so a replica
or an archive of it could wake on another node. Keep the pool configured, because
an unpooled key falls back to the node-wide `archive_after_seconds`. Replication
changes only at a restart; see
[disabling archive failover](deploy.md#disabling-archive-failover) before
changing it on a node with replicated claims.

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
its `VolumeHooks`. Each hook raises on failure, and the controller keeps the
reservation and the access gate through every step.

- `assert_reserved` checks the external reservation and the owning node. It is
  not a lock or a lease renewal; serialize controller operations on one claim.
- `close_access` blocks and drains business traffic on every node, including the
  guest API and direct ports. Issued preview URLs can wake the claim, and
  stopping new URL issuance does not close access.
- `quiesce_and_unmount` stops writers, flushes state, and unmounts normally,
  never lazily or by force. It raises on a busy mount, accounts for raw-device
  users, partitions, and other mount namespaces, and tolerates a disk that is
  already unmounted.
- `mount_and_verify` finds the device by its serial and mounts it as described
  in [attach-only volumes](sandboxd-api.md#attach-only-volumes), then checks the
  filesystem identity and data.
- `open_access` admits traffic only after verification succeeds.

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
Inspect the confirmed claim state and resume from that stage. Do not blindly
repeat writes or formatting. `exec("true")` proves silkd readiness, not disk
attachment or mount readiness. Recover an uncertain gate operation externally,
including a partial reopen.

After a cycle, verify the same claim, backing, serial, filesystem, and payload.
The backing is external to the memory snapshot. The repository tests mock the
HTTP API, so qualify guest unmount, disk integrity, external gates, and fencing
on the real deployment.
