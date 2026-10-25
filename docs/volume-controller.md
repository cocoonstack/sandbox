# Hibernate a sandbox with an external volume controller

Use the live-volume API to remove a data disk before hibernation and restore
it after wake. This example keeps the same claim and catalog registration.
The controller owns the disk reservation and workload access policy throughout.
It does not add disk lifecycle management to sandboxd.

## Prepare the claim and controller

Use a `net: none` claim on its owning node. Egress-lane claims cannot hibernate.
Keep its TTL valid across the sleep interval. Set `replicate_seconds: 0` and
`archive_after_seconds: 0` on its pool, and use `on_expire: "destroy"` on the
claim. Keep that pool configured while the controller owns the claim. If the
pool is removed, the node-wide archive policy becomes the fallback. An
archived or archiving claim refuses renew with 409, so renewing alone cannot
preserve this sleep workflow when archiving is enabled.

Live RW additions require `replicate_seconds: 0`; a replicating pool returns
409 without recording the new target or reserving the volume. Changing this
setting requires a daemon restart. A non-replicating pool may still hold an old
replica or recovery checkpoint reference, which returns 503 with `Retry-After`.
Keep the reservation and access gate closed and retry attachment after that
claim's retirement has been acknowledged. When disabling failover, follow
[the registry transition requirements](deploy.md#disabling-archive-failover).
A fixed delay does not acknowledge retirement. See
[the live-volume retirement contract](sandboxd-api.md#partial-failure-and-recovery).

Keep replication disabled throughout this controller workflow. After detach,
the claim no longer holds a volume and could otherwise be replicated again
before wake and reattachment. RW volumes attached in the initial claim remain
supported on replicating pools, but this does not permit later live RW additions.

Use the current Python SDK with
[per-entry attach-only support](sdk-python.md#changing-volumes), Cocoon v0.7.2 or newer,
and Cloud Hypervisor.

Prepare and format the backing outside sandboxd. Register it on the claim's
node with the configured root token:

```http
PUT /v1/volumes/home
Authorization: Bearer <root-node-token>
Content-Type: application/json

{"path":"/srv/volumes/home.raw","writable":true,"tenants":[]}
```

An empty runtime `tenants` list lets only root initiate attachment. A nonempty
list grants those tenants self-service attachment to their claims, including
reuse after detach. Keep `api_token` configured. The daemon's no-token mode
treats network callers as root.

Bind a root SDK client to the existing tenant claim. Keep the sandbox token
for guest commands and hibernation; the root client token authorizes volume
changes.

```python
from cocoonsandbox import Client

root = Client(owner_addr, api_token=root_node_token)
operator = root.attach(owner_addr, sandbox_id, sandbox_token)
```

Supply these application-specific operations before running the functions below:

- `assert_reserved` verifies that the controller still exclusively reserves this
  backing for this claim. The reservation must survive controller restart and
  the entire sleep interval. Keep it exclusively held during every step and
  renew any lease as needed. An assertion alone is not a lock or lease renewal.
  Serialize operations for the same reservation. Verify the owning node too.
  An owner change requires fencing the old node before another attachment.
- `close_access` blocks and drains business requests before returning. It must
  fail closed. Gate the API and preview listeners on every node that can serve
  the claim, including direct ports and guest API access. A previously issued
  preview URL can reach any node and wake the claim. Preview tokens have no
  per-URL revocation, so stopping new URL issuance is not an access gate.
  The controller's own maintenance calls may still reach silkd.
- `quiesce_and_unmount` stops and reaps writers, flushes application state and
  filesystem writes, and ordinarily unmounts the volume. It raises if any step
  fails. Do not use lazy or forced unmount to turn a busy result into success.
- `mount_and_verify` finds the block device by its catalog serial, mounts the
  existing filesystem, and verifies its expected identity and data. It raises
  before admitting business traffic if the disk, mount, or checks are wrong.
  Start the application behind the closed gate if it needs restarting.
- `open_access` admits business traffic only after those checks have succeeded.

These callbacks are not supplied by sandboxd. An in-process lock or a local
boolean is not a durable reservation or an access gate for other callers.
The controller must enforce their contracts even when a step fails or its
process restarts. sandboxd holds no reservation for the disk once detach and
reference retirement complete. During sleep, root can attach the same catalog
entry to another claim. The controller must prevent that operation through its
own ownership policy. Root-only attachment does not prevent a guest from
accessing an already attached disk, or prevent the tenant from removing it for
cleanup.

Use one attach-only writable volume for this example. The controller mounts it
explicitly and owns filesystem consistency after a crash. sandboxd writes no
`.dirty` marker for attach-only RW. Do not substitute an automatically mounted
RW entry without accounting for its [dirty-marker rules](sandboxd-api.md#attach-only-volumes).

## Controller functions

Pass the controller operations as `VolumeHooks`.
The functions do not release the reservation. Before the final `open_access`
call, exceptions stop the sequence without attempting to open access or run
another lifecycle operation. If a gate hook fails or has an uncertain result,
that hook and the controller must recover the gate. These functions cannot
prove that an external gate closed or undo a partially completed reopen.

The [tested Python example](../sdk/python/examples/volume_controller.py) defines
`VolumeHooks`, `hibernate_volume`, and `resume_volume`. Copy that module into
your controller, then supply the callbacks described above. The SDK's regular
pytest suite exercises these functions through HTTP with mocked node replies.
Those tests check request and response compatibility and failure ordering.
They do not prove guest unmount, disk integrity, or an external access gate.

Call `resume_volume` for initial attachment to an empty running claim, or after
sleep. It wakes through a guest command, then attaches and mounts the disk.
Call `hibernate_volume` after the application has used the mounted disk.
Do not wrap the handle in a context manager that releases the claim when the
controller leaves the sleep function.

The successful remove response is the completion acknowledgment. Its empty
view is an additional check, not a replacement for that acknowledgment.
The function removes only the named volume. If another volume remains, it
refuses to hibernate rather than silently detaching that disk too.

## Recover without reopening access early

If unmount is busy, leave the disk attached and the gate closed. Find and stop
the remaining users before retrying. A guest mount check cannot prove that no
raw-device users, partition users, or other mount namespaces exist.

If a volume request fails, times out, or loses its response, keep the
reservation and gate. GET can show what remains, but an empty GET view alone
does not acknowledge durable retirement. Retry the same remove target and
require success before calling hibernate. Make the guest cleanup hook safe
for a retry where the filesystem is already unmounted. Do not blindly rerun
application writes, formatting, or an entire lifecycle sequence.

If the owning node changes, treat the claim as a new placement even if its ID
is unchanged. Keep access closed and fence the old node from the backing before
reattaching on another node. Node-local admission and catalog registration do
not provide a distributed single-writer lock. This example does not implement
failover fencing or replica transfer.

If hibernate or wake fails, inspect claim state and resume from the confirmed
stage while retaining the reservation. `exec("true")` proves only silkd
readiness. The claim may already have woken because another request reached
it. It still has no data disk until attach, and no data mount until the guest
mount operation succeeds. Any attach, mount, or data-check failure leaves
business traffic blocked.

After a successful cycle, verify the same claim ID, backing identity, disk
serial, filesystem identity, and expected payload. A new VMM process after
wake is expected. The backing is external to the memory snapshot. Keep the
reservation while the resumed claim uses it; release it only through the
controller's normal ownership process.
