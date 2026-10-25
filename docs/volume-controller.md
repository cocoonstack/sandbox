# Hibernate a sandbox with an external volume controller

Use the live-volume API to remove a data disk before hibernation and restore
it after wake. This example keeps the same claim and catalog registration.
The controller owns the disk reservation and workload access policy throughout.
It does not add disk lifecycle management to sandboxd.

## Prepare the claim and controller

Use a `net: none` claim on its owning node. Egress-lane claims cannot hibernate.
Keep its TTL valid across the sleep interval. Use the current Python SDK with
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
  Serialize operations for the same reservation.
- `close_access` blocks and drains business requests before returning. It must
  fail closed. Cover direct ports, guest API access, and requests that can wake
  the claim. The controller's own maintenance calls may still reach silkd.
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
process restarts. Root-only attachment does not prevent a guest from accessing
an already attached disk, or prevent the tenant from removing it for cleanup.

Use one attach-only writable volume for this example. The controller mounts it
explicitly and owns filesystem consistency after a crash. sandboxd writes no
`.dirty` marker for attach-only RW. Do not substitute an automatically mounted
RW entry without accounting for its [dirty-marker rules](sandboxd-api.md#attach-only-volumes).

## Controller functions

Copy this block into the controller and pass its operations as `VolumeHooks`.
The functions do not release the reservation. Before the final `open_access`
call, exceptions stop the sequence without attempting to open access or run
another lifecycle operation. If a gate hook fails or has an uncertain result,
that hook and the controller must recover the gate. These functions cannot
prove that an external gate closed or undo a partially completed reopen.

```python
from collections.abc import Callable
from dataclasses import dataclass

from cocoonsandbox import Sandbox


@dataclass(frozen=True)
class VolumeHooks:
    assert_reserved: Callable[[], None]
    close_access: Callable[[], None]
    quiesce_and_unmount: Callable[[], None]
    mount_and_verify: Callable[[], None]
    open_access: Callable[[], None]


def hibernate_volume(sb: Sandbox, name: str, hooks: VolumeHooks) -> None:
    hooks.close_access()
    hooks.assert_reserved()
    hooks.quiesce_and_unmount()
    view = sb.patch_volumes(remove=[name])
    if view["volumes_want"] or view["volumes"] or view["states"] or view["converging"]:
        raise RuntimeError("volume references remain; keep access closed")
    sb.hibernate()


def resume_volume(sb: Sandbox, name: str, hooks: VolumeHooks) -> None:
    hooks.close_access()
    hooks.assert_reserved()
    sb.exec("true")
    volume = {"name": name, "mode": "rw", "attach_only": True}
    view = sb.patch_volumes(add=[volume])
    if view["volumes_want"] != [volume] or view["volumes"] != [volume] or view["converging"]:
        raise RuntimeError("volume attachment is not settled; keep access closed")
    hooks.mount_and_verify()
    hooks.open_access()
```

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
