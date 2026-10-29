from collections.abc import Callable
from dataclasses import dataclass

from cocoonsandbox import Sandbox, Volume


@dataclass(frozen=True)
class VolumeHooks:
    """Application operations required by the controller functions.

    assert_reserved verifies exclusive backing ownership and the owning node.
    Hold a durable external reservation throughout use, detached sleep, failures,
    and controller restarts. Renew leases separately and serialize operations.
    Fence the old node before attaching at a new placement. This assertion is
    neither a lock nor lease renewal. Tenant ACLs are not a reservation.

    close_access blocks and drains business requests before returning, including
    API, guest API, direct ports, and preview listeners on every serving node.
    Issued preview URLs can wake the claim and have no per-URL revocation.
    Stopping URL issuance is insufficient. Maintenance calls may reach silkd.

    quiesce_and_unmount stops and reaps writers, flushes application and filesystem
    state, and ordinarily unmounts. Raise on failure, including a busy mount.
    Never use lazy or forced unmount. Account for raw-device users, partitions,
    and other mount namespaces. A retry must tolerate an already unmounted disk.

    mount_and_verify finds the device by catalog serial, mounts its existing
    filesystem, and checks its identity and data. Restart the application behind
    the closed gate if needed. Raise before admitting traffic if any check fails.
    open_access admits traffic only after verification succeeds.

    Keep access closed on exceptions, cancellation, or uncertain results. These
    functions cannot prove an external gate closed or undo a partial reopen.
    Recover failed gate operations externally while retaining the reservation.
    """

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
    volume: Volume = {"name": name, "mode": "rw", "attach_only": True}
    view = sb.patch_volumes(add=[volume])
    if view["volumes_want"] != [volume] or view["volumes"] != [volume] or view["converging"]:
        raise RuntimeError("volume attachment is not settled; keep access closed")
    hooks.mount_and_verify()
    hooks.open_access()
