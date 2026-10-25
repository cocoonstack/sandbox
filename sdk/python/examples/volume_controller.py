"""Compose volume detach and restore with controller-owned reservation and access hooks."""

from collections.abc import Callable
from dataclasses import dataclass

from cocoonsandbox import Sandbox, Volume


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
    volume: Volume = {"name": name, "mode": "rw", "attach_only": True}
    view = sb.patch_volumes(add=[volume])
    if view["volumes_want"] != [volume] or view["volumes"] != [volume] or view["converging"]:
        raise RuntimeError("volume attachment is not settled; keep access closed")
    hooks.mount_and_verify()
    hooks.open_access()
