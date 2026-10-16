"""Volume descriptors and live convergence views."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Final, Literal, TypedDict, cast

VolumePhase = Literal["attached", "pending", "detaching"]
VOLUME_ATTACHED: Final[VolumePhase] = "attached"
VOLUME_PENDING: Final[VolumePhase] = "pending"
VOLUME_DETACHING: Final[VolumePhase] = "detaching"


class _VolumeName(TypedDict):
    name: str


class Volume(_VolumeName, total=False):
    """A catalog volume by name, with its guest mount point and an `rw` mode for writable claims."""

    mount: str
    mode: str


class _VolumeState(TypedDict):
    name: str
    state: VolumePhase


class VolumeState(_VolumeState, total=False):
    """One held volume's convergence phase and its last failure, if any."""

    error: str


class _SandboxVolumesResponse(TypedDict):
    volumes_want: list[Volume]
    volumes: list[Volume]
    states: list[VolumeState]
    converging: bool


class SandboxVolumesResponse(_SandboxVolumesResponse, total=False):
    """The desired and applied sets of a sandbox, with per-volume states; error names a refused request."""

    error: str


def _volume_response(reply: dict[str, Any] | None) -> SandboxVolumesResponse | None:
    if (
        reply is None
        or not all(isinstance(reply.get(key), list) for key in ("volumes_want", "volumes", "states"))
        or not isinstance(reply.get("converging"), bool)
    ):
        return None
    return cast(SandboxVolumesResponse, reply)


def _volume_body(volume: str | Mapping[str, str] | Volume, mount: bool) -> dict[str, str]:
    if isinstance(volume, str):
        return {"name": volume}
    if not isinstance(volume, Mapping):
        raise TypeError("volume must be a name string or mapping")
    unknown = sorted(set(volume) - {"name", "mount", "mode"})
    if unknown:
        raise TypeError(
            f"volume mapping accepts only name, mount, and mode, got unexpected key(s): {', '.join(unknown)}"
        )
    if not mount and "mount" in volume:
        raise TypeError("volume mount is meaningless with mount=False, which leaves mounting to the caller")
    body = dict(cast(Mapping[str, str], volume))
    mode = body.get("mode")
    if mode in (None, "", "ro"):
        body.pop("mode", None)
    elif mode != "rw":
        name = volume.get("name")
        prefix = f"volume {name!r}: " if name is not None else ""
        raise TypeError(f"{prefix}mode must be 'rw' or 'ro', got {mode!r}")
    return body
