"""Volume descriptors and live convergence views."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any, Final, Literal, TypedDict, cast

VolumePhase = Literal["attached", "pending", "detaching"]
VOLUME_ATTACHED: Final[VolumePhase] = "attached"
VOLUME_PENDING: Final[VolumePhase] = "pending"
VOLUME_DETACHING: Final[VolumePhase] = "detaching"


class _VolumeName(TypedDict):
    name: str


class Volume(_VolumeName, total=False):
    """A catalog volume by name, with an optional mount, write mode, or caller-managed mounting."""

    mount: str
    mode: str
    attach_only: bool


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


def _volume_bodies(
    volumes: Sequence[str | Mapping[str, str | bool] | Volume], mount: bool
) -> list[dict[str, str | bool]]:
    if isinstance(volumes, (str, bytes, bytearray)) or not isinstance(volumes, Sequence):
        raise TypeError("volumes must be a sequence of volume names or mappings")
    return [_volume_body(volume, mount) for volume in volumes]


def _volume_body(volume: str | Mapping[str, str | bool] | Volume, mount: bool) -> dict[str, str | bool]:
    if isinstance(volume, str):
        return {"name": volume}
    if not isinstance(volume, Mapping):
        raise TypeError("volume must be a name string or mapping")
    unknown = sorted(set(volume) - {"name", "mount", "mode", "attach_only"})
    if unknown:
        raise TypeError(
            "volume mapping accepts only name, mount, mode, and attach_only, "
            f"got unexpected key(s): {', '.join(unknown)}"
        )
    for field in ("name", "mount", "mode"):
        if field in volume and not isinstance(volume[field], str):
            raise TypeError(f"volume {field} must be a string")
    if "attach_only" in volume and not isinstance(volume["attach_only"], bool):
        raise TypeError("volume attach_only must be a boolean")
    if volume.get("attach_only") and volume.get("mount"):
        raise TypeError("volume mount is meaningless with attach_only=True, which leaves mounting to the caller")
    if not mount and "mount" in volume:
        raise TypeError("volume mount is meaningless with mount=False, which leaves mounting to the caller")
    body = dict(cast("Mapping[str, str | bool]", volume))
    mode = body.get("mode")
    if mode in (None, "", "ro"):
        body.pop("mode", None)
    elif mode != "rw":
        name = volume.get("name")
        prefix = f"volume {name!r}: " if name is not None else ""
        raise TypeError(f"{prefix}mode must be 'rw' or 'ro', got {mode!r}")
    return body
