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
    """A catalog volume with a guest mount or caller-managed device attachment."""

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
        or any(not isinstance(entry, dict) for key in ("volumes_want", "volumes") for entry in reply[key])
    ):
        return None
    return cast(SandboxVolumesResponse, reply)


def _volume_body(volume: str | Volume) -> Volume:
    if isinstance(volume, str):
        return {"name": volume}
    if not isinstance(volume, Mapping):
        raise TypeError("volume must be a name string or mapping")
    return cast(Volume, dict(volume))
