"""Volume descriptors and live convergence views."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
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
        not isinstance(reply, dict)
        or not all(
            isinstance(reply.get(key), list) and all(isinstance(entry, dict) for entry in reply[key])
            for key in ("volumes_want", "volumes", "states")
        )
        or not isinstance(reply.get("converging"), bool)
    ):
        return None
    view = _sandbox_response(reply)
    view["states"] = [dict(state) for state in reply["states"]]
    return cast(SandboxVolumesResponse, view)


def _sandbox_response(reply: dict[str, Any]) -> dict[str, Any]:
    view = dict(reply)
    for key in ("volumes", "volumes_want"):
        if isinstance(reply.get(key), list):
            view[key] = _response_volumes(reply[key])
    return view


def _response_volumes(volumes: Iterable[Mapping[str, str | bool] | Volume]) -> list[Volume]:
    # Response mounts identify raw devices; absent request mounts instead select a default path.
    entries = [cast("Volume", dict(volume)) for volume in volumes]
    for entry in entries:
        if not entry.get("mount"):
            entry["attach_only"] = True
    return entries


def _volume_bodies(
    volumes: Iterable[str | Mapping[str, str | bool] | Volume], mount: bool
) -> list[dict[str, str | bool]]:
    if isinstance(volumes, (str, bytes, bytearray, Mapping)) or not isinstance(volumes, Iterable):
        raise TypeError("volumes must be an iterable of volume names or mappings")
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
