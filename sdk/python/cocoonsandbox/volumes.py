from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Literal, TypedDict, cast

from .errors import APIError


class _VolumeName(TypedDict):
    name: str


class Volume(_VolumeName, total=False):
    mount: str
    mode: str


class _VolumeState(TypedDict):
    name: str
    state: Literal["attached", "pending", "detaching"]


class VolumeState(_VolumeState, total=False):
    error: str


class _SandboxVolumesResponse(TypedDict):
    volumes_want: list[Volume]
    volumes: list[Volume]
    states: list[VolumeState]


class SandboxVolumesResponse(_SandboxVolumesResponse, total=False):
    error: str


class VolumeError(APIError):
    """Failed convergence with the server's view, when one was returned."""

    def __init__(self, error: APIError, response: SandboxVolumesResponse | None) -> None:
        super().__init__(error.verb, error.status, error.message)
        self.response = response


def volume_response(reply: dict[str, Any] | None) -> SandboxVolumesResponse | None:
    if reply is None or not all(isinstance(reply.get(key), list) for key in ("volumes_want", "volumes", "states")):
        return None
    return cast(SandboxVolumesResponse, reply)


def volume_body(volume: str | Mapping[str, str] | Volume, mount: bool) -> dict[str, str]:
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
