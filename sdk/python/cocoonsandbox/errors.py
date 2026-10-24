"""Error types raised by the SDK."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .volumes import SandboxVolumesResponse


class SandboxError(Exception):
    """Base class for all SDK errors."""


class SandboxTimeout(SandboxError, TimeoutError):
    """A caller's timeout or deadline expired before the call finished."""


class APIError(SandboxError):
    """A sandboxd control-plane call failed."""

    def __init__(self, verb: str, status: int, message: str, *, reply: dict[str, Any] | None = None) -> None:
        super().__init__(f"{verb}: {message} (HTTP {status})")
        self.verb = verb
        self.status = status
        self.message = message
        self.reply = reply
        self.code = (reply or {}).get("code", "")
        self.volume = (reply or {}).get("volume", "")
        holders = (reply or {}).get("holders")
        self.holders = [
            {key: entry[key] for key in ("sandbox_id", "mode", "state") if isinstance(entry.get(key), str)}
            for entry in (holders if isinstance(holders, list) else [])
            if isinstance(entry, dict)
        ]


class VolumeError(APIError):
    """A volume request failed; response holds the server's complete view when one came back."""

    def __init__(self, error: APIError, response: SandboxVolumesResponse | None) -> None:
        super().__init__(error.verb, error.status, error.message, reply=error.reply)
        self.response = response


class SilkdError(SandboxError):
    """The in-guest daemon answered an error frame; kind is its typed wire kind."""

    def __init__(self, kind: str, message: str) -> None:
        super().__init__(f"{kind}: {message}")
        self.kind = kind
        self.message = message


class ExitError(SandboxError):
    """A command exited non-zero; carries the exit code, stderr, and whatever
    stdout it had produced — a failing build's log is on stdout."""

    def __init__(self, code: int, stderr: str, stdout: str = "") -> None:
        super().__init__(f"exit status {code}: {stderr.strip()}")
        self.code = code
        self.stderr = stderr
        self.stdout = stdout


class ProtocolError(SandboxError):
    """The peer broke the frame protocol (unexpected frame, oversized line)."""


def require_field(reply: dict[str, Any], key: str, verb: str) -> Any:
    """The named member of a control-plane reply, or an APIError naming the verb that got a truncated one."""
    if key not in reply:
        raise APIError(verb, 0, f"reply without {key}")
    return reply[key]
