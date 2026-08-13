"""Error types raised by the SDK."""


class SandboxError(Exception):
    """Base class for all SDK errors."""


class APIError(SandboxError):
    """A sandboxd control-plane call failed."""

    def __init__(self, verb: str, status: int, message: str):
        super().__init__(f"{verb}: {message} (HTTP {status})")
        self.verb = verb
        self.status = status
        self.message = message


class SilkdError(SandboxError):
    """The in-guest daemon answered an error frame.

    kind is the typed wire kind: bad_request, not_found, unimplemented,
    internal.
    """

    def __init__(self, kind: str, message: str):
        super().__init__(f"{kind}: {message}")
        self.kind = kind
        self.message = message


class ExitError(SandboxError):
    """A command exited non-zero; carries the exit code, stderr, and whatever
    stdout it had produced — a failing build's log is on stdout."""

    def __init__(self, code: int, stderr: str, stdout: str = ""):
        super().__init__(f"exit status {code}: {stderr.strip()}")
        self.code = code
        self.stderr = stderr
        self.stdout = stdout


class ProtocolError(SandboxError):
    """The peer broke the frame protocol (unexpected frame, oversized line)."""
