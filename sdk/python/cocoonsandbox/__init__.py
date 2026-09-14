"""Python SDK for the cocoon sandbox control plane: claim a microVM from a
sandboxd node, run commands over the relayed silkd protocol, branch it with
checkpoints, release it. stdlib-only.

    from cocoonsandbox import Client

    client = Client("node:7777", api_token="...")
    with client.new("ghcr.io/cocoonstack/sandbox/rt:24.04") as sb:
        print(sb.exec("echo", "hello"))
"""

from .checkpoint import Checkpoint
from .client import Client
from .errors import APIError, ExitError, ProtocolError, SandboxError, SilkdError
from .sandbox import Lsp, PortConn, Pty, Sandbox, Session, Watcher
from .template import Template

__all__ = [
    "APIError",
    "Checkpoint",
    "Client",
    "ExitError",
    "Lsp",
    "PortConn",
    "ProtocolError",
    "Pty",
    "Sandbox",
    "SandboxError",
    "Session",
    "SilkdError",
    "Template",
    "Watcher",
]
