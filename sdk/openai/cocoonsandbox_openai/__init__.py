"""OpenAI Agents SDK sandbox provider for cocoon microVMs.

    from agents.sandbox import SandboxRunConfig  # SDK side
    from cocoonsandbox_openai import CocoonSandboxClient, CocoonSandboxClientOptions

    client = CocoonSandboxClient()
    session = await client.create(options=CocoonSandboxClientOptions(
        addr="10.0.0.5:7777", api_token="...", template="ghcr.io/cocoonstack/sandbox/rt:24.04"))
"""

from .adapter import (
    CocoonSandboxClient,
    CocoonSandboxClientOptions,
    CocoonSandboxSession,
    CocoonSandboxSessionState,
)

__all__ = [
    "CocoonSandboxClient",
    "CocoonSandboxClientOptions",
    "CocoonSandboxSession",
    "CocoonSandboxSessionState",
]
