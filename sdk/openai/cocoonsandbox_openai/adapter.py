"""OpenAI Agents SDK sandbox provider backed by cocoon microVMs: implements
the SDK's BaseSandboxClient/BaseSandboxSession pair on top of the sync
cocoonsandbox SDK, bridged with asyncio.to_thread. One session is one
claimed sandbox; delete releases it, resume reattaches by id + token."""

from __future__ import annotations

import asyncio
import io
import socket
import uuid
from pathlib import Path
from typing import Literal

from agents.sandbox.manifest import Manifest
from agents.sandbox.session.base_sandbox_session import BaseSandboxSession
from agents.sandbox.session.sandbox_client import BaseSandboxClient, BaseSandboxClientOptions
from agents.sandbox.session.sandbox_session import SandboxSession
from agents.sandbox.session.sandbox_session_state import SandboxSessionState
from agents.sandbox.snapshot import SnapshotBase, SnapshotSpec, resolve_snapshot
from agents.sandbox.types import ExecResult, ExposedPortEndpoint, User
from cocoonsandbox import APIError, Client, Sandbox, SilkdError


class CocoonSandboxClientOptions(BaseSandboxClientOptions):
    """Connection settings for a sandboxd node (or cluster entry node)."""

    type: Literal["cocoon"] = "cocoon"
    addr: str = "127.0.0.1:7777"
    api_token: str = ""
    template: str = "rt:24.04"
    net: str = ""
    ttl_seconds: int = 0


class CocoonSandboxSessionState(SandboxSessionState):
    """Everything needed to reattach: the claim's id, token, and owner."""

    type: Literal["cocoon"] = "cocoon"
    addr: str = ""
    api_token: str = ""
    sandbox_id: str = ""
    sandbox_token: str = ""
    owner: str = ""


class CocoonSandboxSession(BaseSandboxSession):
    """One claimed microVM behind the Agents SDK session surface."""

    state: CocoonSandboxSessionState

    def __init__(self, *, state: CocoonSandboxSessionState) -> None:
        self.state = state
        self._proxies: list[socket.socket] = []

    @classmethod
    def from_state(cls, state: CocoonSandboxSessionState) -> CocoonSandboxSession:
        return cls(state=state)

    async def _prepare_backend_workspace(self) -> None:
        sb = self._sandbox()
        await asyncio.to_thread(sb.mkdir, str(self.state.manifest.root), True)

    async def _exec_internal(self, *command: str | Path, timeout: float | None = None) -> ExecResult:
        # Bound the blocking SDK call by the socket timeout too, so a
        # wait_for cancellation is matched by the worker thread actually
        # unblocking (recv wakes) instead of lingering.
        sb = self._sandbox(timeout=timeout)
        argv = [str(part) for part in command]
        stdout, stderr = bytearray(), bytearray()

        def run() -> int:
            return sb.run(argv, on_stdout=stdout.extend, on_stderr=stderr.extend)

        code = await asyncio.wait_for(asyncio.to_thread(run), timeout=timeout)
        return ExecResult(stdout=bytes(stdout), stderr=bytes(stderr), exit_code=code)

    async def read(self, path: Path, *, user: str | User | None = None) -> io.IOBase:
        sb = self._sandbox()
        try:
            data = await asyncio.to_thread(sb.read_file, str(self._abs(path)))
        except SilkdError as exc:
            if exc.kind == "not_found":
                raise FileNotFoundError(str(path)) from exc
            raise
        return io.BytesIO(data)

    async def write(self, path: Path, data: io.IOBase, *, user: str | User | None = None) -> None:
        sb = self._sandbox()
        payload = data.read()
        await asyncio.to_thread(sb.write_file, str(self._abs(path)), payload)

    async def running(self) -> bool:
        try:
            await asyncio.to_thread(self._sandbox().stat, "/")
        except (APIError, SilkdError, OSError):
            return False
        return True

    async def persist_workspace(self) -> io.IOBase:
        sb = self._sandbox()
        tar = await asyncio.to_thread(sb.pull, str(self.state.manifest.root))
        return io.BytesIO(tar)

    async def hydrate_workspace(self, data: io.IOBase) -> None:
        sb = self._sandbox()
        await asyncio.to_thread(sb.push, str(self.state.manifest.root), data.read())

    async def _resolve_exposed_port(self, port: int) -> ExposedPortEndpoint:
        sb = self._sandbox()
        listener = await asyncio.to_thread(sb.proxy_port, "127.0.0.1:0", port)
        self._proxies.append(listener)
        return ExposedPortEndpoint(host="127.0.0.1", port=listener.getsockname()[1], tls=False)

    async def _shutdown_backend(self) -> None:
        # The sandbox itself outlives shutdown (delete releases it); only the
        # local port proxies belong to this process.
        for listener in self._proxies:
            listener.close()
        self._proxies.clear()

    def _sandbox(self, timeout: float | None = None) -> Sandbox:
        s = self.state
        client = Client(s.addr, api_token=s.api_token, timeout=timeout or 120.0)
        return Sandbox(client=client, id=s.sandbox_id, token=s.sandbox_token, owner=s.owner or s.addr)

    def _abs(self, path: Path | str) -> Path:
        # The SDK hands paths as str or Path; a relative one roots at the
        # manifest workspace.
        path = Path(path)
        if path.is_absolute():
            return path
        return Path(self.state.manifest.root) / path


class CocoonSandboxClient(BaseSandboxClient[CocoonSandboxClientOptions]):
    """Claims a sandbox per session; delete releases, resume reattaches."""

    async def create(
        self,
        *,
        snapshot: SnapshotSpec | SnapshotBase | None = None,
        manifest: Manifest | None = None,
        options: CocoonSandboxClientOptions,
    ) -> SandboxSession:
        client = Client(options.addr, api_token=options.api_token)
        sb = await asyncio.to_thread(
            client.new, options.template, net=options.net, ttl_seconds=options.ttl_seconds
        )
        if manifest is None:
            manifest = Manifest(root="/workspace")
        session_id = uuid.uuid4()
        state = CocoonSandboxSessionState(
            session_id=session_id,
            manifest=manifest,
            snapshot=resolve_snapshot(snapshot, str(session_id)),
            addr=options.addr,
            api_token=options.api_token,
            sandbox_id=sb.id,
            sandbox_token=sb.token,
            owner=sb.owner,
        )
        return self._wrap_session(CocoonSandboxSession.from_state(state))

    async def delete(self, session: SandboxSession) -> SandboxSession:
        inner = session._inner
        if not isinstance(inner, CocoonSandboxSession):
            raise TypeError("CocoonSandboxClient.delete expects a CocoonSandboxSession")
        await inner._shutdown_backend()  # close any local port proxies
        await asyncio.to_thread(inner._sandbox().close)  # 404-safe in the SDK
        return session

    async def resume(self, state: SandboxSessionState) -> SandboxSession:
        if not isinstance(state, CocoonSandboxSessionState):
            raise TypeError("CocoonSandboxClient.resume expects a CocoonSandboxSessionState")
        return self._wrap_session(CocoonSandboxSession.from_state(state))

    def deserialize_session_state(self, payload: dict[str, object]) -> SandboxSessionState:
        return CocoonSandboxSessionState.model_validate(payload)
