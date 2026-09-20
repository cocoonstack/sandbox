"""Adapter unit tests: exercise the BaseSandboxSession hooks against an
in-process fake sandboxd (control plane over http.server, data plane over
the SDK's own conn). No real VM — this binds the adapter's mapping onto the
cocoonsandbox surface, not cocoon itself."""

import asyncio
import io
import json
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

import pytest
from cocoonsandbox import ProtocolError, SilkdError
from cocoonsandbox_openai import CocoonSandboxClient, CocoonSandboxClientOptions, CocoonSandboxSessionState

CLAIMS: list = []


class FakeNode(BaseHTTPRequestHandler):
    def do_POST(self):
        length = int(self.headers.get("Content-Length") or 0)
        body = self.rfile.read(length)
        if self.path == "/v1/claim":
            self.server.claims.append(json.loads(body))
            self._reply(200, {"id": "sb_1", "token": "tok", "owner_addr": self.headers["Host"]})
        elif self.path.endswith("/release"):
            self._reply(204, None)
        else:
            self._reply(404, {"error": "no route"})

    def _reply(self, code, payload):
        self.send_response(code)
        if payload is not None:
            raw = json.dumps(payload).encode()
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(raw)))
            self.end_headers()
            self.wfile.write(raw)
        else:
            self.end_headers()

    def log_message(self, *args):
        pass


@pytest.fixture
def node():
    server = HTTPServer(("127.0.0.1", 0), FakeNode)
    server.claims = CLAIMS
    CLAIMS.clear()
    threading.Thread(target=server.serve_forever, daemon=True).start()
    yield f"127.0.0.1:{server.server_port}"
    server.shutdown()


def test_create_claims_and_state_round_trips(node):
    async def go():
        client = CocoonSandboxClient()
        session = await client.create(options=CocoonSandboxClientOptions(addr=node, template="rt:24.04"))
        inner = session._inner
        assert inner.state.sandbox_id == "sb_1" and inner.state.sandbox_token == "tok"

        payload = client.serialize_session_state(inner.state)
        restored = client.deserialize_session_state(payload)
        assert isinstance(restored, CocoonSandboxSessionState)
        assert restored.sandbox_id == "sb_1" and restored.owner == node

        await client.delete(session)

    asyncio.run(go())


def test_default_lease_outlives_an_agent_run(node):
    async def go():
        client = CocoonSandboxClient()
        session = await client.create(options=CocoonSandboxClientOptions(addr=node))
        await client.delete(session)

    asyncio.run(go())
    assert CLAIMS[0]["ttl_seconds"] == 3600, CLAIMS


def test_operations_share_one_sandbox_handle(node):
    async def go():
        client = CocoonSandboxClient()
        session = await client.create(options=CocoonSandboxClientOptions(addr=node, template="rt:24.04"))
        inner = session._inner
        assert inner._sandbox() is inner._sandbox()
        await client.delete(session)

    asyncio.run(go())


def test_exec_maps_stdio_and_exit(node, monkeypatch):
    class FakeSandbox:
        def __init__(self, **kw):
            self.id = "sb_1"

        def run(self, argv, on_stdout=None, on_stderr=None, timeout=None):
            assert argv == ["echo", "hi"] and timeout is None
            on_stdout(b"hi\n")
            return 0

    async def go():
        client = CocoonSandboxClient()
        session = await client.create(options=CocoonSandboxClientOptions(addr=node))
        inner = session._inner
        monkeypatch.setattr(inner, "_sandbox", lambda: FakeSandbox())
        result = await inner._exec_internal("echo", "hi")
        assert result.exit_code == 0 and result.stdout == b"hi\n"
        assert result.ok()

    asyncio.run(go())


def test_exec_timeout_reaches_the_sdk_and_surfaces_as_timeouterror(node, monkeypatch):
    seen = []

    class FakeSandbox:
        def run(self, argv, on_stdout=None, on_stderr=None, timeout=None):
            seen.append(timeout)
            on_stdout(b"partial\n")
            if timeout is not None:
                raise TimeoutError("cut")
            return 0

    async def go():
        client = CocoonSandboxClient()
        session = await client.create(options=CocoonSandboxClientOptions(addr=node))
        inner = session._inner
        monkeypatch.setattr(inner, "_sandbox", lambda: FakeSandbox())
        with pytest.raises(TimeoutError):
            await inner._exec_internal("sleep", "9", timeout=1.5)
        result = await inner._exec_internal("sleep", "9")
        assert seen == [1.5, None] and result.exit_code == 0

    asyncio.run(go())


def test_read_missing_maps_to_filenotfound(node, monkeypatch):
    class FakeSandbox:
        def read_file(self, path):
            raise SilkdError("not_found", path)

    async def go():
        client = CocoonSandboxClient()
        session = await client.create(options=CocoonSandboxClientOptions(addr=node))
        inner = session._inner
        monkeypatch.setattr(inner, "_sandbox", lambda: FakeSandbox())
        with pytest.raises(FileNotFoundError):
            await inner.read(Path("/nope"))

    asyncio.run(go())


def test_running_is_false_when_the_dial_fails(node, monkeypatch):
    class FakeSandbox:
        def stat(self, path):
            raise ProtocolError("dial 127.0.0.1:1: connection refused")

    async def go():
        client = CocoonSandboxClient()
        session = await client.create(options=CocoonSandboxClientOptions(addr=node))
        inner = session._inner
        monkeypatch.setattr(inner, "_sandbox", lambda: FakeSandbox())
        assert await inner.running() is False

    asyncio.run(go())


def test_write_and_persist_use_tree_verbs(node, monkeypatch):
    calls = {}

    class FakeSandbox:
        def write_file(self, path, data):
            calls["write"] = (path, data)

        def pull(self, path):
            calls["pull"] = path
            return b"tar-bytes"

        def push(self, dest, data):
            calls["push"] = (dest, data)

    async def go():
        client = CocoonSandboxClient()
        session = await client.create(options=CocoonSandboxClientOptions(addr=node))
        inner = session._inner
        monkeypatch.setattr(inner, "_sandbox", lambda: FakeSandbox())
        await inner.write(Path("/workspace/a.txt"), io.BytesIO(b"body"))
        assert calls["write"] == ("/workspace/a.txt", b"body")
        tar = await inner.persist_workspace()
        assert tar.read() == b"tar-bytes" and calls["pull"] == "/workspace"
        await inner.hydrate_workspace(io.BytesIO(b"tar-in"))
        assert calls["push"] == ("/workspace", b"tar-in")

    asyncio.run(go())


def test_create_keeps_the_construction_error_when_the_release_also_fails(node, monkeypatch):
    def bad_snapshot(*args, **kwargs):
        raise ValueError("bad snapshot")

    def bad_close(self):
        raise RuntimeError("node gone")

    monkeypatch.setattr("cocoonsandbox_openai.adapter.resolve_snapshot", bad_snapshot)
    monkeypatch.setattr("cocoonsandbox.Sandbox.close", bad_close)

    async def go():
        client = CocoonSandboxClient()
        with pytest.raises(ValueError, match="bad snapshot"):
            await client.create(options=CocoonSandboxClientOptions(addr=node))

    asyncio.run(go())


def test_create_releases_the_claim_when_state_construction_fails(node, monkeypatch):
    released = []

    def bad_snapshot(*args, **kwargs):
        raise ValueError("bad snapshot")

    monkeypatch.setattr("cocoonsandbox_openai.adapter.resolve_snapshot", bad_snapshot)
    monkeypatch.setattr("cocoonsandbox.Sandbox.close", lambda self: released.append(self.id))

    async def go():
        client = CocoonSandboxClient()
        with pytest.raises(ValueError):
            await client.create(options=CocoonSandboxClientOptions(addr=node))

    asyncio.run(go())
    assert released == ["sb_1"]
