"""Adapter unit tests: exercise the BaseSandboxSession hooks against an
in-process fake sandboxd (control plane over http.server, data plane over
the SDK's own conn). No real VM — this binds the adapter's mapping onto the
cocoonsandbox surface, not cocoon itself."""

import asyncio
import io
import json
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest

from cocoonsandbox_openai import CocoonSandboxClient, CocoonSandboxClientOptions, CocoonSandboxSessionState


class FakeNode(BaseHTTPRequestHandler):
    claims: dict = {}

    def do_POST(self):
        length = int(self.headers.get("Content-Length") or 0)
        _ = self.rfile.read(length)
        if self.path == "/v1/claim":
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
    threading.Thread(target=server.serve_forever, daemon=True).start()
    yield f"127.0.0.1:{server.server_port}"
    server.shutdown()


def test_create_claims_and_state_round_trips(node):
    async def go():
        client = CocoonSandboxClient()
        session = await client.create(options=CocoonSandboxClientOptions(addr=node, template="rt:24.04"))
        inner = session._inner
        assert inner.state.sandbox_id == "sb_1" and inner.state.sandbox_token == "tok"

        # serialize → deserialize must reattach to the same claim.
        payload = client.serialize_session_state(inner.state)
        restored = client.deserialize_session_state(payload)
        assert isinstance(restored, CocoonSandboxSessionState)
        assert restored.sandbox_id == "sb_1" and restored.owner == node

        await client.delete(session)

    asyncio.run(go())


def test_exec_maps_stdio_and_exit(node, monkeypatch):

    class FakeSandbox:
        def __init__(self, **kw):
            self.id = "sb_1"

        def run(self, argv, on_stdout=None, on_stderr=None):
            assert argv == ["echo", "hi"]
            on_stdout(b"hi\n")
            return 0

    async def go():
        client = CocoonSandboxClient()
        session = await client.create(options=CocoonSandboxClientOptions(addr=node))
        inner = session._inner
        monkeypatch.setattr(inner, "_sandbox", lambda timeout=None: FakeSandbox())
        result = await inner._exec_internal("echo", "hi")
        assert result.exit_code == 0 and result.stdout == b"hi\n"
        assert result.ok()

    asyncio.run(go())


def test_read_missing_maps_to_filenotfound(node, monkeypatch):
    from cocoonsandbox import SilkdError

    class FakeSandbox:
        def read_file(self, path):
            raise SilkdError("not_found", path)

    async def go():
        client = CocoonSandboxClient()
        session = await client.create(options=CocoonSandboxClientOptions(addr=node))
        inner = session._inner
        monkeypatch.setattr(inner, "_sandbox", lambda timeout=None: FakeSandbox())
        with pytest.raises(FileNotFoundError):
            await inner.read(adapter_path("/nope"))

    asyncio.run(go())


def adapter_path(p: str):
    from pathlib import Path

    return Path(p)


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
        monkeypatch.setattr(inner, "_sandbox", lambda timeout=None: FakeSandbox())
        await inner.write(adapter_path("/workspace/a.txt"), io.BytesIO(b"body"))
        assert calls["write"] == ("/workspace/a.txt", b"body")
        tar = await inner.persist_workspace()
        assert tar.read() == b"tar-bytes" and calls["pull"] == "/workspace"
        await inner.hydrate_workspace(io.BytesIO(b"tar-in"))
        assert calls["push"] == ("/workspace", b"tar-in")

    asyncio.run(go())
