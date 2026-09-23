"""Fixtures and helpers shared by the suites: in-process fake nodes, addresses
that refuse a connection, and the relay-side upgrade handshake."""

import json
import socket
import threading
import time
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import BinaryIO, Callable

import pytest

from cocoonsandbox import Client, Sandbox


class FakeNode(BaseHTTPRequestHandler):
    routes = {}
    last_headers = {}

    def do_POST(self):
        self._dispatch("POST")

    def do_GET(self):
        self._dispatch("GET")

    def do_DELETE(self):
        self._dispatch("DELETE")

    def log_message(self, *args):
        pass

    def _dispatch(self, method):
        FakeNode.last_headers = dict(self.headers)
        length = int(self.headers.get("Content-Length") or 0)
        body = json.loads(self.rfile.read(length)) if length else {}
        handler = self.routes.get((method, self.path.split("?")[0]))
        if handler is None:
            self._reply(404, {"error": "no route"})
            return
        code, reply = handler(body, self.path)
        self._reply(code, reply)

    def _reply(self, code, payload):
        raw = json.dumps(payload).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)


def sandbox_at(addr: str, **client_kwargs) -> Sandbox:
    return Sandbox(client=Client(addr, **client_kwargs), id="sb_1", token="tok", owner=addr)


def accept_upgrade(conn: socket.socket) -> BinaryIO:
    reader = conn.makefile("rb")
    while reader.readline() not in (b"\r\n", b""):
        pass
    conn.sendall(b"HTTP/1.1 101 Switching Protocols\r\n\r\n")
    return reader


def wait_until(cond: Callable[[], bool], message: str) -> None:
    deadline = time.monotonic() + 3
    while time.monotonic() < deadline:
        if cond():
            return
        time.sleep(0.01)
    raise AssertionError(message)


@pytest.fixture
def spawn_node():
    servers = []

    def spawn(routes):
        handler = type("Node", (FakeNode,), {"routes": routes})
        server = HTTPServer(("127.0.0.1", 0), handler)
        threading.Thread(target=server.serve_forever, daemon=True).start()
        servers.append(server)
        return f"127.0.0.1:{server.server_port}"

    yield spawn
    for server in servers:
        server.shutdown()


@pytest.fixture
def raw_reply():
    servers = []

    def serve(response: bytes) -> str:
        server = socket.create_server(("127.0.0.1", 0))
        servers.append(server)

        def answer():
            conn, _ = server.accept()
            conn.recv(4096)
            conn.sendall(response)
            conn.close()

        threading.Thread(target=answer, daemon=True).start()
        return f"127.0.0.1:{server.getsockname()[1]}"

    yield serve
    for server in servers:
        server.close()


@pytest.fixture
def black_hole():
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    sock.listen(8)
    yield f"127.0.0.1:{sock.getsockname()[1]}"
    sock.close()


@pytest.fixture
def dead_addr():
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()
    return f"127.0.0.1:{port}"
