"""Fixtures and helpers shared by the suites: in-process fake nodes, addresses
that refuse a connection, and the relay-side upgrade handshake."""

import socket
import threading
import time
from http.server import HTTPServer
from typing import BinaryIO, Callable

import pytest
from test_client import FakeNode

from cocoonsandbox import Client, Sandbox


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
def dead_addr():
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()
    return f"127.0.0.1:{port}"
