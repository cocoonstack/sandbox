"""The client timeout bounds the dial and the upgrade, not the frames that
follow: a guest stream outlives it."""

import json
import socket
import threading
import time

import pytest

from cocoonsandbox import Client, Sandbox

TIMEOUT = 0.2


def serve_port_forward(server: socket.socket, quiet: float, ops: list[str]) -> None:
    conn, _ = server.accept()
    reader = conn.makefile("rb")
    while reader.readline() not in (b"\r\n", b""):
        pass
    conn.sendall(b"HTTP/1.1 101 Switching Protocols\r\n\r\n")
    ops.append(json.loads(reader.readline())["op"])
    conn.sendall(b'{"type":"ready"}\n')
    time.sleep(quiet)
    conn.sendall(b'{"type":"data","data":"bGF0ZQ=="}\n{"type":"done"}\n')
    conn.close()


def serve_silence(server: socket.socket) -> None:
    conn, _ = server.accept()
    conn.recv(4096)
    time.sleep(5 * TIMEOUT)
    conn.close()


def test_port_stream_outlives_the_client_timeout():
    server = socket.create_server(("127.0.0.1", 0))
    addr = f"127.0.0.1:{server.getsockname()[1]}"
    ops: list[str] = []
    threading.Thread(target=serve_port_forward, args=(server, 3 * TIMEOUT, ops), daemon=True).start()
    sb = Sandbox(client=Client(addr, timeout=TIMEOUT), id="sb_1", token="tok", owner=addr)
    try:
        with sb.dial_port(5000) as port:
            assert port.recv() == b"late"
            assert port.recv() == b""
    finally:
        server.close()
    assert ops == ["port_forward"]


def test_dial_is_still_bounded_by_the_client_timeout():
    server = socket.create_server(("127.0.0.1", 0))
    addr = f"127.0.0.1:{server.getsockname()[1]}"
    threading.Thread(target=serve_silence, args=(server,), daemon=True).start()
    sb = Sandbox(client=Client(addr, timeout=TIMEOUT), id="sb_1", token="tok", owner=addr)
    started = time.monotonic()
    try:
        with pytest.raises(OSError):
            sb.dial_port(5000)
    finally:
        server.close()
    assert time.monotonic() - started < 3 * TIMEOUT
