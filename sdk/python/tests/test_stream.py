"""The client timeout bounds the dial and the upgrade, not the frames that
follow: a guest stream outlives it."""

import json
import socket
import threading
import time

import pytest

from cocoonsandbox import Client, Sandbox

TIMEOUT = 0.2


class BlockedSendConn:
    def __init__(self) -> None:
        self.aborted = threading.Event()

    def __enter__(self):
        return self

    def __exit__(self, *exc) -> None:
        pass

    def send(self, op: str, **fields) -> None:
        assert self.aborted.wait(5 * TIMEOUT), "exec send was not aborted"
        raise OSError("connection cut")

    def abort(self) -> None:
        self.aborted.set()

    def close(self) -> None:
        pass


def legacy_sandbox(addr: str) -> Sandbox:
    sb = Sandbox(client=Client(addr, timeout=TIMEOUT), id="sb_1", token="tok", owner=addr)
    sb._pool.proto = 1
    return sb


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


def serve_started_then_hang(server: socket.socket, quiet: float) -> None:
    conn, _ = server.accept()
    reader = conn.makefile("rb")
    while reader.readline() not in (b"\r\n", b""):
        pass
    conn.sendall(b"HTTP/1.1 101 Switching Protocols\r\n\r\n")
    reader.readline()
    conn.sendall(b'{"type":"started","pid":7}\n')
    time.sleep(quiet)
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
    sb = legacy_sandbox(addr)
    try:
        with sb.dial_port(5000) as port:
            assert port.recv() == b"late"
            assert port.recv() == b""
    finally:
        server.close()
    assert ops == ["port_forward"]


def test_run_timeout_cuts_a_silent_command():
    server = socket.create_server(("127.0.0.1", 0))
    addr = f"127.0.0.1:{server.getsockname()[1]}"
    threading.Thread(target=serve_started_then_hang, args=(server, 5 * TIMEOUT), daemon=True).start()
    sb = legacy_sandbox(addr)
    started = time.monotonic()
    try:
        with pytest.raises(TimeoutError):
            sb.run(["sleep", "9"], timeout=TIMEOUT)
    finally:
        server.close()
    assert time.monotonic() - started < 3 * TIMEOUT


def test_run_timeout_cuts_a_blocked_exec_send(monkeypatch):
    sb = legacy_sandbox("127.0.0.1:1")
    conn = BlockedSendConn()
    monkeypatch.setattr(sb, "_dial", lambda deadline=None: conn)
    with pytest.raises(TimeoutError):
        sb.run(["echo", "hello"], timeout=TIMEOUT)
    assert conn.aborted.is_set()


def test_run_rejects_a_non_positive_timeout():
    sb = Sandbox(client=Client("127.0.0.1:1", timeout=TIMEOUT), id="sb_1", token="tok", owner="127.0.0.1:1")
    with pytest.raises(ValueError):
        sb.run(["true"], timeout=0)


def test_dial_is_still_bounded_by_the_client_timeout():
    server = socket.create_server(("127.0.0.1", 0))
    addr = f"127.0.0.1:{server.getsockname()[1]}"
    threading.Thread(target=serve_silence, args=(server,), daemon=True).start()
    sb = legacy_sandbox(addr)
    started = time.monotonic()
    try:
        with pytest.raises(OSError):
            sb.dial_port(5000)
    finally:
        server.close()
    assert time.monotonic() - started < 3 * TIMEOUT
