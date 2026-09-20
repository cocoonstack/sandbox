"""The client timeout bounds the dial and the upgrade, not the frames that
follow: a guest stream outlives it."""

import contextlib
import json
import socket
import threading
import time

import pytest
from conftest import accept_upgrade, sandbox_at

from cocoonsandbox import ProtocolError, Sandbox, SandboxTimeout
from cocoonsandbox import conn as conn_module
from cocoonsandbox import sandbox as sandbox_module

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
    return sandbox_at(addr, timeout=TIMEOUT, keep_alive=0)


def serve_port_forward(server: socket.socket, quiet: float, ops: list[str]) -> None:
    conn, _ = server.accept()
    reader = accept_upgrade(conn)
    ops.append(json.loads(reader.readline())["op"])
    conn.sendall(b'{"type":"ready"}\n')
    time.sleep(quiet)
    conn.sendall(b'{"type":"data","data":"bGF0ZQ=="}\n{"type":"done"}\n')
    conn.close()


def serve_started_then_hang(server: socket.socket, quiet: float, kills: list[dict]) -> None:
    conn, _ = server.accept()
    reader = accept_upgrade(conn)
    reader.readline()
    conn.sendall(b'{"type":"started","pid":7}\n')
    killer, _ = server.accept()
    kills.append(json.loads(accept_upgrade(killer).readline()))
    killer.sendall(b'{"type":"done"}\n')
    killer.close()
    time.sleep(quiet)
    conn.close()


def serve_started_then_trickle_the_kill_reply(server: socket.socket, kills: list[dict]) -> None:
    conn, _ = server.accept()
    reader = accept_upgrade(conn)
    reader.readline()
    conn.sendall(b'{"type":"started","pid":7}\n')
    killer, _ = server.accept()
    kills.append(json.loads(accept_upgrade(killer).readline()))
    with contextlib.suppress(OSError):
        for byte in b'{"type":"done"}\n':
            time.sleep(0.75 * TIMEOUT)
            killer.sendall(bytes([byte]))
    killer.close()
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


def test_run_timeout_cuts_a_silent_command_and_kills_it():
    server = socket.create_server(("127.0.0.1", 0))
    addr = f"127.0.0.1:{server.getsockname()[1]}"
    kills: list[dict] = []
    threading.Thread(target=serve_started_then_hang, args=(server, 5 * TIMEOUT, kills), daemon=True).start()
    sb = legacy_sandbox(addr)
    started = time.monotonic()
    try:
        with pytest.raises(TimeoutError):
            sb.run(["sleep", "9"], timeout=TIMEOUT)
    finally:
        server.close()
    assert time.monotonic() - started < 3 * TIMEOUT
    assert kills == [{"v": 1, "op": "kill", "pid": 7}], kills


def test_run_timeout_bounds_the_kill_it_sends(monkeypatch):
    monkeypatch.setattr(sandbox_module, "_KILL_WAIT_SECONDS", TIMEOUT)
    server = socket.create_server(("127.0.0.1", 0))
    addr = f"127.0.0.1:{server.getsockname()[1]}"
    kills: list[dict] = []
    threading.Thread(target=serve_started_then_trickle_the_kill_reply, args=(server, kills), daemon=True).start()
    sb = legacy_sandbox(addr)
    started = time.monotonic()
    try:
        with pytest.raises(TimeoutError):
            sb.run(["sleep", "9"], timeout=TIMEOUT)
    finally:
        server.close()
    assert time.monotonic() - started < 4 * TIMEOUT
    assert kills == [{"v": 1, "op": "kill", "pid": 7}], kills


def test_run_timeout_cuts_a_blocked_exec_send(monkeypatch):
    sb = legacy_sandbox("127.0.0.1:1")
    conn = BlockedSendConn()
    monkeypatch.setattr(sb, "_dial", lambda deadline=None: conn)
    with pytest.raises(TimeoutError):
        sb.run(["echo", "hello"], timeout=TIMEOUT)
    assert conn.aborted.is_set()


def test_run_rejects_a_non_positive_timeout():
    sb = legacy_sandbox("127.0.0.1:1")
    with pytest.raises(ValueError):
        sb.run(["true"], timeout=0)


def test_dial_is_still_bounded_by_the_client_timeout():
    server = socket.create_server(("127.0.0.1", 0))
    addr = f"127.0.0.1:{server.getsockname()[1]}"
    threading.Thread(target=serve_silence, args=(server,), daemon=True).start()
    sb = legacy_sandbox(addr)
    started = time.monotonic()
    try:
        with pytest.raises(SandboxTimeout):
            sb.dial_port(5000)
    finally:
        server.close()
    assert time.monotonic() - started < 3 * TIMEOUT


@pytest.mark.parametrize(("exc", "want"), [(socket.timeout(), SandboxTimeout), (ConnectionResetError(), ProtocolError)])
def test_handshake_failures_are_typed(exc, want):
    assert isinstance(conn_module._handshake_error("agent upgrade", exc), want)
