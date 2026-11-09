"""The client timeout bounds the dial and the upgrade, not the guest stream that follows."""

import contextlib
import json
import socket
import time

import pytest
from cocoonsandbox import ProtocolError, Sandbox, SandboxTimeout
from cocoonsandbox import conn as conn_module
from cocoonsandbox import sandbox as sandbox_module
from conftest import accept_upgrade, bounded, sandbox_at

TIMEOUT = 0.2


def test_port_stream_outlives_the_client_timeout(script_server):
    ops: list[str] = []
    sb = short_timeout_sandbox(script_server(serve_port_forward, 3 * TIMEOUT, ops))
    with sb.dial_port(5000) as port:
        assert port.recv() == b"late"
        assert port.recv() == b""
    assert ops == ["port_forward"]


def test_run_timeout_cuts_a_silent_command_and_kills_it(script_server):
    kills: list[dict] = []
    sb = long_timeout_sandbox(script_server(serve_started_then_hang, kills))
    with pytest.raises(TimeoutError):
        bounded(lambda: sb.run(["sleep", "9"], timeout=TIMEOUT))
    assert kills == [{"v": 1, "op": "kill", "pid": 7}], kills


def test_run_timeout_bounds_the_kill_it_sends(monkeypatch, script_server):
    monkeypatch.setattr(sandbox_module, "_KILL_WAIT_SECONDS", TIMEOUT)
    kills: list[dict] = []
    sb = long_timeout_sandbox(script_server(serve_started_then_trickle_the_kill_reply, kills))
    with pytest.raises(TimeoutError):
        bounded(lambda: sb.run(["sleep", "9"], timeout=TIMEOUT))
    assert kills == [{"v": 1, "op": "kill", "pid": 7}], kills


def test_run_timeout_cuts_a_blocked_exec_send(monkeypatch, black_hole):
    connect = socket.create_connection

    def small_send_buffer(*args, **kwargs):
        sock = connect(*args, **kwargs)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 4096)
        return sock

    monkeypatch.setattr(socket, "create_connection", small_send_buffer)
    sb = long_timeout_sandbox(black_hole)
    with pytest.raises(TimeoutError):
        bounded(lambda: sb.run(["echo", "x" * (1 << 20)], timeout=TIMEOUT))


def test_run_rejects_a_non_positive_timeout():
    sb = short_timeout_sandbox("127.0.0.1:1")
    with pytest.raises(ValueError):
        sb.run(["true"], timeout=0)


def test_dial_is_still_bounded_by_the_client_timeout(script_server):
    sb = short_timeout_sandbox(script_server(serve_silence))
    with pytest.raises(SandboxTimeout):
        bounded(lambda: sb.dial_port(5000))


@pytest.mark.parametrize(("exc", "want"), [(TimeoutError(), SandboxTimeout), (ConnectionResetError(), ProtocolError)])
def test_handshake_failures_are_typed(exc, want):
    assert isinstance(conn_module._handshake_error("agent upgrade", exc), want)


def short_timeout_sandbox(addr: str) -> Sandbox:
    return sandbox_at(addr, timeout=TIMEOUT, keep_alive=0)


def long_timeout_sandbox(addr: str) -> Sandbox:
    return sandbox_at(addr, timeout=60, keep_alive=0)


def serve_port_forward(server: socket.socket, quiet: float, ops: list[str]) -> None:
    conn, _ = server.accept()
    reader = accept_upgrade(conn)
    ops.append(json.loads(reader.readline())["op"])
    conn.sendall(b'{"type":"ready"}\n')
    time.sleep(quiet)
    conn.sendall(b'{"type":"data","data":"bGF0ZQ=="}\n{"type":"done"}\n')
    conn.close()


def serve_started_then_hang(server: socket.socket, kills: list[dict]) -> None:
    conn, _ = server.accept()
    reader = accept_upgrade(conn)
    reader.readline()
    conn.sendall(b'{"type":"started","pid":7}\n')
    killer, _ = server.accept()
    kills.append(json.loads(accept_upgrade(killer).readline()))
    killer.sendall(b'{"type":"done"}\n')
    killer.close()
    reader.read()
    conn.close()


def serve_started_then_trickle_the_kill_reply(server: socket.socket, kills: list[dict]) -> None:
    conn, _ = server.accept()
    reader = accept_upgrade(conn)
    reader.readline()
    conn.sendall(b'{"type":"started","pid":7}\n')
    killer, _ = server.accept()
    kills.append(json.loads(accept_upgrade(killer).readline()))
    with contextlib.suppress(OSError):
        while True:
            time.sleep(0.75 * TIMEOUT)
            killer.sendall(b" ")
    killer.close()
    conn.close()


def serve_silence(server: socket.socket) -> None:
    conn, _ = server.accept()
    with contextlib.suppress(OSError):
        while conn.recv(4096):
            pass
    conn.close()
