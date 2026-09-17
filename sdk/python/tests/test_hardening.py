"""Regression tests for the SDK error-surface hardening."""

import base64
import json
import socket
import threading
import time

import pytest
from conftest import sandbox_at

from cocoonsandbox import APIError, Client, Lsp, ProtocolError, Pty, SandboxError, Session, SilkdError, Watcher
from cocoonsandbox.conn import Conn, dial_agent, remaining_timeout
from cocoonsandbox.frames import FS_CHUNK


def test_dial_agent_rejects_control_chars_in_identity():
    with pytest.raises(APIError):
        dial_agent("127.0.0.1:1", "sb\r\nInjected: 1", "tok", 0.5)
    with pytest.raises(APIError):
        dial_agent("127.0.0.1:1", "sb_1", "tok\r\nX-Evil: 1", 0.5)


def test_dial_agent_wraps_refused_connection(dead_addr):
    with pytest.raises(ProtocolError):
        dial_agent(dead_addr, "sb_1", "tok", 0.5)


def test_watcher_propagates_silkd_error():
    client_sock, guest_sock = socket.socketpair()
    guest_sock.sendall(json.dumps({"type": "error", "kind": "not_found", "message": "gone"}).encode() + b"\n")
    guest_sock.close()
    watcher = Watcher(Conn(client_sock, client_sock.makefile("rb")))
    with pytest.raises(SilkdError):
        for _ in watcher:
            pass


def test_watcher_ends_cleanly_on_garbage_frame():
    client_sock, guest_sock = socket.socketpair()
    guest_sock.sendall(b"not json at all\n")
    guest_sock.close()
    watcher = Watcher(Conn(client_sock, client_sock.makefile("rb")))
    assert list(watcher) == []


@pytest.mark.parametrize(
    ("response", "call", "raises", "status"),
    [
        (b"HTTP/1.1 200 OK\r\nContent-Length: 100\r\n\r\nshort", "info", APIError, None),
        (b"HTTP/1.1 500 Internal Server Error\r\nContent-Length: 100\r\n\r\nshort", "info", APIError, 500),
        (b"HTTP/1.1 200 OK\r\nContent-Length: 3\r\nContent-Type: application/json\r\n\r\nnot", "info", APIError, None),
        (b"HTTP/1.1 500 nope\r\nContent-Length: -1\r\n\r\nboom", "dial", ProtocolError, None),
    ],
    ids=["truncated body", "truncated error body", "malformed json", "negative content-length on dial"],
)
def test_raw_replies_surface_as_typed_errors(raw_reply, response, call, raises, status):
    addr = raw_reply(response)
    with pytest.raises(raises) as exc_info:
        if call == "dial":
            dial_agent(addr, "sb_1", "tok", 0.5)
        else:
            Client(addr).info()
    if status is not None:
        assert exc_info.value.status == status


def test_empty_claim_reply_is_an_api_error(spawn_node):
    addr = spawn_node({("POST", "/v1/claim"): lambda body, path: (200, {})})
    with pytest.raises(APIError, match="reply without id"):
        Client(addr).new("rt:24.04")


def test_pty_read_returns_empty_after_the_exit_frame():
    client_sock, guest_sock = socket.socketpair()
    guest_sock.sendall(json.dumps({"type": "exit", "code": 3}).encode() + b"\n")
    guest_sock.close()
    pty = Pty(sandbox_at("127.0.0.1:1"), Conn(client_sock, client_sock.makefile("rb")), 1)
    assert pty.read() == b""
    assert pty.exit_code == 3
    assert pty.read() == b""


def test_pty_write_chunks_a_large_buffer():
    client_sock, guest_sock = socket.socketpair()
    pty = Pty(sandbox_at("127.0.0.1:1"), Conn(client_sock, client_sock.makefile("rb")), 1)
    frames = []

    def drain():
        reader = guest_sock.makefile("rb")
        for _ in range(3):
            frames.append(json.loads(reader.readline()))

    reader_thread = threading.Thread(target=drain, daemon=True)
    reader_thread.start()
    pty.write(b"x" * (3 * FS_CHUNK))
    reader_thread.join(5)
    assert [f["op"] for f in frames] == ["stdin"] * 3
    assert all(len(base64.b64decode(f["data"])) == FS_CHUNK for f in frames)


def test_session_and_lsp_close_twice(monkeypatch):
    sb = sandbox_at("127.0.0.1:1")
    calls = []

    def gone(op, **fields):
        calls.append(op)
        raise SilkdError("not_found", "no such thing")

    monkeypatch.setattr(sb, "_done_rpc", gone)
    Session(sb, "s1").close()
    with Lsp(sb, "srv") as lsp:
        lsp.stop()
    assert calls == ["session_rm", "lsp_stop", "lsp_stop"]

    def broken(op, **fields):
        raise SilkdError("internal", "boom")

    monkeypatch.setattr(sb, "_done_rpc", broken)
    with pytest.raises(SilkdError):
        Session(sb, "s1").close()


def test_watcher_error_stays_none_after_close():
    client_sock, guest_sock = socket.socketpair()
    guest_sock.sendall(json.dumps({"type": "ready"}).encode() + b"\n")
    watcher = Watcher(Conn(client_sock, client_sock.makefile("rb")))
    events = []
    consumer = threading.Thread(target=lambda: events.extend(watcher), daemon=True)
    consumer.start()
    time.sleep(0.1)
    watcher.close()
    consumer.join(5)
    assert not consumer.is_alive()
    assert events == [] and watcher.error is None


def test_sandbox_timeout_is_both_hierarchies():
    with pytest.raises(SandboxError):
        remaining_timeout(1.0, time.monotonic() - 1, "probe")
    with pytest.raises(TimeoutError):
        remaining_timeout(1.0, time.monotonic() - 1, "probe")
