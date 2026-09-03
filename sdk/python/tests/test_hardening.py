"""Regression tests for the SDK error-surface hardening."""

import json
import socket
import threading

import pytest

from cocoonsandbox import APIError, Client, ProtocolError, SilkdError, Watcher
from cocoonsandbox.conn import Conn, dial_agent


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


def test_request_wraps_truncated_body_as_api_error():
    server = socket.create_server(("127.0.0.1", 0))
    port = server.getsockname()[1]

    def serve():
        conn, _ = server.accept()
        conn.recv(4096)
        conn.sendall(b"HTTP/1.1 200 OK\r\nContent-Length: 100\r\n\r\nshort")
        conn.close()

    threading.Thread(target=serve, daemon=True).start()
    try:
        with pytest.raises(APIError):
            Client(f"127.0.0.1:{port}").info()
    finally:
        server.close()


def test_dial_agent_rejects_negative_content_length():
    server = socket.create_server(("127.0.0.1", 0))
    port = server.getsockname()[1]

    def serve():
        conn, _ = server.accept()
        conn.recv(4096)
        conn.sendall(b"HTTP/1.1 500 nope\r\nContent-Length: -1\r\n\r\nboom")

    threading.Thread(target=serve, daemon=True).start()
    try:
        with pytest.raises(ProtocolError):
            dial_agent(f"127.0.0.1:{port}", "sb_1", "tok", 0.5)
    finally:
        server.close()


def test_error_response_with_truncated_body_wraps_as_api_error():
    server = socket.create_server(("127.0.0.1", 0))
    port = server.getsockname()[1]

    def serve():
        conn, _ = server.accept()
        conn.recv(4096)
        conn.sendall(b"HTTP/1.1 500 Internal Server Error\r\nContent-Length: 100\r\n\r\nshort")
        conn.close()

    threading.Thread(target=serve, daemon=True).start()
    try:
        with pytest.raises(APIError) as exc_info:
            Client(f"127.0.0.1:{port}").info()
    finally:
        server.close()
    assert exc_info.value.status == 500


def test_request_wraps_malformed_json_as_api_error():
    server = socket.create_server(("127.0.0.1", 0))
    port = server.getsockname()[1]

    def serve():
        conn, _ = server.accept()
        conn.recv(4096)
        conn.sendall(b"HTTP/1.1 200 OK\r\nContent-Length: 3\r\nContent-Type: application/json\r\n\r\nnot")
        conn.close()

    threading.Thread(target=serve, daemon=True).start()
    try:
        with pytest.raises(APIError):
            Client(f"127.0.0.1:{port}").info()
    finally:
        server.close()
