"""proxy_port end-to-end with a fake guest PortConn: catches the class of bug
where proxy_port references a helper that does not exist — a static import
can't see it, only running the accept loop does."""

import socket
import threading
import time

from conftest import sandbox_at


class FakePortConn:
    """Echoes what it is sent, prefixed, so the proxy pipe is observable."""

    def __init__(self):
        self._inbox = []
        self._closed = False

    def send(self, data: bytes) -> None:
        self._inbox.append(b"echo:" + data)

    def recv(self) -> bytes:
        deadline = time.monotonic() + 5
        while not self._inbox and not self._closed and time.monotonic() < deadline:
            time.sleep(0.01)
        return self._inbox.pop(0) if self._inbox else b""

    def close_write(self) -> None:
        self._closed = True

    def close(self) -> None:
        self._closed = True


class ClosedPortConn:
    """A guest port whose upstream refuses, reported while the client is idle."""

    def __init__(self):
        self._dialled = False

    def send(self, data: bytes) -> None:
        pass

    def recv(self) -> bytes:
        if not self._dialled:
            self._dialled = True
            time.sleep(0.3)
        return b""

    def close_write(self) -> None:
        pass

    def close(self) -> None:
        pass


def test_proxy_port_accepts_and_pipes(monkeypatch):
    sb = sandbox_at("127.0.0.1:1")
    monkeypatch.setattr(sb, "dial_port", lambda port: FakePortConn())

    listener = sb.proxy_port("127.0.0.1:0", 8080)
    try:
        c = socket.create_connection(listener.getsockname(), timeout=5)
        c.sendall(b"hello")
        c.settimeout(5)
        got, deadline = b"", time.monotonic() + 5
        while b"echo:hello" not in got and time.monotonic() < deadline:
            chunk = c.recv(256)
            if not chunk:
                break
            got += chunk
        assert b"echo:hello" in got, got
        c.close()
    finally:
        listener.close()


def test_proxy_port_ends_the_local_connection_when_the_guest_port_is_closed(monkeypatch):
    sb = sandbox_at("127.0.0.1:1")
    monkeypatch.setattr(sb, "dial_port", lambda port: ClosedPortConn())

    listener = sb.proxy_port("127.0.0.1:0", 8080)
    try:
        c = socket.create_connection(listener.getsockname(), timeout=5)
        c.settimeout(5)
        assert c.recv(256) == b""
        c.close()
    finally:
        listener.close()


def test_proxy_port_retires_its_accept_thread_when_the_listener_closes():
    sb = sandbox_at("127.0.0.1:1", keep_alive=0)
    known = set(threading.enumerate())
    listener = sb.proxy_port("127.0.0.1:0", 80)
    (accept_loop,) = set(threading.enumerate()) - known
    listener.close()
    accept_loop.join(5)
    assert not accept_loop.is_alive()
