"""proxy_port end-to-end with a fake guest PortConn: catches the class of bug
where proxy_port references a helper that does not exist — a static import
can't see it, only running the accept loop does."""

import socket
import time

from cocoonsandbox import Client, Sandbox


class FakePortConn:
    """Echoes what it is sent, prefixed, so the proxy pipe is observable."""

    def __init__(self):
        self._inbox = []
        self._closed = False

    def send(self, data: bytes) -> None:
        self._inbox.append(b"echo:" + data)

    def recv(self) -> bytes:
        deadline = time.time() + 5
        while not self._inbox and not self._closed and time.time() < deadline:
            time.sleep(0.01)
        return self._inbox.pop(0) if self._inbox else b""

    def close_write(self) -> None:
        self._closed = True

    def close(self) -> None:
        self._closed = True


def test_proxy_port_accepts_and_pipes(monkeypatch):
    sb = Sandbox(client=Client("127.0.0.1:1"), id="sb_1", token="tok", owner="127.0.0.1:1")
    monkeypatch.setattr(sb, "dial_port", lambda port: FakePortConn())

    listener = sb.proxy_port("127.0.0.1:0", 8080)
    try:
        c = socket.create_connection(listener.getsockname(), timeout=5)
        c.sendall(b"hello")
        c.settimeout(5)
        got, deadline = b"", time.time() + 5
        while b"echo:hello" not in got and time.time() < deadline:
            chunk = c.recv(256)
            if not chunk:
                break
            got += chunk
        assert b"echo:hello" in got, got
        c.close()
    finally:
        listener.close()


class ClosedPortConn:
    """A guest port whose upstream refuses, so silkd reports the close while
    the local client is still idle and the proxy is blocked reading it."""

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


def test_proxy_port_ends_the_local_connection_when_the_guest_port_is_closed(monkeypatch):
    sb = Sandbox(client=Client("127.0.0.1:1"), id="sb_1", token="tok", owner="127.0.0.1:1")
    monkeypatch.setattr(sb, "dial_port", lambda port: ClosedPortConn())

    listener = sb.proxy_port("127.0.0.1:0", 8080)
    try:
        c = socket.create_connection(listener.getsockname(), timeout=5)
        c.settimeout(5)
        assert c.recv(256) == b""
        c.close()
    finally:
        listener.close()
