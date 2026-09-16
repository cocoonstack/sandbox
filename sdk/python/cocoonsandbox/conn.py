"""Silkd frames over an HTTP(S) Upgrade relay."""

from __future__ import annotations

import contextlib
import select
import socket
import ssl
import threading
import time
import urllib.parse
from collections.abc import Callable, Iterator
from typing import Any, BinaryIO, Protocol, TypeVar

from .errors import APIError, ProtocolError, SilkdError
from .frames import KEEP_ALIVE_PROTO, MAX_FRAME, decode_response, encode_request

KEEP_ALIVE_CONNS = 8

_CloseableT = TypeVar("_CloseableT", bound="_Closeable")


class _Closeable(Protocol):
    """Context-manager mixin for handles whose exit is just close()."""

    def __enter__(self: _CloseableT) -> _CloseableT:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def close(self) -> None: ...


class Conn(_Closeable):
    """A live frame stream to one sandbox's silkd, via the owner node."""

    def __init__(self, sock: socket.socket, reader: BinaryIO) -> None:
        self._sock = sock
        self._reader = reader

    def send(self, op: str, **fields: object) -> None:
        self._sock.sendall(encode_request(op, **fields))

    def abort(self) -> None:
        """Unblocks a socket operation from another thread; close() still owns the socket."""
        with contextlib.suppress(OSError):
            self._sock.shutdown(socket.SHUT_RDWR)

    def recv(self) -> dict[str, Any]:
        """Reads one frame or raises a typed protocol or guest error."""
        try:
            line = self._reader.readline(MAX_FRAME + 1)
        except OSError as exc:
            raise ProtocolError(f"read failed: {exc}") from exc
        if not line:
            raise ProtocolError("connection closed mid-stream")
        if len(line) > MAX_FRAME:
            raise ProtocolError("frame exceeds the 8MiB cap")
        try:
            frame = decode_response(line)
        except ValueError as exc:
            raise ProtocolError(f"malformed frame: {exc}") from exc
        if frame["type"] == "error":
            raise SilkdError(frame.get("kind", "internal"), frame.get("message", ""))
        return frame

    def recv_until(self, *terminal: str) -> Iterator[dict[str, Any]]:
        """Yields frames until one of the terminal types arrives; the terminal frame is yielded last."""
        while True:
            frame = self.recv()
            yield frame
            if frame["type"] in terminal:
                return

    def quiet(self) -> bool:
        """Reports whether the peer has neither hung up nor spoken since the last frame."""
        try:
            readable, _, _ = select.select([self._sock], [], [], 0)
        except (OSError, ValueError):
            return False
        return not readable

    def close(self) -> None:
        self.abort()
        try:
            self._reader.close()
        finally:
            self._sock.close()


class _Parked:
    def __init__(self, conn: Conn, idle: float, evict: Callable[[_Parked], None]) -> None:
        self.conn = conn
        self.timer = threading.Timer(idle, evict, (self,))
        self.timer.daemon = True


class ConnPool:
    """Parks a handle's idle relay connections between calls; silkd serves RPCs back to back from proto 2."""

    def __init__(self, idle: float) -> None:
        self.proto = 0
        self._idle = idle
        self._lock = threading.Lock()
        self._parked: list[_Parked] = []

    def take(self) -> Conn | None:
        """Returns a parked connection whose peer is still there, or None."""
        while True:
            with self._lock:
                if not self._parked:
                    return None
                entry = self._parked.pop()
            entry.timer.cancel()
            if entry.conn.quiet():
                return entry.conn
            entry.conn.close()

    def park(self, conn: Conn) -> None:
        """Keeps conn for the next call until idle passes; a daemon before proto 2 or a zero window closes it."""
        with self._lock:
            keep = self._idle > 0 and self.proto >= KEEP_ALIVE_PROTO and len(self._parked) < KEEP_ALIVE_CONNS
            if keep:
                entry = _Parked(conn, self._idle, self._evict)
                self._parked.append(entry)
                entry.timer.start()
        if not keep:
            conn.close()

    def drain(self) -> None:
        with self._lock:
            parked, self._parked = self._parked, []
        for entry in parked:
            entry.timer.cancel()
            entry.conn.close()

    def _evict(self, entry: _Parked) -> None:
        with self._lock:
            if entry not in self._parked:
                return
            self._parked.remove(entry)
        entry.conn.close()


def dial_agent(
    addr: str,
    sandbox_id: str,
    token: str,
    timeout: float,
    deadline: float | None = None,
    *,
    ssl_context: ssl.SSLContext | None = None,
) -> Conn:
    """Opens one TCP/HTTP Upgrade relay within timeout and the optional deadline."""
    for name, value in (("sandbox id", sandbox_id), ("token", token)):
        if any(c in value for c in "\r\n\0"):
            raise APIError("agent upgrade", 0, f"{name} contains a control character")
    endpoint = urllib.parse.urlsplit(addr if "://" in addr else f"http://{addr}")
    host = endpoint.hostname
    port = endpoint.port or (443 if endpoint.scheme == "https" else 80)
    try:
        sock = socket.create_connection((host, port), timeout=_remaining_timeout(timeout, deadline))
    except OSError as exc:
        raise ProtocolError(f"dial {addr}: {exc}") from exc
    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    reader = None
    try:
        if endpoint.scheme == "https":
            context = ssl_context or ssl.create_default_context()
            sock = context.wrap_socket(sock, server_hostname=host, do_handshake_on_connect=False)
            sock.settimeout(_remaining_timeout(timeout, deadline))
            try:
                sock.do_handshake()
            except OSError as exc:
                raise ProtocolError(f"tls handshake {addr}: {exc}") from exc
        request = (
            f"GET /v1/sandboxes/{sandbox_id}/agent HTTP/1.1\r\n"
            f"Host: {endpoint.netloc}\r\n"
            "Connection: Upgrade\r\n"
            "Upgrade: silkd\r\n"
            f"Authorization: Bearer {token}\r\n"
            "\r\n"
        )
        sock.settimeout(_remaining_timeout(timeout, deadline))
        sock.sendall(request.encode())
        reader = sock.makefile("rb")
        sock.settimeout(_remaining_timeout(timeout, deadline))
        status = reader.readline(1024).decode(errors="replace")
        parts = status.split(" ", 2)
        code = int(parts[1]) if len(parts) > 1 and parts[1].isdigit() else 0
        body_len = 0
        while True:
            sock.settimeout(_remaining_timeout(timeout, deadline))
            header = reader.readline(4096)
            if header in (b"\r\n", b"\n", b""):
                break
            name, _, value = header.decode(errors="replace").partition(":")
            if name.strip().lower() == "content-length":
                try:
                    body_len = int(value.strip())
                    if body_len < 0:
                        raise ValueError("negative content-length")
                except ValueError as exc:
                    raise ProtocolError("invalid content-length in upgrade reply") from exc
        if code != 101:
            sock.settimeout(_remaining_timeout(timeout, deadline))
            body = reader.read(min(body_len, MAX_FRAME)).decode(errors="replace") if body_len else ""
            raise APIError("agent upgrade", code, body.strip() or status.strip())
        _remaining_timeout(timeout, deadline)
        sock.settimeout(None)
        return Conn(sock, reader)
    except Exception:
        if reader is not None:
            reader.close()
        sock.close()
        raise


def _remaining_timeout(timeout: float, deadline: float | None) -> float:
    if deadline is None:
        return timeout
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise TimeoutError("agent dial timed out")
    return min(timeout, remaining)
