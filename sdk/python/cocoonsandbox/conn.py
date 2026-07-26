"""One relayed silkd connection per RPC: a raw TCP socket hand-upgraded to
the sandboxd agent relay (Upgrade: silkd), then newline-JSON frames."""

from __future__ import annotations

import contextlib
import socket
from collections.abc import Iterator

from .errors import APIError, ProtocolError, SilkdError
from .frames import MAX_FRAME, decode_response, encode_request


class Conn:
    """A live frame stream to one sandbox's silkd, via the owner node."""

    def __init__(self, sock: socket.socket, reader):
        self._sock = sock
        self._reader = reader

    def __enter__(self) -> Conn:
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    def send(self, op: str, **fields) -> None:
        self._sock.sendall(encode_request(op, **fields))

    def recv(self) -> dict:
        """Returns the next frame; raises SilkdError on an error frame and
        ProtocolError on EOF or an oversized line."""
        line = self._reader.readline(MAX_FRAME + 1)
        if not line:
            raise ProtocolError("connection closed mid-stream")
        if len(line) > MAX_FRAME:
            raise ProtocolError("frame exceeds the 8MiB cap")
        frame = decode_response(line)
        if frame["type"] == "error":
            raise SilkdError(frame.get("kind", "internal"), frame.get("message", ""))
        return frame

    def recv_until(self, *terminal: str) -> Iterator[dict]:
        """Yields frames until one of the terminal types arrives; the
        terminal frame is yielded last."""
        while True:
            frame = self.recv()
            yield frame
            if frame["type"] in terminal:
                return

    def close_write(self) -> None:
        with contextlib.suppress(OSError):
            self._sock.shutdown(socket.SHUT_WR)

    def close(self) -> None:
        try:
            self._reader.close()
        finally:
            self._sock.close()


def dial_agent(addr: str, sandbox_id: str, token: str, timeout: float) -> Conn:
    """Opens the data-plane connection: TCP dial plus a hand-rolled HTTP
    Upgrade, so nothing pools or proxies underneath the byte stream."""
    # id/token interpolate into the raw request line; CR/LF would inject headers.
    for name, value in (("sandbox id", sandbox_id), ("token", token)):
        if any(c in value for c in "\r\n\0"):
            raise APIError("agent upgrade", 0, f"{name} contains a control character")
    host, port = addr.rsplit(":", 1)
    sock = socket.create_connection((host, int(port)), timeout=timeout)
    # Nagle off: exec/write send small back-to-back frames before the first read.
    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    reader = None
    try:
        request = (
            f"GET /v1/sandboxes/{sandbox_id}/agent HTTP/1.1\r\n"
            f"Host: {addr}\r\n"
            "Connection: Upgrade\r\n"
            "Upgrade: silkd\r\n"
            f"Authorization: Bearer {token}\r\n"
            "\r\n"
        )
        sock.sendall(request.encode())
        reader = sock.makefile("rb")
        status = reader.readline(1024).decode(errors="replace")
        parts = status.split(" ", 2)
        code = int(parts[1]) if len(parts) > 1 and parts[1].isdigit() else 0
        body_len = 0
        while True:
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
            # A bogus content-length must not buffer unbounded bytes.
            body = reader.read(min(body_len, MAX_FRAME)).decode(errors="replace") if body_len else ""
            raise APIError("agent upgrade", code, body.strip() or status.strip())
        return Conn(sock, reader)
    except Exception:
        # makefile() holds a ref on the socket; close it too or the fd lingers.
        if reader is not None:
            reader.close()
        sock.close()
        raise
