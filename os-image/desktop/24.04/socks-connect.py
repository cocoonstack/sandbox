#!/usr/bin/env python3
"""SOCKS5 front end for the guest's HTTP CONNECT proxy on loopback.

The sandbox reaches the network only through that proxy, and applications that
speak no HTTP proxy for their own protocol — Thunderbird's IMAP and SMTP —
speak SOCKS5 instead. Destinations are forwarded by name, so the proxy resolves
them and the guest needs no resolver of its own.

Usage: socks-connect [listen_host:listen_port]; the upstream comes from
http_proxy in the environment.
"""

from __future__ import annotations

import contextlib
import os
import socket
import socketserver
import sys
import threading
from urllib.parse import urlsplit

ATYP_IPV4 = 1
ATYP_NAME = 3
ATYP_IPV6 = 4
CMD_CONNECT = 1
DEFAULT_LISTEN = "127.0.0.1:1080"
DEFAULT_UPSTREAM = "127.0.0.1:3128"
REPLY_GENERAL = 1
REPLY_OK = 0
REPLY_UNREACHABLE = 4
REPLY_UNSUPPORTED = 7
SOCKS5 = 5
UPSTREAM_TIMEOUT = 30.0


class Server(socketserver.ThreadingTCPServer):
    """Threading server that reuses its address and outlives its handlers."""

    allow_reuse_address = True
    daemon_threads = True

    def __init__(self, listen: str, upstream: tuple[str, int]):
        self.upstream = upstream
        host, _, port = listen.rpartition(":")
        super().__init__((host, int(port)), Handler)


class Handler(socketserver.BaseRequestHandler):
    """One SOCKS5 conversation, tunnelled through an upstream CONNECT."""

    def handle(self) -> None:
        client = self.request
        client.settimeout(UPSTREAM_TIMEOUT)
        try:
            if not self._greet(client):
                return
            target = self._target(client)
            if target is None:
                return
            upstream = self._connect(client, *target)
            if upstream is None:
                return
        except OSError:
            return
        client.settimeout(None)
        with upstream:
            splice(client, upstream)

    def _greet(self, client: socket.socket) -> bool:
        version, count = recv_exact(client, 2)
        if version != SOCKS5:
            return False
        recv_exact(client, count)
        client.sendall(bytes([SOCKS5, 0]))
        return True

    def _target(self, client: socket.socket) -> tuple[str, int] | None:
        version, command, _, atyp = recv_exact(client, 4)
        if version != SOCKS5 or command != CMD_CONNECT:
            reply(client, REPLY_UNSUPPORTED)
            return None
        if atyp == ATYP_NAME:
            host = recv_exact(client, recv_exact(client, 1)[0]).decode("idna")
        elif atyp == ATYP_IPV4:
            host = socket.inet_ntop(socket.AF_INET, recv_exact(client, 4))
        elif atyp == ATYP_IPV6:
            host = socket.inet_ntop(socket.AF_INET6, recv_exact(client, 16))
        else:
            reply(client, REPLY_UNSUPPORTED)
            return None
        port = int.from_bytes(recv_exact(client, 2), "big")
        return host, port

    def _connect(self, client: socket.socket, host: str, port: int) -> socket.socket | None:
        authority = f"[{host}]:{port}" if ":" in host else f"{host}:{port}"
        try:
            upstream = socket.create_connection(self.server.upstream, UPSTREAM_TIMEOUT)
        except OSError:
            reply(client, REPLY_GENERAL)
            return None
        try:
            upstream.settimeout(UPSTREAM_TIMEOUT)
            upstream.sendall(f"CONNECT {authority} HTTP/1.1\r\nHost: {authority}\r\n\r\n".encode())
            status = read_head(upstream).split(b" ")
            if len(status) < 2 or status[1] != b"200":
                reply(client, REPLY_UNREACHABLE)
                upstream.close()
                return None
        except OSError:
            upstream.close()
            reply(client, REPLY_GENERAL)
            return None
        upstream.settimeout(None)
        reply(client, REPLY_OK)
        return upstream


def main(argv: list[str]) -> int:
    listen = argv[1] if len(argv) > 1 else DEFAULT_LISTEN
    host, _, port = urlsplit(os.environ.get("http_proxy") or "").netloc.rpartition(":")
    upstream = (host, int(port)) if port.isdigit() else split_hostport(DEFAULT_UPSTREAM)
    with Server(listen, upstream) as server:
        server.serve_forever()
    return 0


def recv_exact(conn: socket.socket, count: int) -> bytes:
    buf = bytearray()
    while len(buf) < count:
        chunk = conn.recv(count - len(buf))
        if not chunk:
            raise OSError("peer closed mid-message")
        buf += chunk
    return bytes(buf)


def read_head(conn: socket.socket) -> bytes:
    buf = bytearray()
    while b"\r\n\r\n" not in buf:
        chunk = conn.recv(4096)
        if not chunk:
            raise OSError("upstream closed before the response head")
        buf += chunk
        if len(buf) > 65536:
            raise OSError("upstream response head too long")
    return bytes(buf).split(b"\r\n", 1)[0]


def reply(conn: socket.socket, code: int) -> None:
    conn.sendall(bytes([SOCKS5, code, 0, ATYP_IPV4, 0, 0, 0, 0, 0, 0]))


def split_hostport(value: str) -> tuple[str, int]:
    host, _, port = value.rpartition(":")
    return host, int(port)


def splice(left: socket.socket, right: socket.socket) -> None:
    pump = threading.Thread(target=copy_stream, args=(right, left), daemon=True)
    pump.start()
    copy_stream(left, right)
    pump.join()


def copy_stream(src: socket.socket, dst: socket.socket) -> None:
    with contextlib.suppress(OSError):
        while True:
            chunk = src.recv(65536)
            if not chunk:
                break
            dst.sendall(chunk)
    # half-close so the peer sees the end of stream and the other pump drains
    for conn, how in ((src, socket.SHUT_RD), (dst, socket.SHUT_WR)):
        with contextlib.suppress(OSError):
            conn.shutdown(how)


if __name__ == "__main__":
    sys.exit(main(sys.argv))
