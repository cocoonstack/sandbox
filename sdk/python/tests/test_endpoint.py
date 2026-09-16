import socket
import threading

import pytest

from cocoonsandbox import Client, ProtocolError
from cocoonsandbox.conn import dial_agent
from cocoonsandbox.endpoint import _endpoint_url


@pytest.mark.parametrize(
    ("addr", "scheme", "want"),
    [
        ("localhost:7777", "http", "http://localhost:7777"),
        ("localhost:443", "https", "https://localhost:443"),
        ("https://example.com/", "http", "https://example.com"),
        ("http://example.com", "https", "http://example.com"),
        ("[::1]:7777", "https", "https://[::1]:7777"),
        ("https://[::1]", "http", "https://[::1]"),
    ],
)
def test_endpoint_url(addr: str, scheme: str, want: str) -> None:
    assert _endpoint_url(addr, scheme).geturl() == want


@pytest.mark.parametrize(
    "addr",
    [
        "",
        "https://",
        "ftp://node",
        "https://u:p@node",
        "https://node/path",
        "https://node?",
        "https://node#",
        "https://node#x",
        "https://node:0",
        "https://node:65536",
        "https://node:bad",
        "https://node\r\nX: bad",
    ],
)
def test_endpoint_rejects_invalid_origins(addr: str) -> None:
    with pytest.raises(ValueError):
        Client(addr)


def test_tls_handshake_timeout_closes_socket() -> None:
    with socket.create_server(("127.0.0.1", 0)) as server:
        done = threading.Event()

        def stall() -> None:
            with server.accept()[0] as conn:
                conn.settimeout(5)
                while conn.recv(4096):
                    pass
            done.set()

        thread = threading.Thread(target=stall, daemon=True)
        thread.start()
        with pytest.raises(ProtocolError):
            dial_agent(f"https://127.0.0.1:{server.getsockname()[1]}", "sb_1", "token", 0.05)
        assert done.wait(5), "timed-out handshake kept its socket open"
        thread.join()
