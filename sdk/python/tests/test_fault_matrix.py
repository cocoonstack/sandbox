"""Fault matrix for the cluster redirect follow and the lookup scatter:
dead first candidate (connection refused), 404 first candidate, all-404 —
and lookup must not pay a hung or dead peer's full timeout."""

import socket
import threading
import time
from http.server import HTTPServer

import pytest
from test_client import FakeNode

from cocoonsandbox import APIError, Client


@pytest.fixture
def spawn_node():
    servers = []

    def spawn(routes):
        handler = type("Node", (FakeNode,), {"routes": routes})
        server = HTTPServer(("127.0.0.1", 0), handler)
        threading.Thread(target=server.serve_forever, daemon=True).start()
        servers.append(server)
        return f"127.0.0.1:{server.server_port}"

    yield spawn
    for server in servers:
        server.shutdown()


@pytest.fixture
def dead_addr():
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()
    return f"127.0.0.1:{port}"


@pytest.fixture
def hung_addr():
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    sock.listen(1)
    yield f"127.0.0.1:{sock.getsockname()[1]}"
    sock.close()


def test_claim_redirect_skips_dead_candidate(spawn_node, dead_addr):
    good = spawn_node({("POST", "/v1/claim"): lambda body, path: (200, {"id": "sb_ok", "token": "t"})})
    entry = spawn_node({("POST", "/v1/claim"): lambda body, path: (200, {"redirect": [dead_addr, good]})})
    sb = Client(entry).new("rt:24.04")
    assert sb.id == "sb_ok" and sb.owner == good


def test_claim_redirect_skips_404_candidate(spawn_node):
    lost = spawn_node({("POST", "/v1/claim"): lambda body, path: (404, {"error": "gone"})})
    good = spawn_node({("POST", "/v1/claim"): lambda body, path: (200, {"id": "sb_2", "token": "t"})})
    entry = spawn_node({("POST", "/v1/claim"): lambda body, path: (200, {"redirect": [lost, good]})})
    assert Client(entry).new("rt:24.04").id == "sb_2"


def test_claim_redirect_all_404_propagates_last(spawn_node):
    a = spawn_node({("POST", "/v1/claim"): lambda body, path: (404, {"error": "gone a"})})
    b = spawn_node({("POST", "/v1/claim"): lambda body, path: (404, {"error": "gone b"})})
    entry = spawn_node({("POST", "/v1/claim"): lambda body, path: (200, {"redirect": [a, b]})})
    with pytest.raises(APIError) as exc:
        Client(entry).new("rt:24.04")
    assert exc.value.status == 404 and "gone b" in exc.value.message


def test_delete_template_skips_dead_owner(spawn_node, dead_addr):
    seen = []

    def delete_ok(body, path):
        seen.append(path)
        return 204, {}

    owner = spawn_node({("DELETE", "/v1/templates"): delete_ok})
    entry = spawn_node({("DELETE", "/v1/templates"): lambda body, path: (200, {"redirect": [dead_addr, owner]})})
    Client(entry).delete_template("tpl")
    assert len(seen) == 1 and "no_redirect=1" in seen[0]


def test_delete_template_all_owners_404(spawn_node):
    a = spawn_node({("DELETE", "/v1/templates"): lambda body, path: (404, {"error": "not held"})})
    b = spawn_node({("DELETE", "/v1/templates"): lambda body, path: (404, {"error": "not held"})})
    entry = spawn_node({("DELETE", "/v1/templates"): lambda body, path: (200, {"redirect": [a, b]})})
    with pytest.raises(APIError) as exc:
        Client(entry).delete_template("tpl")
    assert exc.value.status == 404


def test_delete_template_server_error_raises_immediately(spawn_node):
    touched = []

    def fail(body, path):
        touched.append("bad")
        return 500, {"error": "boom"}

    def ok(body, path):
        touched.append("good")
        return 204, {}

    bad = spawn_node({("DELETE", "/v1/templates"): fail})
    good = spawn_node({("DELETE", "/v1/templates"): ok})
    entry = spawn_node({("DELETE", "/v1/templates"): lambda body, path: (200, {"redirect": [bad, good]})})
    with pytest.raises(APIError) as exc:
        Client(entry).delete_template("tpl")
    assert exc.value.status == 500
    assert touched == ["bad"]


def test_lookup_dead_peer_resolves_fast(spawn_node, dead_addr):
    owner = spawn_node({("GET", "/v1/sandboxes/sb_1/owner"): lambda body, path: (200, {"owner_addr": "10.0.0.9:7777"})})
    entry = spawn_node({
        ("GET", "/v1/sandboxes/sb_1/owner"): lambda body, path: (404, {"error": "not here"}),
        ("GET", "/v1/info"): lambda body, path: (200, {"peers": [dead_addr, owner]}),
    })
    start = time.monotonic()
    sb = Client(entry, timeout=10.0).lookup("sb_1", "tok")
    elapsed = time.monotonic() - start
    assert sb.owner == "10.0.0.9:7777"
    assert elapsed < 2.0, elapsed


def test_lookup_hung_peer_resolves_fast(spawn_node, hung_addr):
    owner = spawn_node({("GET", "/v1/sandboxes/sb_1/owner"): lambda body, path: (200, {"owner_addr": "10.0.0.9:7777"})})
    entry = spawn_node({
        ("GET", "/v1/sandboxes/sb_1/owner"): lambda body, path: (404, {"error": "not here"}),
        ("GET", "/v1/info"): lambda body, path: (200, {"peers": [hung_addr, owner]}),
    })
    start = time.monotonic()
    sb = Client(entry, timeout=10.0).lookup("sb_1", "tok")
    elapsed = time.monotonic() - start
    assert sb.owner == "10.0.0.9:7777"
    assert elapsed < 2.0, elapsed


def test_lookup_all_miss(spawn_node, dead_addr):
    entry = spawn_node({
        ("GET", "/v1/sandboxes/sb_1/owner"): lambda body, path: (404, {"error": "not here"}),
        ("GET", "/v1/info"): lambda body, path: (200, {"peers": [dead_addr]}),
    })
    with pytest.raises(APIError) as exc:
        Client(entry).lookup("sb_1", "tok")
    assert exc.value.status == 404 and "no owner found" in exc.value.message
