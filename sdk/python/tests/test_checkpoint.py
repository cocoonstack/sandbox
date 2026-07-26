"""Checkpoint claim redirect: mirrors the cluster redirect follow tested for
Client.new in test_fault_matrix.py, applied to Checkpoint.new."""

import socket
import threading
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


def test_checkpoint_new_follows_redirect(spawn_node):
    seen = []

    def claim_at_b(body, path):
        seen.append(body)
        return 200, {"id": "sb_ck1", "token": "tok"}

    node_b = spawn_node({("POST", "/v1/checkpoints/ck_1/claim"): claim_at_b})

    entry_hits = []

    def redirect(body, path):
        entry_hits.append(body)
        return 200, {"redirect": [node_b]}

    entry = spawn_node({("POST", "/v1/checkpoints/ck_1/claim"): redirect})

    sb = Client(entry).checkpoint("ck_1").new()
    assert sb.id == "sb_ck1"
    assert sb.owner == node_b
    assert len(entry_hits) == 1
    assert seen[0]["no_redirect"] is True


def test_checkpoint_new_all_candidates_fail(spawn_node):
    # The probed owner transiently fails (500); once exhausted, new() falls
    # back to the origin, which this time fails definitively too.
    path = "/v1/checkpoints/ck_2/claim"
    broken = spawn_node({("POST", path): lambda body, p: (500, {"error": "boom"})})

    calls = []

    def entry_claim(body, p):
        calls.append(body)
        if len(calls) == 1:
            return 200, {"redirect": [broken]}
        return 409, {"error": "origin also failed"}

    entry = spawn_node({("POST", path): entry_claim})
    with pytest.raises(APIError) as exc:
        Client(entry).checkpoint("ck_2").new()
    assert exc.value.status == 409 and "origin also failed" in exc.value.message
    assert len(calls) == 2 and calls[1]["no_redirect"] is True


def test_checkpoint_new_redirect_fallback_heals(spawn_node):
    # The only probed owner is mid-heal (503); once exhausted, new() falls
    # back to the origin, which heals (pulls the checkpoint) locally.
    path = "/v1/checkpoints/ck_5/claim"
    busy = spawn_node({("POST", path): lambda body, p: (503, {"error": "healing"})})

    calls = []

    def entry_claim(body, p):
        calls.append(body)
        if len(calls) == 1:
            return 200, {"redirect": [busy]}
        return 200, {"id": "sb_healed", "token": "t"}

    entry = spawn_node({("POST", path): entry_claim})
    sb = Client(entry).checkpoint("ck_5").new()
    assert sb.id == "sb_healed" and sb.owner == entry
    assert len(calls) == 2 and calls[1]["no_redirect"] is True


def test_checkpoint_new_redirect_never_yields_empty_id(spawn_node, dead_addr):
    # Regression: new() used to hand a bare redirect reply straight to
    # _handle_from, producing a Sandbox with no id/token. It must now follow
    # the redirect and raise when no candidate answers.
    entry = spawn_node({("POST", "/v1/checkpoints/ck_3/claim"): lambda body, path: (200, {"redirect": [dead_addr]})})

    with pytest.raises(APIError):
        Client(entry).checkpoint("ck_3").new()


def test_checkpoint_new_second_level_redirect_fails(spawn_node):
    # A compliant server never redirects a no_redirect retry; if one does
    # anyway, it must be treated as a failed candidate rather than followed.
    path = "/v1/checkpoints/ck_4/claim"
    node_b = spawn_node({("POST", path): lambda body, p: (200, {"redirect": ["127.0.0.1:1"]})})
    entry = spawn_node({("POST", path): lambda body, p: (200, {"redirect": [node_b]})})

    with pytest.raises(APIError):
        Client(entry).checkpoint("ck_4").new()
