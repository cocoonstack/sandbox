"""Fault matrix for the cluster redirect follow and the lookup scatter:
dead first candidate (connection refused), 404 first candidate, a definitive
candidate error that still tries the next one, every candidate exhausted
(then the origin fallback heals, fails, or is skipped for a definitive
error) — and lookup must not pay a hung or dead peer's full timeout."""

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


def test_claim_redirect_all_candidates_fail_then_origin_fallback_fails(spawn_node):
    # Every candidate is a stale miss (404); once exhausted, new() falls
    # back to the origin, which this time fails definitively too.
    a = spawn_node({("POST", "/v1/claim"): lambda body, path: (404, {"error": "gone a"})})
    b = spawn_node({("POST", "/v1/claim"): lambda body, path: (404, {"error": "gone b"})})

    calls = []

    def entry_claim(body, path):
        calls.append(body)
        if len(calls) == 1:
            return 200, {"redirect": [a, b]}
        return 409, {"error": "origin also failed"}

    entry = spawn_node({("POST", "/v1/claim"): entry_claim})
    with pytest.raises(APIError) as exc:
        Client(entry).new("rt:24.04")
    assert exc.value.status == 409 and "origin also failed" in exc.value.message
    # The last candidate's failure must survive into the message: it is what
    # says why the claim left the origin in the first place.
    assert "gone b" in exc.value.message
    assert len(calls) == 2 and calls[1]["no_redirect"] is True


def test_claim_redirect_all_candidates_fail_then_origin_heals(spawn_node):
    # The only candidate holds the record but is full (429); once exhausted,
    # new() falls back to the origin, which heals (provisions locally).
    full_calls = []

    def full_claim(body, path):
        full_calls.append(body)
        return 429, {"error": "full"}

    full = spawn_node({("POST", "/v1/claim"): full_claim})

    calls = []

    def entry_claim(body, path):
        calls.append(body)
        if len(calls) == 1:
            return 200, {"redirect": [full]}
        return 200, {"id": "sb_healed", "token": "t"}

    entry = spawn_node({("POST", "/v1/claim"): entry_claim})
    sb = Client(entry).new("rt:24.04")
    assert sb.id == "sb_healed" and sb.owner == entry
    assert len(calls) == 2 and calls[1]["no_redirect"] is True
    assert len(full_calls) == 1


@pytest.mark.parametrize(("status", "message"), [(409, "no egress"), (403, "tenant not allowed")])
def test_claim_redirect_definitive_error_skips_origin_fallback(spawn_node, status, message):
    # A definitive 4xx is not worth trying another candidate, and not worth
    # falling back to the origin either -- the origin would fail the same way.
    bad = spawn_node({("POST", "/v1/claim"): lambda body, path: (status, {"error": message})})

    calls = []

    def entry_claim(body, path):
        calls.append(body)
        return 200, {"redirect": [bad]}

    entry = spawn_node({("POST", "/v1/claim"): entry_claim})
    with pytest.raises(APIError) as exc:
        Client(entry).new("rt:24.04")
    assert exc.value.status == status
    assert len(calls) == 1


def test_claim_redirect_401_candidate_falls_back_to_origin(spawn_node):
    stale_calls = []

    def stale_claim(body, path):
        stale_calls.append(body)
        return 401, {"error": "invalid api token"}

    stale = spawn_node({("POST", "/v1/claim"): stale_claim})

    calls = []

    def entry_claim(body, path):
        calls.append(body)
        if len(calls) == 1:
            return 200, {"redirect": [stale]}
        return 200, {"id": "sb_local", "token": "t"}

    entry = spawn_node({("POST", "/v1/claim"): entry_claim})
    sb = Client(entry).new("rt:24.04")
    assert sb.id == "sb_local" and sb.owner == entry
    assert len(calls) == 2 and calls[1]["no_redirect"] is True
    assert len(stale_calls) == 1


def test_claim_redirect_candidate_definitive_error_still_tries_next_candidate(spawn_node):
    # Candidate one is wrong for this claim (403, definitive) but candidate
    # two would still succeed -- the per-candidate walk retries broadly, so
    # one ill-suited candidate must not cost a candidate that would answer.
    forbidden = spawn_node({("POST", "/v1/claim"): lambda body, path: (403, {"error": "tenant not allowed"})})
    good = spawn_node({("POST", "/v1/claim"): lambda body, path: (200, {"id": "sb_3", "token": "t"})})
    entry = spawn_node({("POST", "/v1/claim"): lambda body, path: (200, {"redirect": [forbidden, good]})})
    assert Client(entry).new("rt:24.04").id == "sb_3"


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
        ("GET", "/v1/peers"): lambda body, path: (200, {"peers": [dead_addr, owner]}),
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
        ("GET", "/v1/peers"): lambda body, path: (200, {"peers": [hung_addr, owner]}),
    })
    start = time.monotonic()
    sb = Client(entry, timeout=10.0).lookup("sb_1", "tok")
    elapsed = time.monotonic() - start
    assert sb.owner == "10.0.0.9:7777"
    assert elapsed < 2.0, elapsed


def test_lookup_all_miss(spawn_node, dead_addr):
    entry = spawn_node({
        ("GET", "/v1/sandboxes/sb_1/owner"): lambda body, path: (404, {"error": "not here"}),
        ("GET", "/v1/peers"): lambda body, path: (200, {"peers": [dead_addr]}),
    })
    with pytest.raises(APIError) as exc:
        Client(entry).lookup("sb_1", "tok")
    assert exc.value.status == 404 and "no owner found" in exc.value.message
