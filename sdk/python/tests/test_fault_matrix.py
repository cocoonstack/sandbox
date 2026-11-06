"""Fault matrix for the cluster redirect follow and the lookup scatter."""

import errno
import http.client
import socket
import threading
import time

import pytest
from cocoonsandbox import APIError, Client


@pytest.mark.parametrize(
    ("status", "message"),
    [(0, ""), (404, "gone"), (403, "tenant not allowed")],
    ids=["dead", "404", "403_definitive"],
)
def test_claim_redirect_skips_a_failed_candidate(spawn_node, dead_addr, status, message):
    failed = (
        spawn_node({("POST", "/v1/claim"): lambda body, path: (status, {"error": message})}) if status else dead_addr
    )
    good = spawn_node({("POST", "/v1/claim"): lambda body, path: (200, {"id": "sb_ok", "token": "t"})})
    entry = spawn_node({("POST", "/v1/claim"): lambda body, path: (200, {"redirect": [failed, good]})})
    sb = Client(entry).new("rt:24.04")
    assert sb.id == "sb_ok" and sb.owner == good


def test_claim_redirect_skips_an_unreachable_candidate(spawn_node, monkeypatch):
    good = spawn_node({("POST", "/v1/claim"): lambda body, path: (200, {"id": "sb_ok", "token": "t"})})
    entry = spawn_node({("POST", "/v1/claim"): lambda body, path: (200, {"redirect": ["10.0.0.9:7777", good]})})
    connect = http.client.HTTPConnection.connect

    def unreachable(conn):
        if conn.host == "10.0.0.9":
            raise OSError(errno.EHOSTUNREACH, "no route to host")
        connect(conn)

    monkeypatch.setattr(http.client.HTTPConnection, "connect", unreachable)
    sb = Client(entry).new("rt:24.04")
    assert sb.id == "sb_ok" and sb.owner == good


def test_claim_redirect_stops_when_a_candidate_may_have_claimed(spawn_node):
    server = socket.create_server(("127.0.0.1", 0))

    def drop_reply():
        conn, _ = server.accept()
        conn.recv(65536)
        conn.close()

    threading.Thread(target=drop_reply, daemon=True).start()
    lost = f"127.0.0.1:{server.getsockname()[1]}"
    good_calls = []
    good = spawn_node(
        {("POST", "/v1/claim"): lambda body, path: good_calls.append(body) or (200, {"id": "x", "token": "t"})}
    )
    entry_calls = []
    entry = spawn_node(
        {("POST", "/v1/claim"): lambda body, path: entry_calls.append(body) or (200, {"redirect": [lost, good]})}
    )
    try:
        with pytest.raises(APIError):
            Client(entry).new("rt:24.04")
    finally:
        server.close()
    assert not good_calls and len(entry_calls) == 1


def test_claim_redirect_all_candidates_fail_then_origin_fallback_fails(spawn_node):
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
    assert "gone b" in exc.value.message
    assert len(calls) == 2 and calls[1]["no_redirect"] is True


@pytest.mark.parametrize(("status", "message"), [(429, "full"), (401, "invalid api token")])
def test_claim_redirect_retryable_candidate_falls_back_to_origin(spawn_node, status, message):
    candidate_calls = []

    def candidate_claim(body, path):
        candidate_calls.append(body)
        return status, {"error": message}

    candidate = spawn_node({("POST", "/v1/claim"): candidate_claim})

    calls = []

    def entry_claim(body, path):
        calls.append(body)
        if len(calls) == 1:
            return 200, {"redirect": [candidate]}
        return 200, {"id": "sb_healed", "token": "t"}

    entry = spawn_node({("POST", "/v1/claim"): entry_claim})
    sb = Client(entry).new("rt:24.04")
    assert sb.id == "sb_healed" and sb.owner == entry
    assert len(calls) == 2 and calls[1]["no_redirect"] is True
    assert len(candidate_calls) == 1


@pytest.mark.parametrize(("status", "message"), [(409, "no egress"), (403, "tenant not allowed")])
def test_claim_redirect_definitive_error_skips_origin_fallback(spawn_node, status, message):
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


@pytest.mark.parametrize("peer", ["dead_addr", "black_hole"])
def test_lookup_unreachable_peer_resolves_fast(spawn_node, request, peer):
    unreachable = request.getfixturevalue(peer)
    owner = spawn_node({("GET", "/v1/sandboxes/sb_1/owner"): lambda body, path: (200, {"owner_addr": "10.0.0.9:7777"})})
    entry = spawn_node(
        {
            ("GET", "/v1/sandboxes/sb_1/owner"): lambda body, path: (404, {"error": "not here"}),
            ("GET", "/v1/peers"): lambda body, path: (200, {"peers": [unreachable, owner]}),
        }
    )
    start = time.monotonic()
    sb = Client(entry, timeout=10.0).lookup("sb_1", "tok")
    elapsed = time.monotonic() - start
    assert sb.owner == "10.0.0.9:7777"
    assert elapsed < 2.0, elapsed


def test_lookup_all_miss(spawn_node, dead_addr):
    entry = spawn_node(
        {
            ("GET", "/v1/sandboxes/sb_1/owner"): lambda body, path: (404, {"error": "not here"}),
            ("GET", "/v1/peers"): lambda body, path: (200, {"peers": [dead_addr]}),
        }
    )
    with pytest.raises(APIError) as exc:
        Client(entry).lookup("sb_1", "tok")
    assert exc.value.status == 404 and "no owner found" in exc.value.message


@pytest.mark.parametrize("retired", [False, True], ids=["old_node_404", "old_node_retired"])
def test_close_releases_a_sandbox_that_moved(spawn_node, dead_addr, retired):
    released = []

    def release(body, path):
        released.append(path)
        return (204, {})

    adopter = spawn_node(
        {
            ("GET", "/v1/sandboxes/sb_1/owner"): lambda body, path: (200, {}),
            ("POST", "/v1/sandboxes/sb_1/release"): release,
            ("GET", "/v1/peers"): lambda body, path: (200, {"peers": []}),
        }
    )
    if retired:
        entry, old = adopter, dead_addr
    else:
        entry = old = spawn_node(
            {
                ("POST", "/v1/sandboxes/sb_1/release"): lambda body, path: (404, {"error": "unknown sandbox"}),
                ("GET", "/v1/sandboxes/sb_1/owner"): lambda body, path: (404, {"error": "not here"}),
                ("GET", "/v1/peers"): lambda body, path: (200, {"peers": [adopter]}),
            }
        )
    Client(entry).attach(old, "sb_1", "tok").close()
    assert released


@pytest.mark.parametrize("retired", [False, True], ids=["old_node_404", "old_node_retired"])
def test_a_moved_sandbox_retries_a_control_call_at_its_new_owner(spawn_node, dead_addr, retired):
    adopter = spawn_node(
        {
            ("GET", "/v1/sandboxes/sb_1/owner"): lambda body, path: (200, {}),
            ("POST", "/v1/sandboxes/sb_1/renew"): lambda body, path: (200, {"deadline": "2026-11-06T12:00:00Z"}),
        }
    )
    if retired:
        entry, old = adopter, dead_addr
    else:
        entry = old = spawn_node({("GET", "/v1/peers"): lambda body, path: (200, {"peers": [adopter]})})
    sb = Client(entry).attach(old, "sb_1", "tok")
    assert sb.renew(60) == "2026-11-06T12:00:00Z"
    assert sb.owner == adopter


def test_a_released_sandbox_still_answers_404(spawn_node):
    old = spawn_node({("GET", "/v1/peers"): lambda body, path: (200, {"peers": []})})
    sb = Client(old).attach(old, "sb_1", "tok")
    with pytest.raises(APIError) as exc:
        sb.renew(60)
    assert exc.value.status == 404 and sb.owner == old


def test_close_reports_a_retired_node_when_no_node_holds_the_sandbox(spawn_node, dead_addr):
    entry = spawn_node(
        {
            ("GET", "/v1/sandboxes/sb_1/owner"): lambda body, path: (404, {"error": "not here"}),
            ("GET", "/v1/peers"): lambda body, path: (200, {"peers": []}),
        }
    )
    with pytest.raises(APIError) as exc:
        Client(entry).attach(dead_addr, "sb_1", "tok").close()
    assert exc.value.status == 0, exc.value


def test_close_of_a_sandbox_no_node_holds_is_not_an_error(spawn_node):
    gone = spawn_node(
        {
            ("POST", "/v1/sandboxes/sb_1/release"): lambda body, path: (404, {"error": "unknown sandbox"}),
            ("GET", "/v1/sandboxes/sb_1/owner"): lambda body, path: (404, {"error": "not here"}),
            ("GET", "/v1/peers"): lambda body, path: (200, {"peers": []}),
        }
    )
    Client(gone).attach(gone, "sb_1", "tok").close()
