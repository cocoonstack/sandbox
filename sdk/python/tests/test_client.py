"""Control-plane behavior against an in-process fake node: claim happy path,
redirect follow with no_redirect, API error mapping, checkpoint handles."""

import json
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest

from cocoonsandbox import APIError, Client


class FakeNode(BaseHTTPRequestHandler):
    routes = {}

    def do_POST(self):
        self._dispatch("POST")

    def do_GET(self):
        self._dispatch("GET")

    def do_DELETE(self):
        self._dispatch("DELETE")

    def _dispatch(self, method):
        length = int(self.headers.get("Content-Length") or 0)
        body = json.loads(self.rfile.read(length)) if length else {}
        handler = self.routes.get((method, self.path.split("?")[0]))
        if handler is None:
            self._reply(404, {"error": "no route"})
            return
        code, reply = handler(body, self.path)
        self._reply(code, reply)

    def _reply(self, code, payload):
        raw = json.dumps(payload).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    def log_message(self, *args):
        pass


@pytest.fixture
def node():
    server = HTTPServer(("127.0.0.1", 0), FakeNode)
    FakeNode.routes = {}
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield f"127.0.0.1:{server.server_port}"
    server.shutdown()


def test_claim_happy_path(node):
    FakeNode.routes[("POST", "/v1/claim")] = lambda body, path: (
        200, {"id": "sb_1", "token": "tok", "owner_addr": node})
    sb = Client(node).new("rt:24.04")
    assert sb.id == "sb_1" and sb.owner == node


def test_claim_follows_redirect_with_no_redirect(node):
    seen = []

    def claim(body, path):
        seen.append(body)
        if len(seen) == 1:
            return 200, {"redirect": [node]}
        return 200, {"id": "sb_2", "token": "tok"}

    FakeNode.routes[("POST", "/v1/claim")] = claim
    sb = Client(node).new("rt:24.04")
    assert sb.id == "sb_2"
    assert "no_redirect" not in seen[0]
    assert seen[1]["no_redirect"] is True


def test_api_error_carries_server_message(node):
    FakeNode.routes[("POST", "/v1/claim")] = lambda body, path: (409, {"error": "no egress"})
    with pytest.raises(APIError) as exc:
        Client(node).new("rt:24.04", net="egress")
    assert exc.value.status == 409 and "no egress" in exc.value.message


def test_checkpoint_listing_binds_handles(node):
    FakeNode.routes[("GET", "/v1/checkpoints")] = lambda body, path: (
        200, {"checkpoints": [{"id": "ck_0011223344556677", "name": "s1", "sandbox_id": "sb_1"}]})
    ckpts = Client(node).checkpoints()
    assert len(ckpts) == 1 and ckpts[0].id == "ck_0011223344556677"

    FakeNode.routes[("POST", "/v1/checkpoints/ck_0011223344556677/claim")] = lambda body, path: (
        200, {"id": "sb_branch", "token": "t2"})
    branch = ckpts[0].new()
    assert branch.id == "sb_branch"
