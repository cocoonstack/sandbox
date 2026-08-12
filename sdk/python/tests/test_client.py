"""Control-plane claims, redirects, volume discovery, errors, and checkpoints."""

import json
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest

from cocoonsandbox import APIError, Client, Template


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
        200, {"id": "sb_1", "token": "tok", "owner_addr": node, "template_digest": "sha256:task"})
    sb = Client(node).new("rt:24.04")
    assert sb.id == "sb_1" and sb.owner == node
    assert sb.template_digest == "sha256:task"


def test_claim_sends_volumes(node):
    seen = []

    def claim(body, path):
        seen.append(body)
        return 200, {"id": "sb_1", "token": "tok", "volumes": [
            {"name": "imagenet", "mount": "/volumes/imagenet"},
            {"name": "weights-llama", "mount": "/models"},
        ]}

    FakeNode.routes[("POST", "/v1/claim")] = claim
    sb = Client(node).new("rt:24.04", volumes=[
        "imagenet", {"name": "weights-llama", "mount": "/models"}])
    assert seen == [{"template": "rt:24.04", "volumes": [
        {"name": "imagenet"},
        {"name": "weights-llama", "mount": "/models"},
    ]}]
    assert sb.volumes == [
        {"name": "imagenet", "mount": "/volumes/imagenet"},
        {"name": "weights-llama", "mount": "/models"},
    ]


def test_claim_rejects_legacy_volume_tuple(node):
    with pytest.raises(TypeError, match="name string or mapping"):
        Client(node).new("rt:24.04", volumes=[("imagenet", "/datasets/imagenet")])


def test_claim_sends_volume_mode_rw(node):
    seen = []

    def claim(body, path):
        seen.append(body)
        return 200, {"id": "sb_1", "token": "tok", "volumes": [
            {"name": "scratch", "mount": "/data", "mode": "rw"},
        ]}

    FakeNode.routes[("POST", "/v1/claim")] = claim
    sb = Client(node).new("rt:24.04", volumes=[{"name": "scratch", "mount": "/data", "mode": "rw"}])
    assert seen == [{"template": "rt:24.04", "volumes": [
        {"name": "scratch", "mount": "/data", "mode": "rw"},
    ]}]
    assert sb.volumes == [{"name": "scratch", "mount": "/data", "mode": "rw"}]


def test_claim_omits_volume_mode_ro(node):
    seen = []

    def claim(body, path):
        seen.append(body)
        return 200, {"id": "sb_1", "token": "tok"}

    FakeNode.routes[("POST", "/v1/claim")] = claim
    Client(node).new("rt:24.04", volumes=[{"name": "imagenet", "mode": "ro"}])
    Client(node).new("rt:24.04", volumes=[{"name": "imagenet", "mode": ""}])
    # "ro" and "" both normalize to an omitted key, byte-identical to a v1 request.
    assert seen[0]["volumes"] == seen[1]["volumes"] == [{"name": "imagenet"}]


def test_claim_rejects_invalid_volume_mode(node):
    with pytest.raises(TypeError, match="mode must be"):
        Client(node).new("rt:24.04", volumes=[{"name": "imagenet", "mode": "rwx"}])


def test_claim_rejects_unknown_volume_key(node):
    with pytest.raises(TypeError, match="only name, mount, and mode"):
        Client(node).new("rt:24.04", volumes=[{"name": "imagenet", "bogus": "x"}])


def test_template_claim_sends_volumes(node):
    seen = []

    def claim(body, path):
        seen.append(body)
        return 200, {"id": "sb_2", "token": "tok", "volumes": [
            {"name": "imagenet", "mount": "/datasets/imagenet"},
        ]}

    FakeNode.routes[("POST", "/v1/claim")] = claim
    sb = Template(Client(node), node, "task:v1", "none", "small").new(
        volumes=[{"name": "imagenet", "mount": "/datasets/imagenet"}])
    assert seen == [{
        "template": "task:v1",
        "net": "none",
        "size": "small",
        "volumes": [{"name": "imagenet", "mount": "/datasets/imagenet"}],
    }]
    assert sb.volumes == [{"name": "imagenet", "mount": "/datasets/imagenet"}]


def test_template_volume_claim_follows_redirect(node):
    seen = []

    def claim(body, path):
        seen.append(body)
        if len(seen) == 1:
            return 200, {"redirect": [node], "require_promoted": True}
        return 200, {"id": "sb_2", "token": "tok", "volumes": body["volumes"]}

    FakeNode.routes[("POST", "/v1/claim")] = claim
    sb = Template(Client(node), node, "task:v1", "none", "small").new(
        volumes=[{"name": "imagenet", "mount": "/datasets/imagenet"}])
    assert "no_redirect" not in seen[0]
    assert seen[1]["no_redirect"] is True
    assert seen[1]["require_promoted"] is True
    assert seen[1]["volumes"] == [{"name": "imagenet", "mount": "/datasets/imagenet"}]
    assert sb.volumes == seen[1]["volumes"]


def test_volume_catalog(node):
    want = [{
        "name": "imagenet",
        "default_mount": "/volumes/imagenet",
        "size_bytes": 42,
        "available": True,
        "nodes": 3,
    }]
    FakeNode.routes[("GET", "/v1/volumes")] = lambda body, path: (200, {"volumes": want})
    assert Client(node).volumes() == want


def test_volume_catalog_surfaces_writable(node):
    want = [{
        "name": "scratch",
        "default_mount": "/data",
        "size_bytes": 42,
        "available": True,
        "nodes": 1,
        "writable": True,
    }]
    FakeNode.routes[("GET", "/v1/volumes")] = lambda body, path: (200, {"volumes": want})
    assert Client(node).volumes() == want


def test_promote_returns_content_digest(node):
    FakeNode.routes[("POST", "/v1/claim")] = lambda body, path: (
        200, {"id": "sb_1", "token": "tok", "owner_addr": node})
    FakeNode.routes[("POST", "/v1/sandboxes/sb_1/promote")] = lambda body, path: (
        200, {
            "key": {"template": "task:v1", "net": "none", "size": "small"},
            "content_digest": "sha256:promoted",
        })

    tpl = Client(node).new("rt:24.04").promote("task:v1")
    assert tpl.name == "task:v1"
    assert tpl.content_digest == "sha256:promoted"


def test_claim_follows_redirect_with_no_redirect(node):
    seen = []

    def claim(body, path):
        seen.append(body)
        if len(seen) == 1:
            return 200, {"redirect": [node]}
        return 200, {"id": "sb_2", "token": "tok"}

    FakeNode.routes[("POST", "/v1/claim")] = claim
    volumes = [{"name": "imagenet", "mount": "/datasets/imagenet"}]
    sb = Client(node).new("rt:24.04", volumes=volumes)
    assert sb.id == "sb_2"
    assert "no_redirect" not in seen[0]
    assert seen[1]["no_redirect"] is True
    assert seen[0]["volumes"] == seen[1]["volumes"] == [
        {"name": "imagenet", "mount": "/datasets/imagenet"},
    ]


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
