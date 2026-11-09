"""Control-plane claims, redirects, volume discovery, errors, promote, renew, checkpoints, and deadlines."""

import threading
import time
from http.server import ThreadingHTTPServer

import pytest
from cocoonsandbox import APIError, Client, Template
from conftest import FakeNode, bounded, sandbox_at, wait_until


class KeptNode(FakeNode):
    protocol_version = "HTTP/1.1"

    def setup(self):
        super().setup()
        self.server.conns += 1

    def finish(self):
        super().finish()
        self.connection.close()
        self.server.closed += 1

    def _reply(self, code, payload):
        super()._reply(code, payload)
        self.close_connection = self.server.drop_after_reply


@pytest.fixture
def routes():
    return {}


@pytest.fixture
def headers():
    return {}


@pytest.fixture
def node(spawn_node, routes, headers):
    return spawn_node(routes, headers)


def test_claim_returns_the_sandbox_with_its_owner_and_digest(node, routes):
    routes[("POST", "/v1/claim")] = lambda body, path: (
        200,
        {"id": "sb_1", "token": "tok", "owner_addr": node, "template_digest": "sha256:task"},
    )
    sb = Client(node).new("rt:24.04")
    assert sb.id == "sb_1" and sb.owner == node
    assert sb.template_digest == "sha256:task"


def test_claim_sends_volumes(node, routes):
    seen = recording_claim(
        routes,
        {
            "id": "sb_1",
            "token": "tok",
            "volumes": [
                {"name": "imagenet", "mount": "/volumes/imagenet"},
                {"name": "weights-llama", "mount": "/models"},
            ],
        },
    )
    sb = Client(node).new("rt:24.04", volumes=["imagenet", {"name": "weights-llama", "mount": "/models"}])
    assert seen == [
        {
            "template": "rt:24.04",
            "volumes": [
                {"name": "imagenet"},
                {"name": "weights-llama", "mount": "/models"},
            ],
        }
    ]
    assert sb.volumes == [
        {"name": "imagenet", "mount": "/volumes/imagenet"},
        {"name": "weights-llama", "mount": "/models"},
    ]


def test_claim_rejects_invalid_volumes(node):
    with pytest.raises(TypeError, match="name string or mapping"):
        Client(node).new("rt:24.04", volumes=[("imagenet", "/datasets/imagenet")])


@pytest.mark.parametrize(
    "new",
    [
        lambda node: Client(node).new("rt:24.04", volumes="data"),
        lambda node: Template(Client(node), node, "task:v1", "none", "small").new(volumes="data"),
    ],
)
def test_claim_rejects_a_bare_volume_string(node, new):
    with pytest.raises(TypeError, match="volumes must be a list"):
        new(node)


@pytest.mark.parametrize(
    "entry",
    [
        {"name": "scratch", "mount": "/data", "mode": "rw"},
        {"name": "imagenet", "mode": "ro"},
        {"name": "imagenet", "mode": ""},
        {"name": "imagenet", "mount": "/datasets", "attach_only": True, "mode": "future-mode"},
    ],
    ids=["rw", "ro", "empty_mode", "future_mode"],
)
def test_claim_passes_the_volume_entry_through(node, routes, entry):
    seen = recording_claim(routes, {"id": "sb_1", "token": "tok", "volumes": [entry]})
    sb = Client(node).new("rt:24.04", volumes=[entry])
    Template(Client(node), node, "task:v1", "none", "small").new(volumes=[entry])
    assert [request["volumes"] for request in seen] == [[entry], [entry]]
    assert sb.volumes == [entry]


def test_template_claim_asks_for_the_promoted_template(node, routes):
    seen = recording_claim(routes, {"id": "sb_9", "token": "tok"})
    Template(Client(node), node, "task:v1", "none", "small").new()
    assert seen == [
        {"template": "task:v1", "net": "none", "size": "small", "no_redirect": True, "require_promoted": True}
    ]


def test_template_claim_sends_volumes(node, routes):
    seen = recording_claim(
        routes, {"id": "sb_2", "token": "tok", "volumes": [{"name": "imagenet", "mount": "/datasets/imagenet"}]}
    )
    sb = Template(Client(node), node, "task:v1", "none", "small").new(
        volumes=[{"name": "imagenet", "mount": "/datasets/imagenet"}]
    )
    assert seen == [
        {
            "template": "task:v1",
            "net": "none",
            "size": "small",
            "volumes": [{"name": "imagenet", "mount": "/datasets/imagenet"}],
            "require_promoted": True,
        }
    ]
    assert sb.volumes == [{"name": "imagenet", "mount": "/datasets/imagenet"}]


@pytest.mark.parametrize(
    ("new", "promoted"),
    [
        (lambda node, volumes: Client(node).new("rt:24.04", volumes=volumes), False),
        (lambda node, volumes: Template(Client(node), node, "task:v1", "none", "small").new(volumes=volumes), True),
    ],
    ids=["client", "template"],
)
def test_volume_claim_follows_redirect_with_no_redirect(node, routes, new, promoted):
    seen = []

    def claim(body, path):
        seen.append(body)
        if len(seen) == 1:
            return 200, {"redirect": [node], "require_promoted": promoted}
        return 200, {"id": "sb_2", "token": "tok", "volumes": body["volumes"]}

    routes[("POST", "/v1/claim")] = claim
    volumes = [{"name": "imagenet", "mount": "/datasets/imagenet"}]
    sb = new(node, volumes)
    assert sb.id == "sb_2"
    assert "no_redirect" not in seen[0]
    assert seen[1]["no_redirect"] is True
    assert seen[1].get("require_promoted", False) is promoted
    assert seen[0]["volumes"] == seen[1]["volumes"] == volumes
    assert sb.volumes == volumes


def test_volume_catalog_surfaces_writable(node, routes):
    want = [
        {
            "name": "scratch",
            "default_mount": "/data",
            "size_bytes": 42,
            "available": True,
            "nodes": 1,
            "writable": True,
        },
        {
            "name": "imagenet",
            "default_mount": "/volumes/imagenet",
            "size_bytes": 42,
            "available": True,
            "nodes": 3,
        },
    ]
    routes[("GET", "/v1/volumes")] = lambda body, path: (200, {"volumes": want})
    got = Client(node).volumes()
    assert got == want
    assert got[0]["writable"] is True
    assert "writable" not in got[1]


def test_promote_returns_content_digest(node, routes):
    routes[("POST", "/v1/claim")] = lambda body, path: (
        200,
        {"id": "sb_1", "token": "tok", "owner_addr": node},
    )
    routes[("POST", "/v1/sandboxes/sb_1/promote")] = lambda body, path: (
        200,
        {
            "key": {"template": "task:v1", "net": "none", "size": "small"},
            "content_digest": "sha256:promoted",
        },
    )

    tpl = Client(node).new("rt:24.04").promote("task:v1")
    assert tpl.name == "task:v1"
    assert tpl.content_digest == "sha256:promoted"


@pytest.mark.parametrize(("ttl", "sent"), [(90, {"ttl_seconds": 90}), (0, {})])
def test_renew_sends_the_sandbox_token_and_records_the_grant(node, routes, headers, ttl, sent):
    seen = []

    def renew(body, path):
        seen.append(body)
        return 200, {"deadline": "2026-09-23T12:00:00Z"}

    routes[("POST", "/v1/sandboxes/sb_1/renew")] = renew
    sb = sandbox_at(node, api_token="api")
    assert sb.renew(ttl) == "2026-09-23T12:00:00Z"
    assert sb.deadline == "2026-09-23T12:00:00Z"
    assert seen == [sent]
    assert headers["Authorization"] == "Bearer tok"


def test_renew_refusal_leaves_the_deadline(node, routes):
    routes[("POST", "/v1/sandboxes/sb_1/renew")] = lambda body, path: (409, {"error": "sandbox archived"})
    sb = sandbox_at(node)
    sb.deadline = "2026-09-23T11:00:00Z"
    with pytest.raises(APIError) as refused:
        sb.renew(60)
    assert refused.value.status == 409
    assert sb.deadline == "2026-09-23T11:00:00Z"


@pytest.mark.parametrize(
    ("verb", "method", "path"),
    [
        ("drain", "POST", "/v1/drain"),
        ("undrain", "DELETE", "/v1/drain"),
        ("cordon", "POST", "/v1/cordon"),
        ("uncordon", "DELETE", "/v1/cordon"),
    ],
)
def test_node_hold_verbs_hit_their_routes(node, routes, verb, method, path):
    routes[(method, path)] = lambda body, path: (200, {"claimed": 3})
    assert getattr(Client(node), verb)() == {"claimed": 3}


def test_api_error_carries_server_message(node, routes):
    routes[("POST", "/v1/claim")] = lambda body, path: (409, {"error": "no egress"})
    with pytest.raises(APIError) as exc:
        Client(node).new("rt:24.04", net="egress")
    assert exc.value.status == 409 and "no egress" in exc.value.message


def test_checkpoint_listing_binds_handles(node, routes):
    routes[("GET", "/v1/checkpoints")] = lambda body, path: (
        200,
        {"checkpoints": [{"id": "ck_0011223344556677", "name": "s1", "sandbox_id": "sb_1"}]},
    )
    ckpts = Client(node).checkpoints()
    assert len(ckpts) == 1 and ckpts[0].id == "ck_0011223344556677"

    routes[("POST", "/v1/checkpoints/ck_0011223344556677/claim")] = lambda body, path: (
        200,
        {"id": "sb_branch", "token": "t2"},
    )
    branch = ckpts[0].new()
    assert branch.id == "sb_branch"


def test_claim_refuses_a_spent_deadline(node, routes):
    seen = recording_claim(routes, {"id": "sb_1", "token": "tok"})
    with pytest.raises(TimeoutError):
        Client(node).new("rt:24.04", deadline=time.monotonic() - 1)
    assert seen == [], "a spent deadline still reached the node"


def test_claim_deadline_bounds_the_redirect_walk(node, routes, black_hole):
    routes[("POST", "/v1/claim")] = lambda body, path: (200, {"redirect": [black_hole, black_hole]})
    client = Client(node, timeout=60)
    with pytest.raises(TimeoutError):
        bounded(lambda: client.new("rt:24.04", deadline=time.monotonic() + 0.3))


def test_claim_deadline_bounds_the_entry_node(black_hole):
    client = Client(black_hole, timeout=60)
    with pytest.raises(TimeoutError):
        bounded(lambda: client.new("rt:24.04", deadline=time.monotonic() + 0.3))


@pytest.mark.parametrize(("keep_alive", "conns"), [(30.0, 1), (0, 2)])
def test_control_calls_reuse_one_connection(kept_node, keep_alive, conns, fd_floor):
    kept_node.routes[("POST", "/v1/claim")] = lambda body, path: (200, {"id": "sb_1", "token": "tok"})
    client = Client(kept_node.addr, keep_alive=keep_alive)
    client.new("rt:24.04")
    client.new("rt:24.04")
    assert kept_node.conns == conns, kept_node.conns


def test_a_parked_connection_the_node_closed_is_not_reused(kept_node, fd_floor):
    kept_node.routes[("POST", "/v1/claim")] = lambda body, path: (200, {"id": "sb_1", "token": "tok"})
    kept_node.drop_after_reply = True
    client = Client(kept_node.addr)
    client.new("rt:24.04")
    wait_until(lambda: kept_node.closed == 1, "the node never closed the connection")
    assert client.new("rt:24.04").id == "sb_1"
    assert kept_node.conns == 2


def test_close_releases_the_parked_connections(kept_node):
    kept_node.routes[("POST", "/v1/claim")] = lambda body, path: (200, {"id": "sb_1", "token": "tok"})
    with Client(kept_node.addr) as client:
        client.new("rt:24.04")
        assert kept_node.closed == 0
    wait_until(lambda: kept_node.closed == 1, "close left the parked connection open")
    client.close()
    assert client.new("rt:24.04").id == "sb_1"
    assert kept_node.conns == 2


@pytest.fixture
def kept_node():
    server = ThreadingHTTPServer(("127.0.0.1", 0), KeptNode)
    server.routes, server.last_headers = {}, {}
    server.conns, server.closed, server.drop_after_reply = 0, 0, False
    server.addr = f"127.0.0.1:{server.server_port}"
    threading.Thread(target=server.serve_forever, daemon=True).start()
    yield server
    server.shutdown()
    server.server_close()


def recording_claim(routes, reply):
    seen = []

    def claim(body, path):
        seen.append(body)
        return 200, reply

    routes[("POST", "/v1/claim")] = claim
    return seen
