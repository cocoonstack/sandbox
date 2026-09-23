"""Control-plane claims, redirects, volume discovery, errors, promote, renew, checkpoints, and deadlines."""

import threading
import time
from http.server import HTTPServer

import pytest
from conftest import FakeNode, sandbox_at

from cocoonsandbox import APIError, Client, Template


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
        200,
        {"id": "sb_1", "token": "tok", "owner_addr": node, "template_digest": "sha256:task"},
    )
    sb = Client(node).new("rt:24.04")
    assert sb.id == "sb_1" and sb.owner == node
    assert sb.template_digest == "sha256:task"


def test_claim_sends_volumes(node):
    seen = recording_claim(
        {
            "id": "sb_1",
            "token": "tok",
            "volumes": [
                {"name": "imagenet", "mount": "/volumes/imagenet"},
                {"name": "weights-llama", "mount": "/models"},
            ],
        }
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


@pytest.mark.parametrize(
    ("volumes", "match"),
    [
        ([("imagenet", "/datasets/imagenet")], "name string or mapping"),
        ([{"name": "imagenet", "mode": "rwx"}], "volume 'imagenet': mode must be 'rw' or 'ro', got 'rwx'"),
        ([{"name": "imagenet", "bogus": "x"}], r"unexpected key\(s\): bogus"),
    ],
)
def test_claim_rejects_invalid_volumes(node, volumes, match):
    with pytest.raises(TypeError, match=match):
        Client(node).new("rt:24.04", volumes=volumes)


def test_claim_sends_volume_mode_rw(node):
    seen = recording_claim(
        {"id": "sb_1", "token": "tok", "volumes": [{"name": "scratch", "mount": "/data", "mode": "rw"}]}
    )
    sb = Client(node).new("rt:24.04", volumes=[{"name": "scratch", "mount": "/data", "mode": "rw"}])
    assert seen == [
        {
            "template": "rt:24.04",
            "volumes": [
                {"name": "scratch", "mount": "/data", "mode": "rw"},
            ],
        }
    ]
    assert sb.volumes == [{"name": "scratch", "mount": "/data", "mode": "rw"}]


def test_claim_omits_volume_mode_ro(node):
    seen = recording_claim({"id": "sb_1", "token": "tok"})
    Client(node).new("rt:24.04", volumes=[{"name": "imagenet", "mode": "ro"}])
    Client(node).new("rt:24.04", volumes=[{"name": "imagenet", "mode": ""}])
    assert seen[0]["volumes"] == seen[1]["volumes"] == [{"name": "imagenet"}], seen


def test_claim_attaches_volumes_without_mounting(node):
    seen = recording_claim(
        {"id": "sb_1", "token": "tok", "volumes": [{"name": "imagenet"}, {"name": "scratch", "mode": "rw"}]}
    )
    sb = Client(node).new("rt:24.04", volumes=["imagenet", {"name": "scratch", "mode": "rw"}], mount=False)
    assert seen == [
        {
            "template": "rt:24.04",
            "volumes": [{"name": "imagenet"}, {"name": "scratch", "mode": "rw"}],
            "volumes_attach_only": True,
        }
    ]
    assert sb.volumes == [{"name": "imagenet"}, {"name": "scratch", "mode": "rw"}]


def test_template_claim_attaches_volumes_without_mounting(node):
    seen = recording_claim({"id": "sb_2", "token": "tok", "volumes": [{"name": "imagenet"}]})
    sb = Template(Client(node), node, "task:v1", "none", "small").new(volumes=["imagenet"], mount=False)
    assert seen[0]["volumes_attach_only"] is True
    assert seen[0]["volumes"] == [{"name": "imagenet"}]
    assert sb.volumes == [{"name": "imagenet"}]


def test_claim_rejects_mount_without_mounting(node):
    with pytest.raises(TypeError, match="meaningless with mount=False"):
        Client(node).new("rt:24.04", volumes=[{"name": "imagenet", "mount": "/datasets"}], mount=False)
    with pytest.raises(TypeError, match="meaningless with mount=False"):
        Template(Client(node), node, "task:v1", "none", "small").new(
            volumes=[{"name": "imagenet", "mount": "/datasets"}], mount=False
        )


def test_claim_keeps_mounting_by_default(node):
    seen = recording_claim({"id": "sb_1", "token": "tok"})
    Client(node).new("rt:24.04", volumes=["imagenet"])
    assert "volumes_attach_only" not in seen[0]


def test_template_claim_sends_volumes(node):
    seen = recording_claim(
        {"id": "sb_2", "token": "tok", "volumes": [{"name": "imagenet", "mount": "/datasets/imagenet"}]}
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
        }
    ]
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
        volumes=[{"name": "imagenet", "mount": "/datasets/imagenet"}]
    )
    assert "no_redirect" not in seen[0]
    assert seen[1]["no_redirect"] is True
    assert seen[1]["require_promoted"] is True
    assert seen[1]["volumes"] == [{"name": "imagenet", "mount": "/datasets/imagenet"}]
    assert sb.volumes == seen[1]["volumes"]


def test_volume_catalog(node):
    want = [
        {
            "name": "imagenet",
            "default_mount": "/volumes/imagenet",
            "size_bytes": 42,
            "available": True,
            "nodes": 3,
        }
    ]
    FakeNode.routes[("GET", "/v1/volumes")] = lambda body, path: (200, {"volumes": want})
    assert Client(node).volumes() == want


def test_volume_catalog_surfaces_writable(node):
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
    FakeNode.routes[("GET", "/v1/volumes")] = lambda body, path: (200, {"volumes": want})
    got = Client(node).volumes()
    assert got == want
    assert got[0]["writable"] is True
    assert "writable" not in got[1]


def test_promote_returns_content_digest(node):
    FakeNode.routes[("POST", "/v1/claim")] = lambda body, path: (
        200,
        {"id": "sb_1", "token": "tok", "owner_addr": node},
    )
    FakeNode.routes[("POST", "/v1/sandboxes/sb_1/promote")] = lambda body, path: (
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
def test_renew_sends_the_sandbox_token_and_records_the_grant(node, ttl, sent):
    seen = []

    def renew(body, path):
        seen.append(body)
        return 200, {"deadline": "2026-09-23T12:00:00Z"}

    FakeNode.routes[("POST", "/v1/sandboxes/sb_1/renew")] = renew
    sb = sandbox_at(node, api_token="api")
    assert sb.renew(ttl) == "2026-09-23T12:00:00Z"
    assert sb.deadline == "2026-09-23T12:00:00Z"
    assert seen == [sent]
    assert FakeNode.last_headers["Authorization"] == "Bearer tok"


def test_renew_refusal_leaves_the_deadline(node):
    FakeNode.routes[("POST", "/v1/sandboxes/sb_1/renew")] = lambda body, path: (409, {"error": "sandbox archived"})
    sb = sandbox_at(node)
    sb.deadline = "2026-09-23T11:00:00Z"
    with pytest.raises(APIError) as refused:
        sb.renew(60)
    assert refused.value.status == 409
    assert sb.deadline == "2026-09-23T11:00:00Z"


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
    assert (
        seen[0]["volumes"]
        == seen[1]["volumes"]
        == [
            {"name": "imagenet", "mount": "/datasets/imagenet"},
        ]
    )


def test_api_error_carries_server_message(node):
    FakeNode.routes[("POST", "/v1/claim")] = lambda body, path: (409, {"error": "no egress"})
    with pytest.raises(APIError) as exc:
        Client(node).new("rt:24.04", net="egress")
    assert exc.value.status == 409 and "no egress" in exc.value.message


def test_checkpoint_listing_binds_handles(node):
    FakeNode.routes[("GET", "/v1/checkpoints")] = lambda body, path: (
        200,
        {"checkpoints": [{"id": "ck_0011223344556677", "name": "s1", "sandbox_id": "sb_1"}]},
    )
    ckpts = Client(node).checkpoints()
    assert len(ckpts) == 1 and ckpts[0].id == "ck_0011223344556677"

    FakeNode.routes[("POST", "/v1/checkpoints/ck_0011223344556677/claim")] = lambda body, path: (
        200,
        {"id": "sb_branch", "token": "t2"},
    )
    branch = ckpts[0].new()
    assert branch.id == "sb_branch"


def test_claim_refuses_a_spent_deadline(node):
    seen = recording_claim({"id": "sb_1", "token": "tok"})
    with pytest.raises(TimeoutError):
        Client(node).new("rt:24.04", deadline=time.monotonic() - 1)
    assert seen == [], "a spent deadline still reached the node"


def test_claim_deadline_bounds_the_redirect_walk(node, black_hole):
    FakeNode.routes[("POST", "/v1/claim")] = lambda body, path: (200, {"redirect": [black_hole, black_hole]})
    client = Client(node, timeout=2.0)
    started = time.monotonic()
    with pytest.raises(TimeoutError):
        client.new("rt:24.04", deadline=started + 0.3)
    elapsed = time.monotonic() - started
    assert elapsed < 1.5, elapsed


def test_claim_deadline_bounds_the_entry_node(black_hole):
    client = Client(black_hole, timeout=2.0)
    with pytest.raises(TimeoutError):
        client.new("rt:24.04", deadline=time.monotonic() + 0.3)


def recording_claim(reply):
    seen = []

    def claim(body, path):
        seen.append(body)
        return 200, reply

    FakeNode.routes[("POST", "/v1/claim")] = claim
    return seen
