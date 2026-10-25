"""Control-plane claims, redirects, volume discovery, errors, promote, renew, checkpoints, and deadlines."""

import threading
import time
from http.server import HTTPServer, ThreadingHTTPServer

import pytest
from conftest import FakeNode, sandbox_at

from cocoonsandbox import APIError, Client, Template


class KeptNode(FakeNode):
    protocol_version = "HTTP/1.1"
    conns = 0
    drop_after_reply = False

    def setup(self):
        super().setup()
        KeptNode.conns += 1

    def _reply(self, code, payload):
        super()._reply(code, payload)
        self.close_connection = self.drop_after_reply


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
    assert sb.volumes == [
        {"name": "imagenet", "attach_only": True},
        {"name": "scratch", "mode": "rw", "attach_only": True},
    ]


def test_template_claim_attaches_volumes_without_mounting(node):
    seen = recording_claim({"id": "sb_2", "token": "tok", "volumes": [{"name": "imagenet"}]})
    sb = Template(Client(node), node, "task:v1", "none", "small").new(volumes=["imagenet"], mount=False)
    assert seen[0]["volumes_attach_only"] is True
    assert seen[0]["volumes"] == [{"name": "imagenet"}]
    assert sb.volumes == [{"name": "imagenet", "attach_only": True}]


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


def test_template_claim_asks_for_the_promoted_template(node):
    seen = recording_claim({"id": "sb_9", "token": "tok"})
    Template(Client(node), node, "task:v1", "none", "small").new()
    assert seen == [
        {"template": "task:v1", "net": "none", "size": "small", "no_redirect": True, "require_promoted": True}
    ]


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
            "require_promoted": True,
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


@pytest.mark.parametrize(("keep_alive", "conns"), [(30.0, 1), (0, 2)])
def test_control_calls_reuse_one_connection(kept_node, keep_alive, conns, fd_floor):
    KeptNode.routes[("POST", "/v1/claim")] = lambda body, path: (200, {"id": "sb_1", "token": "tok"})
    client = Client(kept_node, keep_alive=keep_alive)
    client.new("rt:24.04")
    client.new("rt:24.04")
    assert KeptNode.conns == conns, KeptNode.conns


def test_a_parked_connection_the_node_closed_is_not_reused(kept_node, fd_floor):
    KeptNode.routes[("POST", "/v1/claim")] = lambda body, path: (200, {"id": "sb_1", "token": "tok"})
    KeptNode.drop_after_reply = True
    client = Client(kept_node)
    client.new("rt:24.04")
    time.sleep(0.1)
    assert client.new("rt:24.04").id == "sb_1"
    assert KeptNode.conns == 2


@pytest.fixture
def kept_node():
    server = ThreadingHTTPServer(("127.0.0.1", 0), KeptNode)
    KeptNode.routes, KeptNode.conns, KeptNode.drop_after_reply = {}, 0, False
    threading.Thread(target=server.serve_forever, daemon=True).start()
    yield f"127.0.0.1:{server.server_port}"
    server.shutdown()


@pytest.mark.parametrize("promoted", [False, True])
def test_claim_rejects_mixed_mounting_before_request(node, promoted):
    entries = [{"name": "mounted-ro"}, {"name": "device-rw", "mode": "rw", "attach_only": True}]
    seen = recording_claim({"id": "sb_1", "token": "tok"})
    client = Client(node)
    with pytest.raises(TypeError, match="claim without volumes"):
        if promoted:
            Template(client, node, "task:v1", "none", "small").new(volumes=entries)
        else:
            client.new("rt:24.04", volumes=entries)
    assert seen == []


@pytest.mark.parametrize("promoted", [False, True])
@pytest.mark.parametrize("global_flag", [False, True])
def test_claim_attach_only_uses_legacy_wire_encoding(node, promoted, global_flag):
    entries = [{"name": "data", "attach_only": not global_flag}, {"name": "work", "mode": "rw", "attach_only": True}]
    wire_entries = [{"name": "data"}, {"name": "work", "mode": "rw"}]
    seen = recording_claim({"id": "sb_1", "token": "tok", "volumes": wire_entries})
    client = Client(node)
    if promoted:
        sb = Template(client, node, "task:v1", "none", "small").new(volumes=entries, mount=not global_flag)
    else:
        sb = client.new("rt:24.04", volumes=entries, mount=not global_flag)
    assert seen[0]["volumes_attach_only"] is True
    assert seen[0]["volumes"] == wire_entries
    assert sb.volumes == [{**entry, "attach_only": True} for entry in wire_entries]
    assert entries[0]["attach_only"] is not global_flag


@pytest.mark.parametrize("promoted", [False, True])
def test_claim_rejects_per_entry_attach_only_mount(node, promoted):
    entries = [{"name": "data", "mount": "/data", "attach_only": True}]
    with pytest.raises(TypeError, match="meaningless with attach_only=True"):
        if promoted:
            Template(Client(node), node, "task:v1", "none", "small").new(volumes=entries)
        else:
            Client(node).new("rt:24.04", volumes=entries)


@pytest.mark.parametrize("promoted", [False, True])
def test_claim_methods_reject_non_volume_sequences_before_request(node, promoted):
    seen = recording_claim({"id": "sb_1", "token": "tok"})
    client = Client(node)
    claim = (
        Template(client, node, "task:v1", "none", "small").new
        if promoted
        else lambda **kw: client.new("rt:24.04", **kw)
    )
    for volumes in ("data", "", b"data", b"", bytearray(), {"name": "data"}):
        with pytest.raises(TypeError):
            claim(volumes=volumes)
    assert seen == []


@pytest.mark.parametrize("promoted", [False, True])
@pytest.mark.parametrize("field", ["name", "mount", "mode"])
def test_claim_methods_reject_non_string_volume_fields_before_request(node, promoted, field):
    seen = recording_claim({"id": "sb_1", "token": "tok"})
    client = Client(node)
    claim = (
        Template(client, node, "task:v1", "none", "small").new
        if promoted
        else lambda **kw: client.new("rt:24.04", **kw)
    )
    volume = {"name": "data", field: True}
    with pytest.raises(TypeError):
        claim(volumes=[volume])
    assert seen == []


@pytest.mark.parametrize("promoted", [False, True])
def test_claim_methods_preserve_empty_volume_sequence_semantics(node, promoted):
    seen = recording_claim({"id": "sb_1", "token": "tok"})
    client = Client(node)
    claim = (
        Template(client, node, "task:v1", "none", "small").new
        if promoted
        else lambda **kw: client.new("rt:24.04", **kw)
    )
    for volumes in (None, [], ()):
        claim(volumes=volumes)
    assert len(seen) == 3
    assert all("volumes" not in body for body in seen)


@pytest.mark.parametrize("promoted", [False, True])
@pytest.mark.parametrize("kind", ["list", "tuple", "generator", "set", "empty-generator"])
def test_claim_volume_iterables_are_consumed_once(node, promoted, kind):
    seen = recording_claim({"id": "sb_1", "token": "tok"})
    consumed = []

    def generate():
        if kind != "empty-generator":
            consumed.append("data")
            yield "data"

    volumes = {"list": ["data"], "tuple": ("data",), "set": {"data"}}.get(kind)
    if volumes is None:
        volumes = generate()
    client = Client(node)
    if promoted:
        Template(client, node, "task:v1", "none", "small").new(volumes=volumes)
        assert seen[0].get("no_redirect", False) is (kind == "empty-generator")
    else:
        client.new("rt:24.04", volumes=volumes)
    if kind == "empty-generator":
        assert "volumes" not in seen[0]
    else:
        assert seen[0]["volumes"] == [{"name": "data"}]
    assert consumed == (["data"] if kind == "generator" else [])


def test_claim_and_summary_response_inference_copies_wire_data(node):
    client = Client(node)
    reply = {"id": "sb_1", "token": "tok", "volumes": [{"name": "raw"}, {"name": "mounted", "mount": "/data"}]}
    FakeNode.routes[("POST", "/v1/claim")] = lambda body, path: (200, reply)
    sb = client.new("rt:24.04")
    assert sb.volumes[0]["attach_only"] is True
    assert "attach_only" not in sb.volumes[1]
    assert "attach_only" not in reply["volumes"][0]
    sb.volumes[0]["name"] = "caller"
    assert sb.volumes_want[0]["name"] == reply["volumes"][0]["name"] == "raw"
    rows = [
        {"id": "sb_1", "volumes": [{"name": "raw"}], "volumes_want": [{"name": "raw", "mount": ""}]},
        {"id": "sb_2", "volumes": [], "volumes_want": []},
        {"id": "sb_3"},
    ]
    FakeNode.routes[("GET", "/v1/sandboxes")] = lambda body, path: (200, {"sandboxes": rows})
    summaries = client.sandboxes()
    assert summaries[0]["volumes"][0]["attach_only"] is True
    assert summaries[0]["volumes_want"][0]["attach_only"] is True
    assert summaries[1]["volumes"] == summaries[1]["volumes_want"] == []
    assert "volumes" not in summaries[2]
    assert "attach_only" not in rows[0]["volumes"][0]


@pytest.mark.parametrize(
    ("path", "payload", "expected"),
    [
        ("/v1/checkpoints/ck_1/claim", {"volumes": [{"name": "raw"}]}, True),
        ("/v1/sandboxes/sb_1/fork", {"children": [{"volumes": [{"name": "raw"}]}]}, True),
        ("/v1/volumes", {"volumes": [{"name": "catalog"}]}, False),
        ("/v1/info", {"metadata": {"volumes": [{"name": "opaque"}]}}, False),
    ],
)
def test_response_decoder_only_interprets_volume_bearing_routes(spawn_node, path, payload, expected):
    addr = spawn_node({("GET", path): lambda body, request_path: (200, payload)})
    reply = Client(addr)._request(addr, "GET", path, None, "inspect")
    if path.endswith("/claim"):
        entry = reply["volumes"][0]
    elif path.endswith("/fork"):
        entry = reply["children"][0]["volumes"][0]
    elif path == "/v1/volumes":
        entry = reply["volumes"][0]
    else:
        entry = reply["metadata"]["volumes"][0]
    assert (entry.get("attach_only") is True) is expected
    assert "attach_only" not in repr(payload)


def test_claim_response_decoding_does_not_depend_on_route(spawn_node):
    path = "/v1/custom-claims"
    payload = {"id": "sb_1", "token": "tok", "volumes": [{"name": "raw"}]}
    addr = spawn_node({("POST", path): lambda body, request_path: (200, payload)})
    sb = Client(addr)._claim_from(addr, {}, path=path)
    assert sb.volumes == [{"name": "raw", "attach_only": True}]
    assert "attach_only" not in payload["volumes"][0]


@pytest.mark.parametrize(
    ("path", "fields"),
    [
        ("/v1/claim", {"volumes": [None]}),
        ("/v1/sandboxes", {"sandboxes": [None]}),
        ("/v1/sandboxes/sb_1/fork", {"children": [None]}),
        ("/v1/sandboxes/sb_1/volumes", {"volumes": ["bad"]}),
    ],
)
def test_malformed_error_details_do_not_hide_http_error(spawn_node, path, fields):
    payload = {"error": "storage unavailable", **fields}
    addr = spawn_node({("GET", path): lambda body, request_path: (503, payload)})
    with pytest.raises(APIError) as caught:
        Client(addr)._request(addr, "GET", path, None, "inspect")
    assert caught.value.status == 503
    assert caught.value.message == "storage unavailable"
    assert caught.value.reply == payload
