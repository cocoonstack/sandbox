import threading
from http.server import HTTPServer

import pytest
from conftest import FakeNode

from cocoonsandbox import APIError, Client


class VolumeNode(FakeNode):
    def do_PUT(self):
        self._dispatch("PUT")


@pytest.fixture
def node():
    server = HTTPServer(("127.0.0.1", 0), VolumeNode)
    FakeNode.routes = {}
    threading.Thread(target=server.serve_forever, daemon=True).start()
    yield f"127.0.0.1:{server.server_port}"
    server.shutdown()


def test_create_inspect_and_delete_use_immutable_identity(node):
    volume = {
        "name": "data",
        "kind": "managed",
        "id": "vol_123",
        "tenant": "t1",
        "node": "node-1",
        "state": "ready",
        "created_at": "2026-10-19T00:00:00Z",
        "size_bytes": 20 << 30,
        "available": True,
        "holders": [{"sandbox_id": "sb_1", "mode": "rw", "state": "attached"}],
    }
    seen = []

    def create(body, path):
        seen.append(("PUT", body, dict(FakeNode.last_headers)))
        return (201 if len(seen) == 1 else 200), {"volume": volume}

    def inspect(body, path):
        seen.append(("GET", body, dict(FakeNode.last_headers)))
        return 200, {"volume": volume}

    def delete(body, path):
        seen.append(("DELETE", body, dict(FakeNode.last_headers)))
        return 204, {}

    path = "/v1/volumes/data"
    FakeNode.routes[("PUT", path)] = create
    FakeNode.routes[("GET", path)] = inspect
    FakeNode.routes[("DELETE", path)] = delete
    client = Client(node, api_token="token")
    assert client.create_volume("data", 20, tenant="t1") == volume
    assert client.create_volume("data", 20, tenant="t1") == volume
    assert client.get_volume("data") == volume
    client.delete_volume("data", volume["id"])
    client.delete_volume("data", volume["id"])
    assert [request[0] for request in seen] == ["PUT", "PUT", "GET", "DELETE", "DELETE"]
    assert all(request[2]["Authorization"] == "Bearer token" for request in seen)
    assert seen[0][1] == {"size_gib": 20, "tenant": "t1"}
    assert seen[3][2]["If-Match"] == seen[4][2]["If-Match"] == '"vol_123"'


@pytest.mark.parametrize("status", [400, 401, 403, 404, 409, 412, 428, 503])
def test_management_error_keeps_safe_details(node, status):
    FakeNode.routes[("GET", "/v1/volumes/data")] = lambda body, path: (
        status,
        {
            "error": "volume unavailable",
            "code": "volume_busy",
            "volume": "data",
            "holders": [{"sandbox_id": "sb_1", "mode": "rw", "state": "retiring", "host_path": "/secret"}],
        },
    )
    with pytest.raises(APIError) as raised:
        Client(node).get_volume("data")
    error = raised.value
    assert error.status == status and error.message == "volume unavailable"
    assert error.code == "volume_busy" and error.volume == "data"
    assert error.holders == [{"sandbox_id": "sb_1", "mode": "rw", "state": "retiring"}]
    assert "/secret" not in str(error) and "/secret" not in repr(error.holders)


def test_incomplete_reply(node):
    FakeNode.routes[("GET", "/v1/volumes/data")] = lambda body, path: (200, {})
    with pytest.raises(APIError, match="reply without volume"):
        Client(node).get_volume("data")


def test_create_volume_forwards_invalid_name(node):
    refusal = "invalid volume name or size_gib (1 through 1024)"
    seen = []

    def refuse(body, path):
        seen.append((path, body))
        return 400, {"error": refusal}

    for path in ("/v1/volumes/bad%2Fname", "/v1/volumes/cocoon-data", "/v1/volumes/data"):
        FakeNode.routes[("PUT", path)] = refuse
    client = Client(node)
    for name, size in (("bad/name", 20), ("cocoon-data", 20), ("data", 0)):
        with pytest.raises(APIError) as raised:
            client.create_volume(name, size)
        assert raised.value.status == 400 and raised.value.message == refusal
    assert seen == [
        ("/v1/volumes/bad%2Fname", {"size_gib": 20}),
        ("/v1/volumes/cocoon-data", {"size_gib": 20}),
        ("/v1/volumes/data", {"size_gib": 0}),
    ]


def test_management_timeout(black_hole):
    with pytest.raises(APIError) as raised:
        Client(black_hole, timeout=0.05).get_volume("data")
    assert raised.value.status == 0


@pytest.mark.parametrize("status", [200, 202, 302])
def test_delete_requires_confirmed_no_content(node, status):
    FakeNode.routes[("DELETE", "/v1/volumes/data")] = lambda body, path: (status, {})
    with pytest.raises(APIError) as raised:
        Client(node).delete_volume("data", "vol_123")
    assert raised.value.status == status


@pytest.mark.parametrize("holders", [1, "invalid", {}, None])
def test_malformed_holder_container_preserves_api_error(node, holders):
    FakeNode.routes[("GET", "/v1/volumes/data")] = lambda body, path: (409, {"error": "busy", "holders": holders})
    with pytest.raises(APIError) as raised:
        Client(node).get_volume("data")
    assert raised.value.status == 409 and raised.value.message == "busy"
    assert raised.value.holders == []
    assert raised.value.reply == {"error": "busy", "holders": holders}


def test_grow_volume_sends_absolute_size(node):
    volume = {"name": "data", "kind": "managed", "id": "vol_123", "state": "ready", "size_bytes": 40 << 30}
    seen = []

    def grow(body, path):
        seen.append((body, dict(FakeNode.last_headers)))
        return 200, {"volume": volume}

    FakeNode.routes[("POST", "/v1/volumes/data/grow")] = grow
    assert Client(node, api_token="token").grow_volume("data", "vol_123", 40) == volume
    assert len(seen) == 1
    body, headers = seen[0]
    assert body == {"size_gib": 40}
    assert headers["If-Match"] == '"vol_123"' and headers["Authorization"] == "Bearer token"


@pytest.mark.parametrize("status", [400, 401, 404, 409, 412, 428, 503])
def test_grow_volume_preserves_errors_without_retry(node, status):
    calls = []

    def grow(body, path):
        calls.append(body)
        return status, {"error": "growth unavailable"}

    FakeNode.routes[("POST", "/v1/volumes/data/grow")] = grow
    with pytest.raises(APIError) as raised:
        Client(node).grow_volume("data", "vol_123", 40)
    assert raised.value.status == status and raised.value.message == "growth unavailable"
    assert len(calls) == 1


def test_grow_volume_timeout(black_hole):
    with pytest.raises(APIError) as raised:
        Client(black_hole, timeout=0.05).grow_volume("data", "vol_123", 40)
    assert raised.value.status == 0
