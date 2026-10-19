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


def test_invalid_inputs_and_incomplete_reply(node):
    calls = []

    def incomplete(body, path):
        calls.append(body)
        return 200, {}

    FakeNode.routes[("GET", "/v1/volumes/data")] = incomplete
    client = Client(node)
    with pytest.raises(ValueError):
        client.create_volume("bad/name", 20)
    with pytest.raises(ValueError):
        client.create_volume("data", 0)
    with pytest.raises(ValueError):
        client.create_volume("data", True)
    with pytest.raises(ValueError):
        client.delete_volume("data", '"weak"')
    assert calls == []
    with pytest.raises(APIError, match="reply without volume"):
        client.get_volume("data")
    assert calls == [{}]


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


@pytest.mark.parametrize(
    "field,value",
    [
        ("id", 123),
        ("created_at", "not-a-time"),
        ("created_at", "2026-10-19T00:00:00+00:99"),
        ("created_at", "2026-10-19T00:00:00+01:60"),
        ("size_bytes", -1),
        ("size_bytes", True),
        ("size_bytes", 1.5),
    ],
)
def test_managed_response_rejects_invalid_field_types(node, field, value):
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
    }
    volume[field] = value
    FakeNode.routes[("GET", "/v1/volumes/data")] = lambda body, path: (200, {"volume": volume})
    with pytest.raises(APIError, match="managed volume"):
        Client(node).get_volume("data")


@pytest.mark.parametrize(
    "timestamp",
    [
        "2026-10-19T00:00:00.1Z",
        "2026-10-19T00:00:00.123456789Z",
        "2026-10-19T00:00:00.123456789+05:30",
    ],
)
def test_managed_response_accepts_server_fractional_timestamp(node, timestamp):
    volume = {
        "name": "data",
        "kind": "managed",
        "id": "vol_123",
        "tenant": "t1",
        "node": "node-1",
        "state": "ready",
        "created_at": timestamp,
        "size_bytes": 20 << 30,
        "available": True,
    }
    FakeNode.routes[("GET", "/v1/volumes/data")] = lambda body, path: (200, {"volume": volume})
    assert Client(node).get_volume("data")["created_at"] == timestamp


@pytest.mark.parametrize("holders", [1, "invalid", {}, None])
def test_malformed_holder_container_preserves_api_error(node, holders):
    FakeNode.routes[("GET", "/v1/volumes/data")] = lambda body, path: (409, {"error": "busy", "holders": holders})
    with pytest.raises(APIError) as raised:
        Client(node).get_volume("data")
    assert raised.value.status == 409 and raised.value.message == "busy"
    assert raised.value.holders == []
    assert raised.value.reply == {"error": "busy", "holders": holders}
