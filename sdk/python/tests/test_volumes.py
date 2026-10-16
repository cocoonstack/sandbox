"""Live volume HTTP contract and partial convergence recovery."""

import pytest
from conftest import FakeNode, sandbox_at

from cocoonsandbox import APIError, VolumeError

PATH = "/v1/sandboxes/sb_1/volumes"
VIEW = {
    "volumes_want": [{"name": "data", "mount": "/data"}],
    "volumes": [{"name": "data", "mount": "/data"}],
    "states": [{"name": "data", "state": "attached"}],
}


@pytest.mark.parametrize("method", ["GET", "PUT", "PATCH"])
def test_volume_requests_use_owner_and_sandbox_token(spawn_node, method):
    seen = []

    def reply(body, path):
        seen.append(body)
        assert FakeNode.last_headers["Authorization"] == "Bearer tok"
        return 200, VIEW

    addr = spawn_node({(method, PATH): reply})
    sb = sandbox_at(addr, api_token="operator-token")
    sb._client.addr = "unused.invalid:7777"
    if method == "GET":
        got = sb.get_volumes()
        assert seen == [{}]
    elif method == "PUT":
        got = sb.set_volumes(["data"])
        assert seen == [{"volumes": [{"name": "data"}]}]
    else:
        got = sb.patch_volumes(add=["data"], remove=["old"])
        assert seen == [{"add": [{"name": "data"}], "remove": ["old"]}]
    assert got == VIEW
    assert sb.volumes == VIEW["volumes"]
    assert sb.volumes_want == VIEW["volumes_want"]
    got["volumes"][0]["name"] = "caller copy"
    got["volumes_want"][0]["name"] = "caller copy"
    assert sb.volumes[0]["name"] == sb.volumes_want[0]["name"] == "data"


@pytest.mark.parametrize("method", ["GET", "PUT", "PATCH"])
def test_volume_error_preserves_typed_partial_view(spawn_node, method):
    partial = {
        "volumes_want": [{"name": "new", "mount": "/new"}],
        "volumes": [{"name": "old", "mount": "/old"}],
        "states": [
            {"name": "new", "state": "pending", "error": "retry"},
            {"name": "old", "state": "detaching"},
        ],
        "error": "convergence failed",
    }
    sb = sandbox_at(spawn_node({(method, PATH): lambda body, path: (500, partial)}))
    with pytest.raises(VolumeError) as caught:
        if method == "GET":
            sb.get_volumes()
        elif method == "PUT":
            sb.set_volumes(["new"])
        else:
            sb.patch_volumes(remove=["old"])
    assert isinstance(caught.value, APIError)
    assert caught.value.status == 500
    assert caught.value.response == partial
    assert sb.volumes_want == partial["volumes_want"]
    assert sb.volumes == partial["volumes"]
    caught.value.response["volumes"][0]["name"] = "caller copy"
    assert sb.volumes[0]["name"] == "old"


def test_validation_error_preserves_last_view(spawn_node):
    sb = sandbox_at(spawn_node({("PUT", PATH): lambda body, path: (400, {"error": "invalid volume"})}))
    sb.volumes = [{"name": "old"}]
    sb.volumes_want = [{"name": "wanted"}]
    with pytest.raises(VolumeError) as caught:
        sb.set_volumes(["new"])
    assert caught.value.response is None
    assert sb.volumes == [{"name": "old"}]
    assert sb.volumes_want == [{"name": "wanted"}]


def test_explicit_empty_set_and_omitted_patch(spawn_node):
    seen = []

    def reply(body, path):
        seen.append(body)
        return 200, {"volumes_want": [], "volumes": [], "states": []}

    sb = sandbox_at(spawn_node({("PUT", PATH): reply, ("PATCH", PATH): reply}))
    with pytest.raises(TypeError, match="explicit"):
        sb.set_volumes(None)
    sb.set_volumes([])
    sb.patch_volumes()
    sb.patch_volumes(add=[], remove=[])
    assert seen == [{"volumes": []}, {}, {"add": [], "remove": []}]
    assert sb.volumes == sb.volumes_want == []


def test_identical_put_retries_partial_failure(spawn_node):
    requests = []

    def reply(body, path):
        requests.append(body)
        if len(requests) == 1:
            return 500, {
                "volumes_want": [{"name": "data", "mount": "/data"}],
                "volumes": [],
                "states": [{"name": "data", "state": "pending"}],
                "error": "retry",
            }
        return 200, VIEW

    sb = sandbox_at(spawn_node({("PUT", PATH): reply}))
    with pytest.raises(VolumeError):
        sb.set_volumes([{"name": "data", "mount": "/data"}])
    assert sb.volumes == []
    got = sb.set_volumes([{"name": "data", "mount": "/data"}])
    assert got == VIEW
    assert requests[0] == requests[1]


def test_volume_timeout_uses_client_deadline(black_hole):
    sb = sandbox_at(black_hole, timeout=2)
    with pytest.raises(APIError) as caught:
        sb.get_volumes(timeout=0.02)
    assert caught.value.status == 0
