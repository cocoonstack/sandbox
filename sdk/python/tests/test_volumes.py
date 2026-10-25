"""Live volume HTTP contract and partial convergence recovery."""

import pytest
from conftest import FakeNode, sandbox_at

from cocoonsandbox import APIError, VolumeError

PATH = "/v1/sandboxes/sb_1/volumes"
VIEW = {
    "volumes_want": [{"name": "data", "mount": "/data"}],
    "volumes": [{"name": "data", "mount": "/data"}],
    "converging": True,
    "states": [{"name": "data", "state": "attached"}],
}


@pytest.mark.parametrize("method", ["GET", "PUT", "PATCH"])
def test_volume_requests_use_owner_and_api_token(spawn_node, method):
    seen = []

    def reply(body, path):
        seen.append(body)
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
    assert FakeNode.last_headers["Authorization"] == "Bearer operator-token"
    assert got == VIEW
    assert sb.volumes == VIEW["volumes"]
    assert sb.volumes_want == VIEW["volumes_want"]
    got["volumes"][0]["name"] = "caller copy"
    got["volumes_want"][0]["name"] = "caller copy"
    assert sb.volumes[0]["name"] == sb.volumes_want[0]["name"] == "data"


@pytest.mark.parametrize("method", ["GET", "PUT", "PATCH"])
def test_volume_error_preserves_typed_partial_view(spawn_node, method):
    partial = {
        "volumes_want": [{"name": "new", "mode": "rw"}],
        "volumes": [{"name": "old"}],
        "converging": False,
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
    expected = {
        **partial,
        "volumes_want": [{**partial["volumes_want"][0], "attach_only": True}],
        "volumes": [{**partial["volumes"][0], "attach_only": True}],
    }
    assert caught.value.response == expected
    assert sb.volumes_want == expected["volumes_want"]
    assert sb.volumes == expected["volumes"]
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
        return 200, {"volumes_want": [], "volumes": [], "converging": False, "states": []}

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
                "converging": False,
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


def test_volume_timeout_bounds_the_call(black_hole):
    sb = sandbox_at(black_hole, timeout=2)
    with pytest.raises(APIError) as caught:
        sb.get_volumes(timeout=0.02)
    assert caught.value.status == 0


@pytest.mark.parametrize("timeout", [0, -1])
def test_volume_methods_reject_non_positive_timeout(black_hole, timeout):
    sb = sandbox_at(black_hole)
    with pytest.raises(ValueError):
        sb.get_volumes(timeout=timeout)


@pytest.mark.parametrize("status", [200, 409])
def test_incomplete_volume_view_preserves_handle(spawn_node, status):
    incomplete = {"volumes_want": [], "volumes": [], "states": []}
    sb = sandbox_at(spawn_node({("GET", PATH): lambda body, path: (status, incomplete)}))
    sb.volumes = [{"name": "old"}]
    sb.volumes_want = [{"name": "wanted"}]
    with pytest.raises(APIError) as caught:
        sb.get_volumes()
    assert sb.volumes == [{"name": "old"}]
    assert sb.volumes_want == [{"name": "wanted"}]
    if status == 409:
        assert isinstance(caught.value, VolumeError)
        assert caught.value.response is None


@pytest.mark.parametrize(
    "call",
    [
        lambda sb: sb.set_volumes("data"),
        lambda sb: sb.patch_volumes(add="data"),
        lambda sb: sb.patch_volumes(remove="old"),
        lambda sb: sb.patch_volumes(remove=b""),
    ],
)
def test_volume_methods_reject_a_bare_string(black_hole, call):
    with pytest.raises(TypeError):
        call(sandbox_at(black_hole))


def test_mixed_volume_modes_roundtrip(spawn_node):
    entries = [
        {"name": "mounted-ro", "mount": "/volumes/mounted-ro"},
        {"name": "mounted-rw", "mount": "/work", "mode": "rw"},
        {"name": "device-ro", "attach_only": True},
        {"name": "device-rw", "mode": "rw", "attach_only": True},
    ]
    view = {"volumes_want": entries, "volumes": entries, "states": [], "converging": False}
    seen = []

    def reply(body, path):
        seen.append(body)
        return 200, view

    sb = sandbox_at(spawn_node({("GET", PATH): reply, ("PUT", PATH): reply, ("PATCH", PATH): reply}))
    got = sb.get_volumes()
    sb.set_volumes(got["volumes_want"])
    sb.patch_volumes(add=got["volumes"])
    assert seen == [{}, {"volumes": entries}, {"add": entries}]
    assert sb.volumes == sb.volumes_want == entries


@pytest.mark.parametrize("value", [None, 0, 1, "true", [], {}])
@pytest.mark.parametrize("method", ["set", "patch"])
def test_attach_only_requires_a_boolean(black_hole, value, method):
    sb = sandbox_at(black_hole)
    with pytest.raises(TypeError, match="attach_only must be a boolean"):
        if method == "set":
            sb.set_volumes([{"name": "data", "attach_only": value}])
        else:
            sb.patch_volumes(add=[{"name": "data", "attach_only": value}])


@pytest.mark.parametrize("method", ["set", "patch"])
def test_attach_only_rejects_a_nonempty_mount(black_hole, method):
    sb = sandbox_at(black_hole)
    with pytest.raises(TypeError, match="meaningless with attach_only=True"):
        if method == "set":
            sb.set_volumes([{"name": "data", "mount": "/data", "attach_only": True}])
        else:
            sb.patch_volumes(add=[{"name": "data", "mount": "/data", "attach_only": True}])


def test_attach_only_empty_mount_and_false_flag_remain_explicit(spawn_node):
    seen = []

    def reply(body, path):
        seen.append(body)
        return 200, {"volumes_want": [], "volumes": [], "states": [], "converging": False}

    sb = sandbox_at(spawn_node({("PUT", PATH): reply}))
    entries = [{"name": "device", "mount": "", "attach_only": True}, {"name": "mounted", "attach_only": False}]
    sb.set_volumes(entries)
    assert seen == [{"volumes": entries}]


@pytest.mark.parametrize("method", ["set", "patch"])
@pytest.mark.parametrize("field", ["name", "mount", "mode"])
def test_live_volume_methods_reject_non_string_fields_before_request(spawn_node, method, field):
    seen = []

    def reply(body, path):
        seen.append(body)
        return 200, {"volumes_want": [], "volumes": [], "states": [], "converging": False}

    sb = sandbox_at(spawn_node({("PUT", PATH): reply, ("PATCH", PATH): reply}))
    volume = {"name": "data", field: True}
    with pytest.raises(TypeError):
        if method == "set":
            sb.set_volumes([volume])
        else:
            sb.patch_volumes(add=[volume])
    assert seen == []


@pytest.mark.parametrize("method", ["set", "patch"])
def test_live_volume_methods_reject_binary_collections_before_request(spawn_node, method):
    seen = []

    def reply(body, path):
        seen.append(body)
        return 200, {"volumes_want": [], "volumes": [], "states": [], "converging": False}

    sb = sandbox_at(spawn_node({("PUT", PATH): reply, ("PATCH", PATH): reply}))
    for volumes in (b"data", b"", bytearray(), {"name": "data"}):
        with pytest.raises(TypeError):
            if method == "set":
                sb.set_volumes(volumes)
            else:
                sb.patch_volumes(add=volumes)
    assert seen == []


@pytest.mark.parametrize("method", ["set", "patch"])
@pytest.mark.parametrize("kind", ["list", "tuple", "generator", "set", "empty-generator"])
def test_live_volume_iterables_are_consumed_once(spawn_node, method, kind):
    seen = []

    def reply(body, path):
        seen.append(body)
        return 200, {"volumes_want": [], "volumes": [], "states": [], "converging": False}

    consumed = []

    def generate():
        if kind != "empty-generator":
            consumed.append("data")
            yield "data"

    volumes = {"list": ["data"], "tuple": ("data",), "set": {"data"}}.get(kind)
    if volumes is None:
        volumes = generate()
    sb = sandbox_at(spawn_node({("PUT", PATH): reply, ("PATCH", PATH): reply}))
    if method == "set":
        sb.set_volumes(volumes)
    else:
        sb.patch_volumes(add=volumes)
    entries = [] if kind == "empty-generator" else [{"name": "data"}]
    assert seen == [{"volumes" if method == "set" else "add": entries}]
    assert consumed == (["data"] if kind == "generator" else [])


@pytest.mark.parametrize("status", [200, 409])
@pytest.mark.parametrize("field", ["volumes", "volumes_want", "states"])
@pytest.mark.parametrize("entry", [None, "bad", []])
def test_malformed_volume_view_preserves_error_and_handle(spawn_node, status, field, entry):
    payload = {**VIEW, "error": "storage unavailable", field: [*VIEW[field], entry]}
    sb = sandbox_at(spawn_node({("GET", PATH): lambda body, path: (status, payload)}))
    sb.volumes = [{"name": "old"}]
    sb.volumes_want = [{"name": "wanted"}]
    with pytest.raises(APIError) as caught:
        sb.get_volumes()
    assert sb.volumes == [{"name": "old"}]
    assert sb.volumes_want == [{"name": "wanted"}]
    if status == 409:
        assert isinstance(caught.value, VolumeError)
        assert caught.value.status == 409
        assert caught.value.message == "storage unavailable"
        assert caught.value.reply == payload
        assert caught.value.response is None
    else:
        assert caught.value.status == 0
        assert caught.value.message == "reply without complete volume view"
