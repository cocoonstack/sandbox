"""Run the documented controller functions through the SDK's HTTP boundary."""

import pytest
from conftest import FakeNode

from cocoonsandbox import Client, VolumeError
from examples.volume_controller import VolumeHooks, hibernate_volume, resume_volume

PATH = "/v1/sandboxes/sb_1"
EMPTY = {"volumes_want": [], "volumes": [], "states": [], "converging": False}
ATTACHED = {
    "volumes_want": [{"name": "home", "mode": "rw"}],
    "volumes": [{"name": "home", "mode": "rw"}],
    "states": [{"name": "home", "state": "attached"}],
    "converging": False,
}


@pytest.fixture
def controller(spawn_node, monkeypatch):
    events = []
    replies = []
    fail = set()
    gate = {"open": True}

    def hook(name):
        def run():
            events.append(name)
            if name == "close":
                gate["open"] = False
            if name in fail:
                raise RuntimeError(name)
            if name == "open":
                gate["open"] = True

        return run

    def patch(body, path):
        assert FakeNode.last_headers["Authorization"] == "Bearer root-token"
        assert not gate["open"]
        events.append(("patch", body))
        if replies:
            return replies.pop(0)
        return 200, EMPTY if "remove" in body else ATTACHED

    def hibernate(body, path):
        assert FakeNode.last_headers["Authorization"] == "Bearer sandbox-token"
        assert not gate["open"]
        events.append("hibernate")
        return 204, {}

    addr = spawn_node({("PATCH", PATH + "/volumes"): patch, ("POST", PATH + "/hibernate"): hibernate})
    client = Client(addr, api_token="root-token")
    sb = client.attach(addr, "sb_1", "sandbox-token")

    def exec_true(*argv):
        assert argv == ("true",)
        assert not gate["open"]
        events.append("wake")
        return ""

    monkeypatch.setattr(sb, "exec", exec_true)
    hooks = VolumeHooks(hook("reserved"), hook("close"), hook("unmount"), hook("verify"), hook("open"))
    return sb, hooks, events, replies, fail, gate


def test_controller_cycle_uses_legacy_replies_and_opens_after_verification(controller):
    sb, hooks, events, _, _, gate = controller
    hibernate_volume(sb, "home", hooks)
    assert not gate["open"]
    resume_volume(sb, "home", hooks)
    assert events == [
        "close",
        "reserved",
        "unmount",
        ("patch", {"remove": ["home"]}),
        "hibernate",
        "close",
        "reserved",
        "wake",
        ("patch", {"add": [{"name": "home", "mode": "rw", "attach_only": True}]}),
        "verify",
        "open",
    ]
    assert gate["open"]
    assert sb.volumes == [{"name": "home", "mode": "rw", "attach_only": True}]


@pytest.mark.parametrize("failure", ["reserved", "unmount"])
def test_controller_never_detaches_when_preconditions_fail(controller, failure):
    sb, hooks, events, _, fail, gate = controller
    fail.add(failure)
    with pytest.raises(RuntimeError, match=failure):
        hibernate_volume(sb, "home", hooks)
    assert not gate["open"]
    assert events == ["close", "reserved"] + (["unmount"] if failure == "unmount" else [])


def test_controller_failed_remove_needs_successful_retry_even_with_empty_view(controller):
    sb, hooks, events, replies, _, gate = controller
    replies.append((503, {**EMPTY, "error": "claim write failed"}))
    with pytest.raises(VolumeError) as caught:
        hibernate_volume(sb, "home", hooks)
    assert caught.value.status == 503
    assert sb.volumes == sb.volumes_want == []
    assert not gate["open"]
    assert "hibernate" not in events

    hibernate_volume(sb, "home", hooks)
    assert [event for event in events if isinstance(event, tuple)] == [("patch", {"remove": ["home"]})] * 2
    assert events[-1] == "hibernate"
    assert not gate["open"]


@pytest.mark.parametrize("remaining", ["volumes_want", "volumes", "states", "converging"])
def test_controller_does_not_hibernate_an_unsettled_remove(controller, remaining):
    sb, hooks, events, replies, _, gate = controller
    replies.append((200, {**EMPTY, remaining: True if remaining == "converging" else [{"name": "other"}]}))
    with pytest.raises(RuntimeError, match="references remain"):
        hibernate_volume(sb, "home", hooks)
    assert "hibernate" not in events
    assert not gate["open"]


@pytest.mark.parametrize("failure", ["attach", "unsettled", "verify"])
def test_controller_restore_failure_keeps_access_closed(controller, failure):
    sb, hooks, events, replies, fail, gate = controller
    if failure == "attach":
        replies.append((400, {"error": "backing missing"}))
    elif failure == "unsettled":
        replies.append((200, {**ATTACHED, "converging": True}))
    else:
        fail.add("verify")
    with pytest.raises(VolumeError if failure == "attach" else RuntimeError):
        resume_volume(sb, "home", hooks)
    assert not gate["open"]
    assert "open" not in events
    assert ("verify" in events) == (failure == "verify")
