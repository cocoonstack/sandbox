from types import SimpleNamespace

import pytest
from conftest import FakeNode

from cocoonsandbox import Client, VolumeError
from examples.volume_controller import VolumeHooks, hibernate_volume, resume_volume

PATH = "/v1/sandboxes/sb_1"
HOME = {"name": "home", "mode": "rw", "attach_only": True}
EMPTY = {"volumes_want": [], "volumes": [], "states": [], "converging": False}
ATTACHED = {
    "volumes_want": [HOME],
    "volumes": [HOME],
    "states": [{"name": "home", "state": "attached"}],
    "converging": False,
}

REMOVE = ("patch", {"remove": ["home"]})
ADD = ("patch", {"add": [HOME]})
REMOVE_STEPS = ["close", "reserved", "unmount", REMOVE]
ADD_STEPS = ["close", "reserved", "wake", ADD]


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
    return SimpleNamespace(sb=sb, hooks=hooks, events=events, replies=replies, fail=fail, gate=gate)


def test_controller_cycle_opens_after_verification(controller):
    hibernate_volume(controller.sb, "home", controller.hooks)
    assert not controller.gate["open"]
    resume_volume(controller.sb, "home", controller.hooks)
    assert controller.events == REMOVE_STEPS + ["hibernate"] + ADD_STEPS + ["verify", "open"]
    assert controller.gate["open"]
    assert controller.sb.volumes == [HOME]


@pytest.mark.parametrize(
    ("failures", "responses", "expected"),
    [
        (("reserved",), [], ["close", "reserved"]),
        (("unmount",), [], ["close", "reserved", "unmount"]),
        ((), [(200, {**EMPTY, "volumes_want": [{"name": "other"}]})], REMOVE_STEPS),
        ((), [(200, {**EMPTY, "volumes": [{"name": "other"}]})], REMOVE_STEPS),
        ((), [(200, {**EMPTY, "states": [{"name": "other"}]})], REMOVE_STEPS),
        ((), [(200, {**EMPTY, "converging": True})], REMOVE_STEPS),
    ],
    ids=["reservation", "unmount", "desired-remains", "applied-remains", "state-remains", "converging"],
)
def test_controller_refuses_sleep(controller, failures, responses, expected):
    controller.fail.update(failures)
    controller.replies.extend(responses)
    with pytest.raises(RuntimeError):
        hibernate_volume(controller.sb, "home", controller.hooks)
    assert controller.events == expected
    assert not controller.gate["open"]


def test_controller_failed_remove_needs_successful_retry_even_with_empty_view(controller):
    controller.replies.append((503, {**EMPTY, "error": "claim write failed"}))
    with pytest.raises(VolumeError) as caught:
        hibernate_volume(controller.sb, "home", controller.hooks)
    assert caught.value.status == 503
    assert controller.sb.volumes == controller.sb.volumes_want == []
    assert not controller.gate["open"]
    assert "hibernate" not in controller.events

    hibernate_volume(controller.sb, "home", controller.hooks)
    assert [event for event in controller.events if isinstance(event, tuple)] == [REMOVE] * 2
    assert controller.events[-1] == "hibernate"
    assert not controller.gate["open"]


@pytest.mark.parametrize(
    ("reply", "failures", "error", "expected"),
    [
        ((400, {"error": "backing missing"}), (), VolumeError, ADD_STEPS),
        ((200, {**ATTACHED, "converging": True}), (), RuntimeError, ADD_STEPS),
        ((200, {**ATTACHED, "volumes": []}), (), RuntimeError, ADD_STEPS),
        ((200, ATTACHED), ("verify",), RuntimeError, ADD_STEPS + ["verify"]),
    ],
    ids=["attach-refused", "converging", "applied-missing", "verification"],
)
def test_controller_restore_failure_keeps_access_closed(controller, reply, failures, error, expected):
    controller.replies.append(reply)
    controller.fail.update(failures)
    with pytest.raises(error):
        resume_volume(controller.sb, "home", controller.hooks)
    assert controller.events == expected
    assert not controller.gate["open"]
