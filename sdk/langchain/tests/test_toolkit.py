"""Toolkit wiring against a fake sandbox: tool names/schemas, exec output
shaping, lazy claim, and double-close safety — no real node."""

import asyncio
import time

import pytest
from cocoonsandbox import SilkdError
from cocoonsandbox_langchain import CocoonToolkit
from cocoonsandbox_langchain.toolkit import CALL_TIMEOUT


def test_tools_shape(monkeypatch):
    kit, _ = hooked(monkeypatch)
    tools = kit.get_tools()
    names = [t.name for t in tools]
    assert names == ["sandbox_exec", "sandbox_write_file", "sandbox_read_file", "sandbox_list_dir"]
    assert all(t.description for t in tools)
    assert kit._sb is None, "get_tools must not claim"


def test_exec_tool_output(monkeypatch):
    kit, _ = hooked(monkeypatch)
    exec_tool = kit.get_tools()[0]
    assert exec_tool.invoke({"command": "echo hi"}) == "ran: echo hi\n"
    out = exec_tool.invoke({"command": "boom"})
    assert "kaboom" in out and "exit code: 3" in out
    assert exec_tool.invoke({"command": "hang"}) == "partial\n\ncut off after 300s"


def test_async_bridge(monkeypatch):
    kit, _ = hooked(monkeypatch)
    exec_tool = kit.get_tools()[0]
    result = asyncio.run(exec_tool.ainvoke({"command": "echo async"}))
    assert result == "ran: echo async\n"


def test_file_tools_round_trip(monkeypatch):
    kit, _ = hooked(monkeypatch)
    _, write, read, list_dir = kit.get_tools()
    assert write.invoke({"path": "/w/a.txt", "content": "body"}) == "wrote /w/a.txt"
    assert read.invoke({"path": "/w/a.txt"}) == "body"
    assert "a.txt" in list_dir.invoke({"path": "/w"})


@pytest.mark.parametrize(
    ("name", "args"),
    [
        ("sandbox_exec", {"command": "echo hi"}),
        ("sandbox_write_file", {"path": "/w/a.txt", "content": "body"}),
        ("sandbox_list_dir", {"path": "/w"}),
    ],
)
def test_first_tool_call_claims_inside_the_call_budget(monkeypatch, name, args):
    kit = CocoonToolkit("127.0.0.1:1")
    seen = []

    def claim(deadline=None):
        seen.append(deadline)
        return FakeSandbox()

    monkeypatch.setattr(kit, "_claim", claim)
    next(t for t in kit.get_tools() if t.name == name).invoke(args)
    assert len(seen) == 1 and seen[0] is not None
    assert 0 < seen[0] - time.monotonic() <= CALL_TIMEOUT, seen[0]


def test_exec_reports_a_claim_that_outlives_the_budget(monkeypatch):
    kit = CocoonToolkit("127.0.0.1:1")

    def claim(deadline=None):
        raise TimeoutError("claim timed out")

    monkeypatch.setattr(kit, "_claim", claim)
    assert kit.get_tools()[0].invoke({"command": "echo hi"}) == f"cut off after {CALL_TIMEOUT}s"


def test_close_releases_once(monkeypatch):
    kit, fake = hooked(monkeypatch)
    kit.get_tools()[0].invoke({"command": "x"})
    kit.close()
    kit.close()
    assert fake.closed == 1


def test_use_after_close_raises(monkeypatch):
    kit, _ = hooked(monkeypatch)
    kit.close()
    with pytest.raises(RuntimeError):
        kit.sandbox()


class FakeSandbox:
    def __init__(self):
        self.closed = 0
        self.files = {}

    def run(self, argv, cwd="", on_stdout=None, on_stderr=None, timeout=None, **_):
        assert argv[:2] == ["sh", "-c"], argv
        assert timeout is not None and 0 < timeout <= 300, timeout
        if argv[2] == "boom":
            on_stderr(b"kaboom\n")
            return 3
        if argv[2] == "hang":
            on_stdout(b"partial\n")
            raise TimeoutError("cut")
        on_stdout(f"ran: {argv[2]}\n".encode())
        return 0

    def write_file(self, path, data):
        self.files[path] = data

    def read_file(self, path):
        return self.files[path]

    def list_dir(self, path):
        return [{"name": "a.txt", "kind": "file", "size": 3}]

    def close(self):
        self.closed += 1


def hooked(monkeypatch):
    kit = CocoonToolkit("127.0.0.1:1")
    fake = FakeSandbox()
    monkeypatch.setattr(kit, "_claim", lambda deadline=None: fake)
    return kit, fake


def test_read_file_reports_a_missing_path_as_a_tool_error(monkeypatch):
    kit, fake = hooked(monkeypatch)

    def missing(path):
        raise SilkdError("not_found", "no such file")

    fake.read_file = missing
    tool = next(t for t in kit.get_tools() if t.name == "sandbox_read_file")
    assert "no such file" in tool.invoke({"path": "/nope"})
