"""Toolkit wiring against a fake sandbox: tool names/schemas, exec output
shaping, lazy claim, and double-close safety — no real node."""

import asyncio

from cocoonsandbox_langchain import CocoonToolkit


class FakeSandbox:
    def __init__(self):
        self.closed = 0
        self.files = {}

    def run(self, argv, cwd="", on_stdout=None, on_stderr=None, **_):
        assert argv[:2] == ["sh", "-c"]
        if argv[2] == "boom":
            on_stderr(b"kaboom\n")
            return 3
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
    monkeypatch.setattr(kit, "_claim", lambda: fake)
    return kit, fake


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


def test_close_releases_once(monkeypatch):
    kit, fake = hooked(monkeypatch)
    kit.get_tools()[0].invoke({"command": "x"})  # forces the claim
    kit.close()
    kit.close()
    assert fake.closed == 1
