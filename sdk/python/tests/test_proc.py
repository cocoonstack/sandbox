"""Process-management verbs against a scripted fake conn: pure frame
plumbing, so the fake pins the framing and terminal semantics."""

from cocoonsandbox import Client, Sandbox


class FakeConn:
    """Answers one RPC with a canned frame list."""

    def __init__(self, frames):
        self._frames = list(frames)
        self.sent = []

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        pass

    def send(self, op, **fields):
        self.sent.append((op, fields))

    def recv(self):
        return self._frames.pop(0)

    def recv_until(self, *terminal):
        while True:
            frame = self.recv()
            yield frame
            if frame["type"] in terminal:
                return

    def close(self):
        pass


def fake_sandbox(monkeypatch, frames):
    sb = Sandbox(client=Client("127.0.0.1:1"), id="sb_1", token="tok", owner="127.0.0.1:1")
    conn = FakeConn(frames)
    monkeypatch.setattr(sb, "_dial", lambda: conn)
    return sb, conn


def test_spawn_returns_pid(monkeypatch):
    sb, conn = fake_sandbox(monkeypatch, [{"type": "started", "pid": 41}])
    assert sb.spawn("sleep", "30") == 41
    op, fields = conn.sent[0]
    assert op == "exec" and fields["detach"] is True and fields["argv"] == ["sleep", "30"]


def test_ps_lists_procs(monkeypatch):
    procs = [{"pid": 41, "argv": ["sleep"], "detached": True, "state": "running",
              "started_at_epoch_secs": 1}]
    sb, _ = fake_sandbox(monkeypatch, [{"type": "procs", "procs": procs}])
    assert sb.ps() == procs


def test_kill_sends_signal(monkeypatch):
    sb, conn = fake_sandbox(monkeypatch, [{"type": "done"}])
    sb.kill(41, signal=15)
    assert conn.sent[0] == ("kill", {"pid": 41, "signal": 15})


def test_logs_running_proc_has_no_code(monkeypatch):
    frames = [{"type": "stdout", "data": b"hello"},
              {"type": "stderr", "data": b"oops"},
              {"type": "done"}]
    sb, _ = fake_sandbox(monkeypatch, frames)
    out, errs = [], []
    assert sb.logs(41, on_stdout=out.append, on_stderr=errs.append) is None
    assert out == [b"hello"] and errs == [b"oops"]


def test_attach_returns_exit_code(monkeypatch):
    frames = [{"type": "stdout", "data": b"late"}, {"type": "exit", "code": 7}]
    sb, _ = fake_sandbox(monkeypatch, frames)
    out = []
    assert sb.attach(41, on_stdout=out.append) == 7
    assert out == [b"late"]
