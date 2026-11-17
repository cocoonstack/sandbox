"""Over plain HTTP the upgrade request leaves with the first frame; a refusal surfaces as an APIError."""

import contextlib
import json
import select
import socket
import threading

import pytest
from cocoonsandbox import APIError, SandboxTimeout
from conftest import bounded, sandbox_at

PAUSE = 0.2
REPLIES = {"fs_mkdir": {"type": "done"}}
REJECT = b"HTTP/1.1 404 Not Found\r\nContent-Type: text/plain\r\nContent-Length: 15\r\n\r\nunknown sandbox"
REJECT_JSON = (
    b'HTTP/1.1 404 Not Found\r\nContent-Type: application/json\r\nContent-Length: 27\r\n\r\n{"error":"unknown sandbox"}'
)


class EarlyFrameAgent:
    """Answers the 101 only once its frames arrive behind the request, then serves frames until the peer leaves."""

    def __init__(self, reply: bytes = b"HTTP/1.1 101 Switching Protocols\r\n\r\n", frames: int = 1) -> None:
        self.reply = reply
        self.frames = frames
        self.early: list[str] = []
        self._server = socket.create_server(("127.0.0.1", 0))
        self.addr = f"127.0.0.1:{self._server.getsockname()[1]}"
        threading.Thread(target=self._serve, daemon=True).start()

    def stop(self) -> None:
        self._server.close()

    def _serve(self) -> None:
        conn, _ = self._server.accept()
        self._server.close()
        with conn, contextlib.suppress(OSError):
            reader = conn.makefile("rb")
            while reader.readline() not in (b"\r\n", b""):
                pass
            conn.settimeout(1)
            for _ in range(self.frames):
                self.early.append(json.loads(reader.readline())["op"])
            conn.settimeout(None)
            conn.sendall(self.reply + b"".join(json.dumps(REPLIES[op]).encode() + b"\n" for op in self.early))
            for line in iter(reader.readline, b""):
                conn.sendall(json.dumps(REPLIES[json.loads(line)["op"]]).encode() + b"\n")


@pytest.mark.parametrize("keep_alive", [0, 30], ids=["dial per call", "kept connection"])
def test_the_first_frame_rides_ahead_of_the_101(keep_alive):
    agent = EarlyFrameAgent(frames=1)
    sb = sandbox_at(agent.addr, timeout=5, keep_alive=keep_alive)
    try:
        sb.mkdir("/w")
    finally:
        sb._pool.drain()
        agent.stop()
    assert agent.early == ["fs_mkdir"], agent.early


@pytest.mark.parametrize("reply", [REJECT, REJECT_JSON], ids=["text body", "json error body"])
def test_rejected_upgrade_is_an_api_error(reply):
    agent = EarlyFrameAgent(reply)
    sb = sandbox_at(agent.addr, timeout=5, keep_alive=0)
    try:
        with pytest.raises(APIError) as refused:
            sb.mkdir("/w")
    finally:
        agent.stop()
    assert (refused.value.status, refused.value.message) == (404, "unknown sandbox"), refused.value
    assert agent.early == ["fs_mkdir"], agent.early


def test_run_timeout_cuts_an_unanswered_upgrade(black_hole):
    sb = sandbox_at(black_hole, timeout=60, keep_alive=0)
    with pytest.raises(SandboxTimeout):
        bounded(lambda: sb.run(["true"], timeout=0.2))


def test_run_feeds_stdin_only_after_the_101(script_server):
    early: list[bool] = []
    sb = sandbox_at(script_server(serve_a_late_101, early), timeout=5, keep_alive=0)
    assert sb.run(["cat"], stdin=b"x") == 0
    assert early == [False], "stdin left while the first recv could still retime the socket"


def serve_a_late_101(server: socket.socket, early: list[bool]) -> None:
    conn, _ = server.accept()
    with conn, contextlib.suppress(OSError):
        seen = b""
        while b"\n" not in seen.partition(b"\r\n\r\n")[2]:
            seen += conn.recv(4096)
        behind = seen.partition(b"\r\n\r\n")[2].partition(b"\n")[2]
        early.append(bool(behind or select.select([conn], [], [], PAUSE)[0]))
        conn.sendall(b'HTTP/1.1 101 Switching Protocols\r\n\r\n{"type":"started","pid":1}\n{"type":"exit","code":0}\n')
        while conn.recv(4096):
            pass
