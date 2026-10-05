"""The relay upgrade: over plain HTTP the request leaves with the first frame and
the 101 is read on the first recv; a refusal surfaces as an APIError."""

import contextlib
import json
import socket
import threading
import time

import pytest
from conftest import sandbox_at

from cocoonsandbox import APIError, SandboxTimeout
from cocoonsandbox.frames import KEEP_ALIVE_PROTO

REPLIES = {
    "info": {"type": "info", "version": "fake", "proto": KEEP_ALIVE_PROTO, "uptime_secs": 0, "procs": 0},
    "fs_mkdir": {"type": "done"},
}
REJECT = b"HTTP/1.1 404 Not Found\r\nContent-Type: text/plain\r\nContent-Length: 15\r\n\r\nunknown sandbox"


class EarlyFrameAgent:
    """Answers the 101 only after a frame has arrived behind the request, then serves frames until the peer leaves."""

    def __init__(self, reply: bytes = b"HTTP/1.1 101 Switching Protocols\r\n\r\n") -> None:
        self.reply = reply
        self.early: list[str] = []
        self._server = socket.create_server(("127.0.0.1", 0))
        self.addr = f"127.0.0.1:{self._server.getsockname()[1]}"
        threading.Thread(target=self._serve, daemon=True).start()

    def stop(self) -> None:
        self._server.close()

    def _serve(self) -> None:
        conn, _ = self._server.accept()
        with conn, contextlib.suppress(OSError):
            reader = conn.makefile("rb")
            while reader.readline() not in (b"\r\n", b""):
                pass
            conn.settimeout(1)
            op = json.loads(reader.readline())["op"]
            self.early.append(op)
            conn.settimeout(None)
            conn.sendall(self.reply)
            while op:
                conn.sendall(json.dumps(REPLIES[op]).encode() + b"\n")
                line = reader.readline()
                op = json.loads(line)["op"] if line else ""


@pytest.mark.parametrize(("keep_alive", "early"), [(0, ["fs_mkdir"]), (30, ["info"])], ids=["no probe", "proto probe"])
def test_first_frame_rides_ahead_of_the_101(keep_alive, early):
    agent = EarlyFrameAgent()
    sb = sandbox_at(agent.addr, timeout=5, keep_alive=keep_alive)
    try:
        sb.mkdir("/w")
    finally:
        sb._pool.drain()
        agent.stop()
    assert agent.early == early, agent.early


def test_rejected_upgrade_is_an_api_error():
    agent = EarlyFrameAgent(REJECT)
    sb = sandbox_at(agent.addr, timeout=5, keep_alive=0)
    try:
        with pytest.raises(APIError) as refused:
            sb.mkdir("/w")
    finally:
        agent.stop()
    assert (refused.value.status, refused.value.message) == (404, "unknown sandbox"), refused.value
    assert agent.early == ["fs_mkdir"], agent.early


def test_run_timeout_cuts_an_unanswered_upgrade():
    server = socket.create_server(("127.0.0.1", 0))
    addr = f"127.0.0.1:{server.getsockname()[1]}"
    held: list[socket.socket] = []
    threading.Thread(target=lambda: held.append(server.accept()[0]), daemon=True).start()
    sb = sandbox_at(addr, timeout=30, keep_alive=0)
    started = time.monotonic()
    try:
        with pytest.raises(SandboxTimeout):
            sb.run(["true"], timeout=0.2)
    finally:
        server.close()
        for conn in held:
            conn.close()
    assert time.monotonic() - started < 2, "the run deadline must cut a hung upgrade under a long client timeout"
