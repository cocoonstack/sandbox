"""One relay connection serves a handle's calls back to back; an old daemon,
a zero window, an idle window and a peer that hung up each fall back to a
fresh dial."""

import base64
import contextlib
import json
import socket
import threading
import time

from cocoonsandbox import Client, Sandbox

INPUT_OPS = ("stdin", "stdin_close", "data", "data_end")


class FakeAgent:
    """A relay-side silkd stand-in answering info, fs_stat and exec echo; proto 1 hangs up after one RPC."""

    def __init__(self, proto: int = 2, hang_up_after: int = 0) -> None:
        self.proto = proto
        self.hang_up_after = hang_up_after
        self.upgrades = 0
        self.hangups = 0
        self.closed = 0
        self._server = socket.create_server(("127.0.0.1", 0))
        self.addr = f"127.0.0.1:{self._server.getsockname()[1]}"
        threading.Thread(target=self._accept, daemon=True).start()

    def stop(self) -> None:
        self._server.close()

    def _accept(self) -> None:
        with contextlib.suppress(OSError):
            while True:
                conn, _ = self._server.accept()
                self.upgrades += 1
                threading.Thread(target=self._serve, args=(conn,), daemon=True).start()

    def _serve(self, conn: socket.socket) -> None:
        reader = conn.makefile("rb")
        with conn, contextlib.suppress(OSError):
            while reader.readline() not in (b"\r\n", b""):
                pass
            conn.sendall(b"HTTP/1.1 101 Switching Protocols\r\n\r\n")
            replies = 0
            while True:
                line = reader.readline()
                if not line:
                    break
                op = json.loads(line)["op"]
                if op in INPUT_OPS:
                    continue
                for frame in self._answer(op):
                    conn.sendall(json.dumps(frame).encode() + b"\n")
                replies += 1
                if self.proto < 2 or replies == self.hang_up_after:
                    conn.shutdown(socket.SHUT_WR)
                    self.hangups += 1
                    while reader.readline():
                        pass
                    break
        self.closed += 1

    def _answer(self, op: str) -> list:
        if op == "info":
            return [{"type": "info", "version": "fake", "proto": self.proto, "uptime_secs": 0, "procs": 0}]
        if op == "fs_stat":
            return [{"type": "stat", "info": {"kind": "dir", "size": 0, "mode": 0o755, "mtime_epoch_secs": 0}}]
        if op == "exec":
            out = base64.b64encode(b"42\n").decode()
            return [{"type": "started", "pid": 1}, {"type": "stdout", "data": out}, {"type": "exit", "code": 0}]
        return [{"type": "error", "kind": "unimplemented", "message": op}]


def handle(agent: FakeAgent, **client_kwargs) -> Sandbox:
    return Sandbox(client=Client(agent.addr, **client_kwargs), id="sb_1", token="tok", owner=agent.addr)


def wait_until(cond, message: str) -> None:
    deadline = time.monotonic() + 3
    while time.monotonic() < deadline:
        if cond():
            return
        time.sleep(0.01)
    raise AssertionError(message)


def test_calls_share_one_connection():
    agent = FakeAgent()
    sb = handle(agent)
    try:
        for _ in range(3):
            assert sb.stat("/")["kind"] == "dir"
        assert sb.exec("echo", "42") == "42\n"
    finally:
        sb._pool.drain()
        agent.stop()
    assert agent.upgrades == 1


def test_old_daemon_dials_per_call():
    agent = FakeAgent(proto=1)
    sb = handle(agent)
    try:
        for _ in range(3):
            assert sb.exec("echo", "42") == "42\n"
    finally:
        agent.stop()
    assert agent.upgrades == 4, "the proto probe plus one dial per call"


def test_keep_alive_off_dials_per_call():
    agent = FakeAgent()
    sb = handle(agent, keep_alive=0)
    try:
        for _ in range(3):
            sb.stat("/")
    finally:
        agent.stop()
    assert agent.upgrades == 3


def test_idle_connection_closes():
    agent = FakeAgent()
    sb = handle(agent, keep_alive=0.05)
    try:
        sb.stat("/")
        wait_until(lambda: agent.closed == 1, "idle connection still open after the keep-alive window")
    finally:
        sb._pool.drain()
        agent.stop()


def test_peer_hang_up_is_noticed_before_reuse():
    agent = FakeAgent(hang_up_after=2)
    sb = handle(agent)
    try:
        sb.stat("/")
        wait_until(lambda: agent.hangups == 1, "the fake never hung up")
        assert sb.stat("/")["kind"] == "dir"
    finally:
        sb._pool.drain()
        agent.stop()
    assert agent.upgrades == 2


def test_close_drains_the_parked_connection(monkeypatch):
    agent = FakeAgent()
    sb = handle(agent)
    monkeypatch.setattr(sb._client, "_request", lambda *args, **kwargs: {})
    try:
        sb.stat("/")
        sb.close()
        wait_until(lambda: agent.closed == 1, "parked connection survived close")
    finally:
        agent.stop()
