"""One relay connection serves a handle's calls back to back; the fallbacks dial afresh."""

import base64
import contextlib
import json
import socket
import threading

import pytest
from conftest import accept_upgrade, sandbox_at, wait_until

from cocoonsandbox import APIError, Client, SandboxError, SilkdError
from cocoonsandbox.errors import _mark_gone

INPUT_OPS = ("stdin", "stdin_close", "data", "data_end")


class FakeAgent:
    """A relay-side silkd stand-in; hang_up_after half-closes after N RPCs, slam closes as a failed relay does."""

    def __init__(self, hang_up_after: int = 0, slam: bool = False) -> None:
        self.hang_up_after = hang_up_after
        self.slam = slam
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
        with conn, contextlib.suppress(OSError):
            reader = accept_upgrade(conn)
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
                if self.slam:
                    break
                if replies == self.hang_up_after:
                    conn.shutdown(socket.SHUT_WR)
                    self.hangups += 1
                    while reader.readline():
                        pass
                    break
        self.closed += 1

    def _answer(self, op: str) -> list:
        if op == "info":
            return [{"type": "info", "version": "fake", "uptime_secs": 0, "procs": 0}]
        if op == "fs_stat":
            return [{"type": "stat", "info": {"kind": "dir", "size": 0, "mode": 0o755, "mtime_epoch_secs": 0}}]
        if op == "fs_find":
            return [{"type": "match", "path": "/work/f", "line": 1, "text": "TODO"}, {"type": "done"}]
        if op == "exec":
            out = base64.b64encode(b"42\n").decode()
            return [{"type": "started", "pid": 1}, {"type": "stdout", "data": out}, {"type": "exit", "code": 0}]
        return [{"type": "error", "kind": "unimplemented", "message": op}]


def test_calls_share_one_connection(fd_floor, relay):
    agent, sb = relay()
    for _ in range(3):
        assert sb.stat("/")["kind"] == "dir"
    assert sb.exec("echo", "42") == "42\n"
    assert sb._pool._parked[0][1]._sock.fileno() >= fd_floor
    assert agent.upgrades == 1, agent.upgrades


def test_a_refused_stream_open_keeps_its_connection(relay):
    agent, sb = relay()
    for _ in range(3):
        with pytest.raises(SilkdError):
            sb.watch("/missing")
    assert agent.upgrades == 1


def test_keep_alive_off_dials_per_call(relay):
    agent, sb = relay(keep_alive=0)
    for _ in range(3):
        sb.stat("/")
    assert agent.upgrades == 3


def test_idle_connection_closes(relay):
    agent, sb = relay(keep_alive=0.05)
    sb.stat("/")
    wait_until(lambda: agent.closed == 1, "idle connection still open after the keep-alive window")


def test_peer_hang_up_is_noticed_before_reuse(fd_floor, relay):
    agent, sb = relay(hang_up_after=1)
    sb.stat("/")
    wait_until(lambda: agent.hangups == 1, "the fake never hung up")
    assert sb.stat("/")["kind"] == "dir"
    assert agent.upgrades == 2


def test_run_timeout_bounds_an_unanswered_request():
    server = socket.create_server(("127.0.0.1", 0))
    cut = threading.Event()

    def serve():
        conn, _ = server.accept()
        conn.settimeout(3)
        with conn, contextlib.suppress(OSError):
            reader = accept_upgrade(conn)
            while reader.readline():
                pass
            cut.set()

    threading.Thread(target=serve, daemon=True).start()
    sb = sandbox_at(f"127.0.0.1:{server.getsockname()[1]}")
    try:
        with pytest.raises(TimeoutError):
            sb.run(["true"], timeout=0.5)
        assert cut.wait(1), "the unanswered request outlived the run timeout"
    finally:
        server.close()


def test_run_stops_feeding_stdin_once_the_command_exits():
    server = socket.create_server(("127.0.0.1", 0))
    release = threading.Event()

    def serve():
        conn, _ = server.accept()
        with conn, contextlib.suppress(OSError):
            reader = accept_upgrade(conn)
            for line in iter(reader.readline, b""):
                op = json.loads(line)["op"]
                if op == "info":
                    info = {"type": "info", "version": "fake", "uptime_secs": 0}
                    conn.sendall(json.dumps(info).encode() + b"\n")
                elif op == "exec":
                    conn.sendall(b'{"type":"started","pid":1}\n{"type":"exit","code":0}\n')
                    release.wait(10)
                    return

    threading.Thread(target=serve, daemon=True).start()
    sb = sandbox_at(f"127.0.0.1:{server.getsockname()[1]}")
    codes = []
    call = threading.Thread(
        target=lambda: codes.append(sb.run(["head", "-c", "10"], stdin=bytes(64 << 20), timeout=0.5)), daemon=True
    )
    try:
        call.start()
        call.join(2)
        assert codes == [0], "run outlived its timeout behind a stdin the guest stopped reading"
        assert sb._pool.take() is None
    finally:
        release.set()
        server.close()


def test_close_drains_the_parked_connection(monkeypatch, relay):
    agent, sb = relay()
    monkeypatch.setattr(sb._client, "_request", lambda *args, **kwargs: {})
    sb.stat("/")
    sb.close()
    wait_until(lambda: agent.closed == 1, "parked connection survived close")


def test_an_upload_the_daemon_rejects_at_open_raises_its_error_frame(relay):
    _, sb = relay(slam=True)
    with pytest.raises(SilkdError):
        sb.write_file("/missing/f", bytes(64 << 20))


def test_a_moved_sandbox_follows_its_new_owner_on_the_next_call(spawn_node):
    agent = FakeAgent()
    try:
        directory = spawn_node(
            {("GET", "/v1/sandboxes/sb_1/owner"): lambda body, path: (200, {"owner_addr": agent.addr})}
        )
        old = spawn_node({("GET", "/v1/peers"): lambda body, path: (200, {"peers": [directory]})})
        sb = Client(old).attach(old, "sb_1", "tok")
        assert sb.stat("/")["kind"] == "dir"
        assert sb.owner == agent.addr
        assert sb.exec("echo", "42") == "42\n"
    finally:
        agent.stop()


def test_a_handle_follows_its_sandbox_off_a_retired_node(spawn_node, dead_addr):
    agent = FakeAgent()
    try:
        directory = spawn_node(
            {("GET", "/v1/sandboxes/sb_1/owner"): lambda body, path: (200, {"owner_addr": agent.addr})}
        )
        entry = spawn_node({("GET", "/v1/peers"): lambda body, path: (200, {"peers": [directory]})})
        sb = Client(entry).attach(dead_addr, "sb_1", "tok")
        assert sb.stat("/")["kind"] == "dir"
        assert sb.owner == agent.addr
    finally:
        agent.stop()


@pytest.mark.parametrize("retired", [False, True], ids=["owner_404", "connection_refused"])
@pytest.mark.parametrize("streamed", [True, False], ids=["find_iter", "find"])
def test_find_retries_at_the_new_owner(spawn_node, dead_addr, relay, retired, streamed):
    agent, _ = relay()
    lookups = []

    def locate(body, path):
        lookups.append(path)
        return 200, {"owner_addr": agent.addr}

    directory = spawn_node({("GET", "/v1/sandboxes/sb_1/owner"): locate})
    old = dead_addr if retired else spawn_node({})
    sb = Client(directory, keep_alive=0).attach(old, "sb_1", "tok")
    if streamed:
        with pytest.raises(SandboxError) as failed:
            list(sb.find_iter("/work", "TODO"))
        assert failed.value._owner_gone == (sb.id, old)
        assert sb.owner == agent.addr
        assert len(lookups) == 1
        assert agent.upgrades == 0
        matches = list(sb.find_iter("/work", "TODO"))
    else:
        matches = sb.find("/work", "TODO")
    assert matches == [{"type": "match", "path": "/work/f", "line": 1, "text": "TODO"}]
    assert sb.owner == agent.addr
    assert len(lookups) == 1
    assert agent.upgrades == 1


def test_a_callback_error_tagged_for_this_sandbox_is_not_replayed(spawn_node):
    owner, other = FakeAgent(), FakeAgent()
    try:
        directory = spawn_node(
            {("GET", "/v1/sandboxes/sb_1/owner"): lambda body, path: (200, {"owner_addr": other.addr})}
        )
        sb = Client(directory).attach(owner.addr, "sb_1", "tok")

        def refuse(_: bytes) -> None:
            raise _mark_gone(APIError("agent upgrade", 404, "gone"), (sb.id, sb.owner))

        with pytest.raises(APIError, match="agent upgrade"):
            sb.run(["echo", "42"], on_stdout=refuse)
        assert other.upgrades == 0
    finally:
        owner.stop()
        other.stop()


def test_another_sandboxs_error_in_a_callback_is_not_replayed(spawn_node, dead_addr):
    owner, other = FakeAgent(), FakeAgent()
    try:
        directory = spawn_node(
            {("GET", "/v1/sandboxes/sb_1/owner"): lambda body, path: (200, {"owner_addr": other.addr})}
        )
        sb = Client(directory).attach(owner.addr, "sb_1", "tok")
        gone = Client(directory).attach(dead_addr, "sb_2", "tok")

        def write_elsewhere(_: bytes) -> None:
            gone.write_file("/f", b"x")

        with pytest.raises(SandboxError):
            sb.run(["echo", "42"], on_stdout=write_elsewhere)
        assert other.upgrades == 0
    finally:
        owner.stop()
        other.stop()


@pytest.fixture
def relay():
    started = []

    def start(hang_up_after=0, slam=False, **client_kwargs):
        agent = FakeAgent(hang_up_after, slam)
        sb = sandbox_at(agent.addr, **client_kwargs)
        started.append((agent, sb))
        return agent, sb

    yield start
    for agent, sb in started:
        sb._pool.drain()
        agent.stop()
