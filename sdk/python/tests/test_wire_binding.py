"""Drives each real Sandbox call site and asserts its frame carries exactly the golden fixture's fields."""

import base64
import contextlib
import json
import socket
import threading

import pytest
from cocoonsandbox import Lsp, Pty, Session
from cocoonsandbox.conn import Conn
from cocoonsandbox.frames import PROTO_VERSION
from conftest import FIXTURES, FakeConn, sandbox_at

CASES = [
    (
        "req_exec",
        [{"type": "started", "pid": 7}, {"type": "exit", "code": 0}],
        lambda sb, f: sb.run(f["argv"], cwd=f["cwd"], env=f["env"], user=f["user"], session=f["session"]),
    ),
    ("req_fs_write", [{"type": "done"}], lambda sb, f: sb.write_file(f["path"], b"x", mode=f.get("mode"))),
    ("req_fs_read", [{"type": "done"}], lambda sb, f: sb.read_file(f["path"])),
    ("req_fs_list", [{"type": "done"}], lambda sb, f: sb.list_dir(f["path"])),
    ("req_fs_stat", [{"type": "stat", "info": {"kind": "file", "size": 1}}], lambda sb, f: sb.stat(f["path"])),
    ("req_fs_mkdir", [{"type": "done"}], lambda sb, f: sb.mkdir(f["path"], parents=f.get("parents", False))),
    ("req_fs_rm", [{"type": "done"}], lambda sb, f: sb.remove(f["path"], recursive=f.get("recursive", False))),
    ("req_fs_rename", [{"type": "done"}], lambda sb, f: sb.rename(f["from"], f["to"])),
    ("req_fs_push", [{"type": "done"}], lambda sb, f: sb.push(f["dest"], b"tar")),
    ("req_fs_pull", [{"type": "done"}], lambda sb, f: sb.pull(f["path"])),
    ("req_fs_find", [{"type": "done"}], lambda sb, f: sb.find(f["path"], f["pattern"], glob=f.get("glob", ""))),
    ("req_fs_replace", [{"type": "done"}], lambda sb, f: sb.replace(f["files"], f["pattern"], f["replacement"])),
    (
        "req_session_create",
        [{"type": "session_created", "id": "sess-1"}],
        lambda sb, f: sb.session(cwd=f["cwd"], env=f["env"]),
    ),
    ("req_session_list", [{"type": "sessions", "sessions": []}], lambda sb, f: sb.sessions()),
    (
        "req_git_clone",
        [{"type": "done"}],
        lambda sb, f: sb.git_clone(f["url"], f["path"], branch=f["branch"], depth=f["depth"], auth=f["auth"]),
    ),
    ("req_git_status", [{"type": "git_status_result", "branch": "main"}], lambda sb, f: sb.git_status(f["path"])),
    ("req_git_add", [{"type": "done"}], lambda sb, f: sb.git_add(f["path"], f["files"])),
    (
        "req_git_commit",
        [{"type": "git_commit_result", "hash": "h"}],
        lambda sb, f: sb.git_commit(f["path"], f["message"], f["author"]),
    ),
    ("req_git_push", [{"type": "done"}], lambda sb, f: sb.git_push(f["path"], auth=f["auth"])),
    ("req_git_pull", [{"type": "done"}], lambda sb, f: sb.git_pull(f["path"], auth=f["auth"])),
    ("req_pty_resize", [{"type": "done"}], lambda sb, f: Pty(sb, None, f["pid"]).resize(f["cols"], f["rows"])),
    ("req_exec_detach", [{"type": "started", "pid": 7}], lambda sb, f: sb.spawn(*f["argv"])),
    ("req_ps", [{"type": "procs", "procs": []}], lambda sb, f: sb.ps()),
    ("req_kill", [{"type": "done"}], lambda sb, f: sb.kill(f["pid"], signal=f["signal"])),
    ("req_logs", [{"type": "exit", "code": 0}, {"type": "done"}], lambda sb, f: sb.logs(f["pid"])),
    ("req_attach", [{"type": "exit", "code": 0}], lambda sb, f: sb.attach(f["pid"])),
    ("req_fs_watch", [{"type": "ready"}], lambda sb, f: sb.watch(f["path"], recursive=f["recursive"]).close()),
    ("req_git_branch", [{"type": "done"}], lambda sb, f: sb.git_create_branch(f["path"], f["name"])),
    ("req_session_rm", [{"type": "done"}], lambda sb, f: Session(sb, f["id"]).close()),
    (
        "req_lsp_start",
        [{"type": "lsp_started", "server_id": "lsp-1"}],
        lambda sb, f: sb.start_lsp(f["language"], root=f["root"]),
    ),
    ("req_lsp_request", [{"type": "ready"}], lambda sb, f: Lsp(sb, f["server_id"]).request().close()),
    ("req_lsp_stop", [{"type": "done"}], lambda sb, f: Lsp(sb, f["server_id"]).stop()),
    ("req_port_forward", [{"type": "ready"}], lambda sb, f: sb.dial_port(f["port"]).close()),
    (
        "req_pty_open",
        [{"type": "started", "pid": 7}],
        lambda sb, f: sb.open_pty(cols=f["cols"], rows=f["rows"], cwd=f["cwd"], env=f["env"], user=f["user"]).close(),
    ),
    ("req_stdin", [], lambda sb, f: write_pty(sb, base64.b64decode(f["data"]))),
    ("req_stdin_close", [{"type": "started", "pid": 7}, {"type": "exit", "code": 0}], lambda sb, f: sb.run(["true"])),
    ("req_data", [{"type": "done"}], lambda sb, f: sb.write_file("/f", base64.b64decode(f["data"]))),
    ("req_data_end", [{"type": "done"}], lambda sb, f: sb.write_file("/f", b"x")),
]

UNSENT = {"req_session_create": {"id"}}
NO_CALL_SITE = {"req_info", "req_tun_up", "req_tun_down"}


class BranchActionConn(FakeConn):
    """Records the action each git_branch verb puts on the wire."""

    def __init__(self, sent):
        super().__init__([])
        self.sent = sent
        self.action = ""

    def send(self, op, **fields):
        self.action = fields.get("action")
        self.sent.append(self.action)

    def recv(self):
        if self.action == "list":
            return {"type": "git_branches", "branches": [], "current": ""}
        return {"type": "done"}


@pytest.mark.parametrize("stem,replies,invoke", CASES, ids=[case[0] for case in CASES])
def test_call_site_matches_fixture(monkeypatch, stem, replies, invoke):
    fixture = json.loads((FIXTURES / f"{stem}.json").read_text())
    sb, sent, thread = fake_sandbox(monkeypatch, replies)
    invoke(sb, fixture)
    thread.join(timeout=5)

    frame = next((frame for frame in sent if frame["op"] == fixture["op"]), None)
    assert frame is not None, f"no {fixture['op']} frame reached the guest: {sent}"
    assert frame["v"] == PROTO_VERSION
    for key, value in frame.items():
        assert key in fixture, f"field {key!r} not in the golden corpus (typo?)"
        if key != "data":
            assert value == fixture[key], f"field {key!r}: {value!r} != {fixture[key]!r}"
    for key in fixture:
        if key in UNSENT.get(stem, ()):
            continue
        assert key in frame, f"fixture field {key!r} was never emitted (dropped field?)"


def test_every_request_fixture_has_a_call_site_or_an_exclusion():
    covered = {case[0] for case in CASES}
    assert covered.isdisjoint(NO_CALL_SITE)
    assert {path.stem for path in FIXTURES.glob("req_*.json")} == covered | NO_CALL_SITE


def test_git_branch_actions_come_from_the_corpus(monkeypatch):
    enums = json.loads((FIXTURES / "enums.json").read_text())
    sb = sandbox_at("127.0.0.1:1", keep_alive=0)
    sent = []
    monkeypatch.setattr(sb, "_dial", lambda deadline=None: BranchActionConn(sent))

    sb.git_branches("/w")
    sb.git_create_branch("/w", "b")
    sb.git_delete_branch("/w", "b")
    sb.git_checkout("/w", "b")
    assert set(sent) == set(enums["git_branch_action"])


def write_pty(sb, data):
    with Pty(sb, sb._dial(), 7) as pty:
        pty.write(data)


def fake_sandbox(monkeypatch, replies):
    """A Sandbox whose _dial yields a real Conn over a socketpair to a scripted guest thread."""
    sent = []
    client_sock, guest_sock = socket.socketpair()

    def guest():
        reader = guest_sock.makefile("rb")
        with contextlib.suppress(OSError, ValueError):
            line = reader.readline()
            if line:
                sent.append(json.loads(line))
            for reply in replies:
                guest_sock.sendall(json.dumps(reply).encode() + b"\n")
            while True:
                line = reader.readline()
                if not line:
                    break
                sent.append(json.loads(line))
        with contextlib.suppress(OSError):
            guest_sock.close()

    thread = threading.Thread(target=guest, daemon=True)
    thread.start()

    sb = sandbox_at("127.0.0.1:1", keep_alive=0)
    monkeypatch.setattr(sb, "_dial", lambda deadline=None: Conn(client_sock, client_sock.makefile("rb")))
    return sb, sent, thread
