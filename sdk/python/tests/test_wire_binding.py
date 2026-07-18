"""Binds the SDK's real call sites to the golden corpus: each case drives an
actual Sandbox method with the fixture's own field values through a real
Conn over a socketpair, then asserts every field the SDK emitted exists in
the fixture with the same value. A misspelled field name (living only in
sandbox.py) fails here; the generic encode round-trip alone cannot see it."""

import contextlib
import json
import pathlib
import socket
import threading

import pytest

from cocoonsandbox import Client
from cocoonsandbox.conn import Conn
from cocoonsandbox.frames import PROTO_VERSION

FIXTURES = pathlib.Path(__file__).parent / ".." / ".." / ".." / "protocol" / "wire" / "fixtures" / "v1"

# op → (fixture stem, replies the fake guest scripts, invoke(sb, fixture)).
# Invocations pass the fixture's own values so the subset assertion binds
# every emitted field name and value to the corpus.
CASES = [
    ("req_exec", [{"type": "started", "pid": 7}, {"type": "exit", "code": 0}],
     lambda sb, f: sb.run(f["argv"], cwd=f["cwd"], env=f["env"], user=f["user"], session=f["session"])),
    ("req_fs_write", [{"type": "done"}],
     lambda sb, f: sb.write_file(f["path"], b"x", mode=f.get("mode"))),
    ("req_fs_read", [{"type": "done"}],
     lambda sb, f: sb.read_file(f["path"])),
    ("req_fs_list", [{"type": "done"}],
     lambda sb, f: sb.list_dir(f["path"])),
    ("req_fs_stat", [{"type": "stat", "info": {"kind": "file", "size": 1}}],
     lambda sb, f: sb.stat(f["path"])),
    ("req_fs_mkdir", [{"type": "done"}],
     lambda sb, f: sb.mkdir(f["path"], parents=f.get("parents", False))),
    ("req_fs_rm", [{"type": "done"}],
     lambda sb, f: sb.remove(f["path"], recursive=f.get("recursive", False))),
    ("req_fs_rename", [{"type": "done"}],
     lambda sb, f: sb.rename(f["from"], f["to"])),
    ("req_fs_push", [{"type": "done"}],
     lambda sb, f: sb.push(f["dest"], b"tar")),
    ("req_fs_pull", [{"type": "done"}],
     lambda sb, f: sb.pull(f["path"])),
    ("req_fs_find", [{"type": "done"}],
     lambda sb, f: sb.find(f["path"], f["pattern"], glob=f.get("glob", ""))),
    ("req_fs_replace", [{"type": "done"}],
     lambda sb, f: sb.replace(f["files"], f["pattern"], f["replacement"])),
    ("req_session_create", [{"type": "session_created", "id": "sess-1"}],
     lambda sb, f: sb.session(cwd=f["cwd"], env=f["env"])),
    ("req_session_list", [{"type": "sessions", "sessions": []}],
     lambda sb, f: sb.sessions()),
    ("req_git_clone", [{"type": "done"}],
     lambda sb, f: sb.git_clone(f["url"], f["path"], branch=f["branch"], depth=f["depth"], auth=f["auth"])),
    ("req_git_status", [{"type": "git_status_result", "branch": "main"}],
     lambda sb, f: sb.git_status(f["path"])),
    ("req_git_add", [{"type": "done"}],
     lambda sb, f: sb.git_add(f["path"], f["files"])),
    ("req_git_commit", [{"type": "git_commit_result", "hash": "h"}],
     lambda sb, f: sb.git_commit(f["path"], f["message"], f["author"])),
    ("req_git_push", [{"type": "done"}],
     lambda sb, f: sb.git_push(f["path"], auth=f["auth"])),
    ("req_git_pull", [{"type": "done"}],
     lambda sb, f: sb.git_pull(f["path"], auth=f["auth"])),
    ("req_pty_resize", [{"type": "done"}],
     lambda sb, f: _pty_stub(sb, f["pid"]).resize(f["cols"], f["rows"])),
    ("req_exec_detach", [{"type": "started", "pid": 7}],
     lambda sb, f: sb.spawn(*f["argv"])),
    ("req_ps", [{"type": "procs", "procs": []}],
     lambda sb, f: sb.ps()),
    ("req_kill", [{"type": "done"}],
     lambda sb, f: sb.kill(f["pid"], signal=f["signal"])),
    ("req_logs", [{"type": "exit", "code": 0}],
     lambda sb, f: sb.logs(f["pid"])),
    ("req_attach", [{"type": "exit", "code": 0}],
     lambda sb, f: sb.attach(f["pid"])),
    ("req_fs_watch", [{"type": "ready"}],
     lambda sb, f: sb.watch(f["path"], recursive=f["recursive"]).close()),
    ("req_git_branch", [{"type": "done"}],
     lambda sb, f: sb.git_create_branch(f["path"], f["name"])),
    ("req_session_rm", [{"type": "done"}],
     lambda sb, f: _session_stub(sb, f["id"]).close()),
    ("req_lsp_start", [{"type": "lsp_started", "server_id": "lsp-1"}],
     lambda sb, f: sb.start_lsp(f["language"], root=f["root"])),
    ("req_lsp_request", [{"type": "ready"}],
     lambda sb, f: _lsp_stub(sb, f["server_id"]).request().close()),
    ("req_lsp_stop", [{"type": "done"}],
     lambda sb, f: _lsp_stub(sb, f["server_id"]).stop()),
    ("req_port_forward", [{"type": "ready"}],
     lambda sb, f: sb.dial_port(f["port"]).close()),
    ("req_pty_open", [{"type": "started", "pid": 7}],
     lambda sb, f: sb.open_pty(cols=f["cols"], rows=f["rows"], cwd=f["cwd"], env=f["env"], user=f["user"]).close()),
]


def _pty_stub(sb, pid):
    from cocoonsandbox.sandbox import Pty

    return Pty(sb, None, pid)


def _session_stub(sb, id):
    from cocoonsandbox.sandbox import Session

    return Session(sb, id)


def _lsp_stub(sb, server_id):
    from cocoonsandbox.sandbox import Lsp

    return Lsp(sb, server_id)


def _fake_sandbox(monkeypatch, replies):
    """A Sandbox whose _dial yields a real Conn over a socketpair; a guest
    thread records inbound frames and answers the scripted replies."""
    from cocoonsandbox.sandbox import Sandbox

    sent = []
    client_sock, guest_sock = socket.socketpair()

    def guest():
        reader = guest_sock.makefile("rb")
        with contextlib.suppress(Exception):
            line = reader.readline()
            if line:
                sent.append(json.loads(line))
            for reply in replies:
                guest_sock.sendall(json.dumps(reply).encode() + b"\n")
            # Drain follow-up frames (data/stdin streams) until EOF.
            while True:
                line = reader.readline()
                if not line:
                    break
                sent.append(json.loads(line))
        with contextlib.suppress(OSError):
            guest_sock.close()

    thread = threading.Thread(target=guest, daemon=True)
    thread.start()

    sb = Sandbox(client=Client("127.0.0.1:1"), id="sb_1", token="tok", owner="127.0.0.1:1")
    monkeypatch.setattr(sb, "_dial", lambda: Conn(client_sock, client_sock.makefile("rb")))
    return sb, sent, thread


@pytest.mark.parametrize("stem,replies,invoke", CASES, ids=lambda c: c if isinstance(c, str) else "")
def test_call_site_matches_fixture(monkeypatch, stem, replies, invoke):
    fixture = json.loads((FIXTURES / f"{stem}.json").read_text())
    sb, sent, thread = _fake_sandbox(monkeypatch, replies)
    invoke(sb, fixture)
    thread.join(timeout=5)

    assert sent, "no frame reached the guest"
    frame = sent[0]
    assert frame["op"] == fixture["op"]
    assert frame["v"] == PROTO_VERSION
    for key, value in frame.items():
        assert key in fixture, f"field {key!r} not in the golden corpus (typo?)"
        if key != "data":
            assert value == fixture[key], f"field {key!r}: {value!r} != {fixture[key]!r}"


def test_enum_error_kinds_match_corpus():
    enums = json.loads((FIXTURES / "enums.json").read_text())
    assert set(enums["error_kind"]) == {"bad_request", "not_found", "unimplemented", "internal"}
    assert set(enums["event_kind"]) == {"created", "modified", "deleted", "renamed"}
