"""Sandbox: the data-plane handle. Every RPC opens one relayed silkd
connection via the owner node; sessions and processes are server-side state,
so nothing is lost between calls — including across a transparent hibernate
wake."""

from __future__ import annotations

import contextlib

from .checkpoint import Checkpoint
from .conn import Conn, dial_agent
from .errors import ExitError, ProtocolError, SandboxError
from .frames import FS_CHUNK
from .template import Template


class Sandbox:
    """One claimed microVM."""

    def __init__(self, client, id: str, token: str, owner: str,
                 deadline: str = "", from_checkpoint: str = ""):
        self._client = client
        self.id = id
        self.token = token
        self.owner = owner
        self.deadline = deadline
        self.from_checkpoint = from_checkpoint

    # -- commands ----------------------------------------------------------

    def exec(self, *argv: str, cwd: str = "", env: dict | None = None,
             user: str = "", session: str = "", stdin: bytes = b"") -> str:
        """Runs argv to completion and returns stdout; a non-zero exit raises
        ExitError carrying stderr."""
        out, err = bytearray(), bytearray()
        code = self.run(list(argv), cwd=cwd, env=env, user=user, session=session,
                        stdin=stdin, on_stdout=out.extend, on_stderr=err.extend)
        if code != 0:
            raise ExitError(code, err.decode(errors="replace"))
        return out.decode(errors="replace")

    def run(self, argv: list[str], cwd: str = "", env: dict | None = None,
            user: str = "", session: str = "", stdin: bytes = b"",
            on_stdout=None, on_stderr=None) -> int:
        """Runs argv streaming stdio through the callbacks (raw bytes — chunk
        boundaries may split multi-byte sequences); returns the exit code."""
        with self._dial() as conn:
            conn.send("exec", argv=argv, cwd=cwd or None, env=env,
                      user=user or None, session=session or None)
            if stdin:
                for off in range(0, len(stdin), FS_CHUNK):
                    conn.send("stdin", data=stdin[off:off + FS_CHUNK])
            conn.send("stdin_close")
            for frame in conn.recv_until("exit"):
                if frame["type"] == "stdout" and on_stdout:
                    on_stdout(frame["data"])
                elif frame["type"] == "stderr" and on_stderr:
                    on_stderr(frame["data"])
                elif frame["type"] == "exit":
                    return frame["code"]
        raise ProtocolError("exec stream ended without an exit frame")

    # -- files --------------------------------------------------------------

    def write_file(self, path: str, data: bytes, mode: int | None = None) -> None:
        """Writes data to path atomically (temp + rename on the guest)."""
        with self._dial() as conn:
            conn.send("fs_write", path=path, mode=mode)
            view = memoryview(data)
            for off in range(0, len(view), FS_CHUNK):
                conn.send("data", data=view[off:off + FS_CHUNK])
            conn.send("data_end")
            _expect(conn, "done")

    def read_file(self, path: str) -> bytes:
        with self._dial() as conn:
            conn.send("fs_read", path=path)
            return _drain_data(conn)

    def list_dir(self, path: str) -> list[dict]:
        with self._dial() as conn:
            conn.send("fs_list", path=path)
            entries: list[dict] = []
            for frame in conn.recv_until("done"):
                entries.extend(frame.get("entries") or [])
            return entries

    def stat(self, path: str) -> dict:
        with self._dial() as conn:
            conn.send("fs_stat", path=path)
            return _expect(conn, "stat")

    def mkdir(self, path: str, parents: bool = False) -> None:
        self._done_rpc("fs_mkdir", path=path, parents=parents or None)

    def remove(self, path: str, recursive: bool = False) -> None:
        self._done_rpc("fs_rm", path=path, recursive=recursive or None)

    def rename(self, src: str, dst: str) -> None:
        self._done_rpc("fs_rename", **{"from": src, "to": dst})

    # -- whole trees ---------------------------------------------------------

    def push(self, dest: str, tar_stream: bytes) -> None:
        """Extracts a tar archive into dest — atomic against a truncated
        stream; the only project-ingestion path on the no-network lane."""
        with self._dial() as conn:
            conn.send("fs_push", dest=dest)
            view = memoryview(tar_stream)
            for off in range(0, len(view), FS_CHUNK):
                conn.send("data", data=view[off:off + FS_CHUNK])
            conn.send("data_end")
            _expect(conn, "done")

    def pull(self, path: str) -> bytes:
        """Returns path (file or tree) as a tar archive."""
        with self._dial() as conn:
            conn.send("fs_pull", path=path)
            return _drain_data(conn)

    # -- search --------------------------------------------------------------

    def find(self, path: str, pattern: str, glob: str = "") -> list[dict]:
        with self._dial() as conn:
            conn.send("fs_find", path=path, pattern=pattern, glob=glob or None)
            return [f for f in conn.recv_until("done") if f["type"] == "match"]

    def replace(self, files: list[str], pattern: str, replacement: str) -> list[dict]:
        with self._dial() as conn:
            conn.send("fs_replace", files=files, pattern=pattern, replacement=replacement)
            return [f for f in conn.recv_until("done") if f["type"] == "replaced"]

    # -- git -------------------------------------------------------------

    def git_clone(self, url: str, path: str, branch: str = "", depth: int = 0, auth: str = "") -> None:
        """Clones into path (egress lane only; the none lane answers a typed
        unimplemented error pointing at push)."""
        self._done_rpc("git_clone", url=url, path=path, branch=branch or None,
                       depth=depth or None, auth=auth or None)

    def git_status(self, path: str) -> dict:
        with self._dial() as conn:
            conn.send("git_status", path=path)
            return _expect(conn, "git_status_result")

    def git_add(self, path: str, files: list[str]) -> None:
        self._done_rpc("git_add", path=path, files=files)

    def git_commit(self, path: str, message: str, author: str) -> str:
        """Commits staged changes; returns the commit hash."""
        with self._dial() as conn:
            conn.send("git_commit", path=path, message=message, author=author)
            return _expect(conn, "git_commit_result").get("hash", "")

    def git_push(self, path: str, auth: str = "") -> None:
        self._done_rpc("git_push", path=path, auth=auth or None)

    def git_pull(self, path: str, auth: str = "") -> None:
        self._done_rpc("git_pull", path=path, auth=auth or None)

    def git_branches(self, path: str) -> dict:
        with self._dial() as conn:
            conn.send("git_branch", path=path, action="list")
            return _expect(conn, "git_branches")

    def git_checkout(self, path: str, name: str) -> None:
        self._done_rpc("git_branch", path=path, action="checkout", name=name)

    # -- watch -----------------------------------------------------------

    def watch(self, path: str, recursive: bool = False) -> Watcher:
        """Streams filesystem events under path; events after the returned
        Watcher exists are guaranteed captured. Close it to stop."""
        conn = self._dial()
        try:
            conn.send("fs_watch", path=path, recursive=recursive or None)
            _expect(conn, "ready")
        except Exception:
            conn.close()
            raise
        return Watcher(conn)

    # -- sessions ------------------------------------------------------------

    def session(self, cwd: str = "", env: dict | None = None) -> Session:
        """Creates a persistent shell: cd/export/aliases survive across exec
        calls routed into it."""
        with self._dial() as conn:
            conn.send("session_create", cwd=cwd or None, env=env)
            created = _expect(conn, "session_created")
        return Session(self, created["id"])

    def sessions(self) -> list[str]:
        with self._dial() as conn:
            conn.send("session_list")
            return _expect(conn, "sessions").get("ids") or []

    # -- lifecycle -----------------------------------------------------------

    def fork(self, count: int, ttl_seconds: int = 0) -> list[Sandbox]:
        """Clones this sandbox into count independent children carrying its
        exact memory and disk state; all-or-nothing."""
        body = {"token": self.token, "count": count}
        if ttl_seconds:
            body["ttl_seconds"] = ttl_seconds
        reply = self._client._post_json(self.owner, f"/v1/sandboxes/{self.id}/fork", body, "fork")
        return [self._client._handle_from(self.owner, child) for child in reply.get("children") or []]

    def hibernate(self) -> None:
        """Snapshots and stops the VM, freeing its memory; the next call that
        reaches the guest wakes it transparently, state intact."""
        self._client._post_json(self.owner, f"/v1/sandboxes/{self.id}/hibernate",
                                {"token": self.token}, "hibernate")

    def checkpoint(self, name: str = "") -> Checkpoint:
        """Captures full state without stopping the sandbox; the returned
        Checkpoint branches fresh sandboxes from that exact moment."""
        body = {"token": self.token}
        if name:
            body["name"] = name
        reply = self._client._post_json(self.owner, f"/v1/sandboxes/{self.id}/checkpoint", body, "checkpoint")
        return Checkpoint(self._client, self.owner, reply["checkpoint"])

    def promote(self, template: str) -> Template:
        """Publishes this sandbox's state as a claimable template on its
        node; the returned handle is bound to that node."""
        reply = self._client._post_json(self.owner, f"/v1/sandboxes/{self.id}/promote",
                                        {"token": self.token, "template": template}, "promote")
        key = reply["key"]
        return Template(self._client, self.owner, key["template"], key.get("net", ""), key.get("size", ""))

    def dial_port(self, port: int) -> PortConn:
        """Opens a byte stream to 127.0.0.1:port inside the guest."""
        conn = self._dial()
        try:
            conn.send("port_forward", port=port)
            _expect(conn, "ready")
        except Exception:
            conn.close()
            raise
        return PortConn(conn)

    def close(self) -> None:
        """Releases the sandbox; its VM is destroyed."""
        self._client._post_json(self.owner, f"/v1/sandboxes/{self.id}/release",
                                {"token": self.token}, "release")

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        with contextlib.suppress(Exception):
            self.close()

    def _dial(self) -> Conn:
        return dial_agent(self.owner, self.id, self.token, self._client.timeout)

    def _done_rpc(self, op: str, **fields) -> None:
        with self._dial() as conn:
            conn.send(op, **fields)
            _expect(conn, "done")


class Session:
    """A persistent shell inside the sandbox, addressed by id."""

    def __init__(self, sandbox: Sandbox, id: str):
        self._sandbox = sandbox
        self.id = id

    def exec(self, *argv: str) -> str:
        """Runs argv inside the session's shell; state persists."""
        return self._sandbox.exec(*argv, session=self.id)

    def close(self) -> None:
        self._sandbox._done_rpc("session_rm", id=self.id)


class Watcher:
    """A live filesystem event stream; iterate for {kind, path} events."""

    def __init__(self, conn: Conn):
        self._conn = conn

    def __iter__(self):
        # The stream is connection-bound: closing the watcher (or the server
        # dropping the conn) ends iteration rather than raising.
        while True:
            try:
                frame = self._conn.recv()
            except (SandboxError, OSError, ValueError):
                return
            if frame["type"] == "event":
                yield frame

    def close(self) -> None:
        self._conn.close()


class PortConn:
    """A byte stream to a guest port, relayed over the silkd connection."""

    def __init__(self, conn: Conn):
        self._conn = conn
        self._eof = False

    def send(self, data: bytes) -> None:
        view = memoryview(data)
        for off in range(0, len(view), FS_CHUNK):
            self._conn.send("data", data=view[off:off + FS_CHUNK])

    def recv(self) -> bytes:
        """Returns the next chunk from the guest; b'' on stream end."""
        if self._eof:
            return b""
        frame = self._conn.recv()
        if frame["type"] in ("done", "exit"):
            self._eof = True
            return b""
        return frame.get("data") or b""

    def close(self) -> None:
        self._conn.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()


def _expect(conn: Conn, frame_type: str) -> dict:
    frame = conn.recv()
    if frame["type"] != frame_type:
        raise ProtocolError(f"expected {frame_type}, got {frame['type']}")
    return frame


def _drain_data(conn: Conn) -> bytes:
    chunks = []
    for frame in conn.recv_until("done"):
        if frame["type"] == "data":
            chunks.append(frame["data"])
    return b"".join(chunks)
