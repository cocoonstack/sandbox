"""Sandbox: the data-plane handle. Every RPC opens one relayed silkd
connection via the owner node; sessions and processes are server-side state,
so nothing is lost between calls — including across a transparent hibernate
wake."""

from __future__ import annotations

import contextlib
import socket
import threading

from .checkpoint import Checkpoint
from .conn import Conn, dial_agent
from .errors import APIError, ExitError, ProtocolError, SandboxError
from .frames import BULK_CHUNK, FS_CHUNK
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

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        with contextlib.suppress(Exception):
            self.close()

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
                _send_chunks(conn, stdin, op="stdin")
            conn.send("stdin_close")
            for frame in conn.recv_until("exit"):
                if frame["type"] == "stdout" and on_stdout:
                    on_stdout(frame["data"])
                elif frame["type"] == "stderr" and on_stderr:
                    on_stderr(frame["data"])
                elif frame["type"] == "exit":
                    return frame["code"]
        raise ProtocolError("exec stream ended without an exit frame")

    def write_file(self, path: str, data: bytes, mode: int | None = None) -> None:
        """Writes data to path atomically (temp + rename on the guest)."""
        with self._dial() as conn:
            conn.send("fs_write", path=path, mode=mode)
            _send_chunks(conn, data)
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
        """Returns {kind, size, mode, mtime_epoch_secs} for path."""
        with self._dial() as conn:
            conn.send("fs_stat", path=path)
            return _expect(conn, "stat")["info"]

    def mkdir(self, path: str, parents: bool = False) -> None:
        self._done_rpc("fs_mkdir", path=path, parents=parents or None)

    def remove(self, path: str, recursive: bool = False) -> None:
        self._done_rpc("fs_rm", path=path, recursive=recursive or None)

    def rename(self, src: str, dst: str) -> None:
        self._done_rpc("fs_rename", **{"from": src, "to": dst})

    def push(self, dest: str, tar_stream: bytes) -> None:
        """Extracts a tar archive into dest — atomic against a truncated
        stream; the only project-ingestion path on the no-network lane."""
        with self._dial() as conn:
            conn.send("fs_push", dest=dest)
            _send_chunks(conn, tar_stream, chunk=BULK_CHUNK)
            conn.send("data_end")
            _expect(conn, "done")

    def pull(self, path: str) -> bytes:
        """Returns path (file or tree) as a tar archive."""
        with self._dial() as conn:
            conn.send("fs_pull", path=path)
            return _drain_data(conn)

    def find(self, path: str, pattern: str, glob: str = "") -> list[dict]:
        with self._dial() as conn:
            conn.send("fs_find", path=path, pattern=pattern, glob=glob or None)
            return [f for f in conn.recv_until("done") if f["type"] == "match"]

    def replace(self, files: list[str], pattern: str, replacement: str) -> list[dict]:
        with self._dial() as conn:
            conn.send("fs_replace", files=files, pattern=pattern, replacement=replacement)
            return [f for f in conn.recv_until("done") if f["type"] == "replaced"]

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

    def git_create_branch(self, path: str, name: str) -> None:
        self._done_rpc("git_branch", path=path, action="create", name=name)

    def git_delete_branch(self, path: str, name: str) -> None:
        self._done_rpc("git_branch", path=path, action="delete", name=name)

    def watch(self, path: str, recursive: bool = False) -> Watcher:
        """Streams filesystem events under path; events after the returned
        Watcher exists are guaranteed captured. Close it to stop."""
        conn, _ = self._open_stream("fs_watch", path=path, recursive=recursive or None)
        return Watcher(conn)

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
            return _expect(conn, "sessions").get("sessions") or []

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
        self._client._request(self.owner, "POST", f"/v1/sandboxes/{self.id}/hibernate",
                              None, "hibernate", bearer=self.token)

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

    def start_lsp(self, language: str, root: str = "") -> Lsp:
        """Spawns the language server the flavor image provides for language;
        the base image ships none, so it raises SilkdError(kind="not_found").
        The returned handle streams JSON-RPC over the relay."""
        with self._dial() as conn:
            conn.send("lsp_start", language=language, root=root or None)
            started = _expect(conn, "lsp_started")
        return Lsp(self, started["server_id"])

    def open_pty(self, cols: int = 80, rows: int = 24, cwd: str = "",
                 env: dict | None = None, user: str = "") -> Pty:
        """Runs the guest shell under a pty; returns a byte-stream handle.
        A pty is a process guest-side: resize goes through its pid."""
        conn, started = self._open_stream("pty_open", expect="started", cols=cols,
                                          rows=rows, cwd=cwd or None, env=env, user=user or None)
        return Pty(self, conn, started["pid"])

    def proxy_port(self, local_addr: str, port: int) -> socket.socket:
        """Serves a guest port on a local listener for unmodified local
        tools; returns the listening socket (close it to stop). local_addr
        is "host:port"; port 0 picks a free one."""
        host, _, lport = local_addr.rpartition(":")
        listener = socket.create_server((host or "127.0.0.1", int(lport)))
        threading.Thread(target=self._proxy_accept_loop,
                         args=(listener, port), daemon=True).start()
        return listener

    def preview_url(self, port: int, ttl_seconds: int = 0) -> str:
        """Mints a shareable URL serving the guest HTTP port from a browser,
        valid for ttl_seconds (the node clamps it to the claim's lease).
        Requires the node to have preview configured."""
        body = {"token": self.token, "port": port}
        if ttl_seconds:
            body["ttl_seconds"] = ttl_seconds
        reply = self._client._post_json(self.owner, f"/v1/sandboxes/{self.id}/preview", body, "preview")
        return reply["url"]

    def dial_port(self, port: int) -> PortConn:
        """Opens a byte stream to 127.0.0.1:port inside the guest."""
        conn, _ = self._open_stream("port_forward", port=port)
        return PortConn(conn)

    def close(self) -> None:
        """Releases the sandbox; its VM is destroyed. Releasing one already
        gone is not an error — double-release and reap races stay silent,
        matching the Go SDK."""
        try:
            self._client._request(self.owner, "POST", f"/v1/sandboxes/{self.id}/release",
                                  None, "release", bearer=self.token)
        except APIError as exc:
            if exc.status != 404:
                raise

    def _dial(self) -> Conn:
        return dial_agent(self.owner, self.id, self.token, self._client.timeout)

    def _open_stream(self, op: str, expect: str = "ready", **fields) -> tuple[Conn, dict]:
        """Dials, sends op, and waits for the handshake frame, closing the
        conn on any failure so no socket leaks on the error path."""
        conn = self._dial()
        try:
            conn.send(op, **fields)
            frame = _expect(conn, expect)
        except Exception:
            conn.close()
            raise
        return conn, frame

    def _proxy_accept_loop(self, listener: socket.socket, port: int) -> None:
        with contextlib.suppress(OSError):
            while True:
                local, _ = listener.accept()
                threading.Thread(target=self._proxy_conn,
                                 args=(local, port), daemon=True).start()

    def _proxy_conn(self, local: socket.socket, port: int) -> None:
        try:
            guest = self.dial_port(port)
        except Exception:
            local.close()
            return

        def pump_out():
            with contextlib.suppress(Exception):
                while True:
                    chunk = guest.recv()
                    if not chunk:
                        break
                    local.sendall(chunk)
            with contextlib.suppress(OSError):
                local.close()

        threading.Thread(target=pump_out, daemon=True).start()
        try:
            while True:
                chunk = local.recv(FS_CHUNK)
                if not chunk:
                    break
                guest.send(chunk)
            guest.close_write()
        except Exception:
            guest.close()

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

class Pty:
    """An interactive shell under a guest pty; read/write are raw bytes."""

    def __init__(self, sandbox: Sandbox, conn: Conn, pid: int):
        self._sandbox = sandbox
        self._conn = conn
        self.pid = pid

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    def read(self) -> bytes:
        """The next output chunk; b'' once the shell exits."""
        frame = self._conn.recv()
        if frame["type"] == "exit":
            return b""
        return frame.get("data") or b""

    def write(self, data: bytes) -> None:
        self._conn.send("stdin", data=data)

    def resize(self, cols: int, rows: int) -> None:
        self._sandbox._done_rpc("pty_resize", pid=self.pid, cols=cols, rows=rows)

    def close(self) -> None:
        self._conn.close()


class Lsp:
    """A language server in the sandbox, spoken to over the relay. silkd is a
    broker: it pipes JSON-RPC bytes; the caller frames and correlates."""

    def __init__(self, sandbox: Sandbox, server_id: str):
        self._sandbox = sandbox
        self.server_id = server_id

    def request(self) -> PortConn:
        """Opens the JSON-RPC byte stream: writes go to the server's stdin,
        recv returns its stdout. A server serves one request for its
        lifetime; closing the stream ends the session and reaps it."""
        conn, _ = self._sandbox._open_stream("lsp_request", server_id=self.server_id)
        return PortConn(conn)

    def stop(self) -> None:
        """Kills the language server."""
        self._sandbox._done_rpc("lsp_stop", server_id=self.server_id)


class PortConn:
    """A byte stream to a guest port, relayed over the silkd connection."""

    def __init__(self, conn: Conn):
        self._conn = conn
        self._eof = False

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    def send(self, data: bytes) -> None:
        _send_chunks(self._conn, data, chunk=BULK_CHUNK)

    def recv(self) -> bytes:
        """Returns the next chunk from the guest; b'' on stream end."""
        if self._eof:
            return b""
        frame = self._conn.recv()
        if frame["type"] in ("done", "exit"):
            self._eof = True
            return b""
        return frame.get("data") or b""

    def close_write(self) -> None:
        """Half-close: signals EOF to the guest side; reads keep working."""
        self._conn.send("data_end")
        self._conn.close_write()

    def close(self) -> None:
        self._conn.close()


def _send_chunks(conn: Conn, data: bytes, op: str = "data", chunk: int = FS_CHUNK) -> None:
    view = memoryview(data)
    for off in range(0, len(view), chunk):
        conn.send(op, data=view[off:off + chunk])


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
