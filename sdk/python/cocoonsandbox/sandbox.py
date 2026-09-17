"""Sandbox: the data-plane handle."""

from __future__ import annotations

import contextlib
import socket
import threading
import time
from collections.abc import Callable, Iterator
from typing import TYPE_CHECKING, Any, cast

from .checkpoint import Checkpoint
from .conn import Conn, ConnPool, _Closeable, remaining_timeout
from .errors import APIError, ExitError, ProtocolError, SandboxError, SandboxTimeout, SilkdError, require_field
from .frames import BULK_CHUNK, FS_CHUNK, KEEP_ALIVE_PROTO
from .template import Template

if TYPE_CHECKING:
    from .client import Client


class Sandbox:
    """One claimed microVM."""

    def __init__(
        self,
        client: Client,
        id: str,
        token: str,
        owner: str,
        deadline: str = "",
        from_checkpoint: str = "",
        template_digest: str = "",
        volumes: list[dict[str, str]] | None = None,
    ) -> None:
        self._client = client
        self.id = id
        self.token = token
        self.owner = owner
        self.deadline = deadline
        self.from_checkpoint = from_checkpoint
        self.template_digest = template_digest
        self.volumes = [dict(volume) for volume in volumes or []]
        self._pool = ConnPool(client.keep_alive)
        self._proto = 0

    def __enter__(self) -> Sandbox:
        return self

    def __exit__(self, *exc: object) -> None:
        try:
            self.close()
        except APIError:
            if exc[0] is None:  # a clean block surfaces a real release failure
                raise

    def exec(
        self,
        *argv: str,
        cwd: str = "",
        env: dict[str, str] | None = None,
        user: str = "",
        session: str = "",
        stdin: bytes = b"",
        timeout: float | None = None,
    ) -> str:
        """Runs argv to completion and returns stdout; a non-zero exit raises ExitError carrying stderr."""
        out, err = bytearray(), bytearray()
        code = self.run(
            list(argv),
            cwd=cwd,
            env=env,
            user=user,
            session=session,
            stdin=stdin,
            on_stdout=out.extend,
            on_stderr=err.extend,
            timeout=timeout,
        )
        if code != 0:
            raise ExitError(code, err.decode(errors="replace"), out.decode(errors="replace"))
        return out.decode(errors="replace")

    def run(
        self,
        argv: list[str],
        cwd: str = "",
        env: dict[str, str] | None = None,
        user: str = "",
        session: str = "",
        stdin: bytes = b"",
        on_stdout: Callable[[bytes], object] | None = None,
        on_stderr: Callable[[bytes], object] | None = None,
        timeout: float | None = None,
    ) -> int:
        """Streams raw output bytes and returns the exit code; timeout bounds the entire call."""
        if timeout is not None and timeout <= 0:
            raise ValueError("timeout must be positive")
        deadline = None if timeout is None else time.monotonic() + timeout
        expired = threading.Event()
        try:
            conn = self._connect(deadline)
        except (ProtocolError, TimeoutError):
            if deadline is not None and time.monotonic() >= deadline:
                raise SandboxTimeout(f"command did not finish within {timeout}s") from None
            raise
        with self._lease(conn):
            watchdog = _arm_watchdog(conn, deadline, expired)
            pump = threading.Thread(target=_feed_stdin, args=(conn, stdin), daemon=True) if stdin else None
            try:
                conn.send(
                    "exec",
                    argv=argv,
                    cwd=cwd or None,
                    env=env,
                    user=user or None,
                    detach=False,
                    session=session or None,
                )
                if pump is not None:
                    pump.start()
                else:
                    with contextlib.suppress(OSError):
                        conn.send("stdin_close")
                code = _pump_stdio(conn, on_stdout, on_stderr)
            except (ProtocolError, OSError):
                if expired.is_set():
                    raise SandboxTimeout(f"command did not finish within {timeout}s") from None
                raise
            except SilkdError:
                if pump is not None:
                    pump.join()
                raise
            finally:
                if watchdog is not None:
                    watchdog.cancel()
            if pump is not None:
                pump.join()  # the pump's frames must not land on the next RPC
            if code is None:
                raise ProtocolError("exec stream ended without an exit frame")
        return code

    def spawn(self, *argv: str, cwd: str = "", env: dict[str, str] | None = None, user: str = "") -> int:
        """Starts a detached process with a bounded output ring and returns its pid."""
        started = self._call(
            "exec", "started", argv=list(argv), cwd=cwd or None, env=env, user=user or None, detach=True
        )
        return cast(int, _need(started, "pid"))

    def ps(self) -> list[dict[str, Any]]:
        """Lists tracked processes: {pid, argv, detached, state, exit_code?, started_at_epoch_secs}."""
        return cast(list[dict[str, Any]], _need(self._call("ps", "procs"), "procs"))

    def kill(self, pid: int, signal: int | None = None) -> None:
        """Signals a tracked process (default SIGKILL); killing one that already exited is a no-op success."""
        self._done_rpc("kill", pid=pid, signal=signal or None)

    def logs(
        self,
        pid: int,
        on_stdout: Callable[[bytes], object] | None = None,
        on_stderr: Callable[[bytes], object] | None = None,
    ) -> int | None:
        """Replays buffered output and returns the exit code, or None if the process still runs."""
        return self._drain_proc("logs", pid, on_stdout, on_stderr, trailing_done=True)

    def attach(
        self,
        pid: int,
        on_stdout: Callable[[bytes], object] | None = None,
        on_stderr: Callable[[bytes], object] | None = None,
    ) -> int | None:
        """Replays then follows output; returns None if the process record disappears."""
        return self._drain_proc("attach", pid, on_stdout, on_stderr)

    def write_file(self, path: str, data: bytes, mode: int | None = None) -> None:
        """Writes data to path atomically (temp + rename on the guest)."""
        with self._lease() as conn:
            conn.send("fs_write", path=path, mode=mode)
            _send_chunks(conn, data)
            conn.send("data_end")
            _expect(conn, "done")

    def read_file(self, path: str) -> bytes:
        with self._lease() as conn:
            conn.send("fs_read", path=path)
            return _drain_data(conn)

    def list_dir(self, path: str) -> list[dict[str, Any]]:
        with self._lease() as conn:
            conn.send("fs_list", path=path)
            entries: list[dict[str, Any]] = []
            for frame in conn.recv_until("done"):
                entries.extend(frame.get("entries") or [])
            return entries

    def stat(self, path: str) -> dict[str, Any]:
        """Returns {kind, size, mode, mtime_epoch_secs} for path."""
        return cast(dict[str, Any], _need(self._call("fs_stat", "stat", path=path), "info"))

    def mkdir(self, path: str, parents: bool = False) -> None:
        self._done_rpc("fs_mkdir", path=path, parents=parents or None)

    def remove(self, path: str, recursive: bool = False) -> None:
        self._done_rpc("fs_rm", path=path, recursive=recursive or None)

    def rename(self, src: str, dst: str) -> None:
        self._done_rpc("fs_rename", **{"from": src, "to": dst})

    def push(self, dest: str, tar_stream: bytes) -> None:
        """Extracts a tar stream into dest; a truncated stream leaves dest untouched."""
        with self._lease() as conn:
            conn.send("fs_push", dest=dest)
            _send_chunks(conn, tar_stream, chunk=BULK_CHUNK)
            conn.send("data_end")
            _expect(conn, "done")

    def pull(self, path: str) -> bytes:
        """Returns path (file or tree) as a tar archive."""
        with self._lease() as conn:
            conn.send("fs_pull", path=path)
            return _drain_data(conn)

    def find(self, path: str, pattern: str, glob: str = "") -> list[dict[str, Any]]:
        return list(self.find_iter(path, pattern, glob))

    def find_iter(self, path: str, pattern: str, glob: str = "") -> Iterator[dict[str, Any]]:
        """Yields matches as they stream."""
        with self._lease() as conn:
            conn.send("fs_find", path=path, pattern=pattern, glob=glob or None)
            for f in conn.recv_until("done"):
                if f["type"] == "match":
                    yield f

    def replace(self, files: list[str], pattern: str, replacement: str) -> list[dict[str, Any]]:
        with self._lease() as conn:
            conn.send("fs_replace", files=files, pattern=pattern, replacement=replacement)
            return [f for f in conn.recv_until("done") if f["type"] == "replaced"]

    def git_clone(self, url: str, path: str, branch: str = "", depth: int = 0, auth: str = "") -> None:
        """Clones into path (egress lane only; the none lane answers a typed unimplemented error pointing at push)."""
        self._done_rpc("git_clone", url=url, path=path, branch=branch or None, depth=depth or None, auth=auth or None)

    def git_status(self, path: str) -> dict[str, Any]:
        return self._call("git_status", "git_status_result", path=path)

    def git_add(self, path: str, files: list[str]) -> None:
        self._done_rpc("git_add", path=path, files=files)

    def git_commit(self, path: str, message: str, author: str) -> str:
        """Commits staged changes; returns the commit hash."""
        return cast(
            str,
            self._call("git_commit", "git_commit_result", path=path, message=message, author=author).get("hash", ""),
        )

    def git_push(self, path: str, auth: str = "") -> None:
        self._done_rpc("git_push", path=path, auth=auth or None)

    def git_pull(self, path: str, auth: str = "") -> None:
        self._done_rpc("git_pull", path=path, auth=auth or None)

    def git_branches(self, path: str) -> dict[str, Any]:
        return self._call("git_branch", "git_branches", path=path, action="list")

    def git_checkout(self, path: str, name: str) -> None:
        self._done_rpc("git_branch", path=path, action="checkout", name=name)

    def git_create_branch(self, path: str, name: str) -> None:
        self._done_rpc("git_branch", path=path, action="create", name=name)

    def git_delete_branch(self, path: str, name: str) -> None:
        self._done_rpc("git_branch", path=path, action="delete", name=name)

    def watch(self, path: str, recursive: bool = False) -> Watcher:
        """Streams filesystem events under path; events after the returned Watcher exists are guaranteed captured."""
        conn, _ = self._open_stream("fs_watch", path=path, recursive=recursive or None)
        return Watcher(conn)

    def session(self, cwd: str = "", env: dict[str, str] | None = None) -> Session:
        """Creates a persistent shell: cd/export/aliases survive across exec calls routed into it."""
        created = self._call("session_create", "session_created", cwd=cwd or None, env=env)
        return Session(self, _need(created, "id"))

    def sessions(self) -> list[str]:
        return self._call("session_list", "sessions").get("sessions") or []

    def fork(self, count: int, ttl_seconds: int = 0) -> list[Sandbox]:
        """Clones memory and disk into independent children, all-or-nothing."""
        body = {"token": self.token, "count": count}
        if ttl_seconds:
            body["ttl_seconds"] = ttl_seconds
        reply = self._client._post_json(self.owner, f"/v1/sandboxes/{self.id}/fork", body, "fork")
        return [self._client._handle_from(self.owner, child, "fork") for child in reply.get("children") or []]

    def hibernate(self) -> None:
        """Snapshots and stops the VM; the next guest call restores its state."""
        self._pool.drain()
        self._client._request(
            self.owner, "POST", f"/v1/sandboxes/{self.id}/hibernate", None, "hibernate", bearer=self.token
        )

    def checkpoint(self, name: str = "") -> Checkpoint:
        """Captures a branchable snapshot without stopping the sandbox."""
        body = {"token": self.token}
        if name:
            body["name"] = name
        reply = self._client._post_json(self.owner, f"/v1/sandboxes/{self.id}/checkpoint", body, "checkpoint")
        return Checkpoint(self._client, self.owner, require_field(reply, "checkpoint", "checkpoint"))

    def promote(self, template: str) -> Template:
        """Publishes a claimable template and returns an owner-bound handle."""
        reply = self._client._post_json(
            self.owner, f"/v1/sandboxes/{self.id}/promote", {"token": self.token, "template": template}, "promote"
        )
        key = require_field(reply, "key", "promote")
        return Template(
            self._client,
            self.owner,
            key["template"],
            key.get("net", ""),
            key.get("size", ""),
            reply.get("content_digest", ""),
        )

    def start_lsp(self, language: str, root: str = "") -> Lsp:
        """Starts the registered language server; missing servers raise not_found."""
        started = self._call("lsp_start", "lsp_started", language=language, root=root or None)
        return Lsp(self, _need(started, "server_id"))

    def open_pty(
        self, cols: int = 80, rows: int = 24, cwd: str = "", env: dict[str, str] | None = None, user: str = ""
    ) -> Pty:
        """Runs the guest shell under a pty; returns a byte-stream handle."""
        conn, started = self._open_stream(
            "pty_open", expect="started", cols=cols, rows=rows, cwd=cwd or None, env=env, user=user or None
        )
        return Pty(self, conn, _need(started, "pid"))

    def proxy_port(self, local_addr: str, port: int) -> socket.socket:
        """Serves a guest port on a local socket; closing the listener stops new connections."""
        host, _, lport = local_addr.rpartition(":")
        listener = socket.create_server((host or "127.0.0.1", int(lport)))
        threading.Thread(target=self._proxy_accept_loop, args=(listener, port), daemon=True).start()
        return listener

    def preview_url(self, port: int, ttl_seconds: int = 0) -> str:
        """Mints a guest HTTP URL whose lifetime is clamped to the claim's lease."""
        body = {"token": self.token, "port": port}
        if ttl_seconds:
            body["ttl_seconds"] = ttl_seconds
        reply = self._client._post_json(self.owner, f"/v1/sandboxes/{self.id}/preview", body, "preview")
        return cast(str, require_field(reply, "url", "preview"))

    def dial_port(self, port: int) -> PortConn:
        """Opens a byte stream to 127.0.0.1:port inside the guest."""
        conn, _ = self._open_stream("port_forward", port=port)
        return PortConn(conn)

    def close(self) -> None:
        """Releases the sandbox; its VM is destroyed."""
        self._pool.drain()
        try:
            self._client._request(
                self.owner, "POST", f"/v1/sandboxes/{self.id}/release", None, "release", bearer=self.token
            )
        except APIError as exc:
            if exc.status != 404:
                raise

    def _dial(self, deadline: float | None = None) -> Conn:
        return self._client._dial(self.owner, self.id, self.token, deadline)

    def _connect(self, deadline: float | None = None) -> Conn:
        """Takes a parked connection or dials one, asking the daemon's proto on a handle's first kept dial."""
        conn = self._pool.take()
        if conn is not None:
            return conn
        conn = self._dial(deadline)
        if self._proto or self._client.keep_alive <= 0:
            return conn
        try:
            conn.settimeout(remaining_timeout(self._client.timeout, deadline, "agent probe"))
            conn.send("info")
            proto = int(_expect(conn, "info").get("proto") or 1)
            conn.settimeout(None)
        except BaseException:
            conn.close()
            raise
        self._proto = proto
        if proto >= KEEP_ALIVE_PROTO:
            return conn
        conn.close()
        return self._dial(deadline)

    @contextlib.contextmanager
    def _lease(self, conn: Conn | None = None) -> Iterator[Conn]:
        """Runs one RPC on conn, dialed when absent; a terminal frame parks it and anything else drops it."""
        if conn is None:
            conn = self._connect()
        try:
            yield conn
        except SilkdError:
            self._park(conn)
            raise
        except BaseException:
            conn.close()
            raise
        self._park(conn)

    def _park(self, conn: Conn) -> None:
        if self._proto >= KEEP_ALIVE_PROTO:
            self._pool.park(conn)
        else:
            conn.close()

    def _open_stream(self, op: str, expect: str = "ready", **fields: object) -> tuple[Conn, dict[str, Any]]:
        conn = self._connect()
        try:
            conn.send(op, **fields)
            frame = _expect(conn, expect)
        except BaseException:
            conn.close()
            raise
        return conn, frame

    def _proxy_accept_loop(self, listener: socket.socket, port: int) -> None:
        with contextlib.suppress(OSError):
            while True:
                local, _ = listener.accept()
                threading.Thread(target=self._proxy_conn, args=(local, port), daemon=True).start()

    def _proxy_conn(self, local: socket.socket, port: int) -> None:
        try:
            guest = self.dial_port(port)
        except (SandboxError, OSError):
            local.close()
            return

        def pump_out() -> None:
            with contextlib.suppress(SandboxError, OSError):
                while True:
                    chunk = guest.recv()
                    if not chunk:
                        break
                    local.sendall(chunk)
            with contextlib.suppress(OSError):
                local.shutdown(socket.SHUT_WR)

        pump = threading.Thread(target=pump_out, daemon=True)
        pump.start()
        try:
            with contextlib.suppress(OSError):
                while True:
                    chunk = local.recv(FS_CHUNK)
                    if not chunk:
                        break
                    guest.send(chunk)
            with contextlib.suppress(SandboxError, OSError):
                guest.close_write()
            pump.join()
        finally:
            guest.close()
            with contextlib.suppress(OSError):
                local.close()

    def _call(self, op: str, expect: str, **fields: object) -> dict[str, Any]:
        with self._lease() as conn:
            conn.send(op, **fields)
            return _expect(conn, expect)

    def _done_rpc(self, op: str, **fields: object) -> None:
        self._call(op, "done", **fields)

    def _drain_proc(
        self,
        op: str,
        pid: int,
        on_stdout: Callable[[bytes], object] | None,
        on_stderr: Callable[[bytes], object] | None,
        trailing_done: bool = False,
    ) -> int | None:
        with self._lease() as conn:
            conn.send(op, pid=pid)
            code = _pump_stdio(conn, on_stdout, on_stderr)
            if trailing_done and code is not None:
                _expect(conn, "done")
            return code


class Session(_Closeable):
    """A persistent shell inside the sandbox, addressed by id."""

    def __init__(self, sandbox: Sandbox, id: str) -> None:
        self._sandbox = sandbox
        self.id = id

    def exec(self, *argv: str) -> str:
        """Runs argv inside the session's shell; state persists."""
        return self._sandbox.exec(*argv, session=self.id)

    def close(self) -> None:
        """Ends the shell; a session already gone is not an error."""
        _rpc_ignoring_not_found(self._sandbox, "session_rm", id=self.id)


class Watcher(_Closeable):
    """A live filesystem event stream; iterate for {kind, path} events."""

    def __init__(self, conn: Conn) -> None:
        self._conn = conn
        self._closed = False
        self.error: Exception | None = None

    def __iter__(self) -> Iterator[dict[str, Any]]:
        while True:
            try:
                frame = self._conn.recv()
            except ProtocolError as e:
                if not self._closed:
                    self.error = e
                return
            if frame["type"] == "event":
                yield frame

    def close(self) -> None:
        self._closed = True
        self._conn.close()


class Pty(_Closeable):
    """An interactive shell under a guest pty; read/write are raw bytes."""

    def __init__(self, sandbox: Sandbox, conn: Conn, pid: int) -> None:
        self._sandbox = sandbox
        self._conn = conn
        self.pid = pid
        self.exit_code: int | None = None
        self._eof = False

    def read(self) -> bytes:
        """The next output chunk; b'' once the shell exits, after which exit_code holds the shell's status."""
        if self._eof:
            return b""
        frame = self._conn.recv()
        if frame["type"] == "exit":
            self.exit_code = frame.get("code")
            self._eof = True
            return b""
        return frame.get("data") or b""

    def write(self, data: bytes) -> None:
        _send_chunks(self._conn, data, op="stdin")

    def resize(self, cols: int, rows: int) -> None:
        self._sandbox._done_rpc("pty_resize", pid=self.pid, cols=cols, rows=rows)

    def close(self) -> None:
        self._conn.close()


class Lsp(_Closeable):
    """A language server in the sandbox, spoken to over the relay."""

    def __init__(self, sandbox: Sandbox, server_id: str) -> None:
        self._sandbox = sandbox
        self.server_id = server_id

    def request(self) -> PortConn:
        """Opens the one JSON-RPC stream a server serves; its end reaps the server, so start a new one to work again."""
        conn, _ = self._sandbox._open_stream("lsp_request", server_id=self.server_id)
        return PortConn(conn)

    def stop(self) -> None:
        """Kills the language server early; one already reaped is not an error."""
        _rpc_ignoring_not_found(self._sandbox, "lsp_stop", server_id=self.server_id)

    close = stop


class PortConn(_Closeable):
    """A byte stream to a guest port, relayed over the silkd connection."""

    def __init__(self, conn: Conn) -> None:
        self._conn = conn
        self._eof = False

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

    def close(self) -> None:
        self._conn.close()


def _send_chunks(conn: Conn, data: bytes, op: str = "data", chunk: int = FS_CHUNK) -> None:
    view = memoryview(data)
    for off in range(0, len(view), chunk):
        conn.send(op, data=view[off : off + chunk])


def _arm_watchdog(conn: Conn, deadline: float | None, expired: threading.Event) -> threading.Timer | None:
    if deadline is None:
        return None

    def cut() -> None:
        expired.set()
        conn.abort()

    watchdog = threading.Timer(max(deadline - time.monotonic(), 0), cut)
    watchdog.daemon = True
    watchdog.start()
    return watchdog


def _feed_stdin(conn: Conn, stdin: bytes) -> None:
    with contextlib.suppress(SandboxError, OSError):  # the reader reports the real failure
        if stdin:
            _send_chunks(conn, stdin, op="stdin")
        conn.send("stdin_close")


def _pump_stdio(
    conn: Conn, on_stdout: Callable[[bytes], object] | None, on_stderr: Callable[[bytes], object] | None
) -> int | None:
    for frame in conn.recv_until("exit", "done"):
        t = frame["type"]
        if t == "stdout" and on_stdout:
            on_stdout(frame["data"])
        elif t == "stderr" and on_stderr:
            on_stderr(frame["data"])
        elif t == "exit":
            return cast(int, frame["code"])
    return None


def _expect(conn: Conn, frame_type: str) -> dict[str, Any]:
    frame = conn.recv()
    if frame["type"] != frame_type:
        raise ProtocolError(f"expected {frame_type}, got {frame['type']}")
    return frame


def _need(frame: dict[str, Any], key: str) -> Any:
    if key not in frame:
        raise ProtocolError(f"{frame['type']} frame without {key}")
    return frame[key]


def _rpc_ignoring_not_found(sandbox: Sandbox, op: str, **fields: object) -> None:
    try:
        sandbox._done_rpc(op, **fields)
    except SilkdError as exc:
        if exc.kind != "not_found":
            raise


def _drain_data(conn: Conn) -> bytes:
    chunks = []
    for frame in conn.recv_until("done"):
        if frame["type"] == "data":
            chunks.append(frame["data"])
    return b"".join(chunks)
