"""LangChain toolkit over cocoon microVM sandboxes: one claimed sandbox
behind a set of StructuredTools (exec, file read/write, list). The claim is
lazy — building an agent costs nothing until a tool actually runs — and the
toolkit is a context manager whose exit releases the sandbox."""

from __future__ import annotations

import asyncio
import json
import threading

from cocoonsandbox import Client, Sandbox
from langchain_core.tools import StructuredTool
from pydantic import BaseModel, Field

# Mirrors the MCP exec contract the tool description states.
CALL_TIMEOUT = 300.0


class ExecInput(BaseModel):
    command: str = Field(description="shell command, run via sh -c inside the sandbox")
    cwd: str = Field(default="", description="working directory (optional)")


class WriteFileInput(BaseModel):
    path: str = Field(description="absolute path in the sandbox")
    content: str = Field(description="file content to write")


class PathInput(BaseModel):
    path: str = Field(description="absolute path in the sandbox")


class CocoonToolkit:
    """LangChain tools backed by one sandbox, claimed on first tool use.

    from_checkpoint branches a fresh sandbox from that checkpoint's captured
    moment instead of claiming a clean template — agents resume from
    prepared state.
    """

    def __init__(self, addr: str, api_token: str = "", template: str = "rt:24.04",
                 net: str = "", ttl_seconds: int = 0, from_checkpoint: str = ""):
        self._client = Client(addr, api_token=api_token, timeout=CALL_TIMEOUT)
        self._template = template
        self._net = net
        self._ttl = ttl_seconds
        self._from_checkpoint = from_checkpoint
        self._lock = threading.Lock()
        self._sb: Sandbox | None = None
        self._closed = False

    def __enter__(self) -> CocoonToolkit:
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    def get_tools(self) -> list[StructuredTool]:
        """The sandbox tool set; sync-native (_run), async via to_thread."""
        return [
            self._tool("sandbox_exec",
                       "Run a shell command in the sandbox and wait for it to exit. "
                       "Returns stdout; a non-empty stderr is appended as a 'stderr:' "
                       "line and a non-zero status as an 'exit code: N' line; a "
                       "command that prints nothing and exits 0 returns '(no output)'. "
                       "The call is cut off after 5 minutes. "
                       "Files and installed packages persist across calls; environment "
                       "variables and the working directory do not.",
                       ExecInput, self._exec),
            self._tool("sandbox_write_file",
                       "Write text to a file in the sandbox, replacing any existing "
                       "file atomically. The parent directory must already exist.",
                       WriteFileInput, self._write_file),
            self._tool("sandbox_read_file",
                       "Return the whole content of a file in the sandbox as text "
                       "(undecodable bytes are replaced); a missing path is a tool "
                       "error. Prefer sandbox_exec with head or tail for large files.",
                       PathInput, self._read_file),
            self._tool("sandbox_list_dir",
                       "List one directory (not recursive) as a JSON array of "
                       "{name, kind, size}; kind is file, dir, symlink, or other.",
                       PathInput, self._list_dir),
        ]

    def close(self) -> None:
        """Releases the sandbox; safe to call twice or before any claim.
        Tools invoked after close raise instead of silently claiming a
        sandbox nothing would ever release."""
        with self._lock:
            sb, self._sb = self._sb, None
            self._closed = True
        if sb is not None:
            sb.close()

    def sandbox(self) -> Sandbox:
        """The claimed sandbox, claiming (or branching) on first use."""
        with self._lock:
            if self._closed:
                raise RuntimeError("toolkit is closed")
            if self._sb is None:
                self._sb = self._claim()
            return self._sb

    def _claim(self) -> Sandbox:
        if self._from_checkpoint:
            return self._client.checkpoint(self._from_checkpoint).new(ttl_seconds=self._ttl)
        return self._client.new(self._template, net=self._net, ttl_seconds=self._ttl)

    def _tool(self, name: str, description: str, schema: type[BaseModel], func) -> StructuredTool:
        async def arun(**kwargs):
            return await asyncio.to_thread(func, **kwargs)

        return StructuredTool.from_function(func=func, coroutine=arun, name=name,
                                            description=description, args_schema=schema)

    def _exec(self, command: str, cwd: str = "") -> str:
        out: list[bytes] = []
        errs: list[bytes] = []
        code = self.sandbox().run(["sh", "-c", command], cwd=cwd,
                                  on_stdout=out.append, on_stderr=errs.append)
        stdout = b"".join(out).decode(errors="replace")
        stderr = b"".join(errs).decode(errors="replace")
        result = stdout
        if stderr:
            result += ("\n" if result else "") + "stderr: " + stderr
        if code != 0:
            result += ("\n" if result else "") + f"exit code: {code}"
        return result or "(no output)"

    def _write_file(self, path: str, content: str) -> str:
        self.sandbox().write_file(path, content.encode())
        return f"wrote {path}"

    def _read_file(self, path: str) -> str:
        return self.sandbox().read_file(path).decode(errors="replace")

    def _list_dir(self, path: str) -> str:
        return json.dumps(self.sandbox().list_dir(path))
