"""LangChain toolkit over cocoon microVM sandboxes: one claimed sandbox
behind a set of StructuredTools (exec, file read/write, list). The claim is
lazy — building an agent costs nothing until a tool actually runs — and the
toolkit is a context manager whose exit releases the sandbox."""

from __future__ import annotations

import asyncio
import json
import threading

from cocoonsandbox import Client, ExitError, Sandbox
from langchain_core.tools import StructuredTool
from pydantic import BaseModel, Field


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
        self._client = Client(addr, api_token=api_token)
        self._template = template
        self._net = net
        self._ttl = ttl_seconds
        self._from_checkpoint = from_checkpoint
        self._lock = threading.Lock()
        self._sb: Sandbox | None = None

    def __enter__(self) -> CocoonToolkit:
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    def get_tools(self) -> list[StructuredTool]:
        """The sandbox tool set; sync-native (_run), async via to_thread."""
        return [
            self._tool("sandbox_exec",
                       "Run a shell command in the sandbox; returns stdout, stderr, "
                       "and the exit code. State on disk persists across calls.",
                       ExecInput, self._exec),
            self._tool("sandbox_write_file",
                       "Write a text file in the sandbox (atomic).",
                       WriteFileInput, self._write_file),
            self._tool("sandbox_read_file",
                       "Read a text file from the sandbox.",
                       PathInput, self._read_file),
            self._tool("sandbox_list_dir",
                       "List a sandbox directory as JSON entries.",
                       PathInput, self._list_dir),
        ]

    def close(self) -> None:
        """Releases the sandbox; safe to call twice or before any claim."""
        with self._lock:
            sb, self._sb = self._sb, None
        if sb is not None:
            sb.close()

    def sandbox(self) -> Sandbox:
        """The claimed sandbox, claiming (or branching) on first use."""
        with self._lock:
            if self._sb is None:
                self._sb = self._claim()
            return self._sb

    def _claim(self) -> Sandbox:
        if self._from_checkpoint:
            for ck in self._client.checkpoints():
                if ck.id == self._from_checkpoint:
                    return ck.new(ttl_seconds=self._ttl)
            raise ValueError(f"unknown checkpoint {self._from_checkpoint}")
        return self._client.new(self._template, net=self._net, ttl_seconds=self._ttl)

    def _tool(self, name: str, description: str, schema: type[BaseModel], func) -> StructuredTool:
        async def arun(**kwargs):
            return await asyncio.to_thread(func, **kwargs)

        return StructuredTool.from_function(func=func, coroutine=arun, name=name,
                                            description=description, args_schema=schema)

    def _exec(self, command: str, cwd: str = "") -> str:
        out: list[bytes] = []
        errs: list[bytes] = []
        try:
            code = self.sandbox().run(["sh", "-c", command], cwd=cwd,
                                      on_stdout=out.append, on_stderr=errs.append)
        except ExitError as exc:  # run() streams; ExitError only from exec paths
            return f"exit {exc.code}\n{exc.stderr}"
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
