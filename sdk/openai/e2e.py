"""Hardware e2e for the OpenAI Agents SDK adapter: drives the SDK's own
SandboxSession surface (start/exec/write/read/persist/resume/delete) against
a real sandboxd node — the exact path the Agents runner takes. Prints
OPENAI-ADAPTER-E2E PASS.

    python3 e2e.py --addr 127.0.0.1:7777 --token e2e --template rt2:24.04
"""

import argparse
import asyncio
import io
import sys

from cocoonsandbox_openai import CocoonSandboxClient, CocoonSandboxClientOptions


async def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--addr", default="127.0.0.1:7777")
    parser.add_argument("--token", default="")
    parser.add_argument("--template", default="rt:24.04")
    args = parser.parse_args()

    client = CocoonSandboxClient()
    opts = CocoonSandboxClientOptions(
        addr=args.addr, api_token=args.token, template=args.template, net="none", ttl_seconds=600
    )

    session = await client.create(options=opts)
    async with session:
        result = await session.exec("echo", "adapter-hello", shell=False)
        assert result.exit_code == 0 and result.stdout == b"adapter-hello\n", result.stdout
        print("  exec: stdout + exit through the SDK session")

        await session.write("/workspace/note.txt", io.BytesIO(b"from-adapter"))
        readback = (await session.read("/workspace/note.txt")).read()
        assert readback == b"from-adapter", readback
        print("  write + read: workspace file round-trips")

        result = await session.exec("cat", "/workspace/note.txt", shell=False)
        assert result.stdout == b"from-adapter", result.stdout
        print("  exec sees the written file (one live sandbox, not per-call)")

        tar = (await session.persist_workspace()).read()
        assert tar and len(tar) > 0
        print(f"  persist_workspace: {len(tar)} tar bytes captured")

    # Resume from the serialized state must reattach to the same sandbox.
    payload = client.serialize_session_state(session._inner.state)
    resumed = await client.resume(client.deserialize_session_state(payload))
    async with resumed:
        result = await resumed.exec("cat", "/workspace/note.txt", shell=False)
        assert result.stdout == b"from-adapter", result.stdout
        print("  resume: reattached to the same sandbox, file still there")

    await client.delete(resumed)
    print("  delete: sandbox released")
    print("OPENAI-ADAPTER-E2E PASS")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
