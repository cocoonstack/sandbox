# OpenAI Agents SDK adapter

`cocoonstack-sandbox-openai` plugs cocoon microVMs into the
[OpenAI Agents SDK](https://openai.github.io/openai-agents-python/) as a
custom sandbox provider, so an Agents run executes its shell/file tools
inside a real hardened VM instead of a local process or container.

```python
from cocoonsandbox_openai import CocoonSandboxClient, CocoonSandboxClientOptions

client = CocoonSandboxClient()
session = await client.create(options=CocoonSandboxClientOptions(
    addr="10.0.0.5:7777",
    api_token="...",
    template="ghcr.io/cocoonstack/sandbox/rt:24.04",
))
async with session:
    result = await session.exec("python3", "script.py", shell=False)
await client.delete(session)
```

Wire it into a run via the SDK's `SandboxRunConfig` the same way the
built-in providers (Docker, UnixLocal) are; `CocoonSandboxClient` is a
drop-in `BaseSandboxClient`.

## How it maps

The adapter implements the SDK's `BaseSandboxClient` / `BaseSandboxSession`
pair over the sync [Python SDK](sdk-python.md), bridged with
`asyncio.to_thread`:

| SDK surface | cocoon |
|---|---|
| `create` | `Client.new` — one claimed sandbox per session |
| session `exec` | `Sandbox.run` (stdout/stderr/exit) |
| `read` / `write` | `Sandbox.read_file` / `write_file`; missing → `FileNotFoundError` |
| `persist_workspace` / `hydrate_workspace` | `Sandbox.pull` / `push` (tar) |
| exposed port | `Sandbox.proxy_port` |
| `delete` | `Sandbox.close` (release) |
| `resume` | reattach by id + token from the serialized session state |

`CocoonSandboxClientOptions` carries the node address, api token, template
ref, network lane (`none`/`egress`), and TTL. The session state is
JSON-serializable, so a run can be resumed against the same sandbox after a
process restart. Requires Python 3.10+ (the Agents SDK floor); the
underlying `cocoonsandbox` stays 3.9+.
