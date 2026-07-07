# cocoonstack-sandbox-openai

OpenAI Agents SDK sandbox provider backed by cocoon microVMs: implements
the SDK's `BaseSandboxClient`/`BaseSandboxSession` pair over the sync
`cocoonstack-sandbox` SDK, bridged with `asyncio.to_thread`.

```python
from cocoonsandbox_openai import CocoonSandboxClient, CocoonSandboxClientOptions

options = CocoonSandboxClientOptions(addr="10.0.0.5:7777", api_token="...")
session = await CocoonSandboxClient().create(options=options)
```

One session is one claimed sandbox; `delete` releases it, `resume`
reattaches from serialized state (id + token). Exec maps to streaming
`run`, workspace persist/hydrate to tar pull/push, exposed ports to local
port proxies. Requires Python >= 3.10 (the `openai-agents` floor).

Full reference: https://cocoonstack.github.io/sandbox/openai-adapter
