# cocoonstack-sandbox-langchain

`pip install cocoonstack-sandbox-langchain`

LangChain tools backed by cocoon microVM sandboxes: a `CocoonToolkit`
claims one sandbox lazily on first tool use and exposes it as
`StructuredTool`s an agent can call — shell exec, file read/write, and
directory listing, with disk state persisting across calls.

```python
from cocoonsandbox_langchain import CocoonToolkit

with CocoonToolkit("10.0.0.5:7777", api_token="...") as kit:
    agent = create_react_agent(model, kit.get_tools())
    agent.invoke({"messages": [("user", "run the tests in /work")]})
```

`from_checkpoint="ck_..."` branches the sandbox from a checkpoint's
captured moment instead of a clean template — agents resume from prepared
state (dependencies installed, repo cloned) in milliseconds.

Sync-native (`_run` calls the stdlib-only cocoonstack-sandbox SDK
directly); async agents get `_arun` bridged via `asyncio.to_thread`.

Full reference: https://cocoonstack.github.io/sandbox/langchain
