# LangChain toolkit

`cocoonstack-sandbox-langchain` turns one sandbox into a LangChain tool
set: `pip install cocoonstack-sandbox-langchain`.

```python
from cocoonsandbox_langchain import CocoonToolkit
from langgraph.prebuilt import create_react_agent

with CocoonToolkit("10.0.0.5:7777", api_token="...",
                   template="ghcr.io/cocoonstack/sandbox/rt:24.04") as kit:
    agent = create_react_agent(model, kit.get_tools())
    agent.invoke({"messages": [("user", "clone the repo and run the tests")]})
```

The toolkit claims its sandbox lazily on the first tool call and releases
it when the context manager exits. Tools (all `StructuredTool`s with typed
schemas, sync-native with `asyncio.to_thread` async bridges):

| tool | what it does |
|---|---|
| `sandbox_exec` | run a shell command; stdout/stderr/exit code; disk state persists across calls |
| `sandbox_write_file` | write a text file (atomic on the guest) |
| `sandbox_read_file` | read a text file |
| `sandbox_list_dir` | list a directory as JSON |

**Branching**: `CocoonToolkit(..., from_checkpoint="ck_...")` claims the
sandbox from a [checkpoint](sdk-python.md#checkpoints--branching-and-time-travel)'s
captured moment instead of a clean template — agents start from prepared
state (dependencies installed, repo cloned) in milliseconds, and every
agent run branches from the same known-good base.
