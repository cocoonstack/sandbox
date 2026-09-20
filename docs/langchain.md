# LangChain toolkit

`cocoonstack-sandbox-langchain` turns one sandbox into a LangChain tool
set: `pip install cocoonstack-sandbox-langchain` (the example below also
needs `langgraph`, which the package does not pull in).

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
| `sandbox_exec` | run a shell command, cut off and killed after 5 minutes with the reply saying so; the budget is one wall clock over the claim, the dial and the command, so a first call that waits on a slow or redirecting cluster still answers inside it; stdout/stderr/exit code; disk state persists across calls |
| `sandbox_write_file` | write a text file (atomic on the guest; the parent directory must exist) |
| `sandbox_read_file` | read a text file |
| `sandbox_list_dir` | list a directory as JSON |

The claim's lease is `ttl_seconds`, one hour by default: nothing renews a
lease, and an agent run outlives the node's 5-minute default. Once the lease
ends the sandbox is gone and every later tool call reports it.

A failed call — a missing path, a guest error, an expired or unreachable
sandbox — comes back to the model as the tool's error text, not as an
exception out of the agent run. Every tool's first call claims the sandbox
inside the same 5-minute budget.

**Branching**: `CocoonToolkit(..., from_checkpoint="ck_...")` — the checkpoint
pins the template, lane and size, so `template` and `net` are ignored — claims the
sandbox from a [checkpoint](sdk-python.md#checkpoints--branching-and-time-travel)'s
captured moment instead of a clean template — agents start from prepared
state (dependencies installed, repo cloned) in milliseconds, and every
agent run branches from the same known-good base.
