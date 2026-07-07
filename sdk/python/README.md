# cocoonstack-sandbox

Python SDK for the cocoon sandbox control plane: microVM sandboxes with
warm-pool claims, checkpoint branching, transparent hibernate/wake, and a
relay-only data plane that works on no-network guests.

```python
from cocoonsandbox import Client

client = Client("10.0.0.5:7777", api_token="...")
with client.new("ghcr.io/cocoonstack/sandbox/rt:24.04") as sb:
    print(sb.exec("echo", "hello"))
    ckpt = sb.checkpoint("after-setup")
    branch = ckpt.new()          # a fresh sandbox at the captured moment
```

stdlib-only and synchronous — `pip install cocoonstack-sandbox` brings no
dependencies. The surface mirrors the Go SDK: exec/run, streaming files,
tar push/pull, find/replace, watch, persistent sessions, git verbs, pty,
port dial/proxy/preview URLs, background processes (spawn/ps/kill/
logs/attach), fork, hibernate, promote, checkpoints, and the LSP broker. Wire fidelity is pinned by the shared protocol fixture
corpus (Rust + Go + Python all round-trip it in CI).

Full reference: https://cocoonstack.github.io/sandbox/sdk-python
