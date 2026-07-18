# silkd

The in-guest product daemon (Rust, tokio): newline-JSON frames over vsock
port 2048, one connection per RPC, 8 MiB frame cap. sandboxd relays client
bytes to it verbatim; it is the entire product surface inside the guest.

Verbs: exec (streaming stdio, detach, sessions), proc table (ps/kill/logs/
attach with a bounded output ring), streaming fs + tar tree push/pull
(both atomic), find/replace, watch (ready-acked), pty, structured git,
guest port relay (`port_forward`), and an LSP broker that spawns the
language server a flavor image ships under `/etc/silkd/lsp.d/<language>`.

- `src/proto.rs` — frame types + caps; the golden corpus in
  `../protocol/wire/fixtures/` is round-tripped by the Rust, Go, and Python
  sides, so wire drift fails CI
- `src/server.rs` — dispatch; one module per verb family
- `tests/` — e2e suites driving the daemon in-process

Protocol reference: [docs/silkd.md](../docs/silkd.md). Built into the base
image by `make silkd-image` + `make base`.
