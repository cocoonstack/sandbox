# silkd

The in-guest product daemon (Rust, tokio): newline-JSON frames over vsock
port 2048, RPCs back to back on one connection, 8 MiB frame cap. sandboxd
relays client bytes to it verbatim; it is the entire product surface inside
the guest.

Verbs: exec (streaming stdio, detach, sessions), proc table (ps/kill/logs/
attach with a bounded output ring), streaming fs + tar tree push/pull
(both atomic), find/replace, watch (ready-acked), pty, structured git,
guest port relay (`port_forward`), an LSP broker that spawns the
language server a flavor image ships under `/etc/silkd/lsp.d/<language>`,
and `tun_up`/`tun_down` for the advanced network mode's tun0 (`src/tun.rs`).
Plus three loopback relays over vsock to the host — the HTTP proxy on
`127.0.0.1:3128`, SOCKS5 on `127.0.0.1:1080`, and DNS on `127.0.0.153:53`
(`src/net_egress.rs`) — the instance-metadata responder on
`169.254.169.254:80` where an image aliases it (`src/imds.rs`), and the lane
verdict that decides whether an exec inherits the proxy variables
(`src/net.rs`).

- `src/proto.rs` — frame types + caps; the golden corpus in
  `../protocol/wire/fixtures/v1/` is round-tripped by the Rust, Go, and Python
  sides, so wire drift fails CI
- `src/server.rs` — dispatch; one module per verb family
- `tests/` — e2e suites driving the daemon in-process

Protocol reference: [docs/silkd.md](../docs/silkd.md). Built into the base
image by `make silkd-image` + `make base`.
