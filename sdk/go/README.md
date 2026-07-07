# cocoon sandbox Go SDK

Stdlib-only Go client for the sandbox control plane and the in-guest silkd
protocol. Full reference: [docs/sdk.md](../../docs/sdk.md).

```go
client, _ := sandbox.Connect("10.0.0.5:7777", sandbox.WithAPIToken("..."))
sb, _ := client.New(ctx, "ghcr.io/cocoonstack/sandbox/rt:24.04")
defer sb.Close()
out, _ := sb.Exec(ctx, "echo", "hello")
```

- `client.go` — claim/redirect follow, `Lookup` scatter, template delete
- `sandbox.go` — exec/run, sessions, fork, hibernate (transparent wake)
- `files.go` / `tree.go` — streaming file verbs, tar push/pull
- `port.go` — `DialPort`/`ProxyPort` (net.Conn over the relay), `PreviewURL`
- `lsp.go` — `StartLsp` + the JSON-RPC byte stream to a flavor's server
- `proc.go` — background process management (Spawn/Ps/Kill/Logs/Attach)
- `checkpoint.go` / `template.go` — branch/rewind and promote handles
- `silkd/` — the wire binding: frame types round-tripping
  `protocol/fixtures/` (drift against the Rust guest fails CI); `silkdtest/`
  is an in-process fake guest for consumers' tests

Module path `github.com/cocoonstack/sandbox/sdk/go`; no dependencies.
