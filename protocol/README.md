# silkd wire protocol fixtures

Golden JSON frames shared by both implementations of the silkd protocol:
- silkd (Rust, guest) parses/emits these in `silkd/src/proto.rs` tests.
- the Go SDK (host) will parse/emit the same corpus in its tests.

`req_*.json` are client→server frames, `resp_*.json` server→client. A frame
that only one side can round-trip is a protocol drift bug. See
`design/sandbox-silkd.md` in cocoon-specs for the verb set.
