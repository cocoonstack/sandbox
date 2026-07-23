# silkd wire protocol fixtures

Golden JSON frames shared by all three implementations of the silkd
protocol:
- silkd (Rust, guest) parses/emits these in `silkd/src/proto.rs` tests.
- `protocol/wire` (Go, host) round-trips the corpus in its tests; it is the
  one Go implementation, consumed by both the Go SDK and sandboxd.
- the Python SDK round-trips it in `sdk/python/tests/test_fixtures.py`.

`req_*.json` are client→server frames, `resp_*.json` server→client. A frame
that only one side can round-trip is a protocol drift bug.

`enums.json` pins each wire enum's full value set (error/event/file kinds,
git branch actions). Frame fixtures carry only one representative value per
enum, so both sides also assert their whole vocabulary against this file —
renaming or adding a variant on one side alone fails that side's test.
