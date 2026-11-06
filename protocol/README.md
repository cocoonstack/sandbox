# silkd wire protocol fixtures

Golden JSON frames under `wire/fixtures/v1`, shared by all three
implementations of the silkd protocol:
- silkd (Rust, guest) parses/emits these in `silkd/src/proto.rs` tests.
- `protocol/wire` (Go, host) round-trips the corpus in its tests; it is the
  one Go implementation, consumed by both the Go SDK and sandboxd.
- the Python SDK round-trips the request frames and checks only that each
  response decodes to the fixture's `type` tag (and `data`), in
  `sdk/python/tests/test_fixtures.py`.

`req_*.json` are client→server frames, `resp_*.json` server→client. A frame
that the Rust or Go side cannot round-trip is a protocol drift bug. The Rust side also
pins the corpus size, so adding a verb without its fixture fails CI.

`enums.json` pins each wire enum's full value set (error/event/file kinds,
git branch actions). Frame fixtures carry only one representative value per
enum, so every implementation also asserts its whole vocabulary against this
file — renaming or adding a variant in one implementation alone fails its test.
