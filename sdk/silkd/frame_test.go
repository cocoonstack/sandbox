package silkd

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

const fixtureDir = "../../protocol/fixtures/v1"

// TestFixtureCorpusRoundTrips is the protocol/README contract: every golden
// frame must decode and re-encode to canonical-JSON equality, both request
// and response sides — a frame only one implementation round-trips is drift.
func TestFixtureCorpusRoundTrips(t *testing.T) {
	entries, err := os.ReadDir(fixtureDir)
	if err != nil {
		t.Fatalf("fixtures dir: %v", err)
	}
	seen := 0
	for _, entry := range entries {
		name := entry.Name()
		if !strings.HasSuffix(name, ".json") || strings.HasPrefix(name, ".") {
			continue
		}
		raw, err := os.ReadFile(filepath.Join(fixtureDir, name))
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		var encoded []byte
		switch {
		case strings.HasPrefix(name, "req_"):
			req, err := DecodeRequest(raw)
			if err != nil {
				t.Errorf("%s: decode: %v", name, err)
				continue
			}
			if encoded, err = EncodeRequest(req); err != nil {
				t.Errorf("%s: encode: %v", name, err)
				continue
			}
		case strings.HasPrefix(name, "resp_"):
			resp, err := DecodeResponse(raw)
			if err != nil {
				t.Errorf("%s: decode: %v", name, err)
				continue
			}
			if encoded, err = EncodeResponse(resp); err != nil {
				t.Errorf("%s: encode: %v", name, err)
				continue
			}
		default:
			continue
		}
		if !jsonEqual(t, raw, encoded) {
			t.Errorf("%s: round-trip drift:\n fixture: %s\n encoded: %s", name, bytes.TrimSpace(raw), encoded)
		}
		seen++
	}
	if seen < 22 {
		t.Fatalf("fixture corpus went missing: %d files", seen)
	}
}

// TestEnumValueSetsMatchFixture pins the full enum value lists
// (order-sensitive): frame fixtures carry one representative value per enum,
// so a variant renamed or added on only one side would otherwise drift
// silently. Twin of silkd's enum_value_sets_match_fixture.
func TestEnumValueSetsMatchFixture(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join(fixtureDir, "enums.json"))
	if err != nil {
		t.Fatalf("enums fixture: %v", err)
	}
	var fixture map[string][]string
	if err := json.Unmarshal(raw, &fixture); err != nil {
		t.Fatalf("parse enums fixture: %v", err)
	}
	want := map[string][]string{
		"error_kind":        {KindBadRequest, KindNotFound, KindUnimplemented, KindInternal},
		"event_kind":        {EventCreated, EventModified, EventDeleted, EventRenamed},
		"file_kind":         {FileKindFile, FileKindDir, FileKindSymlink, FileKindOther},
		"git_branch_action": {BranchList, BranchCreate, BranchDelete, BranchCheckout},
	}
	if !reflect.DeepEqual(fixture, want) {
		t.Errorf("enum value sets drifted:\n fixture: %v\n go:      %v", fixture, want)
	}
}

func TestDataFieldsAreBase64(t *testing.T) {
	frame, err := EncodeResponse(&Stdout{Data: []byte("hi")})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if want := `{"type":"stdout","data":"aGk="}`; string(frame) != want {
		t.Errorf("got %s, want %s", frame, want)
	}

	req, err := DecodeRequest([]byte(`{"v":1,"op":"stdin","data":"aGVsbG8K"}`))
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got := req.(*Stdin).Data; string(got) != "hello\n" {
		t.Errorf("got %q, want hello\\n", got)
	}
}

func TestRequestDataNeverNull(t *testing.T) {
	// silkd's deserializer wants a base64 string; a nil payload must encode
	// as "" — encoding/json's null would abort the RPC as a protocol error.
	for _, req := range []Request{&Stdin{}, &Data{}} {
		frame, err := EncodeRequest(req)
		if err != nil {
			t.Fatalf("%s: %v", req.Op(), err)
		}
		if !strings.Contains(string(frame), `"data":""`) {
			t.Errorf("%s: nil payload encoded as %s", req.Op(), frame)
		}
	}
}

func TestUnknownFieldsIgnored(t *testing.T) {
	req, err := DecodeRequest([]byte(`{"v":1,"op":"exec","argv":["echo"],"detach":false,"future_field":true}`))
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got := req.(*Exec).Argv; len(got) != 1 || got[0] != "echo" {
		t.Errorf("got argv %v", got)
	}
}

func TestUnknownTagsRejected(t *testing.T) {
	if _, err := DecodeRequest([]byte(`{"v":1,"op":"teleport"}`)); err == nil {
		t.Error("unknown op accepted")
	}
	if _, err := DecodeResponse([]byte(`{"type":"warp"}`)); err == nil {
		t.Error("unknown response type accepted")
	}
}

func TestProcInfoExitCodeAbsentWhileRunning(t *testing.T) {
	resp, err := DecodeResponse([]byte(`{"type":"procs","procs":[{"pid":1,"argv":["sleep"],"detached":true,"state":"running","started_at_epoch_secs":1}]}`))
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	procs := resp.(*Procs).Procs
	if procs[0].ExitCode != nil {
		t.Errorf("running proc has exit_code %d", *procs[0].ExitCode)
	}
	encoded, err := EncodeResponse(resp.(*Procs))
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if strings.Contains(string(encoded), "exit_code") {
		t.Errorf("exit_code serialized for a running proc: %s", encoded)
	}
}

func jsonEqual(t *testing.T, a, b []byte) bool {
	t.Helper()
	var av, bv any
	if err := json.Unmarshal(a, &av); err != nil {
		t.Fatalf("unmarshal fixture: %v", err)
	}
	if err := json.Unmarshal(b, &bv); err != nil {
		t.Fatalf("unmarshal encoded: %v", err)
	}
	return reflect.DeepEqual(av, bv)
}
