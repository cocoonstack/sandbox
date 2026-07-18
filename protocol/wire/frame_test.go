package wire

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

const fixtureDir = "fixtures/v1"

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
	// Exact, deliberately: adding a verb means adding its fixture and
	// bumping this — a new frame type outside the corpus is drift waiting
	// to happen.
	if seen != 60 {
		t.Fatalf("fixture corpus: %d frames, want exactly 60", seen)
	}
}

// TestEveryVerbHasAFixture pins corpus completeness: every request and
// response type the SDK knows must appear in at least one golden frame, so
// no verb can exist outside the three-language drift guard.
func TestEveryVerbHasAFixture(t *testing.T) {
	allRequests := []Request{
		Exec{},
		Info{},
		Ps{},
		Kill{},
		Attach{},
		Logs{},
		SessionCreate{},
		SessionList{},
		SessionRm{},
		Stdin{},
		StdinClose{},
		FsWrite{},
		FsRead{},
		FsList{},
		FsStat{},
		FsMkdir{},
		FsRm{},
		FsRename{},
		FsPush{},
		FsPull{},
		FsFind{},
		FsReplace{},
		FsWatch{},
		PtyOpen{},
		PtyResize{},
		PortForward{},
		GitClone{},
		GitStatus{},
		GitAdd{},
		GitCommit{},
		GitPush{},
		GitPull{},
		GitBranch{},
		Data{},
		DataEnd{},
	}
	allResponses := []Response{
		Started{},
		Stdout{},
		Stderr{},
		Exit{},
		Done{},
		Ready{},
		ErrorResp{},
		InfoResp{},
		Procs{},
		DataResp{},
		Entries{},
		Stat{},
		SessionCreated{},
		Sessions{},
		Match{},
		Replaced{},
		Event{},
		GitStatusResult{},
		GitCommitResult{},
		GitBranches{},
	}

	entries, err := os.ReadDir(fixtureDir)
	if err != nil {
		t.Fatalf("fixtures dir: %v", err)
	}
	haveReq, haveResp := map[string]bool{}, map[string]bool{}
	for _, entry := range entries {
		name := entry.Name()
		raw, err := os.ReadFile(filepath.Join(fixtureDir, name))
		if err != nil {
			continue
		}
		switch {
		case strings.HasPrefix(name, "req_"):
			if req, err := DecodeRequest(raw); err == nil {
				haveReq[req.Op()] = true
			}
		case strings.HasPrefix(name, "resp_"):
			if resp, err := DecodeResponse(raw); err == nil {
				haveResp[resp.RespType()] = true
			}
		}
	}
	for _, r := range allRequests {
		if !haveReq[r.Op()] {
			t.Errorf("request %q has no fixture", r.Op())
		}
	}
	for _, r := range allResponses {
		if !haveResp[r.RespType()] {
			t.Errorf("response %q has no fixture", r.RespType())
		}
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

// TestBulkDecodeStrictShape guards the fast bulk slicer: only the exact
// canonical frame takes the fast path, so a "data" match inside another
// field or any malformed JSON must never yield a silently wrong payload.
func TestBulkDecodeStrictShape(t *testing.T) {
	resp, err := DecodeResponse([]byte(`{"type":"stdout","meta":{"data":"WFhY"},"data":"aGk="}`))
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got := string(resp.(*Stdout).Data); got != "hi" {
		t.Errorf("nested field shadowed the data payload: %q", got)
	}
	for _, frame := range []string{
		`{"type":"stdout","data":"aGk="}garbage`,
		`{"type":"stdout","data":"aGk="`,
		`{"type":"stdout"}garbage"data":"QQ=="}`,
		`{"type":"stdout"garbage,"data":"QQ=="}`,
		`{"type":"stdout":,"data":"QQ=="}`,
		`{"type":"stdout",garbage"data":"QQ=="}`,
		`{"type":"stdout","data":"QQ==" }x`,
		"{\"type\":\"stdout\",\"data\":\"Q\nQ==\"}",
		"{\"type\":\"stdout\",\"data\":\"Q\r\nQ==\"}",
	} {
		if _, err := DecodeResponse([]byte(frame)); err == nil {
			t.Errorf("malformed frame accepted: %s", frame)
		}
	}
}

// TestTagAfterOtherKeys pins the tokenizer fallback: producers emit the tag
// first, but the protocol never promised order.
func TestTagAfterOtherKeys(t *testing.T) {
	resp, err := DecodeResponse([]byte(`{"data":"aGk=","type":"stdout"}`))
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got := string(resp.(*Stdout).Data); got != "hi" {
		t.Errorf("got %q, want %q", got, "hi")
	}
	req, err := DecodeRequest([]byte(`{"path":"/","v":1,"op":"fs_stat"}`))
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got := req.(*FsStat).Path; got != "/" {
		t.Errorf("got path %q, want %q", got, "/")
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

func TestAppendBulkRequestMatchesEncodeRequest(t *testing.T) {
	for _, payload := range [][]byte{nil, {}, []byte("hello\x00\xff"), bytes.Repeat([]byte{0xAB}, 300*1024)} {
		want, err := EncodeRequest(Data{Data: payload})
		if err != nil {
			t.Fatalf("EncodeRequest: %v", err)
		}
		got := AppendBulkRequest(nil, "data", payload)
		if !bytes.Equal(got, append(want, '\n')) {
			t.Fatalf("payload len %d: bulk %q, want %q", len(payload), got, want)
		}
	}
}
