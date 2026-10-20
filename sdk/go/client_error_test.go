package sandbox

import (
	"encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"strings"
	"testing"
)

func TestAPIErrorBoundsResponseBody(t *testing.T) {
	const limit = 1 << 20
	const prefix = `{"error":"`
	const suffix = `"}`
	oversizeMessage := fmt.Sprintf("error response body exceeds %d bytes", limit)
	for _, tt := range []struct {
		name        string
		body        string
		terminalErr error
		wantMessage string
		wantBytes   int
	}{
		{name: "normal JSON", body: `{"error":"slow down"}`, wantMessage: "slow down", wantBytes: 21},
		{name: "invalid JSON", body: "not JSON", wantBytes: 8},
		{
			name: "exact limit", body: prefix + strings.Repeat("x", limit-len(prefix)-len(suffix)) + suffix,
			wantMessage: strings.Repeat("x", limit-len(prefix)-len(suffix)), wantBytes: limit,
		},
		{
			name: "oversized valid JSON", body: prefix + strings.Repeat("x", 2*limit) + suffix,
			wantMessage: oversizeMessage, wantBytes: limit + 1,
		},
		{
			name: "oversized invalid JSON", body: strings.Repeat("x", 2*limit),
			wantMessage: oversizeMessage, wantBytes: limit + 1,
		},
		{
			name: "oversized trailing bytes", body: `{"error":"held","code":"volume_busy","holders":[{"sandbox_id":"sb_1"}]}` + strings.Repeat(" ", 2*limit),
			wantMessage: oversizeMessage, wantBytes: limit + 1,
		},
		{
			name: "error after cap", body: strings.Repeat("x", limit+1), terminalErr: errors.New("reader failed after cap"),
			wantMessage: oversizeMessage, wantBytes: limit + 1,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			body := &countedErrorBody{reader: strings.NewReader(tt.body), terminalErr: tt.terminalErr}
			err := apiError("claim", &http.Response{StatusCode: http.StatusTooManyRequests, Body: io.NopCloser(body)})
			got, ok := errors.AsType[*APIError](err)
			if !ok || got.Verb != "claim" || got.Status != http.StatusTooManyRequests {
				t.Fatalf("apiError = %v, want claim 429 APIError", err)
			}
			if body.bytes != tt.wantBytes {
				t.Errorf("read %d response bytes, want %d", body.bytes, tt.wantBytes)
			}
			if body.terminalErrors != 0 {
				t.Errorf("read beyond body cap and observed %d terminal errors", body.terminalErrors)
			}
			if got.Message != tt.wantMessage {
				t.Errorf("message length %d, want %d; matched=%v", len(got.Message), len(tt.wantMessage), got.Message == tt.wantMessage)
			}
			if _, managed := errors.AsType[*ManagedVolumeError](err); managed {
				t.Errorf("returned partially decoded managed error %v", err)
			}
		})
	}
}

func TestAPIErrorReadsCompleteManagedHolders(t *testing.T) {
	holders := make([]VolumeHolder, 100)
	for i := range holders {
		holders[i] = VolumeHolder{SandboxID: fmt.Sprintf("sb_%032d", i), Mode: "ro", State: "claimed"}
	}
	raw, err := json.Marshal(errorResponse{Error: "volume held", Code: "volume_busy", Volume: "data", Holders: holders})
	if err != nil {
		t.Fatal(err)
	}
	if len(raw) <= 4096 {
		t.Fatalf("holder response has %d bytes, want more than former 4096-byte limit", len(raw))
	}
	body := &countedErrorBody{reader: strings.NewReader(string(raw))}
	err = apiError("claim", &http.Response{StatusCode: http.StatusConflict, Body: io.NopCloser(body)})
	got, ok := errors.AsType[*ManagedVolumeError](err)
	if !ok || got.Status != http.StatusConflict || got.Message != "volume held" || got.Code != "volume_busy" || got.Volume != "data" {
		t.Fatalf("apiError = %v, want intact managed volume conflict", err)
	}
	if body.bytes != len(raw) || !reflect.DeepEqual(got.Holders, holders) {
		t.Fatalf("read %d/%d bytes and received %d/%d intact holders", body.bytes, len(raw), len(got.Holders), len(holders))
	}
}

type countedErrorBody struct {
	reader         *strings.Reader
	terminalErr    error
	bytes          int
	terminalErrors int
}

func (r *countedErrorBody) Read(p []byte) (int, error) {
	n, err := r.reader.Read(p)
	r.bytes += n
	if err == io.EOF && r.terminalErr != nil {
		r.terminalErrors++
		return n, r.terminalErr
	}
	return n, err
}
