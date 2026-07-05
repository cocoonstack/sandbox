package sandbox

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestConnectAddress(t *testing.T) {
	tests := []struct {
		name string
		addr string
		want string
		ok   bool
	}{
		{"single", "n1:7777", "n1:7777", true},
		{"seed list uses first", "n1:7777, n2:7777", "n1:7777", true},
		{"empty", "", "", false},
		{"only commas", " , ", "", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c, err := Connect(tt.addr)
			if tt.ok != (err == nil) {
				t.Fatalf("err=%v, want ok=%v", err, tt.ok)
			}
			if tt.ok && c.addr != tt.want {
				t.Errorf("addr %q, want %q", c.addr, tt.want)
			}
		})
	}
}

func TestNewSendsClaim(t *testing.T) {
	var gotBody claimRequest
	var gotAuth string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/v1/claim" {
			t.Errorf("got %s %s", r.Method, r.URL.Path)
		}
		gotAuth = r.Header.Get("Authorization")
		if err := json.NewDecoder(r.Body).Decode(&gotBody); err != nil {
			t.Errorf("decode body: %v", err)
		}
		_ = json.NewEncoder(w).Encode(claimResponse{ID: "sb_1", Token: "tok", Deadline: time.Unix(42, 0)})
	}))
	t.Cleanup(ts.Close)

	c := testClient(t, ts, WithAPIToken("sekret"))
	sb, err := c.New(t.Context(), "python:3.12",
		WithNetwork(NetEgress), WithSize(Medium), WithTimeout(90*time.Second))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	want := claimRequest{Template: "python:3.12", Net: "egress", Size: "medium", TTLSeconds: 90}
	if gotBody != want {
		t.Errorf("body %+v, want %+v", gotBody, want)
	}
	if gotAuth != "Bearer sekret" {
		t.Errorf("auth %q", gotAuth)
	}
	if sb.ID != "sb_1" || sb.token != "tok" {
		t.Errorf("handle %+v", sb)
	}
}

func TestNewSurfacesServerError(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusConflict)
		_ = json.NewEncoder(w).Encode(errorResponse{Error: "node has no egress attachment"})
	}))
	t.Cleanup(ts.Close)

	_, err := testClient(t, ts).New(t.Context(), "rt:24.04")
	if err == nil || !strings.Contains(err.Error(), "no egress attachment") ||
		!strings.Contains(err.Error(), "409") {
		t.Errorf("got %v, want surfaced 409 message", err)
	}
}

func TestCloseReleases(t *testing.T) {
	tests := []struct {
		name   string
		status int
		ok     bool
	}{
		{"released", http.StatusNoContent, true},
		{"already gone", http.StatusNotFound, true},
		{"server failure", http.StatusInternalServerError, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var gotPath, gotAuth string
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				gotPath, gotAuth = r.URL.Path, r.Header.Get("Authorization")
				w.WriteHeader(tt.status)
			}))
			t.Cleanup(ts.Close)

			c := testClient(t, ts)
			sb := &Sandbox{ID: "sb_9", c: c, token: "tok9", owner: c.addr}
			err := sb.Close()
			if tt.ok != (err == nil) {
				t.Fatalf("err=%v, want ok=%v", err, tt.ok)
			}
			if gotPath != "/v1/sandboxes/sb_9/release" || gotAuth != "Bearer tok9" {
				t.Errorf("got %s auth %q", gotPath, gotAuth)
			}
		})
	}
}

func testClient(t *testing.T, ts *httptest.Server, opts ...ClientOption) *Client {
	t.Helper()
	c, err := Connect(strings.TrimPrefix(ts.URL, "http://"), opts...)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	return c
}
