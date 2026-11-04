package peer

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestAdoptAsksPeersUntilOneHoldsTheClaim(t *testing.T) {
	tests := []struct {
		name     string
		statuses []int
		want     bool
	}{
		{"a peer adopts", []int{http.StatusOK}, true},
		{"a draining peer refuses and another adopts", []int{http.StatusNotFound, http.StatusOK}, true},
		{"every peer refuses", []int{http.StatusNotFound, http.StatusServiceUnavailable}, false},
		{"no peer", nil, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			addrs := make([]string, 0, len(tt.statuses))
			for _, status := range tt.statuses {
				ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.Method != http.MethodGet || r.URL.Path != "/v1/sandboxes/sb_1/owner" || r.Header.Get("Authorization") != "Bearer tok" {
						t.Errorf("request %s %s auth %q", r.Method, r.URL.Path, r.Header.Get("Authorization"))
					}
					w.WriteHeader(status)
				}))
				t.Cleanup(ts.Close)
				addrs = append(addrs, strings.TrimPrefix(ts.URL, "http://"))
			}
			a := &Adopter{Peers: func() []string { return addrs }}
			if got := a.Adopt(t.Context(), "sb_1", "tok"); got != tt.want {
				t.Errorf("Adopt = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestAdoptSkipsAnUnreachablePeer(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	addr := strings.TrimPrefix(ts.URL, "http://")
	ts.Close()
	a := &Adopter{Peers: func() []string { return []string{addr} }}
	if a.Adopt(t.Context(), "sb_1", "tok") {
		t.Error("Adopt = true with the only peer down")
	}
}
