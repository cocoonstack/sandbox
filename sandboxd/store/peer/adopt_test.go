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
		room     bool
		want     bool
	}{
		{"a peer adopts", []int{http.StatusOK}, false, true},
		{"a draining peer refuses and another adopts", []int{http.StatusNotFound, http.StatusOK}, false, true},
		{"every peer refuses", []int{http.StatusNotFound, http.StatusServiceUnavailable}, false, false},
		{"no peer", nil, false, false},
		{"a full peer refuses a wake and one with room adopts", []int{http.StatusNotFound, http.StatusOK}, true, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			addrs := make([]string, 0, len(tt.statuses))
			for _, status := range tt.statuses {
				ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.Method != http.MethodGet || r.URL.Path != "/v1/sandboxes/sb_1/owner" || r.Header.Get("Authorization") != "Bearer tok" {
						t.Errorf("request %s %s auth %q", r.Method, r.URL.Path, r.Header.Get("Authorization"))
					}
					if got := r.URL.Query().Get("room") == "1"; got != tt.room {
						t.Errorf("room query %v, want %v", got, tt.room)
					}
					w.WriteHeader(status)
				}))
				t.Cleanup(ts.Close)
				addrs = append(addrs, strings.TrimPrefix(ts.URL, "http://"))
			}
			a := &Adopter{Peers: func() []string { return addrs }}
			if got := a.Adopt(t.Context(), "sb_1", "tok", tt.room); got != tt.want {
				t.Errorf("Adopt = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestAdoptAsksWithTheRootOnlyForAClaimWithoutAToken(t *testing.T) {
	tests := []struct {
		name, token, root, wantAuth string
	}{
		{"a claim token goes as is", "tok", "root", "Bearer tok"},
		{"no claim token asks as the root", "", "root", "Bearer root"},
		{"no claim token and no root sends nothing", "", "", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var gotAuth string
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				gotAuth = r.Header.Get("Authorization")
				w.WriteHeader(http.StatusOK)
			}))
			t.Cleanup(ts.Close)
			a := &Adopter{Peers: func() []string { return []string{strings.TrimPrefix(ts.URL, "http://")} }, Token: tt.root}
			if !a.Adopt(t.Context(), "sb_1", tt.token, false) || gotAuth != tt.wantAuth {
				t.Errorf("auth %q, want %q", gotAuth, tt.wantAuth)
			}
		})
	}
}

func TestAdoptSkipsAnUnreachablePeer(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	addr := strings.TrimPrefix(ts.URL, "http://")
	ts.Close()
	a := &Adopter{Peers: func() []string { return []string{addr} }}
	if a.Adopt(t.Context(), "sb_1", "tok", false) {
		t.Error("Adopt = true with the only peer down")
	}
}
