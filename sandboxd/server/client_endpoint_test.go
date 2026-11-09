package server

import (
	"encoding/json/jsontext"
	"encoding/json/v2"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/cocoonstack/sandbox/sandboxd/types"
)

func TestResponsesNamePeersByTheirClientAddress(t *testing.T) {
	for _, tt := range []struct{ method, path, body, field string }{
		{http.MethodPost, "/v1/claim", `{"template":"rt"}`, "redirect"},
		{http.MethodPost, "/v1/checkpoints/ck_1/claim", `{}`, "redirect"},
		{http.MethodDelete, "/v1/templates?template=rt", "", "redirect"},
		{http.MethodGet, "/v1/peers", "", "peers"},
		{http.MethodGet, "/v1/info", "", "peers"},
	} {
		t.Run(tt.path, func(t *testing.T) {
			p := &clientPlacer{addrs: []string{"private:7777"}, owners: []string{"private:7777"}}
			s := New("", "https://self.example", &fakeManager{}, &fakeDialer{}, p, &fakeProber{owners: []string{"private:7777"}}, nil, nil)
			r := httptest.NewRequest(tt.method, tt.path, strings.NewReader(tt.body))
			w := httptest.NewRecorder()
			s.Handler().ServeHTTP(w, r)
			var body map[string]jsontext.Value
			if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
				t.Fatal(err)
			}
			if w.Code != 200 || string(body[tt.field]) != `["https://peer.example"]` {
				t.Fatalf("response %d: %s", w.Code, w.Body.String())
			}
			if p.addrs[0] != "private:7777" {
				t.Fatal("rewrote the placement source slice")
			}
		})
	}
}

func TestOwnerAddrIsTheAdvertisedClientURL(t *testing.T) {
	s := New("", "https://self.example", &fakeManager{}, &fakeDialer{}, nil, nil, nil, nil)
	if got := s.claimResponse(&types.Sandbox{ID: "sb_1"}).OwnerAddr; got != "https://self.example" {
		t.Fatalf("owner = %q", got)
	}
	r := httptest.NewRequest(http.MethodGet, "/v1/sandboxes/sb_1/owner", nil)
	r.Header.Set("Authorization", "Bearer tok")
	w := httptest.NewRecorder()
	s.Handler().ServeHTTP(w, r)
	if !strings.Contains(w.Body.String(), `"owner_addr":"https://self.example"`) {
		t.Fatalf("owner response: %s", w.Body.String())
	}
}

type clientPlacer struct{ fakePlacer }

func (p *clientPlacer) ClientAddr(string) string { return "https://peer.example" }
