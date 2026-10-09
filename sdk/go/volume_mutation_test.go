package sandbox

import (
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"testing"
)

func TestLiveVolumesUseOwnerAndSandboxToken(t *testing.T) {
	for _, verb := range []string{"attach", "detach"} {
		t.Run(verb, func(t *testing.T) {
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodPost || r.URL.Path != "/v1/sandboxes/sb_1/volumes/"+verb || r.Header.Get("Authorization") != "Bearer sandbox-token" {
					t.Errorf("request: %s %s %s", r.Method, r.URL.Path, r.Header.Get("Authorization"))
				}
				body, _ := io.ReadAll(r.Body)
				want := `{"volumes":[{"name":"data"}]}`
				if verb == "detach" {
					want = `{"names":["data"]}`
				}
				if string(body) != want {
					t.Errorf("body=%s want=%s", body, want)
				}
				_, _ = io.WriteString(w, `{"volumes":[{"name":"remaining","mount":"/data"}]}`)
			}))
			t.Cleanup(ts.Close)
			c := testClient(t, ts, WithAPIToken("operator-token"))
			sb := &Sandbox{ID: "sb_1", token: "sandbox-token", owner: c.addr, c: c}
			var got []Volume
			var err error
			if verb == "attach" {
				got, err = sb.AttachVolumes(t.Context(), Volume{Name: "data"})
			} else {
				got, err = sb.DetachVolumes(t.Context(), "data")
			}
			if err != nil || !slices.Equal(got, []Volume{{Name: "remaining", Mount: "/data"}}) {
				t.Fatalf("got=%v err=%v", got, err)
			}
			got[0].Name = "caller copy"
			if sb.Volumes[0].Name != "remaining" {
				t.Fatal("returned volumes alias public state")
			}
		})
	}
}

func TestLiveVolumeFailurePreservesLastKnownSet(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusConflict)
		_, _ = io.WriteString(w, `{"error":"pending"}`)
	}))
	t.Cleanup(ts.Close)
	c := testClient(t, ts)
	sb := &Sandbox{ID: "sb_1", token: "tok", owner: c.addr, c: c, Volumes: []Volume{{Name: "old"}}}
	if _, err := sb.AttachVolumes(t.Context(), Volume{Name: "new"}); err == nil {
		t.Fatal("missing attach error")
	}
	if _, err := sb.DetachVolumes(t.Context(), "old"); err == nil {
		t.Fatal("missing detach error")
	}
	if !slices.Equal(sb.Volumes, []Volume{{Name: "old"}}) {
		t.Fatal("failed request replaced last known set")
	}
}
