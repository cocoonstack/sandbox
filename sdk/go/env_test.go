package sandbox

import (
	"encoding/json/v2"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
	"time"
)

func TestClaimsCarryTheEnv(t *testing.T) {
	env := map[string]EnvVar{"MODE": {Value: "on"}, "GW_KEY": {Value: "k", Guest: new(false)}}
	want := map[string]map[string]any{"MODE": {"value": "on"}, "GW_KEY": {"value": "k", "guest": false}}
	bodies := make(chan map[string]map[string]any, 1)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Env map[string]map[string]any `json:"env"`
		}
		if err := json.UnmarshalRead(r.Body, &body); err != nil {
			t.Errorf("decode %s: %v", r.URL.Path, err)
		}
		bodies <- body.Env
		_ = json.MarshalWrite(w, claimResponse{ID: "sb_1", Token: "tok", Deadline: time.Unix(42, 0)})
	}))
	t.Cleanup(ts.Close)
	c := testClient(t, ts)
	withEnv := WithEnv(env)
	env["MODE"] = EnvVar{Value: "mutated"}
	ck := &Checkpoint{ID: "ck_1", c: c, addr: c.addr}
	tpl := &Template{Name: "rt", c: c, addr: c.addr, net: "none", size: "small"}
	for name, claim := range map[string]func() error{
		"client":     func() error { _, err := c.New(t.Context(), "rt", withEnv); return err },
		"template":   func() error { _, err := tpl.New(t.Context(), withEnv); return err },
		"checkpoint": func() error { _, err := ck.New(t.Context(), withEnv); return err },
	} {
		if err := claim(); err != nil {
			t.Fatalf("%s claim: %v", name, err)
		}
		if got := <-bodies; !reflect.DeepEqual(got, want) {
			t.Errorf("%s claim env %v, want %v", name, got, want)
		}
	}
}

func TestEnvVerbsUseTheAPITokenOnTheOwner(t *testing.T) {
	type request struct{ method, auth, body string }
	got := make(chan request, 1)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/sandboxes/sb_1/env" {
			t.Errorf("path %s", r.URL.Path)
		}
		body, _ := io.ReadAll(r.Body)
		got <- request{r.Method, r.Header.Get("Authorization"), string(body)}
		if r.Method == http.MethodGet {
			_, _ = io.WriteString(w, `{"env":{"MODE":{"value":"on"},"GW_KEY":{"value":"","guest":false}}}`)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(ts.Close)
	c := testClient(t, ts, WithAPIToken("api"))
	sb := &Sandbox{ID: "sb_1", token: "tok", owner: c.addr, c: c}

	env, err := sb.Env(t.Context())
	if err != nil {
		t.Fatalf("Env: %v", err)
	}
	if want := (map[string]EnvVar{"MODE": {Value: "on"}, "GW_KEY": {Guest: new(false)}}); !reflect.DeepEqual(env, want) {
		t.Errorf("Env %v, want %v", env, want)
	}
	if r := <-got; r != (request{http.MethodGet, "Bearer api", ""}) {
		t.Errorf("read %+v", r)
	}
	for _, tt := range []struct {
		name string
		call func() error
		want request
	}{
		{"set", func() error { return sb.SetEnv(t.Context(), map[string]EnvVar{"MODE": {Value: "off"}}) }, request{http.MethodPut, "Bearer api", `{"env":{"MODE":{"value":"off"}}}`}},
		{"clear", func() error { return sb.SetEnv(t.Context(), nil) }, request{http.MethodPut, "Bearer api", `{"env":{}}`}},
		{"patch", func() error { return sb.PatchEnv(t.Context(), map[string]*EnvVar{"OLD": nil}) }, request{http.MethodPatch, "Bearer api", `{"env":{"OLD":null}}`}},
	} {
		if err := tt.call(); err != nil {
			t.Fatalf("%s: %v", tt.name, err)
		}
		if r := <-got; r != tt.want {
			t.Errorf("%s sent %+v, want %+v", tt.name, r, tt.want)
		}
	}
}
